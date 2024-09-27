using log4net;
using System;
using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine.Helper;
using System.Threading.Tasks;
using System.Net.Sockets;
using Newtonsoft.Json;
using System.Linq;
using System.Collections;
using System.Net.Http;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public enum PIDataMode { PointMode, AFElementMode };
    public class TDEngineTaosxClient : IDisposable
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);      
        private TDEngineTaosSocket taosxSocket;

        static private bool stopTaosxSend;
        private static int maxWaitLength = 1000;
        private readonly Object stLock = new Object();

        private readonly string hostname;
        private readonly int port;
        public bool useAFDatabase;
        TcpClient client;
        NetworkStream stream;
        private MessageBuilder builder;
        ArrowStreamWriter writer;
        ArrowStreamReader reader;
        Stopwatch stopwatch = new Stopwatch();
        const long QueueSize = 30;
        long actualQueueBufferSize = QueueSize;
        DateTime lastSend = DateTime.UtcNow;
        List<KeyValuePair<string, string>> columnNameTypes;
        List<KeyValuePair<string, string>> tags;
        public string tdColomnType;
        public int localPort = 0;
        /// <summary>
        /// 下一批数据的批次号,用于追踪数据的处理进度,遗憾的是这个批号不能随着 RecordBatch 一起发送到 agent.
        /// </summary>
        private long _batchNumber = 0;

        // For PI Point
        public TDEngineTaosxClient(string hostname, int port, string database, string stableName,
            string tdColomnType, List<KeyValuePair<string, string>> tags, int maxWaitLength, bool useAFDatabase) {

            this.hostname = hostname;
            this.port = port;
            this.useAFDatabase = useAFDatabase;
            this.tdColomnType = tdColomnType;
            this.tags = tags;
           

            AckType ackType = AckType.None;
            builder = new MessageBuilder(PIDataMode.PointMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;
            // builder.tagNames = tags;
            builder.tagNames = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(TaosxConstants.POINTNAME, IpcDataTypes.VarCharType),
                new KeyValuePair<string, string>(TaosxConstants.POINTID, IpcDataTypes.VarCharType)
            };
            builder.tagNames.AddRange(tags); 
            builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.PointPath, IpcDataTypes.VarCharType));
            if(useAFDatabase) builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementsPathForPoint,IpcDataTypes.VarCharType));

            builder.tsArrowArray = new TimestampArray.Builder();
            builder.tableUniqKeyArrowArray = new StringArray.Builder();
            TDValueType tdType = TDTypeV1Converter.ToTDType(tdColomnType);
            builder.valArrowArrayList.Add(TDEngineTableFormat.PointValColomn(), new ColumnValueBuilder(tdType));
            builder.statusArrowArrayList.Add(TDEngineTableFormat.PointStatusColomn(), new Int32Array.Builder());

            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>("ts", TDValueType.Timestamp));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TaosxConstants.POINTNAME, TDValueType.String));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TDEngineTableFormat.PointValColomn(), tdType));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TDEngineTableFormat.PointStatusColomn(), TDValueType.Int));
        }

        public long NextBatchNumber()
        {
            return Interlocked.Increment(ref _batchNumber);
        }

        // For AFElement
        public TDEngineTaosxClient(string hostname, int port, string database,
            string stableName,
            List<KeyValuePair<string, string>> columnNameTypes,
            List<KeyValuePair<string, string>> tags,
            int maxWaitLength)
        {
            this.hostname = hostname;
            this.port = port;
            this.columnNameTypes = columnNameTypes;
            this.tags = tags;
            AckType ackType = AckType.Lush;
            builder = new MessageBuilder(PIDataMode.AFElementMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;

            var tagDic = new Dictionary<string, string>();
            if (tags != null)
            {
                tagDic = tags.ToDictionary(pair => pair.Key, pair => pair.Value);
            }
            builder.tagNames = new List<KeyValuePair<string, string>> { };
            if (!tagDic.ContainsKey(TaosxConstants.ELEMENTID))
            {
                builder.tagNames.Add(new KeyValuePair<string, string>(TaosxConstants.ELEMENTID, IpcDataTypes.VarCharType));
            }
            if (!tagDic.ContainsKey(TaosxConstants.ELEMENTNAME))
            {
                builder.tagNames.Add(new KeyValuePair<string, string>(TaosxConstants.ELEMENTNAME, IpcDataTypes.VarCharType));
            }
            if (!tagDic.ContainsKey(StaticConfig.Default.AFTreeTagName))
            {
                builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.AFTreeTagName, IpcDataTypes.VarCharType));
            }
            if (!tagDic.ContainsKey(StaticConfig.Default.ElementCategories))
            {
                builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementCategories, IpcDataTypes.VarCharType));
            }
            builder.tagNames.AddRange(tags);

            builder.tableUniqKeyArrowArray = new StringArray.Builder();
            builder.tsArrowArray = new TimestampArray.Builder();

            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>("ts", TDValueType.Timestamp));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TaosxConstants.ELEMENTID, TDValueType.String));
            foreach (var column in columnNameTypes) {
                string columnType = column.Value;
                TDValueType tdType = TDTypeV1Converter.ToTDType(columnType);
                string tdColName = TDEngineTableFormat.AFValColomn(column.Key);
                string tdStatusColName = TDEngineTableFormat.AFStatusColomn(column.Key);
                builder.valArrowArrayList.Add(tdColName, new ColumnValueBuilder(tdType));
                builder.statusArrowArrayList.Add(tdStatusColName, new Int32Array.Builder());
                builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(tdColName, tdType));
                builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(tdStatusColName, TDValueType.Int));
            }
        }
        public TDEngineTaosxClient Clone()
        {
            if (builder.mode == PIDataMode.AFElementMode)
            {
                var dst = new TDEngineTaosxClient(hostname, port, "", builder.stableName, columnNameTypes, tags, maxWaitLength);
                return dst;
            }
            else {
                var dst = new TDEngineTaosxClient(hostname, port, "", builder.stableName, tdColomnType, tags, maxWaitLength, useAFDatabase);
                return dst;
            }
        }

        private void start() {
            if (!StaticConfig.Default.ForBackfill) {
                Task task = new Task(work);
                task.Start();
            }

            Task responseHandler = new Task(resHandler);
            responseHandler.Start();
        }

        private void resHandler()
        {
            if (TDEngineClient.OnlyTestConnector) return;

            if (stream == null) {
                log.Info($"Stream is null, create arrow reader failed!");
                return;
            }
            reader = new ArrowStreamReader(stream);
            while (stream.CanRead)
            {
                try
                {
                    RecordBatch msg = reader.ReadNextRecordBatch();
                    if (msg != null)
                    {
                        if (msg.ColumnCount > 0)
                        {
                            IArrowArray array = msg.Column(0);
                            switch (array)
                            {
                                case Int32Array int32Array:
                                    if (int32Array.Length > 0)
                                    {
                                        int? nullableValue = int32Array.GetValue(0);
                                        if (nullableValue.HasValue)
                                        {
                                            int code = nullableValue.Value;
                                            if (code == 0)
                                            {
                                                Interlocked.Increment(ref actualQueueBufferSize);
                                            }
                                        }
                                        log.Debug($"Stable:{builder.stableName},localPort:{localPort}.Arrow response code {nullableValue}, QueueSize {actualQueueBufferSize}");
                                    }
                                    else { 
                                        log.Warn($"Stable:{builder.stableName},localPort:{localPort}.Arrow response array length is 0");
                                    }
                                    break;
                                default:
                                    log.Info($"Stable:{builder.stableName},localPort:{localPort}.Unsupported arrow response array type.{array.GetType()}");
                                    break;
                            }
                        }
                        else
                        {
                            log.Warn($"Stable:{builder.stableName},localPort:{localPort}.Arrow response column count is 0");
                        }

                        msg.Dispose();
                    }
                    else
                    {
                        log.Debug($"Stable:{builder.stableName},localPort:{localPort}.no response!");
                        Thread.Sleep(500);
                    }
                }
                catch (Exception e) {
                    log.Debug($"Stable:{builder.stableName},localPort:{localPort}.Exception: Arrow response handle! {e.Message}");
                    Thread.Sleep(500);
                }
            }
            log.Debug($"Stable:{builder.stableName},localPort:{localPort}.stopTaosxSend:{stopTaosxSend},streamCanRead:{stream.CanRead},Arrow response handler exit!");
        }

        private void work() {
            while (!stopTaosxSend)
            {
                if (builder.tableUniqKeyArrowArray.Length > 0)
                {
                    try
                    {
                        if ((DateTime.UtcNow - lastSend).TotalSeconds > 1) {
                            send();
                        }
                        Thread.Sleep(1000);
                    }
                    catch (Exception e)
                    {
                        log.Error($"Stable:{builder.stableName},localPort:{localPort}.Send data to taosx failed! {e.Message}");
                        Thread.Sleep(1000);
                    }
                    Thread.Sleep(1000);
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        public void AddPointValue(string tableUniKey, TDValue record) {
            lock (stLock) 
            {
                builder.tableUniqKeyArrowArray.Append(tableUniKey);
                builder.tsArrowArray.Append(record.Timestamp);
                if (record.Quality == 0)
                {
                    builder.valArrowArrayList[TDEngineTableFormat.PointValColomn()].Append(record.Value);
                    builder.statusArrowArrayList[TDEngineTableFormat.PointStatusColomn()].Append(0);
                }
                else
                {
                    builder.valArrowArrayList[TDEngineTableFormat.PointValColomn()].AppendNull();
                    builder.statusArrowArrayList[TDEngineTableFormat.PointStatusColomn()].Append(record.Quality);
                }
                if (builder.tsArrowArray.Length > maxWaitLength)
                {
                    send();
                }
            }
        }

        // write data
        public void AddTablesValue(in Dictionary<string, Dictionary<string, List<TDValue>>> tablesValue)
        {
            addTablesValue(tablesValue);
        }

        // element id -> timestamp -> values
        public void addTablesValue(in Dictionary<string, Dictionary<string, List<TDValue>>> tables)
        {
            foreach (var table in tables)
            {
                Dictionary<string, TDValue> valDic = new Dictionary<string, TDValue> { };
                Dictionary<string, int> statusDic = new Dictionary<string, int> { };
                foreach (var row in table.Value)
                {
                    valDic.Clear();
                    statusDic.Clear();
                    if (row.Value.Count == 0) continue;
                    DateTime ts = new DateTime();
                    foreach (var value in row.Value)
                    {
                        string columnName = value.Name.ToTDEngineNamingPattern();
                        ts = value.Timestamp;
                        var colValName = TDEngineTableFormat.AFValColomn(in columnName);
                        if (valDic.ContainsKey(colValName))
                        {
                            if (valDic[colValName] != value)
                            {
                                log.Error($"{table.Key}.{columnName} has duplicate value at time {ts}");
                            }
                            continue;
                        }
                        if (value.Quality == 0)
                        {
                            valDic.Add(colValName, value);
                            statusDic.Add(TDEngineTableFormat.AFStatusColomn(in columnName), 0);
                        }
                        else
                        {
                            valDic.Add(colValName, null);
                            statusDic.Add(TDEngineTableFormat.AFStatusColomn(in columnName), value.Quality);
                        }
                    }
                    lock (stLock)
                    {
                        builder.tableUniqKeyArrowArray.Append(table.Key);
                        builder.tsArrowArray.Append(ts);
                        foreach (var objRow in builder.valArrowArrayList)
                        {
                            if (valDic.ContainsKey(objRow.Key))
                            {
                                TDValue value = valDic[objRow.Key];
                                if (value == null) {
                                    objRow.Value.AppendNull();
                                } else
                                {
                                    objRow.Value.Append(value.Value);
                                }
                            }
                            else
                            {
                                objRow.Value.AppendNull();
                            }
                        }
                        foreach (var objRow in builder.statusArrowArrayList)
                        {
                            if (statusDic.ContainsKey(objRow.Key))
                            {
                                objRow.Value.Append(statusDic[objRow.Key]);
                            }
                            else
                            {
                                objRow.Value.AppendNull();
                            }
                        }
                        if (builder.tsArrowArray.Length > maxWaitLength)
                        {
                            send();
                        }
                    }
                }
            }
            if (StaticConfig.Default.ForBackfill && builder.tsArrowArray.Length > 0)
            {
                send();
            }
        }

        /// <summary>
        /// 限制发送数据到 taosX 的速度。
        /// actualQueueBufferSize 代表允许继续发送的批数，初始值为 30，每发送一批数据减 1，每收到一个确认消息加 1。
        /// 当 actualQueueBufferSize 小于等于 0 时，不允许发送数，等待 500ms 再检查 actualQueueBufferSize 的值。
        /// 如果等待是时间超过 20s，将 actualQueueBufferSize 设置为 1，允许继续发送 1 批数据。
        /// 
        /// </summary>
        internal void ArrowMsgQueueWait()
        {
            stopwatch.Reset();
            stopwatch.Start();
            while (true) {
                long buffSize = Interlocked.Read(ref actualQueueBufferSize);
                if (buffSize <= 0)
                {
                    long cost = stopwatch.ElapsedMilliseconds;
                    if (cost > 20000)
                    {
                        log.Warn($"Stable:{builder.stableName},localPort:{localPort},wait {cost} ms, more than 20s");
                        Interlocked.Exchange(ref actualQueueBufferSize, 1);
                    }
                    else if (cost > 500)
                    {
                        log.Info($"Stable:{builder.stableName},localPort:{localPort},wait {cost} ms");
                    }
                    Thread.Sleep(500);
                }
                else if (buffSize > QueueSize)
                {
                    Interlocked.Exchange(ref actualQueueBufferSize, QueueSize);
                    break;
                }
                else {
                    break;
                }
            }
        }

        internal void AddAFElementTableTag(string elementId, List<KeyValuePair<string, string>> tags)
        {
            lock (stLock)
            {
                if (!builder.tagVals.ContainsKey(elementId))
                {
                    builder.tagVals[elementId] = tags;
                }
                else
                {
                    log.Info("Stable:{builder.stableName},Found duplicate elements when add tagVal");
                }
            }
        }

        internal void AddPointTableTag(string tdEngineTableName, List<KeyValuePair<string, string>> tags)
        {
            if (!builder.tagVals.ContainsKey(tdEngineTableName))
            {
                builder.tagVals[tdEngineTableName] = tags;
                //builder.pointIds.Add($"{tdEngineTableName}", pointId);
            }
            else
            {
                log.Info("Stable:{builder.stableName},localPort:{localPort},Found duplicate elements when add pointId");
            }
        }

        public void InitTables() {
            lock (stLock)
            {
                if (builder.tagVals.Count == 0) return;
                var batchNumber = NextBatchNumber();
                var recordBatch = builder.BuildTablesMessage();
                writeRecordBatch(recordBatch);
                log.Info($"Stable:{builder.stableName},localPort:{localPort},Write batch:{batchNumber},Create tables {builder.tagVals.Count}");
                builder.tagVals.Clear();
            }
        }

        public void send() {
            lock (stLock) {
                if (builder.tableUniqKeyArrowArray.Length == 0) return;
                var batchNumber = NextBatchNumber();
                var recordBatch = builder.BuildInsertMessage();
                writeRecordBatch(recordBatch);
                log.Debug($"Stable:{builder.stableName},localPort:{localPort}, Write batch:{batchNumber},records {builder.tableUniqKeyArrowArray.Length},QueueSize {actualQueueBufferSize}");
                clear();
                lastSend = DateTime.UtcNow;
            }
        }

        public void SendControlMessage(string[] values) {
            lock (stLock)
            {
                var batchNumber = NextBatchNumber();
                var recordBatch = builder.BuildControlMessage(values);
                writeRecordBatch(recordBatch);
                log.Info($"Stable:{builder.stableName},localPort:{localPort},Write batch:{batchNumber},Message:{values[0]}");
            }
        }

        private void clear() {
            builder.tableUniqKeyArrowArray.Clear();
            builder.tsArrowArray.Clear();
            foreach (var valArray in builder.valArrowArrayList) {
                valArray.Value.Clear();
            }
            foreach (var statusArray in builder.statusArrowArrayList)
            {
                statusArray.Value.Clear();
            }
        }

        public void Dispose()
        {

        }

        internal void Connect()
        {
            builder.initSchema();
            connectTaosx();
            start();
        }

        private void writeRecordBatch(RecordBatch recordBatch) {
            if (TDEngineClient.OnlyTestConnector) return;

            try
            {
                Interlocked.Decrement(ref actualQueueBufferSize);
                writer.WriteRecordBatch(recordBatch);
            }
            catch (Exception e) {
                log.Error($"Stable:{builder.stableName},localPort:{localPort},Write record batch failed!{e.Message}");
                Thread.Sleep(1000);
                reconnectTaosx();
            }
        }
        public bool isBusy()
        {
            lock (stLock)
            {
                return builder.tableUniqKeyArrowArray.Length > 0;
            }
        }

        private void connectTaosx()
        {
            log.Info($"Stable:{builder.stableName},connectTaosx start...");
            try {
                // taosxSocket.Connect();
                client = new TcpClient(hostname, port);
                // 获取本地端口号
                localPort = ((System.Net.IPEndPoint)client.Client.LocalEndPoint).Port;
                stream = client.GetStream();
                writer = new ArrowStreamWriter(stream, builder.Schema);
                log.Info($"Stable:{builder.stableName},localPort:{localPort},connectTaosx success");
            }
            catch (Exception e) {
                log.Error($"Stable:{builder.stableName}, Connect taosx failed! {e}");
            }
        }
        private void reconnectTaosx() {
            log.Info($"Stable:{builder.stableName},localPort:{localPort},reconnectTaosx start...");
            writer.WriteEnd();
            stream.Close();
            client.Close();
            connectTaosx();
        }

        public PIDataMode WorkMode() {
            return builder.mode;
        }

        internal void Stop()
        { 
            log.Info($"Stable:{builder.stableName},localPort:{localPort},Stop client");
            if (!stopTaosxSend) { 
                stopTaosxSend = true;
                send();
            }

            if (null != writer) writer.WriteEnd();
            if (null != stream) stream.Close();
            if (null != client) client.Close();
            return;
        }
    }
}

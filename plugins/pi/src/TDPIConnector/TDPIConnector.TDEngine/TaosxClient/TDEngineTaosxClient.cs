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

        // For PI Point
        public TDEngineTaosxClient(string hostname, int port, string database, string stableName,
            string tdColomnType, List<KeyValuePair<string, string>> tags, int maxWaitLength, bool useAFDatabase) {
            AckType ackType = AckType.None;
            builder = new MessageBuilder(PIDataMode.PointMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;
            // builder.tagNames = tags;
            builder.tagNames = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>(TaosxConstants.POINTNAME, "String") };
            builder.tagNames.Add(new KeyValuePair<string, string>(TaosxConstants.POINTID, "INT"));
            builder.tagNames.AddRange(tags); 
            builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.PointPath, "String"));
            if(useAFDatabase) builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementsPathForPoint,"String"));

            builder.tsArrowArray = new TimestampArray.Builder();
            builder.tableUniqKeyArrowArray = new StringArray.Builder();
            TDValueType tdType = TDTypeV1Converter.ToTDType(tdColomnType);
            builder.valArrowArrayList.Add(TDEngineTableFormat.PointValColomn(), new ColumnValueBuilder(tdType));
            builder.statusArrowArrayList.Add(TDEngineTableFormat.PointStatusColomn(), new Int32Array.Builder());

            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>("ts", TDValueType.Timestamp));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TaosxConstants.POINTNAME, TDValueType.String));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TDEngineTableFormat.PointValColomn(), tdType));
            builder.columnNameTypes.Add(new KeyValuePair<string, TDValueType>(TDEngineTableFormat.PointStatusColomn(), TDValueType.Int));

            this.hostname = hostname;
            this.port = port;
            this.useAFDatabase = useAFDatabase;
        }

        // For AFElement
        public TDEngineTaosxClient(string hostname, int port, string database,
            string stableName,
            List<KeyValuePair<string, string>> columnNameTypes,
            List<KeyValuePair<string, string>> tags,
            int maxWaitLength)
        {
            AckType ackType = AckType.Lush;
            builder = new MessageBuilder(PIDataMode.AFElementMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;
            builder.tagNames = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>(TaosxConstants.ELEMENTID, "VARCHAR(64)") };
            builder.tagNames.Add(new KeyValuePair<string, string>(TaosxConstants.ELEMENTNAME, "NCHAR(100)"));
            builder.tagNames.AddRange(tags);
            builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.AFTreeTagName, "String"));
            builder.tagNames.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementCategories, "String"));

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
            this.hostname = hostname;
            this.port = port;
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
            while (!stopTaosxSend)
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
                                        log.Debug($"arrow response: code:{nullableValue}");
                                    }
                                    break;
                                default:
                                    log.Info($"Unsupported arrow response array type.{array.GetType()}");
                                    break;
                            }
                        }

                        msg.Dispose();
                    }
                    else
                    {
                        log.Debug($"no response!");
                        Thread.Sleep(500);
                    }
                }
                catch (Exception e) {
                    log.Debug($"Exception: Arrow response handle! {e.Message}");
                    Thread.Sleep(500);
                }
            }
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
                        log.Error($"Send data to taosx failed! {e.Message}");
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
                    builder.valArrowArrayList[TDEngineTableFormat.PointValColomn()].Append(null);
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
                                objRow.Value.Append(value.Value);
                            }
                            else
                            {
                                objRow.Value.Append(null);
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
                                objRow.Value.Append(null);
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
                        log.Info($"ArrowMsgQueueWait cost {cost} ms!");
                        Interlocked.Exchange(ref actualQueueBufferSize, 1);
                    }
                    else if (cost > 500)
                    {
                        log.Info($"ArrowMsgQueueWait cost {cost} ms!");
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
                    log.Info("found duplicate elements when add tagVal");
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
                log.Info("found duplicate elements when add pointId");
            }
        }

        public void InitTables() {
            lock (stLock)
            {
                if (builder.tagVals.Count == 0) return;
                log.Debug($"Stable:{builder.stableName} Write tables into stream start...");

                var recordBatch = builder.BuildTablesMessage();
                writeRecordBatch(recordBatch);
                log.Debug($"Stable:{builder.stableName} Write tables into stream...");
                builder.tagVals.Clear();
            }
        }

        public void send() {
            lock (stLock) {
                if (builder.tableUniqKeyArrowArray.Length == 0) return;
                log.Debug($"Stable:{builder.stableName} Write records into stream start...");
                var recordBatch = builder.BuildInsertMessage();
                writeRecordBatch(recordBatch);
                log.Debug($"Stable:{builder.stableName} Write {builder.tableUniqKeyArrowArray.Length} records into stream end.");
                clear();
                lastSend = DateTime.UtcNow;
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

        private void sendRecordBatch(RecordBatch recordBatch)
        {
            MemoryStream stream = new MemoryStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(stream, recordBatch.Schema);
            writer.WriteRecordBatch(recordBatch);
            byte[] buffer = stream.ToArray();
            //var str = Encoding.UTF8.GetString(buffer);
            //log.Debug($"send recordbatch: {str}");
            var response = taosxSocket.SendData(buffer);

            writer.Dispose();
            // TODO parse response
            return;
        }

        private void writeRecordBatch(RecordBatch recordBatch) {
            if (TDEngineClient.OnlyTestConnector) return;

            try
            {
                Interlocked.Decrement(ref actualQueueBufferSize);
                writer.WriteRecordBatch(recordBatch);
            }
            catch (Exception e) {
                log.Error($"write record batch failed!{e.Message}");
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
                stream = client.GetStream();
                writer = new ArrowStreamWriter(stream, builder.Schema);
            }
            catch (Exception e) {
                log.Error($"Connect taosx failed! {e}");
            }
            log.Info($"Stable:{builder.stableName},SchemaMeta:{JsonConvert.SerializeObject(builder.Schema.Metadata, Formatting.Indented)}");
            log.Info($"Stable:{builder.stableName},connectTaosx success...");
        }
        private void reconnectTaosx() {
            log.Info($"Stable:{builder.stableName},reconnectTaosx start...");
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
            if (!stopTaosxSend) {
                stopTaosxSend = true;
                send();
            }

            // If this empty message is not sent, the read function of the agent will fail
            // and consider the task abnormal.
            // Maybe the new version of Arrow doesn't have this problem. Who knows, I haven't tried it
            writeRecordBatch(builder.BuildInsertMessage());

            if (null != writer) writer.WriteEnd();
            if (null != stream) stream.Close();
            if (null != client) client.Close();
            return;
        }
    }
}

using log4net;
using System;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.IO;
using System.Collections.Generic;
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
        TcpClient client;
        NetworkStream stream;
        private MessageBuilder builder;
        ArrowStreamWriter writer;

        // For PI Point
        public TDEngineTaosxClient(string hostname, int port, string database, string stableName,
            string colomnType, int maxWaitLength) {
            AckType ackType = AckType.None;
            builder = new MessageBuilder(PIDataMode.PointMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;
            builder.tagNames = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("pointid", "INT") };

            builder.tableNameArrowArray = new StringArray.Builder();
            builder.tsArrowArray = new TimestampArray.Builder();
            builder.valArrowArrayList.Add(TDEngineTableFormat.PointValColomn(), new StringArray.Builder());
            builder.statusArrowArrayList.Add(TDEngineTableFormat.PointStatusColomn(), new StringArray.Builder());

            builder.columnNameTypes.Add(new KeyValuePair<string, string>("ts", "TIMESTAMP"));
            builder.columnNameTypes.Add(new KeyValuePair<string, string>(TaosxConstants.TABLENAME, "NCHAR(100)"));
            builder.columnNameTypes.Add(new KeyValuePair<string, string>(TDEngineTableFormat.PointValColomn(), colomnType));
            builder.columnNameTypes.Add(new KeyValuePair<string, string>(TDEngineTableFormat.PointStatusColomn(), "INT"));

            this.hostname = hostname;
            this.port = port;
        }

        // For AFElement
        public TDEngineTaosxClient(string hostname, int port, string database,
            string stableName,
            List<KeyValuePair<string, string>> columnNameTypes,
            List<KeyValuePair<string, string>> tags,
            int maxWaitLength)
        {
            AckType ackType = AckType.None;
            builder = new MessageBuilder(PIDataMode.AFElementMode, stableName, StreamType.Lush, ackType);
            taosxSocket = new TDEngineTaosSocket(hostname, port, ackType != AckType.None);

            stopTaosxSend = false;
            TDEngineTaosxClient.maxWaitLength = maxWaitLength;
            builder.tagNames = tags;

            builder.tableNameArrowArray = new StringArray.Builder();
            builder.tsArrowArray = new TimestampArray.Builder();

            builder.columnNameTypes.Add(new KeyValuePair<string, string>("ts", "TIMESTAMP"));
            builder.columnNameTypes.Add(new KeyValuePair<string, string>(TaosxConstants.TABLENAME, "NCHAR(100)"));
            foreach (var column in columnNameTypes) {
                builder.valArrowArrayList.Add(TDEngineTableFormat.AFValColomn(column.Key), new StringArray.Builder());
                builder.statusArrowArrayList.Add(TDEngineTableFormat.AFStatusColomn(column.Key), new StringArray.Builder());
                builder.columnNameTypes.Add(new KeyValuePair<string, string>(TDEngineTableFormat.AFValColomn(column.Key), $"{column.Value}"));
                builder.columnNameTypes.Add(new KeyValuePair<string, string>(TDEngineTableFormat.AFStatusColomn(column.Key), "INT"));
            }
            this.hostname = hostname;
            this.port = port;
        }

        private void start() {
            Task task = new Task(work);
            task.Start();
        }

        private void work() {
            while (!stopTaosxSend)
            {
                if (builder.tableNameArrowArray.Length > 0)
                {
                    try
                    {
                        send();
                    }
                    catch (Exception e)
                    {
                        log.Error($"Send data to taosx failed! {e.ToString()}");
                        Task.Delay(1000).Wait();
                    }
                }
                else
                {
                    Task.Delay(1000).Wait();
                }
            }
        }

        public void AddPointValue(string table, TDValue record) {
            lock (stLock) 
            {
                builder.tableNameArrowArray.Append(table.ToTDEngineNamingPattern());
                builder.tsArrowArray.Append(record.Timestamp);
                if (record.Quality == 0)
                {
                    builder.valArrowArrayList[TDEngineTableFormat.PointValColomn()].Append($"{record.ValueString}");
                    builder.statusArrowArrayList[TDEngineTableFormat.PointStatusColomn()].Append("0");
                }
                else
                {
                    builder.valArrowArrayList[TDEngineTableFormat.PointValColomn()].Append(null);
                    builder.statusArrowArrayList[TDEngineTableFormat.PointStatusColomn()].Append(record.Quality.ToString());
                }
            }
           
            if (builder.tsArrowArray.Length > maxWaitLength)
            {
                send();
            }
        }

        // write data
        public void AddTablesValue(Dictionary<string, Dictionary<string, List<TDValue>>> tablesValue)
        {
            addTablesValue(tablesValue);
            if (builder.tsArrowArray.Length > maxWaitLength)
            {
                send();
            }
        }
        public void addTablesValue(Dictionary<string, Dictionary<string, List<TDValue>>> tables)
        {
            lock (stLock)
            {
                foreach (var table in tables)
                {
                    foreach (var row in table.Value)
                    {
                        Dictionary<string, string> valDic = new Dictionary<string, string> { };
                        Dictionary<string, int> statusDic = new Dictionary<string, int> { };
                        if (row.Value.Count == 0) continue;
                        DateTime ts = new DateTime();
                        foreach (var value in row.Value)
                        {
                            string columnName = value.Name.ToTDEngineNamingPattern();
                            ts = value.Timestamp;
                            if (value.Quality == 0)
                            {
                                valDic.Add($"{columnName}_val", value.ValueString);
                                statusDic.Add($"{columnName}_status", 0);
                            }
                            else
                            {
                                valDic.Add($"{columnName}_val", null);
                                statusDic.Add($"{columnName}_status", value.Quality);
                            }
                        }
                        builder.tableNameArrowArray.Append(TDEngineProxy.GetFullTableName(table.Key).ToTDEngineNamingPattern());
                        builder.tsArrowArray.Append(ts);
                        foreach (var objRow in builder.valArrowArrayList)
                        {
                            if (valDic.ContainsKey(objRow.Key))
                            {
                                objRow.Value.Append(valDic[objRow.Key]);
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
                                objRow.Value.Append(statusDic[objRow.Key].ToString());
                            }
                            else
                            {
                                objRow.Value.Append(null);
                            }
                        }
                    }
                }
            }
        }

        internal void AddAFElementTableTag(string tdEngineTableName, List<KeyValuePair<string, string>> tags)
        {
            if (!builder.tagVals.ContainsKey(tdEngineTableName))
            {
                builder.tagVals[tdEngineTableName] = tags;
            }
            else {
                log.Info("found duplicate elements when add tagVal");
            }
        }

        internal void AddPointTableTag(string tdEngineTableName, int pointId)
        {
            if (!builder.pointIds.ContainsKey(tdEngineTableName))
            {
                //var tag = new KeyValuePair<string, string>($"pointId", "INT");
                builder.pointIds.Add($"{tdEngineTableName}", pointId);
            }
            else
            {
                log.Info("found duplicate elements when add pointId");
            }
        }

        public void InitTables() {
            lock (stLock)
            {
                if (builder.pointIds.Count == 0 && builder.tagVals.Count == 0) return;
                log.Info($"Stable:{builder.stableName} Write tables into stream start...");

                var recordBatch = builder.BuildTablesMessage();
                writeRecordBatch(recordBatch);
                log.Info($"Stable:{builder.stableName} Write tables into stream...");
            }
        }

        public void send() {
            lock (stLock) {
                if (builder.tableNameArrowArray.Length == 0) return;
                log.Info($"Stable:{builder.stableName} Write records into stream start...");
                var recordBatch = builder.BuildInsertMessage();
                writeRecordBatch(recordBatch);
                log.Info($"Stable:{builder.stableName} Write records into stream end.");
                clear();
            }
        }

        private void clear() {
            builder.tableNameArrowArray.Clear();
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
            try {
                writer.WriteRecordBatch(recordBatch);
            }
            catch (Exception e) {
                log.Error($"write record batch failed!{e}");
                reconnectTaosx();
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
    }
}

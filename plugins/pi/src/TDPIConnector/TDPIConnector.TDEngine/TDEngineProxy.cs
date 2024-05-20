#define CLOUD_LICENSE_ONLY_DISABLED
#define UNUSE_ADAPTER
using log4net;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine.Helper;
using TDPIConnector.TDEngine.TaosxClient;

namespace TDPIConnector.TDEngine
{
    public class TDEngineProxyBuild  {
        public static TDEngineProxy NewTDEngineProxy(string ipcHost, string restHost, string tablesPrefix, int maxWaitLength)
        {
            return new TDEngineProxy(ipcHost, restHost, tablesPrefix, maxWaitLength);
        }
    }
    public class TDEngineProxy : IDisposable
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static string tablesPrefix;

        private string hostname;
        private int port;
        private int maxWaitLength;

        public event EventHandler<Exception> OnExceptionThrown = delegate { };
        public event EventHandler<TDEngineHttpResponseSummary> OnHttpResponseReceived = delegate { };
        public event EventHandler<string> OnServerVersionReceived = delegate { };

        private Dictionary<string, TDEngineTaosxClient> taosxClients = new Dictionary<string, TDEngineTaosxClient>(0);
        // private TDEngineTaosxCommonClient taosxCommonClient;
        private TDEngineClient taosxCommonClient;
        private readonly Object taosxClientsLock = new Object();  // for update dictionary taosxClients

        protected virtual void DoExceptionThrown(Exception e) {
            OnExceptionThrown(this, e);
        }
        protected virtual void DoHttpResponseReceived(TDEngineHttpResponseSummary response)
        {
            OnHttpResponseReceived(this, response);
        }
        protected virtual void DoServerVersionReceived(string version)
        {
            OnServerVersionReceived(this, version);
        }
        public TDEngineProxy() { this.taosxClients = new Dictionary<string, TDEngineTaosxClient>(); }

        public TDEngineProxy(string ipcHost, string restHost, string tablesPrefix, int maxWaitLength)
        {
            TDEngineProxy.tablesPrefix = tablesPrefix;
            string[] ipcAdd = ipcHost.Split(':');
            hostname = ipcAdd[0];
            int.TryParse(ipcAdd[1], out port);
            this.maxWaitLength = maxWaitLength;

            //string[] restAdd = restHost.Split(':');
            //string restHostname = restAdd[0];
            //int restPort;
            //int.TryParse(restAdd[1], out restPort);
#if USE_ADAPTER
            taosxCommonClient = new TDEngineClient(true, restHost, 0, "root", "taosdata", "", tablesPrefix);
#else
            taosxCommonClient = new TDEngineClient(true, restHost, 0, "", "", "", tablesPrefix);
#endif
            this.taosxClients = new Dictionary<string, TDEngineTaosxClient>();
        }
        public virtual void Connect()
        {
            taosxCommonClient.Connect();
        }

        public virtual Task<TDEngineResponse> GetServerVersion()
        {
            return null;
        }
        public virtual Task<TDEngineResponse> ChangeTagValueForAFElements(string db, string tbName, string attriName, string value)
        {
            return taosxCommonClient.ChangeTagValueForAFElements(db, tbName, attriName, value);
        }
        public virtual Task<TDEngineResponse> UpdateAFElementAttributeNULL(string db, string elementName, string attriName, string ts)
        {
            return taosxCommonClient.UpdateAFElementAttributeNULL(db, elementName, attriName, ts);
        }
        public virtual async Task<TDEngineResponse> GetSTables(string database, string stable) {
            return await taosxCommonClient.GetSTables(database, stable);
        }
        public virtual void VerifyLicenseCompability()
        {
            if (!hostname.Contains("cloud.tdengine.com"))
            {
#if CLOUD_LICENSE_ONLY
                throw new TDEngineInvalidOnPremiseLicenseException();
#endif
            }
        }

        public virtual Task<TDValue> GetLastPIValue(string database, string pointName)
        {
            return taosxCommonClient.GetLastPIValue(database, pointName);
        }

        public virtual Task<Dictionary<string, DateTime>> GetLastPIValues(string database, List<string> tableNames, IEnumerable<string> STableNames)
        {
            return taosxCommonClient.GetLastPIValues(database, tableNames, STableNames);
        }

        public virtual Task<TDValue> GetFirstPIValue(string database, string pointName)
        {
            return taosxCommonClient.GetFirstPIValue(database, pointName);
        }
        public virtual Task<TDEngineResponse> CreateSuperTableForPIPoint(string database, string superTableName, string tdColumnType,
            List<KeyValuePair<string, string>> tags, bool useAFDatabase)
        {
            lock (taosxClientsLock)
            {
                if (!taosxClients.ContainsKey(superTableName))
                {
                    var taosxClient = new TDEngineTaosxClient(hostname, port, database, superTableName,
                        tdColumnType, tags, maxWaitLength, useAFDatabase);
                    taosxClients.Add(superTableName, taosxClient);
                    taosxClient.Connect();
                    log.Info($"create PIPoint superTable {superTableName}:{tdColumnType}");
                }
            }
            return Task.FromResult<TDEngineResponse>(null);
        }
        public virtual Task<TDEngineResponse> CreateSuperTableForAFElement(string database, TDSTable sTable)
        {
            var stableName = sTable.Name.ToTDEngineNamingPattern();
            lock (taosxClientsLock)
            {
                if (!taosxClients.ContainsKey(stableName))
                {
                    var columnNameTypes = new List<KeyValuePair<string, string>>();
                    var tags = new List<KeyValuePair<string, string>>();
                    foreach (var column in sTable.Columns)
                    {
                        if (column.IsTDengineTag())
                        {
                            tags.Add(new KeyValuePair<string, string>($"{column.Name}", $"{column.Type}"));
                        }
                        else
                        {
                            columnNameTypes.Add(new KeyValuePair<string, string>($"{column.Name}", $"{column.Type}"));
                        }
                    }
                    var taosxClient = new TDEngineTaosxClient(hostname, port, database,
                        stableName, columnNameTypes, tags, maxWaitLength);
                    taosxClients.Add(stableName, taosxClient);
                    taosxClient.Connect();
                    log.Info($"create AFElements superTable {stableName}");
                }
            }
            return Task.FromResult<TDEngineResponse>(null);
        }
        public virtual Task CreateTablesForAFElements(string database, List<TDTable> elements)
        {
            if (0 == elements.Count) return Task.CompletedTask; ;
            for (int i = 0; i < elements.Count; i++)
            {
                var element = elements[i];
                var tags = new List<KeyValuePair<string, string>>();
                tags.Add(new KeyValuePair<string, string>("element_id", element.ID));
                foreach (TDColumn column in element.Columns)
                {
                    if (column.IsTDengineTag())
                    {
                        // verify tagname and value
                        // tags.Add($"{column.Name}", column.TagValue);
                        tags.Add(new KeyValuePair<string, string>($"{column.Name}", column.TagValue));
                    }
                }
                tags.Add(new KeyValuePair<string, string>(StaticConfig.Default.AFTreeTagName, element.Location));

                string tdEngineTableName = element.Name;
                string stableName = element.STableName.ToTDEngineNamingPattern(); ;
                
                var taosxClient = getTaosxClient(stableName);
                if (taosxClient != null)
                {
                    taosxClient.AddAFElementTableTag(tdEngineTableName, tags);
                }
                else
                {
                    log.Error($"Create stable for AFElement failed, not found {stableName}");
                }
            }
            initAFModeTables();
            return Task.CompletedTask;
        }
        public virtual Task CreateTablesForAFElementsV2(string database, string superTableName, List<TDTable> elements)
        {
            var taosxClient = getTaosxClient(superTableName.ToTDEngineNamingPattern());
            if (taosxClient == null)
            {
                log.Error($"Create stable for AFElement(V2) failed, not found {superTableName}");
                return null;
            }

            for (int i = 0; i < elements.Count; i++)
            {
                var element = elements[i];
                var tags = new List<KeyValuePair<string, string>>();
                tags.Add(new KeyValuePair<string, string>(TaosxConstants.ELEMENTID, element.ID));
                tags.Add(new KeyValuePair<string, string>(TaosxConstants.ELEMENTNAME, element.Name));
                foreach (TDColumn column in element.Columns)
                {
                    if (column.IsTDengineTag())
                    {
                        if (column.TagValue.Length > 100) {
                            log.Debug($"{element.Location} {element.Name}.{column.Name} tag value too long {column.TagValue}！");
                            column.TagValue = column.TagValue.Substring(0, 99);
                        }
                        tags.Add(new KeyValuePair<string, string>($"{column.Name}", column.TagValue.Trim()));
                    }
                }
                tags.Add(new KeyValuePair<string, string>(StaticConfig.Default.AFTreeTagName, element.Location));
                tags.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementCategories, element.Categories));

                taosxClient.AddAFElementTableTag(element.ID, tags);
            }
            taosxClient.InitTables();
            return Task.CompletedTask;
        }
        public virtual void ArrowMsgQueueWait(string superTableName) {
            var taosxClient = getTaosxClient(superTableName.ToTDEngineNamingPattern());
            if (taosxClient != null)
            {
                taosxClient.ArrowMsgQueueWait();
            }
            else
            {
                log.Error($"arrowMsgQueueWait failed, not found {superTableName}");
            }
        }
        public virtual Task CreateTablesForPIPoints(string database, List<TDTable> piPoints)
        {
            for (int i = 0; i < piPoints.Count; i++)
            {
                var piPoint = piPoints[i];
                string tdEngineTableUniKey = piPoint.Name;
                var stableName = piPoint.STableName.ToTDEngineNamingPattern();

                var taosxClient = getTaosxClient(stableName);
                var tags = new List<KeyValuePair<string, string>>();
                tags.Add(new KeyValuePair<string, string>(TaosxConstants.POINTID, piPoint.PointId.ToString()));
                tags.Add(new KeyValuePair<string, string>(TaosxConstants.POINTNAME, piPoint.Name));
                foreach (TDColumn column in piPoint.Columns)
                {
                    if (column.IsTDengineTag())
                    {
                        tags.Add(new KeyValuePair<string, string>($"{column.Name}", column.TagValue));
                    }
                }
                tags.Add(new KeyValuePair<string, string>(StaticConfig.Default.PointPath, piPoint.Location));
                if (taosxClient.useAFDatabase) {
                    tags.Add(new KeyValuePair<string, string>(StaticConfig.Default.ElementsPathForPoint, piPoint.ElementPath));
                }

                if (taosxClient != null)
                {
                    taosxClient.AddPointTableTag(tdEngineTableUniKey, tags);
                }
                else {
                    log.Error($"Create stable for Point failed, not found {stableName}");
                }
            }
            initPointModeTables();
            return Task.CompletedTask;
        }

        public virtual Task<TDEngineResponse> DeleteByTimeRange(string database, string tbName, string startTime, string endTime)
        {
            return taosxCommonClient.DeleteByTimeRange(database, tbName, startTime, endTime);
        }

        private void initPointModeTables()
        {
            lock (taosxClientsLock)
            {
                foreach (var taosxClient in taosxClients)
                {
                    if (taosxClient.Value.WorkMode() == PIDataMode.PointMode)
                    {
                        taosxClient.Value.InitTables();
                    }
                }
            }
        }
        private void initAFModeTables()
        {
            lock(taosxClientsLock)
            {
                foreach (var taosxClient in taosxClients)
                {
                    if (taosxClient.Value.WorkMode() == PIDataMode.AFElementMode)
                    {
                        taosxClient.Value.InitTables();
                    }
                }
            }
        }
        public virtual Task<TDEngineResponse> DropTableForPIPoint(string database, string pointName)
        {
            log.Error("Drop table not support!");
            return Task.FromResult<TDEngineResponse>(null);
        }
        public virtual Task<TDEngineResponse> DropTableForAFElement(string database, TDTable table)
        {
            log.Error("Drop table not support!");
            return Task.FromResult<TDEngineResponse>(null);
        }

        public virtual void InsertBackfillValuesForPI(string database, string superTable, string tableUniKey, List<TDValue> values)
        {
            if (values.Count == 0) return;
            var taosxClient = getTaosxClient(superTable.ToTDEngineNamingPattern());
            if (taosxClient != null)
            {
                foreach (var record in values)
                {
                    taosxClient.AddPointValue(tableUniKey, record);
                }
            }
            else {
                log.Error($"Insert PIPoint data failed! not found client, stable:{superTable}.");
            }
        }
        public virtual Task<TDEngineResponse> InsertValueForPIPoints(string superTable, TDValue value)
        {
            var taosxClient = getTaosxClient(superTable);
            if (taosxClient != null)
            {
                taosxClient.AddPointValue(value.Name, value);
            }
            else
            {
                log.Error($"{superTable} TaosxClient not found!");
            }
            return Task.FromResult<TDEngineResponse>(null);
        }

        public virtual Task<TDEngineResponse> InsertValuesForPIPoints(string database, Dictionary<string, Dictionary<string, List<TDValue>>> tables)
        {
            foreach (var table in tables)
            {
                foreach (var row in table.Value)
                {
                    foreach (var value in row.Value)
                    {
                        var stableName = GetPIPointSTableNameByTDVType(value.ValueType).ToTDEngineNamingPattern();
                        var taosxClient = getTaosxClient(stableName);
                        if (taosxClient != null)
                        {
                            taosxClient.AddPointValue(value.Name, value);
                        }
                        else {
                            log.Error($"{stableName} TaosxClient not found!");
                        }
                    }
                }
            }
            return Task.FromResult<TDEngineResponse>(null);
        }
        public virtual Task<TDEngineResponse> InsertValuesForAFElements(string database, in Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>> stables, in List<string> columnNames)
        {
            foreach (var tables in stables)
            {
                var taosxClient = getTaosxClient(tables.Key.ToTDEngineNamingPattern());
                if (null != taosxClient) {
                    taosxClient.AddTablesValue(tables.Value);
                } else {
                    log.Error($"InsertValuesForAFElements failed! stable {tables.Key} not found!");
                }

            }
            return Task.FromResult<TDEngineResponse>(null);
        }
        private TDEngineTaosxClient getTaosxClient(string superTableName) {
            lock (taosxClientsLock) {
                if (taosxClients.ContainsKey(superTableName))
                {
                    return taosxClients[superTableName];
                }
                else
                {
                    return null;
                }
            }
        }
        public void StopTaosxClient(string superTableName)
        {
            var stName = superTableName.ToTDEngineNamingPattern();
            lock (taosxClientsLock)
            {
                if (taosxClients.ContainsKey(stName))
                {
                    taosxClients[stName].Stop();
                    taosxClients.Remove(stName);
                }
            }
        }
        public virtual void Dispose()
        {
            return;
        }

        public bool IsBusy()
        {
            lock (taosxClientsLock)
            {
                foreach (var taosxClient in taosxClients)
                {
                    if (taosxClient.Value.isBusy()) return true;
                }
                return false;
            }
        }

        private static string prefix = "pitag_";
        public static string GetPIPointSTableNameByTDVType(TDValueType ValueType)
        {
            string tdColumnType = GetPITypeStringFromValType(ValueType);
            string superTableName = $"{prefix}{tdColumnType.Split('(')[0]}";
            return superTableName.ToTDEngineNamingPattern();
        }
        public static string GetPITypeStringFromValType(TDValueType ValueType)
        {
            switch (ValueType)
            {
                case TDValueType.Int:
                    return "INT";
                case TDValueType.Float:
                    return "FLOAT";
                case TDValueType.Double:
                    return "DOUBLE";
                case TDValueType.String:
                    return "NCHAR";
                case TDValueType.Timestamp:
                    return "TIMESTAMP";
            }
            log.Fatal("GetPITypeStringFromValType, PointType not found.");
            throw new Exception("PointType not found.");
        }
        public static string GetFullTableName(string tableName)
        {
            return $"{tablesPrefix}{tableName}";
        }
        public void StopAll() {
            foreach (var taosxClient in taosxClients)
            {
                taosxClient.Value.Stop();
            }
        }
    }
}

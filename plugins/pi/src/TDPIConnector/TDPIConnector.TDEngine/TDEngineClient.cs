#define CLOUD_LICENSE_ONLY
#define UNUSE_ADAPTER

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using TDPIConnector.TDEngine.Exceptions;
using TDPIConnector.TDEngine.Helper;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.TDEngine
{
    public class TDEngineClient : TDEngineProxy
    {
        private readonly HttpClient httpClient;
        private readonly string baseUrl;
        private readonly string queryStringToken;
        private readonly bool forTaosX;
        private readonly byte[] credentialsByteArray = null;

        static public bool OnlyTestConnector = false;

        public TDEngineClient(bool forTaosX, string hostname, int port, string username, string password, string token, string tablesPrefix) : base()
        {
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            if (forTaosX)
            {
                baseUrl = hostname;
            }
            else
            {
                baseUrl = string.Format("{0}:{1}", hostname, port);
            }
            this.queryStringToken = token;
            this.forTaosX = forTaosX;


            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                credentialsByteArray = Encoding.ASCII.GetBytes(string.Format("{0}:{1}", username, password));
            }
        }

        public override void Connect()
        {
            if (TDEngineClient.OnlyTestConnector) return;

            try
            {
                TDEngineResponse resp = this.GetServerVersion().Result;
                string version = resp.Data[0][0].ToString();
                DoServerVersionReceived(version);
                log.Info($"Got taosd version:{version}");
            }
            catch (Exception e)
            {
                log.Fatal($"Get taosd version failed.{e}");
                throw new Exception("Could not connect to TDengine. Please check the settings on the .config file.");
            }
        }

        public override async Task<TDEngineResponse> GetServerVersion()
        {
            string sqlCommand = "select server_version();";
            return await MakeHttpRequest(sqlCommand);
        }

        public async Task<TDEngineResponse> ShowDatabases()
        {
            string sqlCommand = "show databases;";
            return await MakeHttpRequest(sqlCommand);
        }

        public async Task<TDEngineResponse> GetDatabaseTables(string database)
        {
            string sqlCommand = "show tables;";
            return await MakeHttpRequest(sqlCommand, database);
        }
        public override async Task<TDEngineResponse> GetSTables(string database, string stable)
        {
            if (TDEngineClient.OnlyTestConnector) return null;

            string sqlCommand = $"desc {database.ToTDEngineNamingRawPattern()}.{stable.ToTDEngineNamingPattern()};";
            return await MakeHttpRequest(sqlCommand);
        }

        public override async Task<TDValue> GetLastPIValue(string database, string pointName)
        {
            string tdEngineTableName = GetFullTableName(pointName);
            string sqlCommand = $"select * from {database}.{tdEngineTableName} order by ts desc limit 1;";
            TDEngineResponse resp = await MakeHttpRequest(sqlCommand);
            TDValues tdValues = resp.ToTDValues();
            return tdValues.FirstOrDefault();
        }

        public override async Task<Dictionary<string, DateTime>> GetLastPIValues(string database, List<string> tableNames, IEnumerable<string> STableNames)
        {
            STableNames = STableNames.Select(st => st.ToTDEngineNamingPattern()).ToList();
            Dictionary<string, DateTime> lastValueTimestamps = new Dictionary<string, DateTime>();
            Dictionary<string, DateTime> allLastValueTimestamps = new Dictionary<string, DateTime>();

            foreach (var STableName in STableNames)
            {
                if (TDEngineClient.OnlyTestConnector) break;

                string sqlCommand = $"SELECT tbname, LAST_ROW(*) FROM {database}.{STableName} PARTITION BY TBNAME;";
                TDEngineResponse resp = await MakeHttpRequest(sqlCommand);
                foreach (var dataItem in resp.Data)
                {
                    allLastValueTimestamps.Add(dataItem[0], DateTime.Parse(dataItem[1]));
                }
            }

            foreach (var tableName in tableNames)
            {
                string tdEngineTableName = GetFullTableName(tableName);
                if (allLastValueTimestamps.ContainsKey(tdEngineTableName))
                {
                    lastValueTimestamps.Add(tableName, allLastValueTimestamps[tdEngineTableName]);
                }
                else
                {
                    lastValueTimestamps.Add(tableName, DateTime.MinValue);
                }
            }
            return lastValueTimestamps;
        }



        public override async Task<TDValue> GetFirstPIValue(string database, string pointName)
        {
            string tdEngineTableName = GetFullTableName(pointName);
            string sqlCommand = $"select * from {database}.{tdEngineTableName} order by ts asc limit 1;";
            TDEngineResponse resp = await MakeHttpRequest(sqlCommand);
            TDValues tdValues = resp.ToTDValues();
            return tdValues.FirstOrDefault();
        }
        public override async Task<TDEngineResponse> CreateSuperTableForPIPoint(string database, string superTable, string tdColumnType,
            List<KeyValuePair<string, string>> tags, bool useAFDatabase)
        {
            string sqlCommand = $"CREATE STABLE IF NOT EXISTS {superTable.ToTDEngineNamingPattern()} (ts TIMESTAMP, val {tdColumnType}, quality INT) TAGS (pointId INT);";
            return await MakeHttpRequest(sqlCommand, database);
        }
        public override async Task<TDEngineResponse> CreateSuperTableForAFElement(string database, TDSTable sTable)
        {
            string sqlCommand = $"CREATE STABLE IF NOT EXISTS {sTable.Name} (ts TIMESTAMP";
            string tags = string.Empty;
            foreach (TDColumn column in sTable.Columns)
            {
                if (column.IsTDengineTag())
                {
                    tags += $", {column.Name} NCHAR(100)";
                }
                else
                {
                    sqlCommand += $", {column.Name}_val {column.Type}, {column.Name}_status INT";
                }
            }
            tags += $", {StaticConfig.Default.AFTreeTagName} NCHAR(100)";
            sqlCommand += $") TAGS (element_id NCHAR(100){tags});";

            return await MakeHttpRequest(sqlCommand, database);
        }

        //public override async Task<TDEngineResponse> CreateTableForPIPoint(string database, string superTable, string pointName, int pointId)
        //{
        //    string tdEngineTableName = GetFullTableName(pointName).ToTDEngineNamingPattern();
        //    string sqlCommand = $"CREATE TABLE IF NOT EXISTS {tdEngineTableName} USING {superTable.ToTDEngineNamingPattern()} TAGS (\"{pointId}\");";
        //    return await MakeHttpRequest(sqlCommand, database);
        //}

        public override async Task CreateTablesForAFElements(string database, List<TDTable> elements)
        {
            bool hasValues = false;
            StringBuilder sb = new StringBuilder("CREATE TABLE");
            for (int i = 0; i < elements.Count; i++)
            {
                var element = elements[i];
                Dictionary<string, string> tags = new Dictionary<string, string>();
                foreach (TDColumn column in element.Columns)
                {
                    if (column.IsTDengineTag())
                    {
                        tags.Add($"{column.Name}", column.TagValue);
                    }
                }

                string tdEngineTableName = GetFullTableName(element.Name);
                sb.Append($" IF NOT EXISTS {tdEngineTableName} USING {element.STableName.ToTDEngineNamingPattern()} (element_id");
                foreach (KeyValuePair<string, string> tag in tags)
                {
                    sb.Append($", {tag.Key}");
                }
                sb.Append($", {StaticConfig.Default.AFTreeTagName}");
                sb.Append($") TAGS ('{element.ID}'");
                foreach (KeyValuePair<string, string> tag in tags)
                {
                    sb.Append($", '{tag.Value}'");
                }
                sb.Append($", '{element.Location}'");
                sb.Append($")");
                if (i % 1000 == 0 && i > 0)
                {
                    sb.Append(";");
                    await MakeHttpRequest(sb.ToString(), database);
                    sb = new StringBuilder("CREATE TABLE ");
                    hasValues = false;
                }
                else
                {
                    hasValues = true;
                }
            }

            if (hasValues)
            {
                sb.Append(";");
                await MakeHttpRequest(sb.ToString(), database);
            }
        }

        public override async Task CreateTablesForPIPoints(string database, List<TDTable> piPoints)
        {
            bool hasValues = false;
            StringBuilder sb = new StringBuilder("CREATE TABLE ");
            for (int i = 0; i < piPoints.Count; i++)
            {
                var piPoint = piPoints[i];
                string tdEngineTableName = GetFullTableName(piPoint.Name);
                sb.Append($"IF NOT EXISTS {tdEngineTableName} USING {piPoint.STableName.ToTDEngineNamingPattern()} TAGS (\"{piPoint.PointId}\") ");
                if (i % 1000 == 0 && i > 0)
                {
                    sb.Append(";");
                    await MakeHttpRequest(sb.ToString(), database);
                    sb = new StringBuilder("CREATE TABLE ");
                    hasValues = false;
                }
                else
                {
                    hasValues = true;
                }
            }

            if (hasValues)
            {
                sb.Append(";");
                await MakeHttpRequest(sb.ToString(), database);
            }
        }
      
        public override async Task<TDEngineResponse> DropTableForPIPoint(string database, string pointName)
        {
            string tdEngineTableName = GetFullTableName(pointName);
            string sqlCommand = $"DROP TABLE IF EXISTS {tdEngineTableName};";
            return await MakeHttpRequest(sqlCommand, database);
        }
        public override async Task<TDEngineResponse> DropTableForAFElement(string database, TDTable table)
        {
            string tdEngineTableName = GetFullTableName(table.Name);
            string sqlCommand = $"DROP TABLE IF EXISTS {tdEngineTableName};";
            return await MakeHttpRequest(sqlCommand, database);
        }
 
        public override void Dispose()
        {
            this.httpClient.Dispose();
        }
        private async Task<TDEngineResponse> MakeHttpRequest(string sqlCommand, string dbName = null)
        {
            int retryTimes = 0;
            while (true)
            {
                try
                {
                    return await makRequest(sqlCommand, dbName);
                }
                catch (Exception e)
                {
                    Thread.Sleep(500);
                    if (++retryTimes >= StaticConfig.Default.HttpMaxRetryTime)
                    {
                        log.Error($"sql exec retry {StaticConfig.Default.HttpMaxRetryTime} times failed.{sqlCommand}");
                        throw e;
                    }
                }
            }
        }
        private async Task<TDEngineResponse> makRequest(string sqlCommand, string dbName = null)
        {
            try
            {
                string url = this.baseUrl + "/rest/sql";
                if (forTaosX)
                {
#if USE_ADAPTER
                    url = this.baseUrl + "/rest/sql";
#else
                    url = this.baseUrl + "/sql";
#endif
                }
                if (!string.IsNullOrEmpty(dbName))
                {
                    url += "/" + dbName.ToTDEngineNamingRawPattern();
                }
                if (!string.IsNullOrEmpty(this.queryStringToken))
                {
                    url = url + "?token=" + queryStringToken;
                }
                StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
                HttpRequestMessage requestMessage = CreateRequestMessage(sqlCommand, url);
                HttpResponseMessage response = null;
                try
                {
                    response = await this.httpClient.SendAsync(requestMessage);
                }
                catch (Exception e)
                {
                    if (e.InnerException != null &&
                        e.InnerException.InnerException != null &&
                        !string.IsNullOrEmpty(e.InnerException.InnerException.Message) &&
                        e.InnerException.InnerException.Message.StartsWith("No connection could be made because the target machine actively refused it"))
                    {
                        log.Error($"Require to TDengine failed, {e.Message} sql:{sqlCommand}");
                        throw new TDEngineTimeoutException();
                    }
                    else
                    {
                        log.Error($"Require to TDengine failed, {e.Message} sql:{sqlCommand}");
                        throw new TDEngineHttpResponseException(e);
                    }
                }
                int httpStatusCode = (int)response.StatusCode;
                if (!response.IsSuccessStatusCode)
                {
                    string errorContent = await response.Content.ReadAsStringAsync();
                    log.Error($"TaosX Http request failed, {response.StatusCode}: {errorContent} sql:{sqlCommand}");
                    throw new TDEngineHttpResponseException(httpStatusCode, 0);
                }

                string respStr = await response.Content.ReadAsStringAsync();
                TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
                if (!response.IsSuccessStatusCode || resp.Code != 0)
                {
                    log.Error($"Require to TDengine failed, {httpStatusCode} respCode:{resp.Code} sql:{sqlCommand}");
                    throw new TDEngineHttpResponseException(httpStatusCode, resp.Code, resp.Desc);
                }
                DoHttpResponseReceived(new TDEngineHttpResponseSummary(url, resp.Code, (int)response.StatusCode));
                return resp;
            }
            catch (Exception e)
            {
                log.Error($"make http request failed, {e}");
                DoExceptionThrown(e);
                throw;
            }
        }

        private HttpRequestMessage CreateRequestMessage(string sqlCommand, string url)
        {
            HttpRequestMessage requestMessage = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new StringContent(sqlCommand, UnicodeEncoding.UTF8),
                RequestUri = new Uri(url)
            };

            if (credentialsByteArray != null)
            {
                requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentialsByteArray));
            }
            return requestMessage;
        }

        //public override async Task<TDEngineResponse> ChangeTagValueForAFElements(string db, string tbName, string attriName, string value)
        //{
        //    try
        //    {
        //        string sqlCommand = $"ALTER TABLE {db.ToTDEngineNamingRawPattern()}.`{tbName}` " +
        //            $"SET TAG {attriName.ToTDEngineNamingPattern()}='{value}';";
        //        return await MakeHttpRequest(sqlCommand);
        //    }
        //    catch (Exception e)
        //    {
        //        log.Error($"ChangeTagValueForAFElements failed. {e}");
        //        return null;
        //    }
        //}
        public override async Task<TDEngineResponse> UpdateAFElementAttributeNULL(string db, string elementName, string attriName, string ts)
        {
            try
            {
                string sqlCommand = $"INSERT INTO {db.ToTDEngineNamingRawPattern()}.{elementName} " +
                    $"(ts, {attriName.ToTDEngineNamingPattern()}_val, {attriName.ToTDEngineNamingPattern()}_status)" +
                    $" VALUES ('{ts}', NULL, NULL);";
                return await MakeHttpRequest(sqlCommand);
            }
            catch (Exception e)
            {
                log.Error($"ChangeTagValueForAFElements failed. {e}");
                return null;
            }
        }

        //public override async Task<TDEngineResponse> DeleteByTimeRange(string db, string tbName, string startTime, string endTime)
        //{
        //    if (TDEngineClient.OnlyTestConnector) return null;

        //    try
        //    {
        //        string sqlCommand = $"DELETE FROM {db.ToTDEngineNamingRawPattern()}.{tbName} " +
        //            $"WHERE ts >= \'{startTime}\' AND ts <= \'{endTime}\';";
        //        return await MakeHttpRequest(sqlCommand);
        //    }
        //    catch (Exception e)
        //    {
        //        log.Error($"DeleteByTimeRange failed. {e}");
        //        return null;
        //    }
        //}
    }
}

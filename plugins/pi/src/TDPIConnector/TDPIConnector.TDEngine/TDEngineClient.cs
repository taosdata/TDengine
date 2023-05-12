#define CLOUD_LICENSE_ONLY
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
using TDPIConnector.TDEngine.TaosxClient;

namespace TDPIConnector.TDEngine
{
    public class TDEngineClient : TDEngineProxy
    {
        private readonly HttpClient httpClient;
        private readonly string baseUrl;
        private readonly string queryStringToken;
        private readonly string tablesPrefix;
        private readonly bool forTaosX;
        private readonly byte[] credentialsByteArray = null;

        public TDEngineClient(bool forTaosX, string hostname, int port, string username, string password, string token, string tablesPrefix):base()
        {
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            if (forTaosX)
            {
                baseUrl = hostname;
            }
            else {
                baseUrl = string.Format("{0}:{1}", hostname, port);
            }
            this.queryStringToken = token;
            this.tablesPrefix = tablesPrefix;
            this.forTaosX = forTaosX;


            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                credentialsByteArray = Encoding.ASCII.GetBytes(string.Format("{0}:{1}", username, password));
            }
        }

        public override void Connect()
        {
            try
            {
                TDEngineResponse resp = this.GetServerVersion().Result;
                string version = resp.Data[0][0].ToString();
                DoServerVersionReceived(version);
                log.Info($"Got taosd version:{version}");
            }
            catch(Exception e)
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

        public override void VerifyLicenseCompability()
        {
            if (!baseUrl.Contains("cloud.tdengine.com"))
            {

#if CLOUD_LICENSE_ONLY
                throw new TDEngineInvalidOnPremiseLicenseException();
#endif
            }
        }

        public override async Task<TDEngineResponse> CreateDatabase(string dbName)
        {
            if (!baseUrl.Contains("cloud.tdengine.com"))
            {
                string sqlCommand = $"CREATE DATABASE IF NOT EXISTS {dbName.ToTDEngineNamingRawPattern()};";
                return await MakeHttpRequest(sqlCommand);
            }
            else
            {
                return null;
            }
        }
        public override async Task<TDValue> GetLastPIValue(string database, string pointName)
        {
            string tdEngineTableName = GetFullTableName(pointName).ToTDEngineNamingPattern();
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
                string sqlCommand = $"SELECT tbname, LAST_ROW(*) FROM {database}.{STableName} PARTITION BY TBNAME;";
                TDEngineResponse resp = await MakeHttpRequest(sqlCommand);
                foreach (var dataItem in resp.Data)
                {
                    allLastValueTimestamps.Add(dataItem[0], DateTime.Parse(dataItem[1]));
                }
            }
            
            foreach (var tableName in tableNames)
            {
                string tdEngineTableName = GetFullTableName(tableName).ToTDEngineNamingPattern();
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
            string tdEngineTableName = GetFullTableName(pointName).ToTDEngineNamingPattern();
            string sqlCommand = $"select * from {database}.{tdEngineTableName} order by ts asc limit 1;";
            TDEngineResponse resp = await MakeHttpRequest(sqlCommand);
            TDValues tdValues = resp.ToTDValues();
            return tdValues.FirstOrDefault();
        }
        public override async Task<TDEngineResponse>  CreateSuperTableForPIPoint(string database, string superTable, string tdColumnType)
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
                if (string.IsNullOrEmpty(column.ConfigurationItem))
                {
                    sqlCommand += $", {column.Name}_val {column.Type}, {column.Name}_status INT";
                }
                else
                {
                    tags += $", {column.Name}_val {column.Type}";
                }
                if (!string.IsNullOrEmpty(column.Uom))
                {
                    tags += $", {column.Name}_uom NCHAR(100)";
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
                    if (!string.IsNullOrEmpty(column.ConfigurationItem))
                    {
                        tags.Add($"{column.Name}_val", column.ConfigurationItem);
                    }

                    if (!string.IsNullOrEmpty(column.Uom))
                    {
                        tags.Add($"{column.Name}_uom", column.Uom);
                    }
                }

                string tdEngineTableName = GetFullTableName(element.Name).ToTDEngineNamingPattern();
                sb.Append($" IF NOT EXISTS {tdEngineTableName} USING {element.STableName.ToTDEngineNamingPattern()} (element_id");
                foreach (KeyValuePair<string, string> tag in tags)
                {
                    sb.Append($", {tag.Key}");
                }
                sb.Append($", {StaticConfig.Default.AFTreeTagName}");
                sb.Append($") TAGS ('{element.Id}'");
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
                string tdEngineTableName = GetFullTableName(piPoint.Name).ToTDEngineNamingPattern();
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
        //public override async Task<TDEngineResponse> CreateTableForAFElement(string database, TDTable table, TDSTable sTable)
        //{
        //    Dictionary<string, string> tags = new Dictionary<string, string>();
        //    foreach (TDColumn column in table.Columns)
        //    {
        //        if (!string.IsNullOrEmpty(column.ConfigurationItem))
        //        {
        //            tags.Add($"{column.Name}_val", column.ConfigurationItem);
        //        }

        //        if (!string.IsNullOrEmpty(column.Uom))
        //        {
        //            tags.Add($"{column.Name}_uom", column.Uom);
        //        }
        //    }


        //    StringBuilder sb = new StringBuilder((int)(1000000));
        //    sb.Append($"CREATE TABLE IF NOT EXISTS ");
        //    sb.Append(GetFullTableName(table.Name).ToTDEngineNamingPattern());
        //    sb.Append($" USING {sTable.Name} (element_id");
        //    foreach (KeyValuePair<string, string> tag in tags)
        //    {
        //        sb.Append($", {tag.Key}");
        //    }
        //    sb.Append($") TAGS ('{table.Id}'");
        //    foreach (KeyValuePair<string, string> tag in tags)
        //    {
        //        sb.Append($", '{tag.Value}'");
        //    }
        //    sb.Append(");");
        //    string sqlCommand = sb.ToString();
        //    return await MakeHttpRequest(sqlCommand, database);
        //}
        public override async Task<TDEngineResponse> DropTableForPIPoint(string database, string pointName)
        {
            string tdEngineTableName = GetFullTableName(pointName).ToTDEngineNamingPattern();
            string sqlCommand = $"DROP TABLE IF EXISTS {tdEngineTableName};";
            return await MakeHttpRequest(sqlCommand, database);
        }
        public override async Task<TDEngineResponse> DropTableForAFElement(string database, TDTable table)
        {
            string tdEngineTableName = GetFullTableName(table.Name).ToTDEngineNamingPattern();
            string sqlCommand = $"DROP TABLE IF EXISTS {tdEngineTableName};";
            return await MakeHttpRequest(sqlCommand, database);
        }
        //public async Task InsertValuesForPIInSeries(string database, string table, List<TDValue> values)
        //{
        //    List<TDValue> currentValueRequest = new List<TDValue>();
        //    int j = 0;
        //    do
        //    {
        //        currentValueRequest.Add(values[j]);
        //        if ((j != 0 && j % 500 == 0) || (j == values.Count() - 1))
        //        {

        //            string sqlCommand = GenerateSqlCommandForInsertInPI(table, values);
        //            var resp = await MakeHttpRequest(sqlCommand, database);
        //            currentValueRequest.Clear();
        //        }
        //        j++;
        //    } while (j < values.Count());
        //}

        public override void InsertBackfillValuesForPI(string database, string superTable, string table, List<TDValue> values)
        {
            List<string> sqlCommands = new List<string>();
            List<TDValue> currentValueRequest = new List<TDValue>();
            List<List<TDValue>> valuesBatch = new List<List<TDValue>>();
            int j = 0;
            do
            {
                currentValueRequest.Add(values[j]);
                if ((j != 0 && j % 1000 == 0) || (j == values.Count() - 1))
                {
                    valuesBatch.Add(new List<TDValue>(currentValueRequest));
                    currentValueRequest.Clear();
                }
                j++;
            } while (j < values.Count());

            Parallel.ForEach(valuesBatch, (valuesList) =>
            {
                string sqlCommand = this.GenerateSqlCommandForInsertInPI(table, valuesList);
                sqlCommands.Add(sqlCommand);
            });


            List<Task> currentListOfTasks = new List<Task>();
            for (int i = 0; i < sqlCommands.Count; i++)
            {
                Task task = MakeHttpRequest(sqlCommands[i], database);
                currentListOfTasks.Add(task);

                if (currentListOfTasks.Count >= 30)
                {
                    Task.WaitAll(currentListOfTasks.ToArray());
                    currentListOfTasks.Clear();
                }
            }
            Task.WaitAll(currentListOfTasks.ToArray());
        }
        private string GenerateSqlCommandForInsertInPI(string pointName, List<TDValue> values)
        {
            string tdEngineTableName = GetFullTableName(pointName).ToTDEngineNamingPattern();

            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO {tdEngineTableName} VALUES ");
            foreach (TDValue value in values)
            {
                if (value.Quality == 0)
                {
                    sb.Append($"('{value.TimestampString}', {value.ValueString}, 0) ");
                }
                else
                {
                    sb.Append($"('{value.TimestampString}', NULL, {value.Quality}) ");
                }
            }

            return sb.ToString();
        }
        public override async Task<TDEngineResponse> InsertValuesForPIPoints(string database, Dictionary<string, Dictionary<string, List<TDValue>>> tables)
        {
            await Task.Delay(0);
            List<Task> tasks = new List<Task>();
            string sqlCommand;
            if (tables.Count == 0) return null;

            //build value string for each tables
            List<StringBuilder> tableList = new List<StringBuilder>();
            int insertCount = 0;

            foreach (var table in tables)
            {
                string tdEngineTableName = GetFullTableName(table.Key).ToTDEngineNamingPattern();
                StringBuilder sb = new StringBuilder();
                foreach (var row in table.Value)
                {
                    sb.Append($" {tdEngineTableName} VALUES ");

                    //add timestamp
                    sb.Append($"('{row.Key}'");
                    //add values
                    foreach (var value in row.Value)
                    {
                        if (value.Quality == 0)
                            sb.Append($", {value.ValueString}, 0");
                        else//todo: 0 is status, needs to be fixed
                            sb.Append($", NULL, {value.Quality}");
                        insertCount++;
                    }
                    sb.Append(") ");

                    if (insertCount >= 5000)
                    {
                        tableList.Add(sb);
                        sqlCommand = CreateSqlCommandForInsertingAFValues(tableList);
                        Task t = MakeHttpRequest(sqlCommand, database);
                        tasks.Add(t);
                        tableList = new List<StringBuilder>();
                        sb = new StringBuilder();
                        insertCount = 0;
                    }
                }
                tableList.Add(sb);
            }

            if (insertCount > 0)
            {
                sqlCommand = CreateSqlCommandForInsertingAFValues(tableList);
                Task tf = MakeHttpRequest(sqlCommand, database);
                tasks.Add(tf);
            }
            Task.WaitAll(tasks.ToArray());
            return null;
        }
        public override async Task<TDEngineResponse> InsertValuesForAFElements(string database, Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>> stables, List<string> columnNames)
        {
            await Task.Delay(0);
            List<Task> tasks = new List<Task>();
            string sqlCommand;
            if (stables.Count == 0) return null;

            //build column list
            Dictionary<string, string> columns = new Dictionary<string, string>();

            foreach (string column in columnNames)
            {
                string columnName = column.ToTDEngineNamingPattern();
                columns.Add(column, $", {columnName}_val, {columnName}_status");
            }

            //build value string for each tables
            List<StringBuilder> tableList = new List<StringBuilder>();
            int insertCount = 0;

            foreach (var tables in stables) {
                foreach (var table in tables.Value)
                {
                    string tdEngineTableName = GetFullTableName(table.Key).ToTDEngineNamingPattern();
                    StringBuilder sb = new StringBuilder();
                    foreach (var row in table.Value)
                    {
                        sb.Append($" {tdEngineTableName} (ts ");
                        foreach (var value in row.Value)
                        {
                            sb.Append(columns[value.Name]);
                        }
                        sb.Append(") VALUES ");

                        //add timestamp
                        sb.Append($"('{row.Key}'");
                        //add values
                        foreach (var value in row.Value)
                        {
                            if (value.Quality == 0)
                                sb.Append($", {value.ValueString}, 0");
                            else//todo: 0 is status, needs to be fixed
                                sb.Append($", NULL, {value.Quality}");
                            insertCount++;
                        }
                        sb.Append(") ");

                        if (insertCount >= 5000)
                        {
                            tableList.Add(sb);
                            sqlCommand = CreateSqlCommandForInsertingAFValues(tableList);
                            Task t = MakeHttpRequest(sqlCommand, database);
                            tasks.Add(t);
                            tableList = new List<StringBuilder>();
                            sb = new StringBuilder();
                            insertCount = 0;
                        }
                    }
                    tableList.Add(sb);
                }
            }

            if (insertCount > 0)
            {
                sqlCommand = CreateSqlCommandForInsertingAFValues(tableList);
                Task tf = MakeHttpRequest(sqlCommand, database);
                tasks.Add(tf);
            }
            Task.WaitAll(tasks.ToArray());
            return null;
        }

        public override void Dispose()
        {
            this.httpClient.Dispose();
        }
        private async Task<TDEngineResponse> MakeHttpRequest(string sqlCommand, string dbName = null)
        {
            int retryTimes = 0;
            while (true) {
                try
                {
                    return await makRequest(sqlCommand, dbName);
                }
                catch (Exception e) {
                    Thread.Sleep(500);
                    if (++retryTimes >= StaticConfig.Default.HttpMaxRetryTime) {
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
                    url = this.baseUrl + "/sql";
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
                    log.Error($"TaosX Http request failed, {response.StatusCode}: {errorContent} ");
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
        private string CreateSqlCommandForInsertingAFValues(List<StringBuilder> tableList)
        {
            StringBuilder sqlCommand = new StringBuilder($"INSERT INTO ");
            foreach (var item in tableList)
            {
                sqlCommand.Append(item);
            }
            sqlCommand.Append(";");
            return sqlCommand.ToString();
        }
    }
}

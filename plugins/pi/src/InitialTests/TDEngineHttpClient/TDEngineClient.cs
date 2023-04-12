using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using TDEngineHttpClient.Helper;
using TDEngineHttpClient.Models;

namespace TDEngineHttpClient
{
    public class TDEngineClient : IDisposable
    {
        private HttpClient httpClient;
        private string baseUrl;
        private string queryStringToken;

        public TDEngineClient(string hostname, int port)
        {
            this.baseUrl = string.Format("http://{0}:{1}", hostname, port);
        }

        public TDEngineClient(string cloudUrl, string token)
        {
            this.baseUrl = cloudUrl;
            this.queryStringToken = token;
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        public TDEngineClient(string hostname, int port, string token) : this(hostname, port)
        {
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            this.httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Taosd ", token);
        }

        public TDEngineClient(string hostname, int port, string username, string password) : this(hostname, port)
        {
            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            var byteArray = Encoding.ASCII.GetBytes(string.Format("{0}:{1}", username, password));
            this.httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
        }

        public async Task Connect()
        {
            await this.ShowDatabases();
        }

        public async Task<TDEngineResponse> ShowDatabases()
        {
            string url = this.baseUrl + "/rest/sql";
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = "show databases;";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await ConvertToString(response);
            TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
            return resp;
        }
        public async Task<TDEngineResponse> CreateDatabase(string dbName)
        {
            dbName = dbName.ToTDEngineNamingPattern();
            string url = this.baseUrl + "/rest/sql";
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = $"CREATE DATABASE IF NOT EXISTS {dbName};";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await ConvertToString(response);
            TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
            return resp;
        }

        public async Task<TDEngineResponse> GetDatabaseTables(string database)
        {
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = "show tables;";
            var stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await ConvertToString(response);
            TDEngineResponse resp = JsonConvert.DeserializeObject<TDEngineResponse>(respStr);
            return resp;
        }

        public async Task<string> GetTableContent(string database, string table)
        {
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = $"select * from {table};";
            var stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            return await ConvertToString(response);
        }

        public async Task InsertValuesForPIInSeries(string database, string table, List<TDValue> values)
        {
            database = database.ToTDEngineNamingPattern();
            table = table.ToTDEngineNamingPattern();

            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            List<TDValue> currentValueRequest = new List<TDValue>();
            int j = 0;
            do
            {
                currentValueRequest.Add(values[j]);
                if ((j != 0 && j % 500 == 0) || (j == values.Count() - 1))
                {

                    string sqlCommand = GenerateSqlCommandForInsertInPI(table, values);

                    StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);

                    HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
                    string resp = await ConvertToString(response);
                    Console.WriteLine($"\n{DateTime.Now.ToString()} - TDEngine - Sending values - Count: {j}\n");
                    currentValueRequest.Clear();
                }
                j++;
            } while (j < values.Count());
        }

        public async Task InsertValuesForPI(string database, string table, List<TDValue> values)
        {
            List<string> sqlCommands = new List<string>();
            List<TDValue> currentValueRequest = new List<TDValue>();
            List<List<TDValue>> valuesBatch = new List<List<TDValue>>();
            int j = 0;
            do
            {
                currentValueRequest.Add(values[j]);
                if ((j != 0 && j % 5000 == 0) || (j == values.Count() - 1))
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

            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }

            List<Task> currentListOfTasks = new List<Task>();
            for (int i = 0; i < sqlCommands.Count; i++)
            {
                Console.WriteLine($"\n{DateTime.Now.ToString()} - TDEngine - Sending values - Number: {i}\n");
                StringContent stringContent = new StringContent(sqlCommands[i], UnicodeEncoding.UTF8);
                Task task = this.httpClient.PostAsync(url, stringContent);
                currentListOfTasks.Add(task);

                if (currentListOfTasks.Count == 30)
                {
                    Task.WaitAll(currentListOfTasks.ToArray());
                    currentListOfTasks.Clear();
                }
            }

            await Task.Delay(0);
        }

        public async Task InsertValuesForAF(string databaseName, string elementName, List<TDValues> tdValuesList)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            List<string> sqlCommands = new List<string>();
            List<TDValues> currentValueRequest = new List<TDValues>();
            List<List<TDValues>> valuesBatch = new List<List<TDValues>>();
            int j = 0;
            do
            {
                currentValueRequest.Add(tdValuesList[j]);
                if ((j != 0 && j % 500 == 0) || (j == tdValuesList.Count() - 1))
                {
                    valuesBatch.Add(new List<TDValues>(currentValueRequest));
                    currentValueRequest.Clear();
                }
                j++;
            } while (j < tdValuesList.Count());

            Console.WriteLine("Part 1 = " + stopwatch.ElapsedMilliseconds + "ms");

            Parallel.ForEach(valuesBatch, (valuesList) =>
            {
                string sqlCommand = this.GenerateSqlCommandForInsertInAF(elementName, valuesList);
                sqlCommands.Add(sqlCommand);
            });

            Console.WriteLine("Part 2 = " + stopwatch.ElapsedMilliseconds + "ms");

            string url = this.baseUrl + "/rest/sql/" + databaseName;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }

            List<Task> currentListOfTasks = new List<Task>();
            for (int i = 0; i < sqlCommands.Count; i++)
            {
                //Console.WriteLine($"\n{DateTime.Now.ToString()} - TDEngine - Sending values - Number: {i}\n");
                StringContent stringContent = new StringContent(sqlCommands[i], UnicodeEncoding.UTF8);
                Task task = this.httpClient.PostAsync(url, stringContent);
                currentListOfTasks.Add(task);

                if (currentListOfTasks.Count == 30)
                {
                    Task.WaitAll(currentListOfTasks.ToArray());
                    currentListOfTasks.Clear();
                }
            }
            await Task.Delay(0);
        }

        public async Task InsertValuesForAFInSeries(string database, string elementName, List<TDValues> tdValuesList)
        {
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            List<TDValues> currentValueRequest = new List<TDValues>();
            int j = 0;
            do
            {
                currentValueRequest.Add(tdValuesList[j]);
                if ((j != 0 && j % 500 == 0) || (j == tdValuesList.Count() - 1))
                {

                    string sqlCommand = GenerateSqlCommandForInsertInAF(elementName, tdValuesList);

                    StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);

                    HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
                    string resp = await ConvertToString(response);
                    Console.WriteLine($"\n{DateTime.Now.ToString()} - TDEngine - Sending values - Count: {j}\n");
                    currentValueRequest.Clear();
                }
                j++;
            } while (j < tdValuesList.Count());
        }

        public async Task<string> CreateSuperTableForAFElement(string database, string superTable, IEnumerable<TDColumn> columns)
        {
            database = database.ToTDEngineNamingPattern();
            superTable = superTable.ToTDEngineNamingPattern();

            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = $"CREATE STABLE IF NOT EXISTS {superTable} (ts TIMESTAMP";
            string tags = string.Empty;
            foreach (TDColumn column in columns)
            {
                sqlCommand += $", {column.Name}_val {column.Type}, {column.Name}_status INT";
                if (!string.IsNullOrEmpty(column.Uom))
                {
                    tags += $", {column.Name}_uom NCHAR(100)";
                }
            }
            sqlCommand += $") TAGS (element_id NCHAR(100){tags});";

            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            return await ConvertToString(response);
        }

        public async Task<string> CreateSuperTableForPIPoint(string database, string superTable, string tdColumnType)
        {
            database = database.ToTDEngineNamingPattern();
            superTable = superTable.ToTDEngineNamingPattern();
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = $"CREATE STABLE IF NOT EXISTS {superTable} (ts TIMESTAMP, val {tdColumnType}, quality INT) TAGS (pointId INT);";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            return await ConvertToString(response);
        }

        public async Task<string> CreateTableForPIPoint(string database, string superTable, string table, int pointId)
        {
            database = database.ToTDEngineNamingPattern();
            superTable = superTable.ToTDEngineNamingPattern();
            table = table.ToTDEngineNamingPattern();
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = $"CREATE TABLE IF NOT EXISTS {table} USING {superTable} TAGS (\"{pointId}\");";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            return await ConvertToString(response);
        }

        public async Task<string> CreateTableForAFElement(string database, string superTable, string table, Guid elementId, List<string> uoms)
        {
            database = database.ToTDEngineNamingPattern();
            superTable = superTable.ToTDEngineNamingPattern();
            table = table.ToTDEngineNamingPattern();
            string url = this.baseUrl + "/rest/sql/" + database;
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string tags = string.Empty;
            if (uoms != null)
            {
                foreach (var uom in uoms)
                {
                    if (!string.IsNullOrEmpty(uom))
                    {
                        tags += $", \"{uom}\"";
                    }
                }
            }
            string sqlCommand = $"CREATE TABLE IF NOT EXISTS {table} USING {superTable} TAGS (\"{elementId.ToString()}\"{tags});";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            return await ConvertToString(response);
        }

        private string GenerateSqlCommandForInsertInAF(string table, List<TDValues> tdValuesList)
        {

            table = table.ToTDEngineNamingPattern();

            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO {table} VALUES ");

            foreach (TDValues tdValues in tdValuesList)
            {
                sb.Append($"('{tdValues[0].TimestampString}'");

                foreach (TDValue value in tdValues)
                {
                    if (value.Quality == 0)
                    {
                        sb.Append($", {value.ValueString}, 0");
                    }
                    else
                    {
                        sb.Append($", NULL, {Convert.ToString(value.Quality)}");
                    }
                    //Console.WriteLine(stopwatch.Elapsed.TotalMilliseconds);
                }
                sb.Append(") ");
            }

            return sb.ToString();
        }

        private string GenerateSqlCommandForInsertInPI(string table, List<TDValue> values)
        {
            table = table.ToTDEngineNamingPattern();

            StringBuilder sb = new StringBuilder((int)(1000000));
            sb.Append($"INSERT INTO {table} VALUES ");
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

        public async Task<string> GetServerVersion()
        {
            string url = this.baseUrl + "/rest/sql";
            if (!string.IsNullOrEmpty(this.queryStringToken))
            {
                url = url + "?token=" + queryStringToken;
            }
            string sqlCommand = "select server_version();";
            StringContent stringContent = new StringContent(sqlCommand, UnicodeEncoding.UTF8);
            HttpResponseMessage response = await this.httpClient.PostAsync(url, stringContent);
            string respStr = await ConvertToString(response);
            //TDEngineResponse<List<List<string>>, List<List<string>>> resp = JsonConvert.DeserializeObject<TDEngineResponse<List<List<string>>, List<List<string>>>>(respStr);
            return respStr;
        }

        private async Task<string> ConvertToString(HttpResponseMessage response)
        {
            return await response.Content.ReadAsStringAsync();
        }

        public void Dispose()
        {
            this.httpClient.Dispose();
        }
    }
}

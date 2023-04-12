using System;
using System.Threading.Tasks;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR.TDEngineClient
{
    public class TDEngineHttpClient : IDisposable
    {
        private readonly TDHttpClient httpClient;


        public TDEngineHttpClient(string cloudUrl, int port, string token)
        {
            this.httpClient = new TDHttpClient(cloudUrl, port, token);
        }

        public TDEngineHttpClient(string hostname, int port, string username, string password)
        {
            this.httpClient = new TDHttpClient(hostname, port, username, password);
        }

        public TDPIStream GetTDPIStreamFromTable(string database, string table, string column)
        {
            return TDPIStream.CreateTDPIStreamForTable(httpClient, database, table, column);
        }

        public TDPIStream GetTDPIStreamFromPI(string database, string point)
        {
            return TDPIStream.CreateTDPIStreamForPIPoint(httpClient, database, point);
        }

        internal TDPIStream GetTDPIStreamFromAF(string database, string element, string attribute)
        {
            return TDPIStream.CreateTDPIStreamForAFElement(httpClient, database, element, attribute);
        }

        public async Task Connect()
        {
            await this.ShowDatabases();
        }

        public async Task<TDEngineResponse> ShowDatabases()
        {
            string sqlCommand = "show databases;";
            return await this.httpClient.RetrieveDataAsync(sqlCommand);
        }


        public async Task<TDEngineResponse> CreateDatabase(string database)
        {
            database = database.ToDatabaseName();
            string sqlCommand = $"CREATE DATABASE IF NOT EXISTS {database};";
            return await this.httpClient.RetrieveDataAsync(sqlCommand);
        }

        public async Task<TDEngineResponse> GetDatabaseTables(string database)
        {
            string sqlCommand = "show tables;";
            return await this.httpClient.RetrieveDataAsync(sqlCommand, database);
        }
        public async Task<TDEngineResponse> GetServerVersionAsync()
        {
            string sqlCommand = "select server_version();";
            return await this.httpClient.RetrieveDataAsync(sqlCommand);
        }

        public TDEngineResponse GetServerVersion()
        {
            string sqlCommand = "select server_version();";
            return this.httpClient.RetrieveData(sqlCommand);
        }


        public void Dispose()
        {
            this.httpClient.Dispose();
        }

        internal TDEngineResponse CreateTableForPIPoint(string database, string table, string superTable)
        {
            string sqlCommand = $"CREATE TABLE IF NOT EXISTS {table.ToDatabaseName()} USING {superTable.ToDatabaseName()} TAGS (\"-1\");";
            return this.httpClient.RetrieveData(sqlCommand, database);
        }

        internal TDEngineResponse CreateSuperTableForPIPoint(string database, string superTable, string tdColumnType)
        {
            if (tdColumnType.ToLower() == "nchar")
            {
                tdColumnType += "(100)";
            }
            string sqlCommand = $"CREATE STABLE IF NOT EXISTS {superTable.ToDatabaseName()} (ts TIMESTAMP, val {tdColumnType}, quality INT) TAGS (pointId INT);";
            return this.httpClient.RetrieveData(sqlCommand, database);
        }


    }
}

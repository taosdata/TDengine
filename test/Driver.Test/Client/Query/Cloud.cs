using System;
using System.Runtime.CompilerServices;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;
using Xunit.Sdk;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        private void RunCloudTest(Action<string, string> testAction, string testName)
        {
            var db = "cs_test";
            if (string.IsNullOrEmpty(this._cloudConnectString))
            {
                _output.WriteLine($"Cloud connection string is not set. Skipping {testName}.");
                return;
            }

            testAction(this._cloudConnectString, db);
        }

        [Fact]
        public void CloudQueryTest()
        {
            RunCloudTest((conn, db) => this.QueryTest(conn, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudQueryTest));
        }

        [Fact]
        public void CloudQueryWithReqIDMSTest()
        {
            RunCloudTest((conn, db) => this.QueryWithReqIDTest(conn, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudQueryWithReqIDMSTest));
        }

        [Fact]
        public void CloudStmtMSTest()
        {
            RunCloudTest((conn, db) => this.StmtTest(conn, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudStmtMSTest));
        }


        [Fact]
        public void CloudStmtWithReqIDMSTest()
        {
            RunCloudTest((conn, db) => this.StmtWithReqIDTest(conn, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudStmtWithReqIDMSTest));
        }

        [Fact]
        public void CloudStmtColumnsMSTest()
        {
            RunCloudTest((conn, db) => this.StmtBindColumnsTest(conn, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudStmtColumnsMSTest));
        }

        [Fact]
        public void CloudVarbinaryTest()
        {
            RunCloudTest(this.VarbinaryTest, nameof(CloudVarbinaryTest));
        }

        [Fact]
        public void CloudInfluxDBTest()
        {
            RunCloudTest(this.InfluxDBTest, nameof(CloudInfluxDBTest));
        }

        [Fact]
        public void CloudTelnetTest()
        {
            RunCloudTest(this.TelnetTest, nameof(CloudTelnetTest));
        }

        [Fact]
        public void CloudSMLJsonTest()
        {
            RunCloudTest(this.SMLJsonTest, nameof(CloudSMLJsonTest));
        }

        [Fact]
        public void CloudConcurrencyTest()
        {
            RunCloudTest(this.QueryConcurrencyTest, nameof(CloudConcurrencyTest));
        }
        
                
        [Fact]
        public void CloudQueryWithConnectionTimezoneMSTest()
        {
            RunCloudTest((conn, db) => this.QueryWithConnectionTimezoneTest(conn, "Europe/Paris",db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                nameof(CloudStmtColumnsMSTest));
        }
    }
}
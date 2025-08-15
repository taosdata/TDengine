using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void NativeQueryMSTest()
        {
            var db = "query_test_ms";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeQueryUSTest()
        {
            var db = "query_test_us";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeQueryNSTest()
        {
            var db = "query_test_ns";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeQueryWithReqIDMSTest()
        {
            var db = "query_test_reqid_ms";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeQueryWithReqIDUSTest()
        {
            var db = "query_test_reqid_us";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeQueryWithReqIDNSTest()
        {
            var db = "query_test_reqid_ns";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtMSTest()
        {
            var db = "stmt_test_ms";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtUSTest()
        {
            var db = "stmt_test_us";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtNSTest()
        {
            var db = "stmt_test_ns";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtWithReqIDMSTest()
        {
            var db = "stmt_test_req_ms";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtWithReqIDUSTest()
        {
            var db = "stmt_test_req_us";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtWithReqIDNSTest()
        {
            var db = "stmt_test_req_ns";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtColumnsMSTest()
        {
            var db = "stmt_columns_test_ms";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtColumnsUSTest()
        {
            var db = "stmt_columns_test_us";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtColumnsNSTest()
        {
            var db = "stmt_columns_test_ns";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeVarbinaryTest()
        {
            var db = "varbinary_test";
            this.VarbinaryTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeInfluxDBTest()
        {
            var db = "influxdb_test";
            this.InfluxDBTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeTelnetTest()
        {
            var db = "telnet_test";
            this.TelnetTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeSMLJsonTest()
        {
            var db = "sml_json_test";
            this.SMLJsonTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeQueryConcurrencyTest()
        {
            var db = "query_concurrency_test";
            this.QueryConcurrencyTest(this._nativeConnectString, db);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneMSTest()
        {
            var db = "query_conn_tz_ms_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneUSTest()
        {
            var db = "query_conn_tz_us_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneNSTest()
        {
            var db = "query_conn_tz_ns_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        
        [Fact]
        public void NativeStmtMSBindTimestampTest()
        {
            var db = "stmt_bind_stmt_test_ms";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtUSBindTimestampTest()
        {
            var db = "stmt_bind_stmt_test_us";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtNSBindTimestampTest()
        {
            var db = "stmt_bind_stmt_test_ns";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        [Fact]
        public void NativeStmtTestWrongTypeMSTest()
        {
            var db = "stmt_wrong_test_ms";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        [Fact]
        public void NativeStmtTestWrongTypeUSTest()
        {
            var db = "stmt_wrong_test_us";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        [Fact]
        public void NativeStmtTestWrongTypeNSTest()
        {
            var db = "stmt_wrong_test_ns";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
    }
}
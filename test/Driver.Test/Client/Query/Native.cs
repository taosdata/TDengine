using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void NativeQueryMSTest()
        {
            const string db = "query_test_ms";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeQueryUSTest()
        {
            const string db = "query_test_us";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeQueryNSTest()
        {
            const string db = "query_test_ns";
            this.QueryTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeQueryWithReqIDMSTest()
        {
            const string db = "query_test_reqid_ms";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeQueryWithReqIDUSTest()
        {
            const string db = "query_test_reqid_us";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeQueryWithReqIDNSTest()
        {
            const string db = "query_test_reqid_ns";
            this.QueryWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtMSTest()
        {
            const string db = "stmt_test_ms";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtUSTest()
        {
            const string db = "stmt_test_us";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtNSTest()
        {
            const string db = "stmt_test_ns";
            this.StmtTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtWithReqIDMSTest()
        {
            const string db = "stmt_test_req_ms";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtWithReqIDUSTest()
        {
            const string db = "stmt_test_req_us";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtWithReqIDNSTest()
        {
            const string db = "stmt_test_req_ns";
            this.StmtWithReqIDTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtColumnsMSTest()
        {
            const string db = "stmt_columns_test_ms";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtColumnsUSTest()
        {
            const string db = "stmt_columns_test_us";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtColumnsNSTest()
        {
            const string db = "stmt_columns_test_ns";
            this.StmtBindColumnsTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeVarbinaryTest()
        {
            const string db = "varbinary_test";
            this.VarbinaryTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeInfluxDBTest()
        {
            const string db = "influxdb_test";
            this.InfluxDBTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeTelnetTest()
        {
            const string db = "telnet_test";
            this.TelnetTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeSMLJsonTest()
        {
            const string db = "sml_json_test";
            this.SMLJsonTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeQueryConcurrencyTest()
        {
            const string db = "query_concurrency_test";
            this.QueryConcurrencyTest(this._nativeConnectString, db);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneMSTest()
        {
            const string db = "query_conn_tz_ms_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneUSTest()
        {
            const string db = "query_conn_tz_us_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        
        [Fact]
        public void NativeQueryWithConnectionTimezoneNSTest()
        {
            const string db = "query_conn_tz_ns_test";
            QueryWithConnectionTimezoneTest(this._nativeConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        
        [Fact]
        public void NativeStmtMSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_ms";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeStmtUSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_us";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeStmtNSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_ns";
            this.StmtBindTimestampTest(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        [Fact]
        public void NativeStmtTestWrongTypeMSTest()
        {
            const string db = "stmt_wrong_test_ms";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        [Fact]
        public void NativeStmtTestWrongTypeUSTest()
        {
            const string db = "stmt_wrong_test_us";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        [Fact]
        public void NativeStmtTestWrongTypeNSTest()
        {
            const string db = "stmt_wrong_test_ns";
            this.StmtTestWrongType(this._nativeConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeStmtTestBindTagWithoutTable()
        {
            const string db = "stmt_bind_tag_no_table";
            this.StmtTestBindTagWithoutTable(this._nativeConnectString, db);
        }
        
        [Fact]
        public void NativeStmtQuery()
        {
            const string db = "stmt_query_test";
            this.StmtQuery(this._nativeConnectString,db);
        }

        [Fact]
        public void NativeStmtErrorProcessTest()
        {
            const string db = "stmt_error_process_test";
            this.StmtErrorProcessTest(this._nativeConnectString, db);
        }

        [Fact]
        public void NativeStmtBindTags()
        {
            const string db = "stmt_bind_tags_test";
            this.StmtBindTagsTest(this._nativeConnectString, db);
        }
    }
}
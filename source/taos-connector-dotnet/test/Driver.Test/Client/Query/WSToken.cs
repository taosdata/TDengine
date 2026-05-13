using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryMSTest()
        {
            const string db = "ws_query_test_ms_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryUSTest()
        {
            const string db = "ws_query_test_us_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryNSTest()
        {
            const string db = "ws_query_test_ns_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithReqIDMSTest()
        {
            const string db = "ws_query_test_reqid_ms_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithReqIDUSTest()
        {
            const string db = "ws_query_test_reqid_us_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithReqIDNSTest()
        {
            const string db = "ws_query_test_reqid_ns_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtMSTest()
        {
            const string db = "ws_stmt_test_ms_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtUSTest()
        {
            const string db = "ws_stmt_test_us_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtNSTest()
        {
            const string db = "ws_stmt_test_ns_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtWithReqIDMSTest()
        {
            const string db = "ws_stmt_test_req_ms_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtWithReqIDUSTest()
        {
            const string db = "ws_stmt_test_req_us_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtWithReqIDNSTest()
        {
            const string db = "ws_stmt_test_req_ns_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtColumnsMSTest()
        {
            const string db = "ws_stmt_columns_test_ms_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtColumnsUSTest()
        {
            const string db = "ws_stmt_columns_test_us_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtColumnsNSTest()
        {
            const string db = "ws_stmt_columns_test_ns_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenVarbinaryTest()
        {
            const string db = "ws_varbinary_test_token";
            this.VarbinaryTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenInfluxDBTest()
        {
            const string db = "ws_influxdb_test_token";
            this.InfluxDBTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenTelnetTest()
        {
            const string db = "ws_telnet_test_token";
            this.TelnetTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenSMLJsonTest()
        {
            const string db = "ws_sml_json_test_token";
            this.SMLJsonTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryConcurrencyTest()
        {
            const string db = "ws_query_concurrency_test_token";
            this.QueryConcurrencyTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithConnectionTimezoneMSTest()
        {
            const string db = "ws_query_conn_tz_ms_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithConnectionTimezoneUSTest()
        {
            const string db = "ws_query_conn_tz_us_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenQueryWithConnectionTimezoneNSTest()
        {
            const string db = "ws_query_conn_tz_ns_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtMSBindTimestampTest()
        {
            const string db = "ws_stmt_bind_stmt_test_ms_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtUSBindTimestampTest()
        {
            const string db = "ws_stmt_bind_stmt_test_us_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtNSBindTimestampTest()
        {
            const string db = "ws_stmt_bind_stmt_test_ns_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtTestWrongTypeMSTest()
        {
            const string db = "ws_stmt_wrong_test_ms_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtTestWrongTypeUSTest()
        {
            const string db = "ws_stmt_wrong_test_us_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtTestWrongTypeNSTest()
        {
            const string db = "ws_stmt_wrong_test_ns_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtTestBindTagWithoutTable()
        {
            const string db = "ws_stmt_bind_tag_no_table_token";
            this.StmtTestBindTagWithoutTable(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtQuery()
        {
            const string db = "ws_stmt_query_test_token";
            this.StmtQuery(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtErrorProcessTest()
        {
            const string db = "ws_stmt_error_process_test_token";
            this.StmtErrorProcessTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenStmtBindTags()
        {
            const string db = "ws_stmt_bind_tags_test_token";
            this.StmtBindTagsTest(this._wsTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void WebSocketTokenConnectionAvailable()
        {
            this.ConnectionAvailable(this._wsTokenConnectString);
        }
    }
}
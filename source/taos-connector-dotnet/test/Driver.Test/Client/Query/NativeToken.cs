using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public class EnterpriseFactAttribute : FactAttribute
    {
        public override string Skip => !Client.IsEnterpriseTest ? "Enterprise edition is required for token-based authentication. Skipping." : string.Empty;
    }

    public partial class Client
    {
        [EnterpriseFactAttribute]
        public void NativeTokenQueryMSTest()
        {
            const string db = "query_test_ms_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryUSTest()
        {
            const string db = "query_test_us_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryNSTest()
        {
            const string db = "query_test_ns_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithReqIDMSTest()
        {
            const string db = "query_test_reqid_ms_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithReqIDUSTest()
        {
            const string db = "query_test_reqid_us_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithReqIDNSTest()
        {
            const string db = "query_test_reqid_ns_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtMSTest()
        {
            const string db = "stmt_test_ms_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtUSTest()
        {
            const string db = "stmt_test_us_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtNSTest()
        {
            const string db = "stmt_test_ns_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtWithReqIDMSTest()
        {
            const string db = "stmt_test_req_ms_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtWithReqIDUSTest()
        {
            const string db = "stmt_test_req_us_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtWithReqIDNSTest()
        {
            const string db = "stmt_test_req_ns_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtColumnsMSTest()
        {
            const string db = "stmt_columns_test_ms_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtColumnsUSTest()
        {
            const string db = "stmt_columns_test_us_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtColumnsNSTest()
        {
            const string db = "stmt_columns_test_ns_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenVarbinaryTest()
        {
            const string db = "varbinary_test_token";
            this.VarbinaryTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenInfluxDBTest()
        {
            const string db = "influxdb_test_token";
            this.InfluxDBTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenTelnetTest()
        {
            const string db = "telnet_test_token";
            this.TelnetTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenSMLJsonTest()
        {
            const string db = "sml_json_test_token";
            this.SMLJsonTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryConcurrencyTest()
        {
            const string db = "query_concurrency_test_token";
            this.QueryConcurrencyTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithConnectionTimezoneMSTest()
        {
            const string db = "query_conn_tz_ms_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithConnectionTimezoneUSTest()
        {
            const string db = "query_conn_tz_us_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenQueryWithConnectionTimezoneNSTest()
        {
            const string db = "query_conn_tz_ns_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db,
                TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtMSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_ms_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtUSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_us_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtNSBindTimestampTest()
        {
            const string db = "stmt_bind_stmt_test_ns_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtTestWrongTypeMSTest()
        {
            const string db = "stmt_wrong_test_ms_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtTestWrongTypeUSTest()
        {
            const string db = "stmt_wrong_test_us_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtTestWrongTypeNSTest()
        {
            const string db = "stmt_wrong_test_ns_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtTestBindTagWithoutTable()
        {
            const string db = "stmt_bind_tag_no_table_token";
            this.StmtTestBindTagWithoutTable(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtQuery()
        {
            const string db = "stmt_query_test_token";
            this.StmtQuery(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtErrorProcessTest()
        {
            const string db = "stmt_error_process_test_token";
            this.StmtErrorProcessTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenStmtBindTags()
        {
            const string db = "stmt_bind_tags_test_token";
            this.StmtBindTagsTest(this._nativeTokenConnectString, db);
        }

        [EnterpriseFactAttribute]
        public void NativeTokenConnectionAvailable()
        {
            this.ConnectionAvailable(this._nativeTokenConnectString);
        }
    }
}
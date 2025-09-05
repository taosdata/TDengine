using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public static class WSAction
    {
        public const string Version = "version";
        public const string Conn = "conn";
        public const string Query = "query";
        public const string Fetch = "fetch";
        public const string FetchBlock = "fetch_block";
        public const string FreeResult = "free_result";

        public const string STMTInit = "init";
        public const string STMTPrepare = "prepare";
        public const string STMTSetTableName = "set_table_name";
        public const string STMTSetTags = "set_tags";
        public const string STMTBind = "bind";
        public const string STMTAddBatch = "add_batch";
        public const string STMTExec = "exec";
        public const string STMTClose = "close";
        public const string STMTGetTagFields = "get_tag_fields";
        public const string STMTGetColFields = "get_col_fields";
        public const string STMTUseResult = "use_result";

        public const string STMT2Init = "stmt2_init";
        public const string STMT2Prepare = "stmt2_prepare";
        public const string STMT2Exec = "stmt2_exec";
        public const string STMT2Result = "stmt2_result";
        public const string STMT2Close = "stmt2_close";

        public const string SchemalessWrite = "insert";
    }

    public static class WSActionBinary
    {
        public const int SetTagsMessage = 1;
        public const int BindMessage = 2;
        public const int BinaryQueryMessage = 6;
        public const int FetchRawBlockMessage = 7;
        public const int Stmt2BindMessage = 9;
    }

    public static class WSTMQAction
    {
        public const string TMQSubscribe = "subscribe";
        public const string TMQPoll = "poll";
        public const string TMQFetch = "fetch";
        public const string TMQFetchBlock = "fetch_block";
        public const string TMQFetchRaw = "fetch_raw";
        public const string TMQFetchJsonMeta = "fetch_json_meta";
        public const string TMQCommit = "commit";
        public const string TMQUnsubscribe = "unsubscribe";
        public const string TMQGetTopicAssignment = "assignment";
        public const string TMQSeek = "seek";
        public const string TMQCommitOffset = "commit_offset";
        public const string TMQCommitted = "committed";
        public const string TMQPosition = "position";
        public const string TMQListTopics = "list_topics";
    }

    public class WSActionReq<T>
    {
        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("args")] public T Args { get; set; }
    }
}
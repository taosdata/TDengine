using System.Collections.Generic;
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQSubscribeReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("user")] public string User { get; set; }

        [JsonProperty("password")] public string Password { get; set; }

        [JsonProperty("db")] public string Db { get; set; }

        [JsonProperty("group_id")] public string GroupId { get; set; }

        [JsonProperty("client_id")] public string ClientId { get; set; }

        [JsonProperty("offset_rest")] public string OffsetRest { get; set; }

        [JsonProperty("topics")] public List<string> Topics { get; set; }

        [JsonProperty("auto_commit")] public string AutoCommit { get; set; }

        [JsonProperty("auto_commit_interval_ms")]
        public string AutoCommitIntervalMs { get; set; }

        [JsonProperty("snapshot_enable")] public string SnapshotEnable { get; set; }

        [JsonProperty("with_table_name")] public string WithTableName { get; set; }

        //session_timeout_ms
        [JsonProperty("session_timeout_ms")] public string SessionTimeoutMs { get; set; }

        //max_poll_interval_ms
        [JsonProperty("max_poll_interval_ms")] public string MaxPollIntervalMs { get; set; }

        // other config
        [JsonProperty("config")] public Dictionary<string, string> Config { get; set; }
    }
}
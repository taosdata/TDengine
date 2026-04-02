using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmt2PrepareReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("stmt_id")] public ulong StmtId { get; set; }

        [JsonProperty("sql")] public string SQL { get; set; }

        [JsonProperty("get_fields")] public bool GetFields { get; set; }
    }
}
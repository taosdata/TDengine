using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmt2BindResp:IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("stmt_id")] public ulong StmtId { get; set; }
    }
}
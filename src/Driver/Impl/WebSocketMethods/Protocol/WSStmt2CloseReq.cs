using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmt2CloseReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("stmt_id")] public ulong StmtId { get; set; }
    }
}
using Newtonsoft.Json;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public class WSStmt2ExecResp:IWSBaseResp
    {
        [JsonProperty("code")]
        public int Code { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("action")]
        public string Action { get; set; }

        [JsonProperty("req_id")]
        public ulong ReqId { get; set; }

        [JsonProperty("timing")]
        public long Timing { get; set; }

        [JsonProperty("stmt_id")]
        public ulong StmtId { get; set; }

        [JsonProperty("affected")]
        public int Affected { get; set; }
    }
}
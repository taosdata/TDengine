using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSQueryReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("sql")] public string Sql { get; set; }
    }
}
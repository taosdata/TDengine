using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSSchemalessReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("protocol")] public int Protocol { get; set; }

        [JsonProperty("precision")] public string Precision { get; set; }

        [JsonProperty("ttl")] public int TTL { get; set; }

        [JsonProperty("data")] public string Data { get; set; }
    }
}
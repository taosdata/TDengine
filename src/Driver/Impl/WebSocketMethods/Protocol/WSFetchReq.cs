using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSFetchReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("id")] public ulong ResultId { get; set; }
    }
}
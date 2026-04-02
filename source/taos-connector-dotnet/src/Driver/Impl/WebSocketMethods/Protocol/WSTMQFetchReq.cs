using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQFetchReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("message_id")] public ulong MessageId { get; set; }
    }
}
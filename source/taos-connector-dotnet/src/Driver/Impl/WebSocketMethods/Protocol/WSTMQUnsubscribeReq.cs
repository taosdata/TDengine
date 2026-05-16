using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQUnsubscribeReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }
    }
}
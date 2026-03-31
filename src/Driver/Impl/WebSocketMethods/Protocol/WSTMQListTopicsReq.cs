using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQListTopicsReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }
    }
}
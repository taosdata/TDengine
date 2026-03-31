using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQGetTopicAssignmentReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("topic")] public string Topic { get; set; }
    }
}
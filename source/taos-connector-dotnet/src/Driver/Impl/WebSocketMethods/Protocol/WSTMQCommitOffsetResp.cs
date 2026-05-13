using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQCommitOffsetResp : IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("topic")] public string Topic { get; set; }

        [JsonProperty("vgroup_id")] public int VGroupId { get; set; }

        [JsonProperty("offset")] public long Offset { get; set; }
    }
}
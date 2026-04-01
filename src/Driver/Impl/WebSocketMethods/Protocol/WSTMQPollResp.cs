using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQPollResp : IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("have_message")] public bool HaveMessage { get; set; }

        [JsonProperty("topic")] public string Topic { get; set; }

        [JsonProperty("database")] public string Database { get; set; }

        [JsonProperty("vgroup_id")] public int VgroupId { get; set; }

        [JsonProperty("message_type")] public int MessageType { get; set; }

        [JsonProperty("message_id")] public ulong MessageId { get; set; }

        [JsonProperty("offset")] public long Offset { get; set; }
    }
}
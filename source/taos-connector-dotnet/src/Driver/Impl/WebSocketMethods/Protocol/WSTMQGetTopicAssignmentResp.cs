using System.Collections.Generic;
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQGetTopicAssignmentResp:IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("assignment")] public List<WSTMQAssignment> Assignment { get; set; }
    }

    public class WSTMQAssignment
    {
        [JsonProperty("vgroup_id")] public int VGroupId { get; set; }

        [JsonProperty("offset")] public long Offset { get; set; }

        [JsonProperty("begin")] public long Begin { get; set; }

        [JsonProperty("end")] public long End { get; set; }
    }
}
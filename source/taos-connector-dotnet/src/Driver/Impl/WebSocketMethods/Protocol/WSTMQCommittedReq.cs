using System.Collections.Generic;
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQCommittedReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("topic_vgroup_ids")] public List<WSTopicVgroupId> TopicVgroupIds { get; set; }
    }
}
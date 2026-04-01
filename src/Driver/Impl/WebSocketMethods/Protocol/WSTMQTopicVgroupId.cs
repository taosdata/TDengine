using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTopicVgroupId
    {
        [JsonProperty("topic")] public string Topic { get; set; }

        [JsonProperty("vgroup_id")] public int VGroupId { get; set; }
    }
}
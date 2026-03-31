using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSFetchResp : IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("id")] public ulong ResultId { get; set; }

        [JsonProperty("completed")] public bool Completed { get; set; }

        [JsonProperty("lengths")] public int[] Lengths { get; set; }

        [JsonProperty("rows")] public int Rows { get; set; }
    }
}
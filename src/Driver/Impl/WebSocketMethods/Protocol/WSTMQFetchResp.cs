using System.Collections.Generic;
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQFetchResp : IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("message_id")] public ulong MessageId { get; set; }

        [JsonProperty("completed")] public bool Completed { get; set; }

        [JsonProperty("table_name")] public string TableName { get; set; }

        [JsonProperty("rows")] public int Rows { get; set; }

        [JsonProperty("fields_count")] public int FieldsCount { get; set; }

        [JsonProperty("fields_names")] public List<string> FieldsNames { get; set; }

        [JsonProperty("fields_types")] public List<byte> FieldsTypes { get; set; }

        [JsonProperty("fields_lengths")] public List<long> FieldsLengths { get; set; }

        [JsonProperty("precision")] public int Precision { get; set; }
    }
}
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public interface IWSMetaResp
    {
        [JsonProperty("fields_count")] int FieldsCount { get; set; }

        [JsonProperty("fields_names")] string[] FieldsNames { get; set; }

        [JsonProperty("fields_types")] byte[] FieldsTypes { get; set; }

        [JsonProperty("fields_lengths")] long[] FieldsLengths { get; set; }

        [JsonProperty("precision")] int Precision { get; set; }

        [JsonProperty("fields_precisions")] byte[] FieldsPrecisions { get; set; }

        [JsonProperty("fields_scales")] byte[] FieldsScales { get; set; }
    }
}
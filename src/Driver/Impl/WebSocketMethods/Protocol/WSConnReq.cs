using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSConnReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }
        [JsonProperty("user")] public string User { get; set; }
        [JsonProperty("password")] public string Password { get; set; }
        [JsonProperty("db")] public string Db { get; set; }
        [JsonProperty("tz")] public string Timezone { get; set; }
        [JsonProperty("app")] public string App { get; set; }
        
        // connector
        [JsonProperty("connector")] public string Connector { get; set; }
        // bearer_token
        [JsonProperty("bearer_token")] public string BearerToken { get; set; }
    }
}
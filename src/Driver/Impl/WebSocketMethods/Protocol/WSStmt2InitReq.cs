using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmt2InitReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }
        [JsonProperty("single_stb_insert")] public bool SingleStbInsert { get; set; }

        [JsonProperty("single_table_bind_once")]
        public bool SingleTableBindOnce { get; set; }
    }
}
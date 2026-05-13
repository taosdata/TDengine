using System.Collections.Generic;
using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmt2PrepareResp:IWSBaseResp
    {
        [JsonProperty("code")] public int Code { get; set; }

        [JsonProperty("message")] public string Message { get; set; }

        [JsonProperty("action")] public string Action { get; set; }

        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("timing")] public long Timing { get; set; }

        [JsonProperty("stmt_id")] public ulong StmtId { get; set; }
        
        [JsonProperty("is_insert")] public bool IsInsert { get; set; }
        
        [JsonProperty("fields")] public List<Stmt2AllField> Fields { get; set; }
        
        [JsonProperty("fields_count")] public int FieldsCount { get; set; }
    }
    
    public class Stmt2AllField
    {
        [JsonProperty("name")] public string Name { get; set; }

        [JsonProperty("field_type")] public sbyte FieldType { get; set; }

        [JsonProperty("precision")] public byte Precision { get; set; }

        [JsonProperty("scale")] public byte Scale { get; set; }

        [JsonProperty("bytes")] public int Bytes { get; set; }
        
        [JsonProperty("bind_type")] public byte BindType { get; set; }
    }
}
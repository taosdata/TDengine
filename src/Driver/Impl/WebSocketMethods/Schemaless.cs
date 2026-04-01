using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public partial class Connection
    {
        public WSSchemalessResp SchemalessInsert(string lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            var uReqId = (ulong)reqId;
            return SendJsonBackJson<WSSchemalessReq, WSSchemalessResp>(WSAction.SchemalessWrite, new WSSchemalessReq
            {
                ReqId = uReqId,
                Protocol = (int)protocol,
                Precision = TDengineConstant.SchemalessPrecisionString(precision),
                TTL = ttl,
                Data = lines,
            },uReqId);
        }
    }
}
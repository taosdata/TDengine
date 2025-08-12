using System;
using System.Diagnostics;
using System.Text;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public partial class Connection : BaseConnection
    {
        private readonly string _user;
        private readonly string _password;
        private readonly string _db;
        private readonly string _timezone = string.Empty;

        public Connection(string addr, string user, string password, string db, TimeSpan connectTimeout = default,
            TimeSpan readTimeout = default, TimeSpan writeTimeout = default, bool enableCompression = false,
            TimeZoneInfo connectionTimezone = null) : base(
            addr, connectTimeout, readTimeout, writeTimeout, enableCompression)
        {
            _user = user;
            _password = password;
            _db = db;
            if (connectionTimezone != null)
            {
                _timezone = connectionTimezone.Id;
            }
        }

        public void Connect()
        {
            var reqId = _GetReqId();
            SendJsonBackJson<WSConnReq, WSConnResp>(WSAction.Conn, new WSConnReq
            {
                ReqId = reqId,
                User = _user,
                Password = _password,
                Db = _db,
                Timezone = _timezone,
                App = TDengineConstant.ProcessName
            }, reqId);
        }

        public WSQueryResp BinaryQuery(string sql, ulong reqid = 0)
        {
            if (reqid == 0)
            {
                reqid = _GetReqId();
            }

            //p0 uin64  req_id
            //p0+8 uint64  message_id
            //p0+16 uint64 action
            //p0+24 uint16 version
            //p0+26 uint32 sql_len
            //p0+30 raw sql
            var src = Encoding.UTF8.GetBytes(sql);
            var req = new byte[30 + src.Length];
            WriteUInt64ToBytes(req, reqid, 0);
            WriteUInt64ToBytes(req, 0, 8);
            WriteUInt64ToBytes(req, WSActionBinary.BinaryQueryMessage, 16);
            WriteUInt16ToBytes(req, 1, 24);
            WriteUInt32ToBytes(req, (uint)src.Length, 26);
            Buffer.BlockCopy(src, 0, req, 30, src.Length);

            return SendBinaryBackJson<WSQueryResp>(req, reqid);
        }

        public byte[] FetchRawBlockBinary(ulong resultId)
        {
            //p0 uin64  req_id
            //p0+8 uint64  message_id
            //p0+16 uint64 action
            //p0+24 uint16 version
            var req = new byte[32];
            var reqId = _GetReqId();
            WriteUInt64ToBytes(req, reqId, 0);
            WriteUInt64ToBytes(req, resultId, 8);
            WriteUInt64ToBytes(req, WSActionBinary.FetchRawBlockMessage, 16);
            WriteUInt64ToBytes(req, 1, 24);
            return SendBinaryBackBytes(req, reqId);
        }

        public void FreeResult(ulong resultId)
        {
            var reqId = _GetReqId();
            SendJson(WSAction.FreeResult, new WSFreeResultReq
            {
                ReqId = reqId,
                ResultId = resultId
            }, reqId);
        }
    }
}
using System;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public partial class Connection
    {
    
        public WSStmt2InitResp Stmt2Init(ulong reqId)
        {
            return SendJsonBackJson<WSStmt2InitReq, WSStmt2InitResp>(WSAction.STMT2Init, new WSStmt2InitReq
            {
                ReqId = reqId,
                SingleStbInsert = true,
                SingleTableBindOnce = true,
            },reqId);
        }

        public WSStmt2PrepareResp Stmt2Prepare(ulong stmtId,string sql)
        {
            var reqId = _GetReqId();
            return SendJsonBackJson<WSStmt2PrepareReq, WSStmt2PrepareResp>(WSAction.STMT2Prepare, new WSStmt2PrepareReq
            {
                ReqId = reqId,
                StmtId = stmtId,
                SQL = sql,
                GetFields = true,
            },reqId);
        }
        
        public WSStmt2BindResp Stmt2Bind(ulong stmtId, byte[]req)
        {
            //p0 uin64  req_id
            //p0+8 uint64  stmt_id
            //p0+16 uint64 (1 (set tag) 2 (bind))
            //p0+24 uint16 version
            //p0+26 uint32 col_index
            //p0+30 bindData
            var reqId = _GetReqId();
            WriteUInt64ToBytes(req, reqId,0);
            WriteUInt64ToBytes(req, stmtId,8);
            WriteUInt64ToBytes(req,WSActionBinary.Stmt2BindMessage,16);
            WriteUInt16ToBytes(req, 1, 24);
            WriteUInt32ToBytes(req, 0xffffffff, 26); //col_index(-1)
            return SendBinaryBackJson<WSStmt2BindResp>(req,reqId);
        }
        
        public WSStmt2ExecResp Stmt2Exec(ulong stmtId)
        {
            var reqId = _GetReqId();
            return SendJsonBackJson<WSStmt2ExecReq, WSStmt2ExecResp>(WSAction.STMT2Exec, new WSStmt2ExecReq
            {
                ReqId =reqId,
                StmtId = stmtId
            },reqId);
        }
        
        public WSStmt2UseResultResp Stmt2UseResult(ulong stmtId)
        {
            var reqId = _GetReqId();
            return SendJsonBackJson<WSStmt2UseResultReq, WSStmt2UseResultResp>(WSAction.STMT2Result,
                new WSStmt2UseResultReq
                {
                    ReqId = reqId,
                    StmtId = stmtId
                },reqId);
        }
        public void Stmt2Close(ulong stmtId)
        {
            var reqId = _GetReqId();
            SendJson(WSAction.STMT2Close, new WSStmt2CloseReq
            {
                ReqId = reqId,
                StmtId = stmtId
            },reqId);
        }
        
    }
}
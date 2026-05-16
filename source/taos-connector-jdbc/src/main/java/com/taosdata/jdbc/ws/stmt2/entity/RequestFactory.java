package com.taosdata.jdbc.ws.stmt2.entity;


import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Request;

/**
 * generate id for request
 */
public class RequestFactory {

    private RequestFactory() {
    }

    public static Request generateInit(long reqId, boolean singleStbInsert, boolean singleTableBindOnce) {
        InitReq initReq = new InitReq();
        initReq.setReqId(reqId);
        initReq.setSingleStbInsert(singleStbInsert);
        initReq.setSingleTableBindOnce(singleTableBindOnce);
        return new Request(Action.STMT2_INIT.getAction(), initReq);
    }

    public static Request generatePrepare(long stmtId, long reqId, String sql) {
        PrepareReq prepareReq = new PrepareReq();
        prepareReq.setReqId(reqId);
        prepareReq.setStmtId(stmtId);
        prepareReq.setSql(sql);
        return new Request(Action.STMT2_PREPARE.getAction(), prepareReq);
    }

    public static Request generateExec(long stmtId, long reqId) {
        ExecReq req = new ExecReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_EXEC.getAction(), req);
    }
    public static Request generateClose(long stmtId, long reqId) {
        CloseReq req = new CloseReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_CLOSE.getAction(), req);
    }
    public static Request generateUseResult(long stmtId, long reqId) {
        ResultReq req = new ResultReq();
        req.setReqId(reqId);
        req.setStmtId(stmtId);
        return new Request(Action.STMT2_USE_RESULT.getAction(), req);
    }
}

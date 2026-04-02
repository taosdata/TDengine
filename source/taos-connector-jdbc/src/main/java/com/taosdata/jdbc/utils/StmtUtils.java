package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.RequestFactory;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class StmtUtils {
    private static final Logger log = LoggerFactory.getLogger(StmtUtils.class);
    private StmtUtils() {}
    public static Stmt2PrepareResp initStmtWithRetry(Transport transport, String sql, int retryTimes) throws SQLException {
        SQLException lastError = TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN);
        for (int i = 0; i < retryTimes; i++) {
            long reqId = ReqId.getReqID();
            long stmtId = 0;
            long reconnectCount = transport.getReconnectCount();
            try{
                Request request = RequestFactory.generateInit(reqId, true, true);
                Stmt2Resp resp = (Stmt2Resp) transport.send(request);
                if (Code.SUCCESS.getCode() != resp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                }
                stmtId = resp.getStmtId();
                Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
                Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare, false, transport.getConnectionParam().getRequestTimeout());

                if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
                    Request close = RequestFactory.generateClose(stmtId, reqId);
                    transport.sendWithoutResponse(close);
                    throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
                }
                return prepareResp;
            } catch (SQLException e) {
                lastError = e;
                log.error("Error in initStmtWithRetry, stmt id: {}, req id: {}" +
                                "retry times: {}, code: {}, msg: {}",
                        stmtId,
                        reqId,
                        i,
                        e.getErrorCode(), e.getMessage());

                // check if connection is reestablished, if so, need to reinit stmt obj and retry
                int realReconnectCount = transport.getReconnectCount();
                if (reconnectCount != realReconnectCount) {
                    log.error("connection reestablished, need to init stmt obj");
                    continue;
                }

                // retry timeout without waiting
                if (e.getErrorCode() == TSDBErrorNumbers.ERROR_QUERY_TIMEOUT){
                    continue;
                }

                // network issue, retry with waiting
                if (e.getErrorCode() == TSDBErrorNumbers.ERROR_CONNECTION_CLOSED
                        || e.getErrorCode() == TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION) {
                    continue;
                }
                throw e;
            }
        }
        throw lastError;
    }
}

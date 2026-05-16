package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StmtUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WSRetryableStmt extends WSStatement {
    private static final Logger log = LoggerFactory.getLogger(WSRetryableStmt.class);

    // Operation type constants
    private static final int OPERATION_TYPE_WRITE = 1;
    private static final int OPERATION_TYPE_QUERY = 2;

    protected final ConnectionParam param;
    protected StmtInfo stmtInfo;
    protected final AtomicReference<SQLException> lastError = new AtomicReference<>(null);
    protected final AtomicInteger batchInsertedRowsInner;
    private long reconnectCount;

    public WSRetryableStmt(AbstractConnection connection,
                           ConnectionParam param,
                           String database,
                           Transport transport,
                           Long instanceId,
                           StmtInfo stmtInfo,
                           AtomicInteger batchInsertedRows) {
        super(transport, database, connection, instanceId, param.getZoneId());
        this.param = param;
        this.stmtInfo = stmtInfo;
        this.batchInsertedRowsInner = batchInsertedRows;
        this.reconnectCount = transport.getReconnectCount();
    }

    public void initStmt(int retryTimes) throws SQLException {
        Stmt2PrepareResp prepareResp = StmtUtils.initStmtWithRetry(transport, stmtInfo.getSql(), retryTimes);
        stmtInfo.setStmtId(prepareResp.getStmtId());
    }

    private void modifyStmtIdAndReqId(ByteBuf rawBlock, long stmtId, long reqId) {
        int originalWriterIndex = rawBlock.writerIndex();
        try {
            rawBlock.writerIndex(0);
            rawBlock.writeLongLE(reqId);
            rawBlock.writeLongLE(stmtId);
        } finally {
            rawBlock.writerIndex(originalWriterIndex);
        }
    }

    public void writeBlockWithRetry(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            executeWithRetry(rawBlock, OPERATION_TYPE_WRITE, param.isEnableAutoConnect());
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    public void writeBlockWithRetrySync(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            executeWithRetry(rawBlock, OPERATION_TYPE_WRITE, param.isEnableAutoConnect());
            if (lastError.get() != null) {
                SQLException e = lastError.get();
                lastError.set(null);
                throw e;
            }
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    public ResultResp queryWithRetry(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            ResultResp resultResp = (ResultResp) executeWithRetry(rawBlock, OPERATION_TYPE_QUERY, param.isEnableAutoConnect());
            if (lastError.get() != null) {
                SQLException e = lastError.get();
                lastError.set(null);
                throw e;
            }
            return resultResp;
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    private Object executeWithRetry(ByteBuf orgRawBlock, int operationType, boolean isRetry) throws SQLException {
        ByteBuf rawBlock = orgRawBlock.duplicate();
        int originalReaderIndex = orgRawBlock.readerIndex();
        int originalWriterIndex = orgRawBlock.writerIndex();

        int retryCount = 1;
        if (isRetry) {
            retryCount = param.getRetryTimes();
        }
        for (int i = 0; i < retryCount; i++) {
            if (i > 0) {
                rawBlock = orgRawBlock.copy();
                rawBlock.readerIndex(originalReaderIndex);
                rawBlock.writerIndex(originalWriterIndex);
            }

            long reqId = ReqId.getReqID();
            try {
                if (reconnectCount != transport.getReconnectCount() && param.isEnableAutoConnect()) {
                    initStmt(1);
                    reconnectCount = transport.getReconnectCount();
                }

                modifyStmtIdAndReqId(rawBlock, stmtInfo.getStmtId(), reqId);

                // Execute bind operation
                Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                        reqId, rawBlock, false, this.getQueryTimeoutInMs());
                if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
                }

                // Execute operation
                reqId = ReqId.getReqID();
                Request request = RequestFactory.generateExec(stmtInfo.getStmtId(), reqId);
                Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request, false, this.getQueryTimeoutInMs());
                if (Code.SUCCESS.getCode() != resp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                }

                // Process result based on operation type
                if (operationType == OPERATION_TYPE_WRITE) {
                    int affectedRows = resp.getAffected();
                    batchInsertedRowsInner.addAndGet(affectedRows);
                    return affectedRows;
                } else if (operationType == OPERATION_TYPE_QUERY) {
                    // Get query result
                    reqId = ReqId.getReqID();
                    request = RequestFactory.generateUseResult(stmtInfo.getStmtId(), reqId);
                    ResultResp useResultResp = (ResultResp) transport.send(request, false, this.getQueryTimeoutInMs());
                    if (Code.SUCCESS.getCode() != resp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                    }
                    return useResultResp;
                } else {
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
                }
            } catch (SQLException e) {
                // Handle exception based on operation type
                boolean shouldContinue = handleException(e, i, reconnectCount, operationType);
                if (!shouldContinue) {
                    lastError.set(e);
                    break;
                }

                // Check if connection is reestablished, if so need to reinitialize stmt object
                if (reconnectCount != transport.getReconnectCount()) {
                    log.error("connection reestablished, need to init stmt obj");
                    initStmt(1);
                    reconnectCount = transport.getReconnectCount();
                }
            } finally {
                log.trace("buffer {}, refCnt: {}", Integer.toHexString(System.identityHashCode(rawBlock)), rawBlock.refCnt());
            }
        }

        return null;
    }

    private boolean handleException(SQLException e, int retryCount, long reconnectCount, int operationType) {
        String operationName = (operationType == OPERATION_TYPE_WRITE) ? "writeBlockWithRetry" : "queryWithRetry";

        if (retryCount == param.getRetryTimes() - 1) {
            lastError.set(e);
            return false; // Exception will be thrown externally
        }

        log.error("Error in {}, stmt id: {}, retry times: {}, code: {}, msg: {}",
                operationName, stmtInfo.getStmtId(), retryCount, e.getErrorCode(), e.getMessage());

        // Check if retry is needed
        return shouldRetry(e, reconnectCount);
    }

    private boolean shouldRetry(SQLException e, long reconnectCount) {
        // Check if connection is reestablished
        int realReconnectCount = transport.getReconnectCount();
        if (reconnectCount != realReconnectCount) {
            return true;
        }

        // Timeout error, retry immediately without waiting
        if (e.getErrorCode() == TSDBErrorNumbers.ERROR_QUERY_TIMEOUT) {
            return true;
        }

        // Network issue, retry after waiting
        if (e.getErrorCode() == TSDBErrorNumbers.ERROR_CONNECTION_CLOSED ||
                e.getErrorCode() == TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION) {
            return true;
        }

        return false;
    }

    public void releaseStmt() throws SQLException {
        if (stmtInfo.getStmtId() != 0 && transport.isConnected()) {
            long reqId = ReqId.getReqID();
            Request close = RequestFactory.generateClose(stmtInfo.getStmtId(), reqId);
            transport.send(close, this.getQueryTimeoutInMs());
        }
    }
}
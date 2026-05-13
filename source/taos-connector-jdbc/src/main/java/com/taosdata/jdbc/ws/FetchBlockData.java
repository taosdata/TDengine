package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.BlockData;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.FetchState;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.utils.FetchDataUtil;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.QueryResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FetchBlockData {
    private static final Logger log = LoggerFactory.getLogger(FetchBlockData.class);

    private final Transport transport;
    private final long queryId;
    private final long reqId;
    private static final int CACHE_SIZE = 5;
    private final BlockingQueue<BlockData> blockingQueue = new LinkedBlockingQueue<>(CACHE_SIZE);
    private final ForkJoinPool dataHandleExecutor = Utils.getForkJoinPool();
    private final List<RestfulResultSet.Field> fields;
    private volatile FetchState fetchState = FetchState.STOPPED;
    private final long timeoutMs;
    private final int precision;

    public FetchBlockData(Transport transport,
                          QueryResp response,
                          List<RestfulResultSet.Field> fields,
                          int timeoutMs,
                          int precision) {
        this.transport = transport;
        this.queryId = response.getId();
        this.reqId = response.getReqId();
        this.fields = fields;
        this.timeoutMs = timeoutMs;
        this.precision = precision;
    }

    public void handleReceiveBlockData(FetchBlockNewResp resp) throws InterruptedException {

        BlockData blockData = BlockData.getEmptyBlockData(fields, precision);
        resp.init();

        if (Code.SUCCESS.getCode() != resp.getCode()) {
            blockData.setReturnCode(resp.getCode());
            blockData.setErrorMessage(resp.getMessage());
            blockData.doneWithNoData();
            blockingQueue.put(blockData);

            fetchState = FetchState.FINISHED_ERROR;
            return;
        }
        if (resp.isCompleted()) {
            blockData.setCompleted(true);
            blockData.doneWithNoData();
            blockingQueue.put(blockData);
            FetchDataUtil.getFetchMap().remove(reqId);
            return;
        }

        blockData.setBuffer(resp.getBuffer());
        blockingQueue.put(blockData);
        dataHandleExecutor.submit(blockData::handleData);

        if (blockingQueue.remainingCapacity() > 0 && FetchDataUtil.getFetchMap().get(reqId) != null){
            try {
                transport.sendFetchBlockAsync(reqId, queryId);
                return;
            } catch (SQLException e) {
                log.error("Error when sending fetch block request:", e);
                blockData.setReturnCode(TSDBErrorNumbers.ERROR_UNKNOWN);
                blockData.setErrorMessage("ErrorCode: " + e.getErrorCode() + ", Error when sending fetch block request: " + e.getMessage());
                blockingQueue.put(blockData);
           }
        }
        fetchState = FetchState.STOPPED;
    }


    public BlockData getBlockData() throws SQLException {
        if (fetchState == FetchState.STOPPED
                || (fetchState == FetchState.FINISHED_ERROR && blockingQueue.isEmpty())) {
            transport.sendFetchBlockAsync(reqId, queryId);
            fetchState = FetchState.FETCHING;
        }

        BlockData blockData;
        try {
            blockData = blockingQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FETCH DATA INTERRUPTED");
        }

        if (blockData == null) {
            // timeout occurred
            fetchState = FetchState.STOPPED;
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, "Fetch block data timeout after " + timeoutMs + " ms");
        }
        return blockData;
    }
}

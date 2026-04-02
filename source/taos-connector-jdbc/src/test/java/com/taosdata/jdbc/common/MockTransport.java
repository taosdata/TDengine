package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.ws.FetchBlockData;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.QueryResp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class MockTransport extends Transport {
    private FetchBlockData target;
    private int sendFetchCount = 0;

    public static final long TEST_QUERY_ID = 12345;
    public static final long TEST_REQ_ID = 67890;
    public static final int TIMEOUT_MS = 100;
    public static final int PRECISION = 0;
    public static final List<RestfulResultSet.Field> TEST_FIELDS = Arrays.asList(
            new RestfulResultSet.Field("ts", 0, 8, "timestamp", TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, 0, 0),
            new RestfulResultSet.Field("name", 0, 4, "name", TSDBConstants.TSDB_DATA_TYPE_INT, 0, 0)
    );

    public MockTransport() throws SQLException {
        super();
    }

    public void setTarget(FetchBlockData target) {
        this.target = target;
    }

    @Override
    public void sendFetchBlockAsync(long reqId, long queryId) {
        sendFetchCount++;
    }

    public void simulateReceiveResponse(FetchBlockNewResp resp) throws InterruptedException {
        target.handleReceiveBlockData(resp);
    }

    public int getSendFetchCount() {
        return sendFetchCount;
    }

    public void clearSendFetchCount() {
        sendFetchCount = 0;
    }

    public static QueryResp createQueryResp() {
        QueryResp resp = new QueryResp();
        resp.setId(TEST_QUERY_ID);
        resp.setReqId(TEST_REQ_ID);
        return resp;
    }
}
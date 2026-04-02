package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.BlockData;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.MockTransport;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.QueryResp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class FetchBlockDataTest {
    private MockTransport transport;
    private FetchBlockData fetchBlockData;

    private final byte[] normalData = {
            7, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            34, -23, 70, 39, 0, 0, 0, 0, 16, 0,
            -16, -89, 41, 10, 77, 16, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 121, 0, 0, 0, 1, 0, 0,
            0, 121, 0, 0, 0, 6, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, -128, 0, 0, 0,
            0, 0, 0, 0, 0, 9, 8, 0, 0, 0,
            4, 4, 0, 0, 0, 48, 0, 0, 0, 24,
            0, 0, 0, 0, 48, 6, -100, -94, -104, 1,
            0, 0, -121, 6, -100, -94, -104, 1, 0, 0,
            -21, 6, -100, -94, -104, 1, 0, 0, -15, 6,
            -100, -94, -104, 1, 0, 0, 79, 7, -100, -94,
            -104, 1, 0, 0, 82, 7, -100, -94, -104, 1,
            0, 0, 0, 100, 0, 0, 0, 100, 0, 0,
            0, 100, 0, 0, 0, 100, 0, 0, 0, 100,
            0, 0, 0, 100, 0, 0, 0, 0
    };

    @Before
    public void setup() throws SQLException {
    transport = new MockTransport();
        QueryResp queryResp = MockTransport.createQueryResp();
        fetchBlockData = new FetchBlockData(
                transport,
                queryResp,
                MockTransport.TEST_FIELDS,
                MockTransport.TIMEOUT_MS,
                MockTransport.PRECISION
        );
        transport.setTarget(fetchBlockData);
    }

    public FetchBlockNewResp getNormalRes(){
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeBytes(normalData);
        //
        return new FetchBlockNewResp(buffer);
    }

    // test normal response
    @Test
    public void testNormalDataBlock() throws Exception {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeBytes(normalData);
        //
        FetchBlockNewResp resp = new FetchBlockNewResp(buffer);
        // 触发响应处理
        transport.simulateReceiveResponse(resp);
        transport.clearSendFetchCount();

        // 获取数据块
        BlockData blockData = fetchBlockData.getBlockData();
        blockData.waitTillOK();

        assertNotNull(blockData);
        assertEquals(6, blockData.getNumOfRows());
        assertEquals(1, transport.getSendFetchCount()); // 验证发送了fetch请求
    }

    // test error response
    @Test
    public void testErrorResponse() throws Exception {

        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeLongLE(7); // action id
        buffer.writeShortLE(1); // version
        buffer.writeLongLE(0); // time
        buffer.writeLongLE(MockTransport.TEST_REQ_ID); //  result id
        buffer.writeIntLE(1); // code
        buffer.writeIntLE("Syntax error".length()); // msg len
        buffer.writeBytes("Syntax error".getBytes(StandardCharsets.UTF_8)); // msg
        buffer.writeLongLE(MockTransport.TEST_QUERY_ID); //  result id
        buffer.writeByte(0); // isCompleted
        buffer.writeIntLE(0); // BlockLen

        FetchBlockNewResp resp = new FetchBlockNewResp(buffer);
        transport.simulateReceiveResponse(resp);

        BlockData blockData = fetchBlockData.getBlockData();
        blockData.waitTillOK();
        assertEquals(1, blockData.getReturnCode());
        assertEquals("Syntax error", blockData.getErrorMessage());
    }

    @Test
    public void testErrorResponseWithRetry() throws Exception {

        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeLongLE(7); // action id
        buffer.writeShortLE(1); // version
        buffer.writeLongLE(0); // time
        buffer.writeLongLE(MockTransport.TEST_REQ_ID); //  result id
        buffer.writeIntLE(1); // code
        buffer.writeIntLE("Syntax error".length()); // msg len
        buffer.writeBytes("Syntax error".getBytes(StandardCharsets.UTF_8)); // msg
        buffer.writeLongLE(MockTransport.TEST_QUERY_ID); //  result id
        buffer.writeByte(0); // isCompleted
        buffer.writeIntLE(0); // BlockLen

        FetchBlockNewResp resp = new FetchBlockNewResp(buffer);
        transport.simulateReceiveResponse(resp);

        BlockData blockData = fetchBlockData.getBlockData();
        blockData.waitTillOK();
        assertEquals(1, blockData.getReturnCode());
        assertEquals("Syntax error", blockData.getErrorMessage());

        transport.clearSendFetchCount();
        try {
            fetchBlockData.getBlockData();
        } catch (Exception e){
            // Expected exception due to error response
            assertTrue(e instanceof SQLException);
            assertEquals(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, ((SQLException) e).getErrorCode());
        }
        Assert.assertEquals(1, transport.getSendFetchCount());
    }

    // test completed response
    @Test
    public void testCompletedResponse() throws Exception {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeLongLE(7); // action id
        buffer.writeShortLE(1); // version
        buffer.writeLongLE(0); // time
        buffer.writeLongLE(MockTransport.TEST_REQ_ID); //  result id
        buffer.writeIntLE(0); // code
        buffer.writeIntLE(0); // msg len
        buffer.writeLongLE(MockTransport.TEST_QUERY_ID); //  result id
        buffer.writeByte(1); // isCompleted
        buffer.writeIntLE(0); // BlockLen

        FetchBlockNewResp resp = new FetchBlockNewResp(buffer);

        transport.simulateReceiveResponse(resp);
        BlockData blockData = fetchBlockData.getBlockData();
        blockData.waitTillOK();
        assertTrue(blockData.isCompleted());
    }

    // test timeout behavior
    @Test
    public void testGetBlockDataTimeout() {
        long startTime = System.currentTimeMillis();
        try {
            fetchBlockData.getBlockData();
            fail("Expected timeout exception not thrown");
        } catch (SQLException e) {
            long duration = System.currentTimeMillis() - startTime;
            assertTrue(duration >= MockTransport.TIMEOUT_MS);
            assertEquals(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getErrorCode());
            assertTrue(e.getMessage().contains("timeout"));
        }
    }

    @Test
    public void testGetBlockDataTimeoutWithRetry() throws InterruptedException, SQLException {
        long startTime = System.currentTimeMillis();
        try {
            fetchBlockData.getBlockData();
            fail("Expected timeout exception not thrown");
        } catch (SQLException e) {
            long duration = System.currentTimeMillis() - startTime;
            assertTrue(duration >= MockTransport.TIMEOUT_MS);
            assertEquals(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getErrorCode());
            assertTrue(e.getMessage().contains("timeout"));
        }
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
        buffer.writeBytes(normalData);
        FetchBlockNewResp resp = new FetchBlockNewResp(buffer);

        fetchBlockData.handleReceiveBlockData(resp);
        BlockData blockData = fetchBlockData.getBlockData();
        blockData.waitTillOK();

        Assert.assertNotNull(blockData);
    }

    // test backpressure handling
    @Test
    public void testBackpressure() throws Exception {
        for (int i = 0; i < 5; i++) {
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(200);
            buffer.writeBytes(normalData);

            FetchBlockNewResp resp = new FetchBlockNewResp(buffer);
            transport.simulateReceiveResponse(resp);
        }

        transport.clearSendFetchCount();
        fetchBlockData.getBlockData(); // consume one block

        // the transport should have sent fetch requests for new block
        assertEquals(1, transport.getSendFetchCount());
    }
}
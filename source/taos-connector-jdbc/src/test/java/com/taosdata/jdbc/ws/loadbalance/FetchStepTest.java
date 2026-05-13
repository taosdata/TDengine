package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.FetchBlockHealthCheckResp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class FetchStepTest {

    @Mock
    private BgHealthCheck context;
    @Mock
    private WSClient wsClient;
    @Mock
    private InFlightRequest inFlightRequest;
    @Mock
    private ConnectionParam param;
    @Mock
    private StepFlow flow;
    private FetchStep fetchStep;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        fetchStep = new FetchStep();

        // Bind mocks to context
        when(context.getWsClient()).thenReturn(wsClient);
       // Key: Mock the send method to automatically release ByteBuf during execution.
        doAnswer(invocation -> {
            // get the first argument of send method (ByteBuf)
            ByteBuf buf = invocation.getArgument(0);
            if (buf != null && buf.refCnt() > 0) {
                buf.release(); // release ByteBuf to avoid memory leak
            }
            return null;
        }).when(wsClient).send(any(ByteBuf.class));

        when(context.getInFlightRequest()).thenReturn(inFlightRequest);
        when(context.getParam()).thenReturn(param);
        when(context.getResultId()).thenReturn(123456L); // Mock fixed resultId
    }

    /**
     * Test: When WebSocket client is not open, return CONNECT step
     */
    @Test
    public void execute_wsClientNotOpen_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(false);
        int expectedInterval = 8;
        when(context.getNextInterval()).thenReturn(expectedInterval);

        // Act
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(wsClient, never()).send(any(ByteBuf.class));
        verify(inFlightRequest, never()).put(any(FutureResponse.class));
    }

    /**
     * Test: When inFlightRequest.put throws exception, return CONNECT step with 0 interval
     */
    @Test
    public void execute_putThrowsException_returnsConnectStepWithZeroInterval() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        doThrow(new RuntimeException("Storage full")).when(inFlightRequest).put(any(FutureResponse.class));

        // Act
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(wsClient).send(any(ByteBuf.class)); // Verify ByteBuf is sent
    }

    /**
     * Test: When response is successful and clusterAlive is true, return FETCH2 step
     */
    @Test
    public void execute_responseClusterAliveTrue_returnsFetch2Step() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(6000); // Mock timeout
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Manually complete with successful response (clusterAlive=true)
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);

        byte[] buffer = StringUtils.hexToBytes("07000000000000000100fac7070000000000050030be0629e42300000000000000000100000000000000002b000000010000002b0000000100000001000000000000800000000000000000040400000004000000000100000000");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp successResp = new FetchBlockHealthCheckResp(buf);
        futureResponseCaptor.getValue().getFuture().complete(successResp);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.FETCH2, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        // Verify request is removed from inFlightRequest
        verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
        verify(context, never()).cleanUp(); // cleanUp is not called
    }

    /**
     * Test: When response is successful and clusterAlive is true, return FETCH2 step
     */
    @Test
    public void execute_responseClusterAliveTrue2_returnsFetch2Step() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(6000); // Mock timeout
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Manually complete with successful response (clusterAlive=true)
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);

        byte[] buffer = StringUtils.hexToBytes("07000000000000000100fac7070000000000050030be0629e42300000000000000000100000000000000002b000000010000002b0000000100000001000000000000800000000000000000040400000004000000000200000000");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp successResp = new FetchBlockHealthCheckResp(buf);
        futureResponseCaptor.getValue().getFuture().complete(successResp);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.FETCH2, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        // Verify request is removed from inFlightRequest
        verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
        verify(context, never()).cleanUp(); // cleanUp is not called
    }

    /**
     * Test: When response is successful but clusterAlive is false, return FETCH2 step
     */
    @Test
    public void execute_responseClusterAliveFalse_returnsFreeResultStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(6000);
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Manually complete with response (clusterAlive=false)
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);
        byte[] buffer = StringUtils.hexToBytes("07000000000000000100fac7070000000000050030be0629e42300000000000000000100000000000000002b000000010000002b0000000100000001000000000000800000000000000000040400000004000000000000000000");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp failResp = new FetchBlockHealthCheckResp(buf);
        futureResponseCaptor.getValue().getFuture().complete(failResp);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.FETCH2, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
        verify(context, times(1)).cleanUp();
    }

    /**
     * Test: When response throws exception (e.g., timeout), return FREE_RESULT step with 0 interval and call cleanUp
     */
    @Test
    public void execute_responseException_returnsFreeResultStepAndCleanUp() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(6000);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Manually trigger exception
        CompletableFuture<StepResponse> future = fetchStep.execute(context, flow);
        SQLTimeoutException testException = new SQLTimeoutException("Request timed out");
        futureResponseCaptor.getValue().getFuture().completeExceptionally(testException);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.FREE_RESULT, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(context).cleanUp(); // Verify cleanUp is called
    }

    /**
     * Test: Verify correctness of data written to ByteBuf (reqId, resultId, actionType, version)
     */
    @Test
    public void execute_sendByteBuf_verifyWrittenData() throws SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(6000);
        doNothing().when(inFlightRequest).put(any(FutureResponse.class));
        doNothing().when(wsClient).send(any(ByteBuf.class));

        ArgumentCaptor<ByteBuf> byteBufCaptor = ArgumentCaptor.forClass(ByteBuf.class);

        // Act
        fetchStep.execute(context, flow);
        verify(wsClient).send(byteBufCaptor.capture());
        ByteBuf capturedBuf = byteBufCaptor.getValue();

        // Assert: Verify data in writing order (LE = Little Endian)
        long writtenReqId = capturedBuf.readLongLE();
        long writtenResultId = capturedBuf.readLongLE();
        long writtenActionType = capturedBuf.readLongLE();
        byte[] writtenVersion = new byte[2];
        capturedBuf.readBytes(writtenVersion);

        Assert.assertEquals(context.getResultId(), writtenResultId);
        Assert.assertEquals(7L, writtenActionType);
        Assert.assertArrayEquals(new byte[]{1, 0}, writtenVersion);
        Assert.assertTrue(writtenReqId > 0); // ReqId.getReqID() returns non-zero value

        capturedBuf.release(); // Release ByteBuf to avoid memory leak
    }
    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
    }
}
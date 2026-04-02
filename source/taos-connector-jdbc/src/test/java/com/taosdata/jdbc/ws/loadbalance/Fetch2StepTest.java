package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.Endpoint;
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
import org.mockito.Spy;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
    * Unit tests for Fetch2Step, covering different execution scenarios of the 2-step fetch process
    */
public class Fetch2StepTest {

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

    private Fetch2Step fetch2Step;
    private final Endpoint endpoint = new Endpoint("test-endpoint", 6041, false);

    @Spy
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        fetch2Step = new Fetch2Step(rebalanceManager);

        when(context.getWsClient()).thenReturn(wsClient);
        when(context.getInFlightRequest()).thenReturn(inFlightRequest);
        when(context.getParam()).thenReturn(param);
        when(context.getEndpoint()).thenReturn(endpoint);
    }

    /**
        * Test scenario: WebSocket client not open, should return CONNECT step with next interval
        */
    @Test
    public void execute_wsClientNotOpen_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        when(wsClient.isOpen()).thenReturn(false);
        int expectedInterval = 5;
        when(context.getNextInterval()).thenReturn(expectedInterval);

        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        StepResponse response = future.get();

        assertEquals(StepEnum.CONNECT, response.getStep());
        assertEquals(expectedInterval, response.getWaitSeconds());
        verify(wsClient, never()).send(any(ByteBuf.class));
        verify(inFlightRequest, never()).put(any(FutureResponse.class));
    }

    /**
        * Test scenario: InFlightRequest.put throws exception, should return CONNECT step with 0 interval
        */
    @Test
    public void execute_putThrowsException_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        when(wsClient.isOpen()).thenReturn(true);
        doThrow(new IllegalStateException("Cache full")).when(inFlightRequest).put(any(FutureResponse.class));

        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        StepResponse response = future.get();

        assertEquals(StepEnum.CONNECT, response.getStep());
        assertEquals(0, response.getWaitSeconds());
        verify(wsClient).send(any(ByteBuf.class));
    }

    /**
        * Test scenario: Response completed, should return CLOSE step and trigger endpointUp
        */
    @Test
    public void execute_responseCompleted_returnsFinishStep() throws ExecutionException, InterruptedException, SQLException {
        doNothing().when(rebalanceManager).endpointUp(any(ConnectionParam.class), any(Endpoint.class));
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);

        byte[] buffer = StringUtils.hexToBytes("070000000000000001003ac0060000000000050030be0629e4230000000000000000010000000000000001");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp completedResp = new FetchBlockHealthCheckResp(buf);
        completedResp.setCompleted(true);

        futureResponseCaptor.getValue().getFuture().complete(completedResp);

        StepResponse response = future.get();
        assertEquals(StepEnum.CLOSE, response.getStep());
        assertEquals(0, response.getWaitSeconds());
        verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
        verify(context, never()).cleanUp();
        verify(rebalanceManager, times(1)).endpointUp(context.getParam(), endpoint);
    }

    /**
        * Test scenario: Response not completed, should return FETCH2 step without endpointUp
        */
    @Test
    public void execute_responseNotCompleted_returnsFetch2Step() throws ExecutionException, InterruptedException, SQLException {
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        byte[] buffer = StringUtils.hexToBytes("07000000000000000100fac7070000000000050030be0629e42300000000000000000100000000000000002b000000010000002b0000000100000001000000000000800000000000000000040400000004000000000100000000");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();

        buf.writeBytes(buffer);
        FetchBlockHealthCheckResp notCompletedResp = new FetchBlockHealthCheckResp(buf);
        notCompletedResp.setCompleted(false);

        futureResponseCaptor.getValue().getFuture().complete(notCompletedResp);

        StepResponse response = future.get();
        assertEquals(StepEnum.FETCH2, response.getStep());
        assertEquals(0, response.getWaitSeconds());
        verify(inFlightRequest).remove(action, futureResponseCaptor.getValue().getId());
        verify(rebalanceManager, never()).endpointUp(any(), any());
        verify(context, never()).cleanUp();
    }

    /**
        * Test scenario: Response throws exception, should return FREE_RESULT step with recovery interval
        */
    @Test
    public void execute_responseException_returnsFreeResultWithRecovery() throws ExecutionException, InterruptedException, SQLException {
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(3000);
        int expectedInterval = 10;
        when(context.getRecoveryInterval()).thenReturn(expectedInterval);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        CompletableFuture<StepResponse> future = fetch2Step.execute(context, flow);
        SQLTimeoutException testException = new SQLTimeoutException("Request timed out");
        futureResponseCaptor.getValue().getFuture().completeExceptionally(testException);

        StepResponse response = future.get();
        assertEquals(StepEnum.FREE_RESULT, response.getStep());
        assertEquals(expectedInterval, response.getWaitSeconds());
        verify(context).cleanUp();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
        System.gc();
        Thread.sleep(500);
    }
}
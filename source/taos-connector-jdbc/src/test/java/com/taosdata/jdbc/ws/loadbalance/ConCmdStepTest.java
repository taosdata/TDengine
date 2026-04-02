package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.ConnectResp;
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

public class ConCmdStepTest {

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
    private ConCmdStep conCmdStep;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        // Inject mock logger into ConCmdStep via test constructor
        conCmdStep = new ConCmdStep();

        // Bind mocks to context
        when(context.getWsClient()).thenReturn(wsClient);
        when(context.getInFlightRequest()).thenReturn(inFlightRequest);
        when(context.getParam()).thenReturn(param);
    }

    /**
     * Test: When WebSocket client is not open, return CONNECT step
     */
    @Test
    public void execute_wsClientNotOpen_returnsConnectStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(false);
        int expectedInterval = 1;
        when(context.getNextInterval()).thenReturn(expectedInterval);

        // Act
        CompletableFuture<StepResponse> future = conCmdStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(wsClient, never()).send(anyString());
        verify(inFlightRequest, never()).put(any(FutureResponse.class));
    }

    /**
     * Test: When inFlightRequest.put throws exception, return CON_CMD step with recovery interval
     */
    @Test
    public void execute_putThrowsException_returnsConCmdWithRecoveryInterval() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        doThrow(new IllegalStateException("Cache full")).when(inFlightRequest).put(any(FutureResponse.class));
        int expectedInterval = 2;
        when(context.getRecoveryInterval()).thenReturn(expectedInterval);

        // Act
        CompletableFuture<StepResponse> future = conCmdStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());

        verify(wsClient).send(anyString());
    }

    /**
     * Test: When response is successful (status code SUCCESS), return QUERTY step
     */
    @Test
    public void execute_responseSuccess_returnsQueryStep() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(5000);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());

        // Act: Manually complete with successful response
        CompletableFuture<StepResponse> future = conCmdStep.execute(context, flow);
        ConnectResp successResp = new ConnectResp();
        successResp.setCode(Code.SUCCESS.getCode());
        futureResponseCaptor.getValue().getFuture().complete(successResp);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.QUERY, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(inFlightRequest).remove(anyString(), anyLong());
        verify(context, never()).cleanUp();
    }

    /**
     * Test: When response is successful but status code is not SUCCESS, return CON_CMD step and clean up
     */
    @Test
    public void execute_responseFailedCode_returnsConCmdWithRecovery() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(5000);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());
        int expectedInterval = 3;
        when(context.getRecoveryInterval()).thenReturn(expectedInterval);

        // Act: Manually complete with failed response
        CompletableFuture<StepResponse> future = conCmdStep.execute(context, flow);
        ConnectResp failResp = new ConnectResp();
        failResp.setCode(-1);
        futureResponseCaptor.getValue().getFuture().complete(failResp);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(inFlightRequest).remove(anyString(), anyLong());
        verify(context).cleanUp();
    }

    /**
     * Test: When response throws exception (e.g., timeout), return CON_CMD step and clean up
     */
    @Test
    public void execute_responseException_returnsConCmdWithRecovery() throws ExecutionException, InterruptedException, SQLException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);
        when(param.getRequestTimeout()).thenReturn(5000);
        ArgumentCaptor<FutureResponse> futureResponseCaptor = ArgumentCaptor.forClass(FutureResponse.class);
        doNothing().when(inFlightRequest).put(futureResponseCaptor.capture());
        int expectedInterval = 3;
        when(context.getRecoveryInterval()).thenReturn(expectedInterval);

        // Act: Manually trigger exception
        CompletableFuture<StepResponse> future = conCmdStep.execute(context, flow);
        SQLTimeoutException testException = new SQLTimeoutException("Request timed out");
        futureResponseCaptor.getValue().getFuture().completeExceptionally(testException);

        // Assert
        StepResponse response = future.get();
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(context).cleanUp();
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
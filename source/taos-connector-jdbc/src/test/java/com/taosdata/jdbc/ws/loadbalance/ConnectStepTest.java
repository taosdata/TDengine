package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.WSClient;
import io.netty.channel.Channel;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class ConnectStepTest {

    @Mock
    private Channel mockChannel;
    @Mock
    private BgHealthCheck context;
    @Mock
    private WSClient wsClient;
    @Mock
    private StepFlow flow;
    private ConnectStep connectStep;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        connectStep = new ConnectStep();

        // Bind mocks to context
        when(context.getWsClient()).thenReturn(wsClient);
        when(mockChannel.isActive()).thenReturn(true);
    }

    /**
     * Test: When WSClient exists and is open, return CON_CMD step with 0 interval
     */
    @Test
    public void execute_wsClientOpen_returnsConCmdStep() throws ExecutionException, InterruptedException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(true);

        // Act
        CompletableFuture<StepResponse> future = connectStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(wsClient, never()).getChannelAsync(); // getChannelAsync should not be called
        verify(context, never()).cleanUp();
    }

    /**
     * Test: When WSClient is not open, but getChannelAsync succeeds, return CON_CMD step
     */
    @Test
    public void execute_wsClientNotOpen_getChannelSuccess_returnsConCmdStep() throws ExecutionException, InterruptedException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(false);
        CompletableFuture<Channel> channelFuture = CompletableFuture.completedFuture(mockChannel); // Mock successful channel acquisition
        when(wsClient.getChannelAsync()).thenReturn(channelFuture);

        // Act
        CompletableFuture<StepResponse> future = connectStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(wsClient).getChannelAsync();
        verify(context, never()).cleanUp();
    }

    /**
     * Test: When WSClient is not open and getChannelAsync fails, return CONNECT step with next interval and clean up
     */
    @Test
    public void execute_wsClientNotOpen_getChannelFailed_returnsConnectStep() throws ExecutionException, InterruptedException {
        // Arrange
        when(wsClient.isOpen()).thenReturn(false);
        CompletableFuture<Channel> channelFuture = new CompletableFuture<>();
        channelFuture.completeExceptionally(TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Connection failed"));
        when(wsClient.getChannelAsync()).thenReturn(channelFuture);

        int expectedInterval = 5;
        when(context.getNextInterval()).thenReturn(expectedInterval);

        // Act
        CompletableFuture<StepResponse> future = connectStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CONNECT, response.getStep());
        Assert.assertEquals(expectedInterval, response.getWaitSeconds());
        verify(wsClient).getChannelAsync();
        verify(context).cleanUp(); // Verify cleanUp is called on failure
    }

    /**
     * Test: When WSClient is null, getChannelAsync is called and handles success
     */
    @Test
    public void execute_wsClientNull_getChannelSuccess_returnsConCmdStep() throws ExecutionException, InterruptedException {
        // Arrange
        when(context.getWsClient()).thenReturn(null); // WSClient is null
        WSClient newWsClient = mock(WSClient.class);
        when(context.getWsClient()).thenReturn(newWsClient); // Reset to mock WSClient
        when(newWsClient.isOpen()).thenReturn(false);

        CompletableFuture<Channel> channelFuture = CompletableFuture.completedFuture(mockChannel);
        when(newWsClient.getChannelAsync()).thenReturn(channelFuture);

        // Act
        CompletableFuture<StepResponse> future = connectStep.execute(context, flow);
        StepResponse response = future.get();

        // Assert
        Assert.assertEquals(StepEnum.CON_CMD, response.getStep());
        Assert.assertEquals(0, response.getWaitSeconds());
        verify(newWsClient).getChannelAsync();
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
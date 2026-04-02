package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.entity.Action;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FreeResultStep} class.
 * Covers core business logic branches including WebSocket client status checks and request sending validation.
 */
public class FreeResultStepTest {

    @Mock
    private BgHealthCheck context;
    @Mock
    private WSClient wsClient;
    @Mock
    private StepFlow flow;
    private FreeResultStep freeResultStep;

    // Mock fixed ResultId, consistent with the original FetchStepTest
    private static final long MOCK_RESULT_ID = 123456L;
    // Mock fixed waiting interval for health check
    private static final int MOCK_NEXT_INTERVAL = 8;

    /**
     * Initialize mock objects and test instance before each test method execution.
     * Binds mock dependencies to the BgHealthCheck context.
     */
    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        freeResultStep = new FreeResultStep();

        // Bind mock objects to BgHealthCheck context
        when(context.getWsClient()).thenReturn(wsClient);
        when(context.getResultId()).thenReturn(MOCK_RESULT_ID);
        when(context.getNextInterval()).thenReturn(MOCK_NEXT_INTERVAL);
    }

    /**
     * Test Scenario: When WebSocket client is not open, the step should return CONNECT with the specified waiting interval.
     * Verification Points:
     * 1. Returned step is StepEnum.CONNECT
     * 2. Waiting seconds match the value from context.getNextInterval()
     * 3. WSClient.send() is never invoked
     */
    @Test
    public void execute_wsClientNotOpen_returnsConnectStep() {
        // Arrange: Mock WebSocket client as closed
        when(wsClient.isOpen()).thenReturn(false);

        // Act: Execute the step and get result (join() avoids checked exceptions from get())
        CompletableFuture<StepResponse> future = freeResultStep.execute(context, flow);
        StepResponse response = future.join();

        // Assert: Verify step and waiting interval
        Assert.assertEquals("Step should be CONNECT when WS client is closed", StepEnum.CONNECT, response.getStep());
        Assert.assertEquals("Waiting interval should match mock value", MOCK_NEXT_INTERVAL, response.getWaitSeconds());
        verify(wsClient, never()).send(anyString()); // Verify send() is not called
    }

    /**
     * Test Scenario: When WebSocket client is open, the step should send FreeResult request and return QUERY step.
     * Verification Points:
     * 1. Returned step is StepEnum.QUERY with correct waiting interval
     * 2. WSClient.send() is invoked exactly once with valid request string
     */
    @Test
    public void execute_wsClientOpen_sendsFreeResultRequest_returnsQueryStep() {
        // Arrange: Mock WebSocket client as open
        when(wsClient.isOpen()).thenReturn(true);

        // Capture the string parameter passed to WSClient.send()
        ArgumentCaptor<String> reqStringCaptor = ArgumentCaptor.forClass(String.class);

        // Act: Execute the step and get result
        CompletableFuture<StepResponse> future = freeResultStep.execute(context, flow);
        StepResponse response = future.join();

        // Assert 1: Verify returned step and waiting interval
        Assert.assertEquals("Step should be QUERY when WS client is open", StepEnum.QUERY, response.getStep());
        Assert.assertEquals("Waiting interval should match mock value", MOCK_NEXT_INTERVAL, response.getWaitSeconds());

        // Assert 2: Verify send() is invoked once and capture the request string
        verify(wsClient, times(1)).send(reqStringCaptor.capture());
        String actualReqString = reqStringCaptor.getValue();
        Assert.assertNotNull("Sent request string should not be null", actualReqString);

        // Assert 3: Validate core fields in the request string
        String expectedAction = Action.FREE_RESULT.getAction();
        Assert.assertTrue("Request should contain FREE_RESULT action", actualReqString.contains(expectedAction));
        Assert.assertTrue("Request should contain mock ResultId", actualReqString.contains(String.valueOf(MOCK_RESULT_ID)));
    }

    /**
     * Global setup before all test methods:
     * Set Netty's ResourceLeakDetector to PARANOID level for strict memory leak detection.
     */
    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    /**
     * Global cleanup after all test methods:
     * 1. Trigger garbage collection to release unused resources
     * 2. Verify BgHealthCheck instance count is zero (prevent memory leaks)
     */
    @AfterClass
    public static void tearDown() {
        System.gc();
        Assert.assertEquals("BgHealthCheck instance count should be zero", 0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
    }
}
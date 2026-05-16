package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.EndpointInfo;
import com.taosdata.jdbc.common.ConnectionParam;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for RebalanceManager, covering cluster initialization, connection count statistics,
 * rebalancing judgment, and endpoint state management.
 */
public class RebalanceManagerTest {

    private final Endpoint endpoint1 = new Endpoint("endpoint1", 6041, false);
    private final Endpoint endpoint2 = new Endpoint("endpoint2", 6042, false);
    private final Endpoint endpoint3 = new Endpoint("endpoint3", 6043, true);
    @Mock
    private ConnectionParam connectionParam;
    private RebalanceManager rebalanceManager;
    private List<Endpoint> testEndpoints;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        rebalanceManager = RebalanceManager.getInstance();

        // Clear instance maps to avoid cross-test contamination
        clearPrivateMap(rebalanceManager, RebalanceManager.class, "clusterRebalanceMap");
        clearPrivateMap(rebalanceManager, RebalanceManager.class, "endpointInfoMap");
        clearPrivateMap(rebalanceManager, RebalanceManager.class, "endpointClusterMap");

        // Reset global rebalancing state
        resetPrivateAtomicBoolean(rebalanceManager, RebalanceManager.class, "globalRebalancing", false);

        // Initialize test endpoints and mock params
        testEndpoints = new ArrayList<>();
        testEndpoints.add(endpoint1);
        testEndpoints.add(endpoint2);
        testEndpoints.add(endpoint3);

        when(connectionParam.getEndpoints()).thenReturn(testEndpoints);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(10);
        when(connectionParam.getRebalanceThreshold()).thenReturn(20);
    }

    /**
     * Get private field via reflection
     */
    private static Field getPrivateField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Private field '" + fieldName + "' not found in " + clazz.getName(), e);
        }
    }

    /**
     * Clear private ConcurrentHashMap (instance field) via reflection
     */
    private static void clearPrivateMap(Object instance, Class<?> clazz, String fieldName) {
        try {
            Field field = getPrivateField(clazz, fieldName);
            ConcurrentHashMap<?, ?> map = (ConcurrentHashMap<?, ?>) field.get(instance);
            map.clear();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to clear private map '" + fieldName + "'", e);
        }
    }

    /**
     * Reset private AtomicBoolean (instance field) via reflection
     */
    private static void resetPrivateAtomicBoolean(Object instance, Class<?> clazz, String fieldName, boolean value) {
        try {
            Field field = getPrivateField(clazz, fieldName);
            AtomicBoolean atomicBoolean = (AtomicBoolean) field.get(instance);
            atomicBoolean.set(value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to reset private AtomicBoolean '" + fieldName + "'", e);
        }
    }

    /**
     * Invoke private instance method via reflection
     */
    @SuppressWarnings("unchecked")
    private static <T> T invokePrivateMethod(Object instance, Class<?> clazz, String methodName, Class<?>[] paramTypes, Object... params) {
        try {
            Method method = clazz.getDeclaredMethod(methodName, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(instance, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Private method '" + methodName + "' not found in " + clazz.getName(), e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke private method '" + methodName + "'", e);
        }
    }

    @Test
    public void newCluster_ShouldInitializeClusterAndEndpoints() {
        rebalanceManager.newCluster(testEndpoints);

        assertNotNull(rebalanceManager.getEndpointInfo(endpoint1));
        assertNotNull(rebalanceManager.getEndpointInfo(endpoint2));
        assertNotNull(rebalanceManager.getEndpointInfo(endpoint3));

        EndpointInfo info1 = rebalanceManager.getEndpointInfo(endpoint1);
        assertEquals(0, info1.getConnectCount());
        assertTrue(info1.isOnline());
        assertFalse(rebalanceManager.isRebalancing(endpoint1));
    }

    @Test
    public void newCluster_DuplicateCluster_ShouldNotReinitialize() {
        rebalanceManager.newCluster(testEndpoints);
        int initialCount = rebalanceManager.getEndpointInfo(endpoint1).getConnectCount();

        rebalanceManager.newCluster(testEndpoints);

        assertEquals(initialCount, rebalanceManager.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void collectEndpointCountStats_NormalDistribution_ShouldReturnCorrectStats() {
        rebalanceManager.newCluster(testEndpoints);
        rebalanceManager.incrementConnectionCount(endpoint1);
        rebalanceManager.incrementConnectionCount(endpoint2);
        rebalanceManager.incrementConnectionCount(endpoint2);
        rebalanceManager.incrementConnectionCount(endpoint3);
        rebalanceManager.incrementConnectionCount(endpoint3);
        rebalanceManager.incrementConnectionCount(endpoint3);

        int minIndex = rebalanceManager.getMinConnectionEndpointIndex(connectionParam);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(5);
        boolean needRebalance = invokePrivateMethod(
                rebalanceManager,
                RebalanceManager.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        assertEquals(0, minIndex);
        assertTrue(needRebalance);
    }

    @Test
    public void collectEndpointCountStats_EmptyEndpoints_ShouldReturnDefaultStats() {
        List<Endpoint> emptyEndpoints = new ArrayList<>();
        when(connectionParam.getEndpoints()).thenReturn(emptyEndpoints);

        int minIndex = rebalanceManager.getMinConnectionEndpointIndex(connectionParam);
        boolean needRebalance = invokePrivateMethod(
                rebalanceManager,
                RebalanceManager.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        assertEquals(-1, minIndex);
        assertFalse(needRebalance);
    }

    @Test
    public void collectEndpointCountStats_SameConnectionCount_ShouldReturnFirstMinIndex() {
        rebalanceManager.newCluster(testEndpoints);
        rebalanceManager.incrementConnectionCount(endpoint1);
        rebalanceManager.incrementConnectionCount(endpoint2);
        rebalanceManager.incrementConnectionCount(endpoint3);

        int minIndex = rebalanceManager.getMinConnectionEndpointIndex(connectionParam);
        assertEquals(0, minIndex);
    }

    @Test
    public void needRebalancingInner_ShouldRebalance_WhenThresholdMet() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 3);
        setEndpointConnectCount(endpoint2, 4);
        setEndpointConnectCount(endpoint3, 5);

        boolean result = invokePrivateMethod(
                rebalanceManager,
                RebalanceManager.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        assertTrue(result);
    }

    @Test
    public void needRebalancingInner_ShouldNotRebalance_WhenTotalCountInsufficient() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 2);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);

        boolean result = invokePrivateMethod(
                rebalanceManager,
                RebalanceManager.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        assertFalse(result);
    }

    @Test
    public void needRebalancingInner_ShouldNotRebalance_WhenThresholdNotMet() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 3);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);

        boolean result = invokePrivateMethod(
                rebalanceManager,
                RebalanceManager.class,
                "needRebalancingInner",
                new Class[]{ConnectionParam.class},
                connectionParam
        );

        assertFalse(result);
    }

    @Test
    public void getMinConnectionEndpointIndex_ShouldReturnCorrectIndex() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 5);
        setEndpointConnectCount(endpoint2, 2);
        setEndpointConnectCount(endpoint3, 4);

        int minIndex = rebalanceManager.getMinConnectionEndpointIndex(connectionParam);
        assertEquals(1, minIndex);
    }

    @Test
    public void handleRebalancing_RebalanceNeeded_ShouldSetRebalancingState() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 10);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 5);
        rebalanceManager.endpointUp(connectionParam, endpoint2);

        boolean result = rebalanceManager.handleRebalancing(connectionParam, endpoint1);

        assertTrue(result);
        assertTrue(rebalanceManager.isRebalancing(endpoint1));
        assertTrue(rebalanceManager.isRebalancing());
    }

    @Test
    public void handleRebalancing_RebalanceDone_ShouldResetRebalancingState() {
        rebalanceManager.newCluster(testEndpoints);
        setEndpointConnectCount(endpoint1, 0);
        setEndpointConnectCount(endpoint2, 3);
        setEndpointConnectCount(endpoint3, 3);
        when(connectionParam.getRebalanceConBaseCount()).thenReturn(3);

        rebalanceManager.endpointUp(connectionParam, endpoint1);
        assertTrue(rebalanceManager.isRebalancing(endpoint1));

        setEndpointConnectCount(endpoint1, 3);
        boolean result = rebalanceManager.handleRebalancing(connectionParam, endpoint1);

        assertFalse(result);
        assertFalse(rebalanceManager.isRebalancing(endpoint1));
        assertFalse(rebalanceManager.isRebalancing());
    }

    @Test
    public void incrementConnectionCount_ShouldIncreaseCount() {
        rebalanceManager.newCluster(testEndpoints);
        int initialCount = rebalanceManager.getEndpointInfo(endpoint1).getConnectCount();

        rebalanceManager.incrementConnectionCount(endpoint1);

        assertEquals(initialCount + 1, rebalanceManager.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void decrementConnectionCount_ShouldDecreaseCount() {
        rebalanceManager.newCluster(testEndpoints);
        rebalanceManager.incrementConnectionCount(endpoint1);
        int initialCount = rebalanceManager.getEndpointInfo(endpoint1).getConnectCount();

        rebalanceManager.decrementConnectionCount(endpoint1);

        assertEquals(initialCount - 1, rebalanceManager.getEndpointInfo(endpoint1).getConnectCount());
    }

    @Test
    public void connected_ShouldMarkEndpointOnline() {
        rebalanceManager.newCluster(testEndpoints);
        rebalanceManager.disconnected(connectionParam, 0, null);

        rebalanceManager.connected(testEndpoints, 0);

        assertTrue(rebalanceManager.getEndpointInfo(endpoint1).isOnline());
    }

    @Test
    public void disconnected_ShouldMarkEndpointOfflineAndDecrementCount() {
        rebalanceManager.newCluster(testEndpoints);
        rebalanceManager.connected(testEndpoints, 0);
        setEndpointConnectCount(endpoint1, 1);
        when(connectionParam.getCloudToken()).thenReturn("");

        rebalanceManager.disconnected(connectionParam, 0, null);

        assertFalse(rebalanceManager.getEndpointInfo(endpoint1).isOnline());
        assertEquals(0, rebalanceManager.getEndpointInfo(endpoint1).getConnectCount());
    }

    /**
     * Utility method to set endpoint connection count
     */
    private void setEndpointConnectCount(Endpoint endpoint, int count) {
        for (int i = 0; i < count; i++) {
            rebalanceManager.incrementConnectionCount(endpoint);
        }
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
        assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }
}
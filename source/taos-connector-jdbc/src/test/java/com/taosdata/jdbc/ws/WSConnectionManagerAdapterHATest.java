package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WSConnectionManagerAdapterHATest {
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();

    @After
    public void tearDown() {
        rebalanceManager.clearAllForTest();
    }

    @Test
    public void mergeDiscoveredEndpointsAddsNewUniqueEndpointsAndClients() throws Exception {
        ConnectionParam param = newParam("seed:6041");
        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        manager.mergeDiscoveredEndpoints(new String[]{"seed:6041", "node2:6041", "node3:6041"});

        Assert.assertEquals(Arrays.asList(
                new Endpoint("seed", 6041, false),
                new Endpoint("node2", 6041, false),
                new Endpoint("node3", 6041, false)), param.getEndpoints());
        Assert.assertEquals(3, manager.getClientCountForTest());
        Assert.assertNotNull(rebalanceManager.getEndpointInfo(new Endpoint("node2", 6041, false)));
        Assert.assertNotNull(rebalanceManager.getEndpointInfo(new Endpoint("node3", 6041, false)));
    }

    @Test
    public void mergeDiscoveredEndpointsIgnoresNullBlankInvalidAndDuplicateEntries() throws Exception {
        ConnectionParam param = newParam("seed:6041");
        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        manager.mergeDiscoveredEndpoints(new String[]{null, "", " ", "bad-port:abc", "seed:6041", "node2:6041", "node2:6041"});

        List<Endpoint> endpoints = param.getEndpoints();
        Assert.assertEquals(2, endpoints.size());
        Assert.assertEquals(new Endpoint("seed", 6041, false), endpoints.get(0));
        Assert.assertEquals(new Endpoint("node2", 6041, false), endpoints.get(1));
        Assert.assertEquals(2, manager.getClientCountForTest());
    }

    @Test
    public void mergeDiscoveredEndpointsSetsMergedEndpointsOnce() throws Exception {
        ConnectionParam param = spy(newParam("seed:6041"));
        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        manager.mergeDiscoveredEndpoints(new String[]{"node2:6041", "node3:6041"});

        verify(param, times(1)).setEndpoints(anyList());
        Assert.assertEquals(Arrays.asList(
                new Endpoint("seed", 6041, false),
                new Endpoint("node2", 6041, false),
                new Endpoint("node3", 6041, false)), param.getEndpoints());
        Assert.assertEquals(3, manager.getClientCountForTest());
    }

    @Test
    public void mergeDiscoveredEndpointsDoesNothingWhenAdapterHaDisabled() throws Exception {
        ConnectionParam param = new ConnectionParam.Builder(Arrays.asList(new Endpoint("seed", 6041, false)))
                .setUserAndPassword("root", "taosdata")
                .setConnectionTimeout(1000)
                .setRequestTimeout(1000)
                .build();
        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        manager.mergeDiscoveredEndpoints(new String[]{"node2:6041"});

        Assert.assertEquals(1, param.getEndpoints().size());
        Assert.assertEquals(1, manager.getClientCountForTest());
    }

    @Test
    public void mergeDiscoveredEndpointsDoesNothingWhenSlaveClusterConfigured() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, "seed:6041");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ADAPTER_HA, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "slave");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041");
        properties.setProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, "1000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "1000");
        ConnectionParam param = ConnectionParam.getParam(properties);
        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        manager.mergeDiscoveredEndpoints(new String[]{"node2:6041"});

        Assert.assertEquals(2, param.getEndpoints().size());
        Assert.assertFalse(param.getEndpoints().contains(new Endpoint("node2", 6041, false)));
        Assert.assertEquals(2, manager.getClientCountForTest());
    }

    @Test
    public void constructorExpandsAdapterHaEndpointsFromKnownCluster() throws Exception {
        rebalanceManager.expandCluster(Arrays.asList(
                new Endpoint("seed", 6041, false),
                new Endpoint("node2", 6041, false),
                new Endpoint("node3", 6041, false)));
        ConnectionParam param = newParam("seed:6041");

        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        Assert.assertEquals(3, param.getEndpoints().size());
        Assert.assertTrue(param.getEndpoints().contains(new Endpoint("node2", 6041, false)));
        Assert.assertTrue(param.getEndpoints().contains(new Endpoint("node3", 6041, false)));
        Assert.assertEquals(3, manager.getClientCountForTest());
    }

    @Test
    public void constructorDoesNotExpandKnownClusterWhenAdapterHaDisabled() throws Exception {
        rebalanceManager.expandCluster(Arrays.asList(
                new Endpoint("seed", 6041, false),
                new Endpoint("node2", 6041, false)));
        ConnectionParam param = new ConnectionParam.Builder(Arrays.asList(new Endpoint("seed", 6041, false)))
                .setUserAndPassword("root", "taosdata")
                .setConnectionTimeout(1000)
                .setRequestTimeout(1000)
                .build();

        WSConnectionManager manager = new WSConnectionManager(WSFunction.WS, param, new InFlightRequest(64));

        Assert.assertEquals(1, param.getEndpoints().size());
        Assert.assertEquals(1, manager.getClientCountForTest());
    }

    @Test
    public void transportMergeDiscoveredEndpointsDelegatesToConnectionManager() throws Exception {
        ConnectionParam param = newParam("seed:6041");
        Transport transport = new Transport(WSFunction.WS, param, new InFlightRequest(64));

        transport.mergeDiscoveredEndpoints(new String[]{"seed:6041", "node2:6041"});

        Assert.assertEquals(2, transport.getConnectionParam().getEndpoints().size());
        Assert.assertEquals(new Endpoint("node2", 6041, false), transport.getConnectionParam().getEndpoints().get(1));
    }

    private ConnectionParam newParam(String endpoint) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, endpoint);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ADAPTER_HA, "true");
        properties.setProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, "1000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "1000");
        return ConnectionParam.getParam(properties);
    }
}

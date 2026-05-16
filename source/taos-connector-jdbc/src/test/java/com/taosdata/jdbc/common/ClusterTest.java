package com.taosdata.jdbc.common;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class ClusterTest {

    @Test
    public void testConstructorWithVarargs() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster = new Cluster(endpoint1, endpoint2);

        Set<Endpoint> expectedEndpoints = new HashSet<>();
        Collections.addAll(expectedEndpoints, endpoint1, endpoint2);

        assertEquals(expectedEndpoints, cluster.getEndpoints());
    }

    @Test
    public void testConstructorWithSet() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Set<Endpoint> endpoints = new HashSet<>();
        Collections.addAll(endpoints, endpoint1, endpoint2);

        Cluster cluster = new Cluster(endpoints);

        assertEquals(endpoints, cluster.getEndpoints());
    }

    @Test
    public void testGetEndpoints() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);
        Cluster cluster = new Cluster(endpoint);

        Set<Endpoint> endpoints = cluster.getEndpoints();

        assertNotNull(endpoints);
        assertEquals(1, endpoints.size());
        assertTrue(endpoints.contains(endpoint));
    }

    @Test
    public void testEqualsAndHashCode() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster1 = new Cluster(endpoint1, endpoint2);
        Cluster cluster2 = new Cluster(endpoint2, endpoint1);

        assertEquals(cluster1, cluster2);
        assertEquals(cluster1.hashCode(), cluster2.hashCode());
    }

    @Test
    public void testNotEquals() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster1 = new Cluster(endpoint1);
        Cluster cluster2 = new Cluster(endpoint2);

        assertNotEquals(cluster1, cluster2);
    }

    @Test
    public void testToString() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);
        Cluster cluster = new Cluster(endpoint);

        String expected = "Cluster{endpoints=[" + endpoint.toString() + "]}";
        assertEquals(expected, cluster.toString());
    }
}
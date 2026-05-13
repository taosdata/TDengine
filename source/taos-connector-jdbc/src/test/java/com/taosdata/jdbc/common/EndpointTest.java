package com.taosdata.jdbc.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class EndpointTest {

    @Test
    public void testConstructorAndGetters() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals("127.0.0.1", endpoint.getHost());
        assertEquals(8080, endpoint.getPort());
        assertFalse(endpoint.isIpv6());
    }

    @Test
    public void testEquals_SameObject() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint, endpoint);
    }

    @Test
    public void testEquals_DifferentObjectSameValues() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint1, endpoint2);
        assertEquals(endpoint1.hashCode(), endpoint2.hashCode());
    }

    @Test
    public void testEquals_DifferentValues() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, true);

        assertNotEquals(endpoint1, endpoint2);
    }

    @Test
    public void testEquals_NullObject() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertNotNull(endpoint);
    }

    @Test
    public void testEquals_DifferentClass() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertNotEquals("Some String", endpoint);
    }

    @Test
    public void testHashCode() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint1.hashCode(), endpoint2.hashCode());
    }

    @Test
    public void testToString_Ipv4() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals("127.0.0.1:8080", endpoint.toString());
    }

    @Test
    public void testToString_Ipv6() {
        Endpoint endpoint = new Endpoint("::1", 8080, true);

        assertEquals("[::1]:8080", endpoint.toString());
    }
}
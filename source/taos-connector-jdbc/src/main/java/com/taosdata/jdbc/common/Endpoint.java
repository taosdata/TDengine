package com.taosdata.jdbc.common;

import java.util.Objects;

public class Endpoint {
    private final String host;
    private final int port;
    private final boolean isIpv6;
    public Endpoint(String host, int port, boolean isIpv6) {
        this.host = host;
        this.port = port;
        this.isIpv6 = isIpv6;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isIpv6() {
        return isIpv6;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Endpoint endpoint = (Endpoint) o;
        return port == endpoint.port
                && isIpv6 == endpoint.isIpv6
                && Objects.equals(host, endpoint.host);
    }
    @Override
    public int hashCode() {
        return Objects.hash(host, port, isIpv6);
    }

    public String toString() {
        return (isIpv6 ? "[" + host + "]" : host) + ":" + port;
    }
}

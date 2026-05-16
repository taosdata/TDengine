package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class NetConfiguration extends Configuration {
    public static final int DEFAULT_PORT = 8899;
    public static final String DEFAULT_HOST = "127.0.0.1";

    private String host;
    private int port;

    public NetConfiguration() {
        super(ConfigurationType.NET);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

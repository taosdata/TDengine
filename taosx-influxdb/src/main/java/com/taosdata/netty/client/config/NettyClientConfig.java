package com.taosdata.netty.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "netty-client")
@Data
public class NettyClientConfig {

    private String host;
    private int port;
    private boolean soKeepalive;
    private boolean tcpNoDelay;
    private int idleReader;
    private int idleWriter;
    private int idleAll;
    private int unPongRetryTimes;
}

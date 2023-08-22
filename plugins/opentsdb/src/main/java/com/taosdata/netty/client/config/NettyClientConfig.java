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
@ConfigurationProperties(prefix = "taosx", ignoreInvalidFields = true)
@Data
public class NettyClientConfig {

    private String host;
    private int port;
    private boolean soKeepalive = true;
    private boolean tcpNoDelay = true;
    private int idleReader = 10;
    private int idleWriter = 0;
    private int idleAll = 0;
    private int unPongRetryTimes = 3;
}

package com.taosdata.netty.client.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "taosx", ignoreInvalidFields = true)
@Getter
@Setter
public class NettyClientConfig {

    private String host;
    private int port;
    private boolean soKeepalive = true;
    private boolean tcpNoDelay = true;
    private int idleReader = 30;
    private int idleWriter = 0;
    private int idleAll = 0;
    private int unPongRetryTimes = 3;
}

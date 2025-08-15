package com.taosdata.config;

import com.taosdata.config.dto.ThreadConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 性能配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "performance", ignoreInvalidFields = true)
@Getter
@Setter
public class PerformanceConfig {

    private int delay = 10000;
    private int limitConnect = 1;
    private int limitBatch = 500;
    private int limitSpeed = 50000;
    private int retryTimes = 3;
    private long retryInterval = 200;
    private int readWindow = 1;
    private int maxThread = 50;
    private long queueSizeT = 1000;
    private long queueSizeD = 200000;
    @Resource
    private ThreadConfig thread;
}

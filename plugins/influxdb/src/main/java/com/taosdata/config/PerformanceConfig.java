package com.taosdata.config;

import com.taosdata.config.dto.ThreadConfig;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 性能配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "performance", ignoreInvalidFields = true)
@Data
public class PerformanceConfig {

    private int limitConnect = 1;
    private int limitBatch = 500;
    private int limitSpeed = 50000;
    private int retryTimes = 3;
    private long retryInterval = 200;
    private String readWindow = "M";
    private int maxThread = 50;
    private long queueSizeT = 1000;
    private long queueSizeD = 200000;
    private ThreadConfig thread;
}

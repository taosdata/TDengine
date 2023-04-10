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
@ConfigurationProperties(prefix = "performance")
@Data
public class PerformanceConfig {

    private int limitConnect;
    private int limitBatch;
    private int limitSpeed;
    private int retryTimes;
    private long retryInterval;
    private String readWindow;
    private int maxThread;
    private int queueSizeT;
    private int queueSizeD;
    private ThreadConfig thread;
}

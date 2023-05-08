package com.taosdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 源数据库（influxdb）默认配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "influx", ignoreInvalidFields = true)
@Data
public class InfluxdbConfig {

    private String url;
    private String token;
    private String orgId;
    private int maxTotal = 20;
    private int maxIdle = 10;
    private int minIdle = 5;
    private int initialSize = 5;
}

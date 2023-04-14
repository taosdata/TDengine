package com.taosdata.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 源数据库（influxdb）默认配置
 *
 * @author ZYP
 */
@Configuration
@Data
public class InfluxdbConfig {

    @Value("${influx.url}")
    private String url;

    @Value("${influx.token}")
    private String token;

    @Value("${influx.orgId}")
    private String orgId;

    @Value("${influx.maxTotal}")
    private int maxTotal;

    @Value("${influx.maxIdle}")
    private int maxIdle;

    @Value("${influx.minIdle}")
    private int minIdle;

    @Value("${influx.initialSize}")
    private int initialSize;
}

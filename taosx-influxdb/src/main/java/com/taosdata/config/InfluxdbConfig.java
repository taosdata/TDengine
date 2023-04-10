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

    @Value("${influx.authType}")
    private String authType;

    @Value("${influx.username}")
    private String username;

    @Value("${influx.password}")
    private String password;

    @Value("${influx.token}")
    private String token;

    @Value("${influx.orgId}")
    private String orgId;

    @Value("${influx.buckets}")
    private String[] buckets;
}

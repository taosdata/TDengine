package com.taosdata.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 源数据库（influxdb）默认配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "influx", ignoreInvalidFields = true)
@Getter
@Setter
public class InfluxdbConfig {

    private String url;
    private String version;
    private String username;
    private String password;
    private String token;
    private String orgId;
    private boolean addDbrp = false;
    private int maxTotal = 60;
    private int maxIdle = 10;
    private int minIdle = 5;
    private int initialSize = 5;
}

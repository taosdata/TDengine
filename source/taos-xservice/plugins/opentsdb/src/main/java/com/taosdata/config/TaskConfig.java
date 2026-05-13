package com.taosdata.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * 任务配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "task", ignoreInvalidFields = true)
@Getter
@Setter
public class TaskConfig {

    private String mode = "normal";
    private Set<String> metrics;
    private String beginTime;
    private String endTime;
    private Map<String, Long> breakpoint;
    private String logLevel = "info";
    private String timestampFieldName = "timestamp";
    private String valueFieldName = "value";
    private String tableNamePattern;
}

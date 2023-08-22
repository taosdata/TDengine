package com.taosdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * 任务配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "task", ignoreInvalidFields = true)
@Data
public class TaskConfig {

    private String mode = "normal";
    private Set<String> metrics;
    private String beginTime;
    private String endTime;
}

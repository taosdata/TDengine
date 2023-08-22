package com.taosdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 源数据库（OpenTSDB）默认配置
 *
 * @author ZYP
 */
@Component
@ConfigurationProperties(prefix = "opents", ignoreInvalidFields = true)
@Data
public class OpentsdbConfig {

    private String url;

    private String apiMetrics = "suggest";
    private String apiData = "api/query";
}

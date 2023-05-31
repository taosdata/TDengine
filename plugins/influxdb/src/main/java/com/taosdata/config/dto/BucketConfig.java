package com.taosdata.config.dto;

import lombok.Data;

import java.util.List;

/**
 * Influxdb Bucket列表配置
 *
 * @author ZYP
 */
@Data
public class BucketConfig {

    private String bucket;
    private List<MeasurementConfig> measurements;
}

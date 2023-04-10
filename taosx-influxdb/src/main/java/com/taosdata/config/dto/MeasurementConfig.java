package com.taosdata.config.dto;

import lombok.Data;

/**
 * Influxdb Measurement列表配置
 *
 * @author ZYP
 */
@Data
public class MeasurementConfig {

    private String measurement;
    private String[] cols;
}

package com.taosdata.config.dto;

import lombok.Data;

/**
 * 线程配置
 *
 * @author ZYP
 */
@Data
public class ThreadConfig {

    /**
     * MessageThread线程
     */
    private long processMessageInterval = 1;
    private long processMessageEmptyInterval = 10;

    /**
     * PushPrepareThread线程
     */
    private long readMetricDataBatch = 1000;
    private long pushPrepareInterval = 10;

    /**
     * PushThread线程
     */
    private long pushInterval = 0;
    private long pushEmptyInterval = 10;

    /**
     * MetricThread线程
     */
    private long createMetricInterval = 5;
    private long createMetricFullInterval = 200;

    /**
     * MetricDataThread线程
     */
    private long readMetricInterval = 1;
    private long readMetricFullInterval = 200;

    /**
     * ScheduleThread线程
     */
    private long scheduleInterval = 5;

    /**
     * MonitorThread线程
     */
    private long monitorInterval = 200;
}

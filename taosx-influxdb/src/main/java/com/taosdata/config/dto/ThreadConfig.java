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
    private long processMessageInterval;
    private long processMessageEmptyInterval;

    /**
     * PushThread线程
     */
    private long readBucketDataBatch;
    private long pushInterval;
    private long pushEmptyInterval;
    private long pushNotFullInterval;

    /**
     * BucketThread线程
     */
    private long createBucketInterval;
    private long createBucketFullInterval;

    /**
     * BucketDataThread线程
     */
    private long readBucketBatch;
    private long readBucketInterval;
    private long readBucketFullInterval;

    /**
     * ScheduleThread线程
     */
    private long scheduleInterval;

    /**
     * MonitorThread线程
     */
    private long monitorInterval;
}

package com.taosdata.config.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * 线程配置
 *
 * @author ZYP
 */
@Getter
@Setter
public class ThreadConfig {

    /**
     * MessageThread线程
     */
    private long processMessageInterval = 1;
    private long processMessageEmptyInterval = 10;

    /**
     * PushPrepareThread线程
     */
    private long readBucketDataBatch = 1000;
    private long pushPrepareInterval = 10;

    /**
     * PushThread线程
     */
    private long pushInterval = 0;
    private long pushEmptyInterval = 10;

    /**
     * BucketThread线程
     */
    private long createBucketInterval = 1;
    private long createBucketFullInterval = 200;

    /**
     * BucketDataThread线程
     */
    private long readBucketBatch = 1000;
    private long readBucketInterval = 1;
    private long readBucketFullInterval = 200;

    /**
     * ScheduleThread线程
     */
    private long scheduleInterval = 5;

    /**
     * MonitorThread线程
     */
    private long monitorInterval = 200;
}

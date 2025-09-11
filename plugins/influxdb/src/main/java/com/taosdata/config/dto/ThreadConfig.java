package com.taosdata.config.dto;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

/**
 * 线程配置
 *
 * @author ZYP
 */
@Component
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
    private long readBucketDataBatch = 200000;
    private long pushPrepareInterval = 10;

    /**
     * PushThread线程
     */
    private long pushInterval = 0;
    private long pushEmptyInterval = 20;

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
    private long readBucketFullInterval = 10;

    /**
     * ScheduleThread线程
     */
    private long scheduleInterval = 5;

    /**
     * MonitorThread线程
     */
    private long monitorInterval = 200;
}

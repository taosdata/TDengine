package com.taosdata.caches;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计信息缓存
 *
 * @author ZYP
 */
public class StatisticCache {

    /**
     * 读取数据任务总数（估计）
     */
    public static int totalReadTaskEstimated = 0;

    /**
     * 已读取数据量、已推送数据量
     */
    public static AtomicLong totalRead = new AtomicLong();
    public static AtomicLong totalPush = new AtomicLong();

    /**
     * 已创建的读取任务，字符串格式为Bucket,Measurement,StartTime,StopTime
     */
    public static ConcurrentSkipListSet<String> createdTaskSet = new ConcurrentSkipListSet<>();

    /**
     * 已完成的读取任务，字符串格式为Bucket,Measurement,StartTime,StopTime
     */
    public static ConcurrentSkipListSet<String> completedTaskSet = new ConcurrentSkipListSet<>();

    /**
     * 记录已创建的读取数据任务
     *
     * @param key
     */
    public static void noteCreatedTask(String key) {
        createdTaskSet.add(key);
    }

    /**
     * 记录已创建的读取数据任务
     *
     * @param bucket
     * @param measurement
     * @param startTime
     * @param stopTime
     */
    public static void noteCreatedTask(String bucket, String measurement, String startTime, String stopTime) {
        noteCreatedTask(bucket + "," + measurement + "," + startTime + "," + stopTime);
    }

    /**
     * 记录已完成的读取数据任务
     *
     * @param key
     */
    public static void noteCompletedTask(String key) {
        completedTaskSet.add(key);
    }

    /**
     * 记录已完成的读取数据任务
     *
     * @param bucket
     * @param measurement
     * @param startTime
     * @param stopTime
     */
    public static void noteCompletedTask(String bucket, String measurement, String startTime, String stopTime) {
        noteCompletedTask(bucket + "," + measurement + "," + startTime + "," + stopTime);
    }
}

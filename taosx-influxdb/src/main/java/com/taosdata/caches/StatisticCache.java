package com.taosdata.caches;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计信息缓存
 *
 * @author ZYP
 */
public class StatisticCache {

    /**
     * 已读取数据量、已推送数据量
     */
    public static AtomicLong totalRead = new AtomicLong();
    public static AtomicLong totalPush = new AtomicLong();

    /**
     * 已完成的读取任务，字符串格式为Bucket,Measurement,StartTime,StopTime
     */
    public static Set<String> completedTaskSet = new LinkedHashSet<>();

    /**
     * 记录已完成的读取数据任务
     *
     * @param bucket
     * @param measurement
     * @param startTime
     * @param stopTime
     */
    public static void noteCompletedTask(String bucket, String measurement, String startTime, String stopTime) {
        completedTaskSet.add(bucket + "," + measurement + "," + startTime + "," + stopTime);
    }
}

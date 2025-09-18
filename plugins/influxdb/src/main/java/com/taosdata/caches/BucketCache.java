package com.taosdata.caches;

import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.threads.BucketDataThread;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bucket相关缓存
 *
 * @author ZYP
 */
public class BucketCache {

    /**
     * BucketName-Entity
     */
    public static LinkedHashMap<String, InfluxdbBucketEntity> bucketMap = new LinkedHashMap<>();

    /**
     * BucketName,Measurement-Entity
     */
    public static LinkedHashMap<String, InfluxdbMeasurementEntity> measurementMap = new LinkedHashMap<>();

    /**
     * BucketName,Measurement-firstTimestamp
     */
    public static LinkedHashMap<String, Instant> measurementFirstTimestampMap = new LinkedHashMap<>();

    /**
     * BucketName,Measurement-读取数据任务队列
     */
    private static ConcurrentHashMap<String, Queue<BucketDataThread>> bucketDataThreadQueueMap = new ConcurrentHashMap<>();

    /**
     * BucketName,Measurement-读取数据任务阻塞标识
     */
    private static ConcurrentHashMap<String, Boolean> bucketDataThreadBlockedMap = new ConcurrentHashMap<>();

    /**
     * BucketName,Measurement-子表数量
     */
    private static ConcurrentHashMap<String, Integer> measurementSubtableAmountMap = new ConcurrentHashMap<>();

    /**
     * BucketName,Measurement-根据“子表数量”计算得到的limit数量
     */
    private static ConcurrentHashMap<String, Long> measurementQueryLimitMap = new ConcurrentHashMap<>();

    /**
     * 添加Bucket子线程并获取队列大小
     *
     * @param key:             BucketName,Measurement
     * @param bucketDataThread
     * @return
     */
    public static int addBucketDataThread(String key, BucketDataThread bucketDataThread) {
        if (!bucketDataThreadQueueMap.containsKey(key)) {
            bucketDataThreadQueueMap.put(key, new LinkedList<>());
        }
        bucketDataThreadQueueMap.get(key).add(bucketDataThread);
        // 返回当前队列大小
        return bucketDataThreadQueueMap.get(key).size();
    }

    /**
     * 获取Bucket子线程
     *
     * @param key: BucketName,Measurement
     * @return
     */
    public static BucketDataThread getBucketDataThread(String key) {
        if (!bucketDataThreadQueueMap.containsKey(key)) {
            return null;
        }
        return bucketDataThreadQueueMap.get(key).poll();
    }

    /**
     * 获取队列大小
     *
     * @param key: BucketName,Measurement
     * @return
     */
    public static int getBucketDataThreadQueueSize(String key) {
        return bucketDataThreadQueueMap.getOrDefault(key, new LinkedList<>()).size();
    }

    /**
     * 获取所有队列整体大小
     *
     * @return
     */
    public static int getBucketDataThreadQueueTotal() {
        int total = 0;
        for (String key : bucketDataThreadQueueMap.keySet()) {
            total += getBucketDataThreadQueueSize(key);
        }
        return total;
    }

    /**
     * 添加读取数据任务阻塞
     *
     * @param key: BucketName,Measurement
     */
    public static void setBucketDataThreadBlocked(String key) {
        bucketDataThreadBlockedMap.put(key, true);
    }

    /**
     * 释放读取数据任务阻塞
     *
     * @param key: BucketName,Measurement
     */
    public static void releaseBucketDataThreadBlocked(String key) {
        bucketDataThreadBlockedMap.put(key, false);
    }

    /**
     * 获取读取数据任务阻塞标识
     *
     * @param key: BucketName,Measurement
     * @return
     */
    public static boolean isBucketDataThreadBlocked(String key) {
        return bucketDataThreadBlockedMap.getOrDefault(key, false);
    }

    /**
     * 获取所有队列阻塞大小
     *
     * @return
     */
    public static int getBucketDataThreadQueueBlocked() {
        int total = 0;
        for (boolean blocked : bucketDataThreadBlockedMap.values()) {
            if (blocked) {
                total++;
            }
        }
        return total;
    }

    /**
     * 生成“读取数据任务”的 Key
     *
     * @param bucket
     * @param measurement
     * @return
     */
    public static String generateBucketDataThreadKey(String bucket, String measurement) {
        return bucket + "," + measurement;
    }

    /**
     * 根据子表数量与列数量更新指定measurement的读取limit
     *
     * @param key
     * @param subtableAmount
     * @param queueSizeLimit
     * @param defaultLimit
     * @return
     */
    public static void updateQueryLimit(String key, int subtableAmount, long queueSizeLimit, long defaultLimit) {
        if (subtableAmount > measurementSubtableAmountMap.getOrDefault(key, 0)) {
            // 更新子表数量
            measurementSubtableAmountMap.put(key, subtableAmount);
            // 更新查询limit
            long queryLimit = queueSizeLimit / subtableAmount;
            if (queryLimit < 1) {
                measurementQueryLimitMap.put(key, 1L);
            } else if (queryLimit > defaultLimit) {
                measurementQueryLimitMap.put(key, defaultLimit);
            } else {
                measurementQueryLimitMap.put(key, queryLimit);
            }
        }
    }

    /**
     * 获取指定measurement的读取limit
     *
     * @param key
     * @return
     */
    public static long getQueryLimit(String key) {
        return measurementQueryLimitMap.getOrDefault(key, 1000L);
    }
}

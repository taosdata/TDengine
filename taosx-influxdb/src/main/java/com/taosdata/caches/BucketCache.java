package com.taosdata.caches;

import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.threads.BucketDataThread;

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
     * MeasurementName-Entity
     */
    public static LinkedHashMap<String, InfluxdbMeasurementEntity> measurementMap = new LinkedHashMap<>();

    /**
     * BucketName-读取数据任务队列
     */
    private static ConcurrentHashMap<String, Queue<BucketDataThread>> bucketDataThreadQueueMap = new ConcurrentHashMap<>();

    /**
     * 添加Bucket子线程并获取队列大小
     *
     * @param bucket
     * @param bucketDataThread
     * @return
     */
    public static int addBucketDataThread(String bucket, BucketDataThread bucketDataThread) {
        if (!bucketDataThreadQueueMap.containsKey(bucket)) {
            bucketDataThreadQueueMap.put(bucket, new LinkedList<>());
        }
        bucketDataThreadQueueMap.get(bucket).add(bucketDataThread);
        // 返回当前队列大小
        return bucketDataThreadQueueMap.get(bucket).size();
    }

    /**
     * 获取Bucket子线程
     *
     * @param bucket
     * @return
     */
    public static BucketDataThread getBucketDataThread(String bucket) {
        if (!bucketDataThreadQueueMap.containsKey(bucket)) {
            return null;
        }
        return bucketDataThreadQueueMap.get(bucket).poll();
    }

    /**
     * 获取队列大小
     *
     * @param bucket
     * @return
     */
    public static int getBucketDataThreadQueueSize(String bucket) {
        return bucketDataThreadQueueMap.getOrDefault(bucket, new LinkedList<>()).size();
    }

    /**
     * 获取所有队列整体大小
     *
     * @return
     */
    public static int getBucketDataThreadQueueTotal() {
        int total = 0;
        for (String bucket : bucketDataThreadQueueMap.keySet()) {
            total += getBucketDataThreadQueueSize(bucket);
        }
        return total;
    }
}

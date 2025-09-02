package com.taosdata.caches;

import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据缓存
 *
 * @author ZYP
 */
public class BucketDataCache {

    /**
     * Influxdb原始数据
     */
    private static Queue<InfluxdbBucketDataEntity> bucketDataQueue = new ConcurrentLinkedQueue<>();

    /**
     * Influxdb原始数据（按Bucket/Measurement/Table拆分后）
     */
    private static ConcurrentHashMap<String, Queue<InfluxdbBucketDataEntity>> bucketDataQueueMap = new ConcurrentHashMap<>();

    /**
     * Bucket/Measurement/Table-Socket连接
     */
    public static ConcurrentHashMap<String, Channel> socketMap = new ConcurrentHashMap<>();

    /**
     * 读取倍率，经验值
     */
    public static final long readDataRatio1 = 2;
    public static final long readDataRatio2 = 70;

    /**
     * 添加数据并获取队列大小
     *
     * @param influxdbBucketDataEntity
     * @return
     */
    /*public static int addBucketData(InfluxdbBucketDataEntity influxdbBucketDataEntity) {
        // 放入队列中
        bucketDataQueue.add(influxdbBucketDataEntity);
        // 返回当前队列大小
        return bucketDataQueue.size();
    }*/

    /**
     * 批量添加数据并获取队列大小
     *
     * @param influxdbBucketDataEntityList
     * @return
     */
    public static int addBucketData(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        // 放入队列中
        bucketDataQueue.addAll(influxdbBucketDataEntityList);
        // 返回当前队列大小
        return bucketDataQueue.size();
    }

    /**
     * 添加数据并获取队列大小
     *
     * @param key
     * @param influxdbBucketDataEntity
     * @return
     */
    public static int addBucketData(String key, InfluxdbBucketDataEntity influxdbBucketDataEntity) {
        if (!bucketDataQueueMap.containsKey(key)) {
            bucketDataQueueMap.put(key, new ConcurrentLinkedQueue<>());
        }
        bucketDataQueueMap.get(key).add(influxdbBucketDataEntity);
        // 返回当前队列大小
        return bucketDataQueueMap.get(key).size();
    }

    /**
     * 获取指定数量的数据
     *
     * @param batch
     * @return
     */
    public static List<InfluxdbBucketDataEntity> getBucketData(long batch) {
        List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
        // 遍历获取
        for (long i = 0; influxdbBucketDataEntityList.size() < batch; i++) {
            InfluxdbBucketDataEntity influxdbBucketDataEntity = bucketDataQueue.poll();
            // 非空则放入列表，空则中断
            if (influxdbBucketDataEntity != null) {
                influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
            } else {
                if (i >= batch * readDataRatio1) {
                    break;
                }
            }
        }
        return influxdbBucketDataEntityList;
    }

    /**
     * 获取指定数量的数据
     *
     * @param key
     * @param batch
     * @return
     */
    public static List<InfluxdbBucketDataEntity> getBucketData(String key, long batch) {
        List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
        // 遍历获取
        for (long i = 0; influxdbBucketDataEntityList.size() < batch; i++) {
            InfluxdbBucketDataEntity influxdbBucketDataEntity = bucketDataQueueMap.getOrDefault(key, new ConcurrentLinkedQueue<>()).poll();
            // 非空则放入列表，空则中断
            if (influxdbBucketDataEntity != null) {
                influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
            } else {
                if (i >= batch * readDataRatio2) {
                    break;
                }
            }
        }
        return influxdbBucketDataEntityList;
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getBucketDataQueueSize() {
        return bucketDataQueue.size();
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getBucketDataQueueSize(String key) {
        return bucketDataQueueMap.getOrDefault(key, new ConcurrentLinkedQueue<>()).size();
    }

    /**
     * 获取队列全部大小
     *
     * @return
     */
    public static int getBucketDataQueueTotalSize() {
        // 主队列长度
        AtomicInteger total = new AtomicInteger(bucketDataQueue.size());
        // 拆分后队列长度
        bucketDataQueueMap.values().stream().forEach(queue -> total.addAndGet(queue.size()));
        // 返回总数
        return total.get();
    }

    /**
     * 根据bucket,measurement获取队列全部大小
     *
     * @param key
     * @return
     */
    public static int getBucketDataQueueTotalSize(String key) {
        AtomicInteger total = new AtomicInteger();
        bucketDataQueueMap.forEach((k, v) -> {
            if (k.startsWith(key)) {
                total.addAndGet(v.size());
            }
        });
        return total.get();
    }

    /**
     * 获取BucketData的key集合
     *
     * @return
     */
    public static Set<String> getBucketDataKeySet() {
        return bucketDataQueueMap.keySet();
    }

    /**
     * 获取BucketData的队列为空的key集合
     *
     * @return
     */
    /*public static Set<String> getBucketDataEmptyKeySet() {
        Set<String> keySet = new HashSet<>();
        bucketDataQueueMap.forEach((k, v) -> {
            if (v != null && v.size() == 0) {
                keySet.add(k);
            }
        });
        return keySet;
    }*/

    /**
     * 删除指定队列记录
     *
     * @param key
     */
    public static void removeBucketDataKey(String key) {
        bucketDataQueueMap.remove(key);
    }
}

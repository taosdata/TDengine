package com.taosdata.caches;

import com.taosdata.model.entity.OpentsdbDataEntity;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据缓存
 *
 * @author ZYP
 */
public class MetricDataCache {

    /**
     * OpenTSDB原始数据
     */
    private static Queue<OpentsdbDataEntity> metricDataQueue = new ConcurrentLinkedQueue<>();

    /**
     * OpenTSDB原始数据（按Metric/Table拆分后）
     */
    private static ConcurrentHashMap<String, Queue<OpentsdbDataEntity>> metricDataQueueMap = new ConcurrentHashMap<>();

    /**
     * Metric/Table-Socket连接
     */
    public static ConcurrentHashMap<String, Channel> socketMap = new ConcurrentHashMap<>();

    /**
     * 添加数据并获取队列大小
     *
     * @param opentsdbDataEntity
     * @return
     */
    public static int addMetricData(OpentsdbDataEntity opentsdbDataEntity) {
        // 放入队列中
        metricDataQueue.add(opentsdbDataEntity);
        // 返回当前队列大小
        return metricDataQueue.size();
    }

    /**
     * 批量添加数据并获取队列大小
     *
     * @param opentsdbDataEntityList
     * @return
     */
    public static int addMetricData(List<OpentsdbDataEntity> opentsdbDataEntityList) {
        // 放入队列中
        metricDataQueue.addAll(opentsdbDataEntityList);
        // 返回当前队列大小
        return metricDataQueue.size();
    }

    /**
     * 添加数据并获取队列大小
     *
     * @param key
     * @param opentsdbDataEntity
     * @return
     */
    public static int addMetricData(String key, OpentsdbDataEntity opentsdbDataEntity) {
        if (!metricDataQueueMap.containsKey(key)) {
            metricDataQueueMap.put(key, new ConcurrentLinkedQueue<>());
        }
        metricDataQueueMap.get(key).add(opentsdbDataEntity);
        // 返回当前队列大小
        return metricDataQueueMap.get(key).size();
    }

    /**
     * 获取指定数量的数据
     *
     * @param batch
     * @return
     */
    public static List<OpentsdbDataEntity> getMetricData(long batch) {
        List<OpentsdbDataEntity> opentsdbDataEntityList = new ArrayList<>();
        // 遍历获取
        for (int i = 0; i < batch; i++) {
            OpentsdbDataEntity opentsdbDataEntity = metricDataQueue.poll();
            // 非空则放入列表，空则中断
            if (opentsdbDataEntity != null) {
                opentsdbDataEntityList.add(opentsdbDataEntity);
            } else {
                break;
            }
        }
        return opentsdbDataEntityList;
    }

    /**
     * 获取指定数量的数据
     *
     * @param key
     * @param batch
     * @return
     */
    public static List<OpentsdbDataEntity> getMetricData(String key, long batch) {
        List<OpentsdbDataEntity> opentsdbDataEntityList = new ArrayList<>();
        // 遍历获取
        for (int i = 0; i < batch; i++) {
            OpentsdbDataEntity opentsdbDataEntity = metricDataQueueMap.getOrDefault(key, new ConcurrentLinkedQueue<>()).poll();
            // 非空则放入列表，空则中断
            if (opentsdbDataEntity != null) {
                opentsdbDataEntityList.add(opentsdbDataEntity);
            } else {
                break;
            }
        }
        return opentsdbDataEntityList;
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getMetricDataQueueSize() {
        return metricDataQueue.size();
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getMetricDataQueueSize(String key) {
        return metricDataQueueMap.getOrDefault(key, new ConcurrentLinkedQueue<>()).size();
    }

    /**
     * 获取队列全部大小
     *
     * @return
     */
    public static int getMetricDataQueueTotalSize() {
        // 主队列长度
        AtomicInteger total = new AtomicInteger(metricDataQueue.size());
        // 拆分后队列长度
        metricDataQueueMap.values().stream().forEach(queue -> total.addAndGet(queue.size()));
        // 返回总数
        return total.get();
    }

    /**
     * 获取MetricData的key集合
     *
     * @return
     */
    public static Set<String> getMetricDataKeySet() {
        return metricDataQueueMap.keySet();
    }

    /**
     * 获取MetricData的队列为空的key集合
     *
     * @return
     */
    public static Set<String> getMetricDataEmptyKeySet() {
        Set<String> keySet = new HashSet<>();
        metricDataQueueMap.forEach((k, v) -> {
            if (v != null && v.size() == 0) {
                keySet.add(k);
            }
        });
        return keySet;
    }

    /**
     * 删除指定队列记录
     *
     * @param key
     */
    public static void removeMetricDataKey(String key) {
        metricDataQueueMap.remove(key);
    }
}

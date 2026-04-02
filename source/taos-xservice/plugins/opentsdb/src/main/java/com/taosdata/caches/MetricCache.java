package com.taosdata.caches;

import com.taosdata.model.entity.OpentsdbMetricEntity;
import com.taosdata.threads.MetricDataThread;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric相关缓存
 *
 * @author ZYP
 */
public class MetricCache {

    /**
     * Metric-Entity
     */
    public static LinkedHashMap<String, OpentsdbMetricEntity> metricMap = new LinkedHashMap<>();

    /**
     * Metric-读取数据任务队列
     */
    private static ConcurrentHashMap<String, Queue<MetricDataThread>> metricDataThreadQueueMap = new ConcurrentHashMap<>();

    /**
     * Metric-读取数据任务阻塞标识
     */
    private static ConcurrentHashMap<String, Boolean> metricDataThreadBlockedMap = new ConcurrentHashMap<>();

    /**
     * 添加Metric子线程并获取队列大小
     *
     * @param metric
     * @param metricDataThread
     * @return
     */
    public static int addMetricDataThread(String metric, MetricDataThread metricDataThread) {
        if (!metricDataThreadQueueMap.containsKey(metric)) {
            metricDataThreadQueueMap.put(metric, new LinkedList<>());
        }
        metricDataThreadQueueMap.get(metric).add(metricDataThread);
        // 返回当前队列大小
        return metricDataThreadQueueMap.get(metric).size();
    }

    /**
     * 获取Metric子线程
     *
     * @param metric
     * @return
     */
    public static MetricDataThread getMetricDataThread(String metric) {
        if (!metricDataThreadQueueMap.containsKey(metric)) {
            return null;
        }
        return metricDataThreadQueueMap.get(metric).poll();
    }

    /**
     * 获取队列大小
     *
     * @param metric
     * @return
     */
    public static int getMetricDataThreadQueueSize(String metric) {
        return metricDataThreadQueueMap.getOrDefault(metric, new LinkedList<>()).size();
    }

    /**
     * 获取所有队列整体大小
     *
     * @return
     */
    public static int getMetricDataThreadQueueTotal() {
        int total = 0;
        for (String key : metricDataThreadQueueMap.keySet()) {
            total += getMetricDataThreadQueueSize(key);
        }
        return total;
    }

    /**
     * 添加读取数据任务阻塞
     *
     * @param metric
     */
    public static void setMetricDataThreadBlocked(String metric) {
        metricDataThreadBlockedMap.put(metric, true);
    }

    /**
     * 释放读取数据任务阻塞
     *
     * @param metric
     */
    public static void releaseMetricDataThreadBlocked(String metric) {
        metricDataThreadBlockedMap.put(metric, false);
    }

    /**
     * 获取读取数据任务阻塞标识
     *
     * @param metric
     * @return
     */
    public static boolean isMetricDataThreadBlocked(String metric) {
        return metricDataThreadBlockedMap.getOrDefault(metric, false);
    }

    /**
     * 获取所有队列阻塞大小
     *
     * @return
     */
    public static int getMetricDataThreadQueueBlocked() {
        int total = 0;
        for (boolean blocked : metricDataThreadBlockedMap.values()) {
            if (blocked) {
                total++;
            }
        }
        return total;
    }
}

package com.taosdata.caches;

import com.taosdata.model.entity.OpentsdbMetricEntity;
import com.taosdata.threads.MetricDataThread;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Queue;

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
    private static Queue<MetricDataThread> metricDataThreadQueue = new LinkedList<>();

    /**
     * 添加Metric子线程并获取队列大小
     *
     * @param metricDataThread
     * @return
     */
    public static int addMetricDataThread(MetricDataThread metricDataThread) {
        // 添加到队列
        metricDataThreadQueue.add(metricDataThread);
        // 返回当前队列大小
        return metricDataThreadQueue.size();
    }

    /**
     * 获取Metric子线程
     *
     * @return
     */
    public static MetricDataThread getMetricDataThread() {
        return metricDataThreadQueue.poll();
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getMetricDataThreadQueueSize() {
        return metricDataThreadQueue.size();
    }
}

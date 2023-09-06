package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.MetricDataCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.OpentsdbDataEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.NettyClient;
import com.taosdata.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 推送数据准备线程
 *
 * @author ZYP
 */
public class PushPrepareThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    public PushPrepareThread() {

    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * Socket客户端
     */
    private NettyClient nettyClient = ApplicationContextProvider.getBean(NettyClient.class);

    /**
     * 连接等待次数
     */
    private int connectWaitCount = 0;

    @Override
    public void run() {
        while (LocalConfig.isRunPushPrepareThread) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "PushPrepareThread";
                }
                logger.debug(this.name + "#Thread Start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 读取内存中的数据
                List<OpentsdbDataEntity> opentsdbDataEntityList = MetricDataCache.getMetricData(performanceConfig.getThread().getReadMetricDataBatch());
                /* 1.按Metric/Table拆分队列 */
                filter(opentsdbDataEntityList);
                /* 2.清理空队列及其对应的socket与thread */
                /* 2023.05.12 不主动断开socket连接
                // 获取数据为空的key集合
                Set<String> metricDataEmptyKeySet = MetricDataCache.getMetricDataEmptyKeySet();
                // 遍历关闭连接并清理内存
                metricDataEmptyKeySet.stream().forEach(key -> {
                    // 断开Socket连接
                    if (StringUtils.isNotEmpty(key) && MetricDataCache.socketMap.containsKey(key)) {
                        ChannelFuture channelFuture = MetricDataCache.socketMap.get(key).close();
                        channelFuture.addListener((ChannelFutureListener) future -> MetricDataCache.socketMap.remove(key));
                    }
                    // 从MetricDataCache中删除
                    MetricDataCache.removeMetricDataKey(key);
                });*/
                /* 3.为新队列创建socket与thread */
                // 内存中所有队列
                Set<String> metricDataKeySet = MetricDataCache.getMetricDataKeySet();
                // 遍历，如果不存在线程则新建连接与线程
                metricDataKeySet.stream().forEach(key -> {
                    // 判断是否存在并且状态正常
                    if (!MetricDataCache.socketMap.containsKey(key) || !MetricDataCache.socketMap.get(key).isOpen()) {
                        // 创建连接并启动推送线程
                        nettyClient.run(key);
                        // 等待连接成功
                        while (!MetricDataCache.socketMap.containsKey(key) && this.connectWaitCount++ <= 500) {
                            try {
                                Thread.sleep(10);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        // 重置计数
                        this.connectWaitCount = 0;
                    }
                });
                // 线程结束
                sleep(this.performanceConfig.getThread().getPushPrepareInterval(), start, StatusEnums.NORMAL);
            } catch (InterruptedException e) {
                exception(start, StatusEnums.EXCEPTION, e);
                break;
            } catch (Exception e) {
                exception(start, StatusEnums.EXCEPTION, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    logger.error(this.name + "#Thread sleep exception#" + e.getMessage(), e);
                }
            }
        }
        exit();
    }

    /**
     * 线程睡眠
     *
     * @param interval
     * @param start
     * @param statusEnums
     * @throws InterruptedException
     */
    private void sleep(long interval, long start, StatusEnums statusEnums) throws InterruptedException {
        // 线程结束
        long end = System.currentTimeMillis();
        logger.debug(this.name + "#Thread finished (Take time " + (end - start) + " ms)#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc());
        // 睡眠
        Thread.sleep(interval);
    }

    /**
     * 线程异常
     *
     * @param start
     * @param e
     */
    private void exception(long start, StatusEnums statusEnums, Exception e) {
        // 线程结束
        long end = System.currentTimeMillis();
        logger.error(this.name + "#Thread exception (Take time" + (end - start) + " ms)#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 断开所有连接
        MetricDataCache.socketMap.values().forEach(channel -> channel.close());
        // 获取所有子队列
        Set<String> keySet = MetricDataCache.getMetricDataKeySet();
        // 遍历写回主队列
        keySet.stream().forEach(key -> MetricDataCache.addMetricData(MetricDataCache.getMetricData(key, MetricDataCache.getMetricDataQueueSize(key))));
        // 线程结束
        logger.info(this.name + "#Thread completed and exited#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 按Metric/Table拆分队列
     *
     * @param opentsdbDataEntityList
     * @return
     */
    private void filter(List<OpentsdbDataEntity> opentsdbDataEntityList) {
        if (opentsdbDataEntityList == null || opentsdbDataEntityList.size() == 0) {
            return;
        }
        // 遍历数据
        opentsdbDataEntityList.forEach(opentsdbDataEntity -> {
            // 根据Metric与Tags生成表名
            generateTableName(opentsdbDataEntity);
            // 拆分依据metric,table
            String key = opentsdbDataEntity.getMetric() + "," + opentsdbDataEntity.getTable();
            // 写入内存队列
            MetricDataCache.addMetricData(key, opentsdbDataEntity);
        });
    }

    /**
     * 根据Metric与Tags生成表名
     *
     * @param opentsdbDataEntity
     */
    private void generateTableName(OpentsdbDataEntity opentsdbDataEntity) {
        // Metric
        String metric = opentsdbDataEntity.getMetric();
        // Tags
        Map<String, Object> tags = opentsdbDataEntity.getTags();
        // 判断tags是否存在
        if (tags == null || tags.isEmpty()) {
            // 仅拼接下划线
            opentsdbDataEntity.setTable(metric + "_");
        } else {
            // 拼接Metric
            String tableName = metric;
            // 遍历拼接
            for (Object tag : tags.values()) {
                tableName += "_" + tag;
            }
            opentsdbDataEntity.setTable(tableName);
        }
    }
}

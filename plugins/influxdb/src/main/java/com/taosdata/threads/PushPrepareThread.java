package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
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
                List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = BucketDataCache.getBucketData(performanceConfig.getThread().getReadBucketDataBatch());
                /* 1.按Bucket/Measurement/Table拆分队列 */
                filter(influxdbBucketDataEntityList);
                /* 2.清理空队列及其对应的socket与thread */
                /* 2023.05.12 不主动断开socket连接
                // 获取数据为空的key集合
                Set<String> bucketDataEmptyKeySet = BucketDataCache.getBucketDataEmptyKeySet();
                // 遍历关闭连接并清理内存
                bucketDataEmptyKeySet.stream().forEach(key -> {
                    // 断开Socket连接
                    if (StringUtils.isNotEmpty(key) && BucketDataCache.socketMap.containsKey(key)) {
                        ChannelFuture channelFuture = BucketDataCache.socketMap.get(key).close();
                        channelFuture.addListener((ChannelFutureListener) future -> BucketDataCache.socketMap.remove(key));
                    }
                    // 从BucketDataCache中删除
                    BucketDataCache.removeBucketDataKey(key);
                });*/
                /* 3.为新队列创建socket与thread */
                // 内存中所有队列
                Set<String> bucketDataKeySet = BucketDataCache.getBucketDataKeySet();
                // 遍历，如果不存在线程则新建连接与线程
                bucketDataKeySet.stream().forEach(key -> {
                    // 判断是否存在并且状态正常
                    if (!BucketDataCache.socketMap.containsKey(key) || !BucketDataCache.socketMap.get(key).isOpen()) {
                        // 创建连接并启动推送线程
                        nettyClient.run(key);
                        // 等待连接成功
                        while (!BucketDataCache.socketMap.containsKey(key) && this.connectWaitCount++ <= 500) {
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
        BucketDataCache.socketMap.values().forEach(channel -> channel.close());
        // 获取所有子队列
        Set<String> keySet = BucketDataCache.getBucketDataKeySet();
        // 遍历写回主队列
        keySet.stream().forEach(key -> BucketDataCache.addBucketData(BucketDataCache.getBucketData(key, BucketDataCache.getBucketDataQueueSize(key))));
        // 线程结束
        logger.info(this.name + "#Thread completed and exited#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 按Bucket/Measurement/Table拆分队列
     *
     * @param influxdbBucketDataEntityList
     * @return
     */
    private void filter(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        if (influxdbBucketDataEntityList == null || influxdbBucketDataEntityList.size() == 0) {
            return;
        }
        // 遍历数据
        influxdbBucketDataEntityList.forEach(influxdbBucketDataEntity -> {
            // Influxdb中自带表名不可靠，根据Measurement与Tags生成表名
            generateTableName(influxdbBucketDataEntity);
            // 拆分依据bucket,measurement,table
            String key = influxdbBucketDataEntity.getInfluxdbMeasurementEntity().getBucket() + "," + influxdbBucketDataEntity.getMeasurement() + "," + influxdbBucketDataEntity.getTable();
            // 写入内存队列
            BucketDataCache.addBucketData(key, influxdbBucketDataEntity);
        });
    }

    /**
     * 根据Measurement与Tags生成表名并替换
     *
     * @param influxdbBucketDataEntity
     */
    private void generateTableName(InfluxdbBucketDataEntity influxdbBucketDataEntity) {
        // Measurement
        String measurement = influxdbBucketDataEntity.getMeasurement();
        // Tags
        Map<String, Object> tags = influxdbBucketDataEntity.getTags();
        // 判断tags是否存在
        if (tags == null || tags.isEmpty()) {
            // 仅拼接下划线
            influxdbBucketDataEntity.setTable(measurement + "_");
        } else {
            // 拼接Measurement
            String tableName = measurement;
            // 遍历拼接
            for (Object tag : tags.values()) {
                tableName += "_" + tag;
            }
            influxdbBucketDataEntity.setTable(tableName);
        }
    }
}

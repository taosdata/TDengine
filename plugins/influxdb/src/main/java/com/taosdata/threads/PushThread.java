package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.arrow.ArrowUtils;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 推送数据线程
 *
 * @author ZYP
 */
public class PushThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    /**
     * 数据源Bucket,Measurement,Table
     */
    private String dataSourceKey;

    /**
     * Socket通道
     */
    private Channel channel;

    public PushThread(String dataSourceKey, Channel channel) {
        this.dataSourceKey = dataSourceKey;
        this.channel = channel;
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * 批次首条
     */
    private boolean first = true;

    /**
     * 首条schema
     */
    private Map<String, String> fieldMap = new HashMap<>();

    /**
     * 已创建的子表集合
     */
    private Set<String> createdSubtableSet = new HashSet<>();

    /**
     * 当前线程/schema的arrow工具类
     */
    private ArrowUtils arrowUtils = null;

    /**
     * 空跑总时间统计，超过 5 分钟则断开连接
     */
    private long emptyTimes = 0;

    @Override
    public void run() {
        while (this.channel.isOpen()) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "PushThread";
                }
                logger.debug(this.name + "#Thread Start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 读取内存中的数据
                long _start = System.currentTimeMillis();
                List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = BucketDataCache.getBucketData(this.dataSourceKey, this.performanceConfig.getLimitBatch());
                // 判断是否读到数据
                if (influxdbBucketDataEntityList == null || influxdbBucketDataEntityList.size() == 0) {
                    // 判断空跑次数
                    if (this.emptyTimes >= 300000) {
                        if (this.arrowUtils != null) {
                            this.channel.writeAndFlush(this.arrowUtils.closeArrow()).sync();
                        }
                        this.channel.close();
                        logger.info(this.name + "#Thread exit with no any data coming#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                        break;
                    }
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getPushEmptyInterval(), start, StatusEnums.NORMAL);
                    this.emptyTimes += System.currentTimeMillis() - _start;
                    continue;
                } else {
                    this.emptyTimes = 0;
                }
                // 速度控制
                FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).cycleCheck(influxdbBucketDataEntityList.size(), this.performanceConfig.getLimitSpeed());
                // 推送数据
                push(influxdbBucketDataEntityList);
                // 线程结束
                sleep(this.performanceConfig.getThread().getPushInterval(), start, StatusEnums.NORMAL);
            } catch (Throwable e) {
                exit();
                this.logger.error(this.name + "#Push Thread meet error#err msg: " + e.getMessage() + " Throwable:" + e);
                try {
                    this.channel.close();
                } catch (Throwable e2) {
                    this.logger.warn(this.name + "#try close channel, could ignore#err msg: " + e2.getMessage() + " Throwable:" + e2);
                }
                break;
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
        this.logger.debug(this.name + "#Thread finished (Take time " + (end - start) + " ms)#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
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
        this.logger.error(this.name + "#Thread exception (Take time" + (end - start) + " ms)#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        this.logger.info(this.name + "#Thread completed and exited#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除连接信息
        BucketDataCache.socketMap.remove(this.dataSourceKey);
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 推送数据到taosx
     *
     * @param influxdbBucketDataEntityList
     */
    private void push(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        // TODO 获取并判断响应
        try {
            // 判断列表不为空
            if (influxdbBucketDataEntityList != null && influxdbBucketDataEntityList.size() > 0) {
                // 获取内存中最新的measurement信息
                InfluxdbMeasurementEntity latestMeasurementEntity = BucketCache.measurementMap.get(BucketCache.generateBucketDataThreadKey(influxdbBucketDataEntityList.get(0).getInfluxdbMeasurementEntity().getBucket(), influxdbBucketDataEntityList.get(0).getMeasurement()));
                // 全局变量本地化
                Map<String, String> latestFieldMap = new HashMap<>();
                latestFieldMap.putAll(latestMeasurementEntity.getFieldMap());
                // 对比内存中数据与最新数据是否有差异
                if (this.fieldMap.isEmpty()) {
                    // 使用最新缓存
                    this.fieldMap.putAll(latestFieldMap);
                } else if (!this.fieldMap.equals(latestFieldMap)) {
                    // 需要更新
                    influxdbBucketDataEntityList.forEach(influxdbBucketDataEntity -> influxdbBucketDataEntity.getInfluxdbMeasurementEntity().getFieldMap().putAll(latestFieldMap));
                    // 数据写回
                    BucketDataCache.addBucketData(influxdbBucketDataEntityList);
                    // 断开连接
                    this.channel.writeAndFlush(this.arrowUtils.closeArrow()).sync();
                    // 中止操作
                    return;
                }
            } else {
                return;
            }
            // 根据Measurement获取arrow初始化信息
            if (this.first || this.arrowUtils == null) {
                this.arrowUtils = new ArrowUtils(influxdbBucketDataEntityList.get(0).getInfluxdbMeasurementEntity());
            }
            // 所有子表信息
            Map<String, Map<String, Object>> subtableMap = new HashMap<>();
            influxdbBucketDataEntityList.forEach(influxdbBucketDataEntity -> {
                if (!createdSubtableSet.contains(influxdbBucketDataEntity.getTable()) && !subtableMap.containsKey(influxdbBucketDataEntity.getTable())) {
                    subtableMap.put(influxdbBucketDataEntity.getTable(), influxdbBucketDataEntity.getTags());
                    createdSubtableSet.add(influxdbBucketDataEntity.getTable());
                }
            });
            // 转化并发送数据
            if (!subtableMap.isEmpty()) {
                this.channel.writeAndFlush(this.arrowUtils.transformSubtable(subtableMap, this.first)).sync();
            }
            this.channel.writeAndFlush(this.arrowUtils.transformDataByTime(influxdbBucketDataEntityList)).sync();
            // 修改当前线程/schema的首条标记
            this.first = false;
            // 记录统计信息
            StatisticCache.totalPush.addAndGet(influxdbBucketDataEntityList.size());
        } catch (Exception e) {
            this.logger.error("Push data failed, write back to queue. exception: ", e);
            // 写回
            BucketDataCache.addBucketData(influxdbBucketDataEntityList);
        }
    }
}

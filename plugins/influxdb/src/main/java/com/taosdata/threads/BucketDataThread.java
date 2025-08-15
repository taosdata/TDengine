package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.service.InfluxdbService;
import com.taosdata.service.impl.InfluxdbServiceImpl;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.exception.ArtificialException;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bucket数据读取线程
 *
 * @author ZYP
 */
public class BucketDataThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    @Getter
    private String name;

    /**
     * influxdb orgId
     */
    private String orgId;

    /**
     * influxdb bucket
     */
    private String bucket;

    /**
     * influxdb measurement
     */
    private String measurement;

    /**
     * 读取开始时间、结束时间
     */
    private String startTime;
    private String stopTime;

    /**
     * 由bucket,measurement,period组成的唯一标识
     */
    @Getter
    private String key;

    public BucketDataThread(String orgId, String bucket, String measurement, String startTime, String stopTime) {
        this.orgId = orgId;
        this.bucket = bucket;
        this.measurement = measurement;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.key = bucket + "," + measurement + "," + startTime + "," + stopTime;
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * influxdb数据库操作
     */
    private InfluxdbService influxdbService = ApplicationContextProvider.getBean(InfluxdbServiceImpl.class);

    /**
     * 数据读取位置
     */
    private long offset = 0L;

    @Override
    public void run() {
        while (true) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "BucketDataThread";
                }
                logger.debug(this.name + "#Thread Start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断内存中数据队列大小
                if (BucketDataCache.getBucketDataQueueTotalSize() >= performanceConfig.getQueueSizeD()) {
                    // 睡眠后继续
                    sleep(performanceConfig.getThread().getReadBucketFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 获取计算后的读取限制
                long queryLimit = BucketCache.getQueryLimit(BucketCache.generateBucketDataThreadKey(this.bucket, this.measurement));
                // 获取所有字段
                Map<String, String> fieldMap = influxdbService.selectAllFields(this.bucket, this.measurement);
                // 数据量
                AtomicLong amount = new AtomicLong();

                if (influxdbService.getInfluxdbVersion().startsWith("1")) {
                    List<List<Pair<String, String>>> tagSet = influxdbService.getTagSet(bucket, measurement);
                    // 按 tag set 去查询
                    for (List<Pair<String, String>> tagkv : tagSet) {
                        StringBuilder sb = new StringBuilder();
                        for (Pair<String, String> p : tagkv) {
                            String tag = p.getLeft();
                            String v = p.getRight();
                            sb.append(" and ");
                            sb.append(String.format("\"%s\"='%s'", tag, v));
                        }
                        String tagCondition = sb.substring(5);
                        try {
                            List<InfluxdbBucketDataEntity> entityList = influxdbService.selectBucketDataV1(this.bucket, this.measurement, tagCondition, this.startTime, this.stopTime, queryLimit, this.offset);
                            addToBucketDataCache(entityList, amount, start);
                        } catch (ArtificialException ae) {
                            logger.error("querying data from InfluxDB v1.x occurred error, {}:{}:{}:{}-{}", this.bucket, this.measurement, tagCondition, this.startTime, this.stopTime, ae);
                        }
                    }
                } else {
                    // 遍历字段，使查询条件更精细化，提高整体响应速度
                    for (String field : fieldMap.keySet()) {
                        try {
                            // 读取数据
                            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = influxdbService.selectBucketData(this.orgId, this.bucket, this.measurement, field, this.startTime, this.stopTime, queryLimit, this.offset);
                            addToBucketDataCache(influxdbBucketDataEntityList, amount, start);
                        } catch (ArtificialException ae) {
                            logger.error("querying data from InfluxDB v2.x occurred error, {}:{}:{}:{}-{}", this.bucket, this.measurement, field, this.startTime, this.stopTime, ae);
                        }
                    }
                }
                if (amount.get() > 0) {
                    // 更新offset
                    this.offset += queryLimit;
                } else {
                    // 记录任务完成信息
                    StatisticCache.noteCompletedTask(this.key);
                    // 终止
                    break;
                }
                // 线程结束
                sleep(performanceConfig.getThread().getReadBucketInterval(), start, StatusEnums.NORMAL);
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
     * 放入数据队列，并判断是否需要降速
     * @param influxdbBucketDataEntityList
     * @param amount
     * @param start
     * @throws InterruptedException
     */
    private void addToBucketDataCache(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList, AtomicLong amount, long start) throws InterruptedException  {
        // 判断数据长度
        if (influxdbBucketDataEntityList != null && !influxdbBucketDataEntityList.isEmpty()) {
            // 写入数据队列
            BucketDataCache.addBucketData(influxdbBucketDataEntityList);
            // 判断内存中数据队列大小
            while (BucketDataCache.getBucketDataQueueTotalSize() >= performanceConfig.getQueueSizeD()) {
                logger.debug("BucketDatacache full sleep, total size:{}, queueSizeD: {}", BucketDataCache.getBucketDataQueueTotalSize(), performanceConfig.getQueueSizeD());
                // 睡眠后继续
                sleep(performanceConfig.getThread().getReadBucketFullInterval(), start, StatusEnums.NORMAL);
            }
            // 更新速度
            FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).cycleCheck(influxdbBucketDataEntityList.size(), -1);
            // 记录统计信息
            StatisticCache.totalRead.addAndGet(influxdbBucketDataEntityList.size());
            // 累加本次数据量
            amount.addAndGet(influxdbBucketDataEntityList.size());
        }
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
        logger.error(this.name + "#Thread exception (Take time " + (end - start) + " ms)#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        logger.info(this.name + "#Thread completed and exited, timeRange=[{}-{}]#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15) + "", startTime, stopTime);
        // 清除线程信息
        StatusCache.forgetThread(this.name);
        // 释放阻塞
        BucketCache.releaseBucketDataThreadBlocked(BucketCache.generateBucketDataThreadKey(this.bucket, this.measurement));
    }
}

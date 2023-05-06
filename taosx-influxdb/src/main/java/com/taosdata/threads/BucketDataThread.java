package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.service.InfluxdbService;
import com.taosdata.service.impl.InfluxdbServiceImpl;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
                logger.debug(this.name + "#线程运行开始#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断内存中数据队列大小
                if (BucketDataCache.getBucketDataQueueTotalSize() >= performanceConfig.getQueueSizeD()) {
                    // 睡眠后继续
                    sleep(performanceConfig.getThread().getReadBucketFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 读取数据
                List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = influxdbService.selectBucketData(this.orgId, this.bucket, this.measurement, this.startTime, this.stopTime, this.performanceConfig.getThread().getReadBucketBatch(), this.offset);
                // 更新速度
                FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).cycleCheck(influxdbBucketDataEntityList.size(), -1);
                // 判断数据长度
                if (influxdbBucketDataEntityList != null && influxdbBucketDataEntityList.size() > 0) {
                    // 写入数据队列
                    BucketDataCache.addBucketData(influxdbBucketDataEntityList);
                    // 记录统计信息
                    StatisticCache.totalRead.addAndGet(influxdbBucketDataEntityList.size());
                    // 更新offset
                    this.offset += influxdbBucketDataEntityList.size();
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
                    logger.error(this.name + "#线程睡眠异常#" + e.getMessage(), e);
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
        logger.debug(this.name + "#线程运行结束（耗时" + (end - start) + "ms）#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
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
        logger.error(this.name + "#线程运行异常（耗时" + (end - start) + "ms）#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        logger.info(this.name + "#线程正常退出#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }
}

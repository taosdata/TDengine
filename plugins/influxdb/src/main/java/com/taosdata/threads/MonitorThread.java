package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.MessageCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.influxdb.InfluxdbPoolAutoConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 监控线程
 *
 * @author ZYP
 */
public class MonitorThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger("sys-monitor");

    /**
     * 线程名
     */
    private String name;

    public MonitorThread() {
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * influxdb连接池
     */
    private InfluxdbPoolAutoConfig influxdbPool = ApplicationContextProvider.getBean(InfluxdbPoolAutoConfig.class);

    /**
     * 上次log输出时间
     */
    private Date lastTime;

    @Override
    public void run() {
        while (true) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "MonitorThread";
                }
                logger.debug(this.name + "#线程运行开始#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                /* 更新内存队列信息 */
                StatusCache.noteQueue("Bucket", -1, BucketCache.bucketMap.size());
                StatusCache.noteQueue("Measurement", -1, BucketCache.measurementMap.size());
                StatusCache.noteQueue("ThreadQueue", performanceConfig.getQueueSizeT(), BucketCache.getBucketDataThreadQueueTotal());
                StatusCache.noteQueue("DataQueue", performanceConfig.getQueueSizeD(), BucketDataCache.getBucketDataQueueTotalSize());
                StatusCache.noteQueue("ReqMessage", -1, MessageCache.getReqMessageQueueSize());
                StatusCache.noteQueue("ResMessage", -1, MessageCache.getResMessageQueueSize());
                /* 更新Influxdb连接信息 */
                // 更新连接数信息
                StatusCache.noteInfluxdb(influxdbPool.getPool().getCreatedCount(), influxdbPool.getPool().getDestroyedCount(), influxdbPool.getPool().getBorrowedCount(), influxdbPool.getPool().getReturnedCount());
                // 连接池关闭或连接数不正常则FAILED，否则NORMAL
                if (influxdbPool.getPool().isClosed() || influxdbPool.getPool().getCreatedCount() <= influxdbPool.getPool().getDestroyedCount()) {
                    StatusCache.noteInfluxdb(StatusEnums.FAILED);
                } else {
                    StatusCache.noteInfluxdb(StatusEnums.NORMAL);
                }
                // TODO 判断线程数量是否正常

                // TODO 判断推送速度是否正常

                // TODO 判断成功率是否正常

                // TODO 根据内存、速度、成功率等状态判断综合状态

                // TODO 输出监控信息

                // 判断上次输出时间，每隔一分钟输出一次完整信息
                if (this.lastTime == null || (System.currentTimeMillis() - this.lastTime.getTime()) > 10000) {
                    logger.info(StatusCache.toPrintString());
                    // 更新lastTime
                    this.lastTime = new Date();
                }
                // 线程结束
                sleep(this.performanceConfig.getThread().getMonitorInterval(), start, StatusEnums.NORMAL);
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

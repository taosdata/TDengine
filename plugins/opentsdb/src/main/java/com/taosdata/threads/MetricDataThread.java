package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.MetricDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.OpentsdbDataEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.service.OpentsdbService;
import com.taosdata.service.impl.OpentsdbServiceImpl;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Metric数据读取线程
 *
 * @author ZYP
 */
public class MetricDataThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    @Getter
    private String name;

    /**
     * opentsdb metric
     */
    private String metric;

    /**
     * 读取开始时间、结束时间
     */
    private String startTime;
    private String stopTime;

    /**
     * 由metric,period组成的唯一标识
     */
    @Getter
    private String key;

    public MetricDataThread(String metric, String startTime, String stopTime) {
        this.metric = metric;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.key = metric + "," + startTime + "," + stopTime;
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * opentsdb数据库操作
     */
    private OpentsdbService opentsdbService = ApplicationContextProvider.getBean(OpentsdbServiceImpl.class);

    @Override
    public void run() {
        while (true) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "MetricDataThread";
                }
                logger.debug(this.name + "#Thread Start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断内存中数据队列大小
                if (MetricDataCache.getMetricDataQueueTotalSize() >= performanceConfig.getQueueSizeD()) {
                    // 睡眠后继续
                    sleep(performanceConfig.getThread().getReadMetricFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 读取数据
                List<OpentsdbDataEntity> opentsdbDataEntityList = opentsdbService.fetchData(null, this.metric, this.startTime, this.stopTime);
                // 更新速度
                FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).cycleCheck(opentsdbDataEntityList.size(), -1);
                // 判断数据长度
                if (opentsdbDataEntityList != null && opentsdbDataEntityList.size() > 0) {
                    // 写入数据队列
                    MetricDataCache.addMetricData(opentsdbDataEntityList);
                    // 记录统计信息
                    for (OpentsdbDataEntity opentsdbDataEntity : opentsdbDataEntityList) {
                        if (opentsdbDataEntity != null && opentsdbDataEntity.getDps() != null) {
                            StatisticCache.totalRead.addAndGet(opentsdbDataEntity.getDps().size());
                        }
                    }
                }
                // 记录任务完成信息
                StatisticCache.noteCompletedTask(this.key);
                // 终止
                break;
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
        logger.error(this.name + "#Thread exception (Take time " + (end - start) + " ms)#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        logger.debug(this.name + "#Thread completed and exited, timeRange=[{}-{}]#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15) + "", startTime, stopTime);
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }
}

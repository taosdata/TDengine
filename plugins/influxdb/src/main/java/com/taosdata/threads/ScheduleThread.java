package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Bucket数据读取任务调度线程
 *
 * @author ZYP
 */
public class ScheduleThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    /**
     * 线程池
     */
    private ThreadPoolExecutor threadPoolExecutor;

    public ScheduleThread(int threadPoolSize) {
        // 将corePoolSize与maxPoolSize设置为相同的线程数，这样可以减少在处理过程中创建线程的开销
        this.threadPoolExecutor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    /**
     * 任务配置
     */
    private TaskConfig taskConfig = ApplicationContextProvider.getBean(TaskConfig.class);

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    @Override
    public void run() {
        while (LocalConfig.isRunScheduleThread) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "ScheduleThread";
                }
                logger.debug(this.name + "#Thread Start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断线程池大小（等待队列超过线程数量）
                if (this.threadPoolExecutor.getQueue().size() >= this.performanceConfig.getMaxThread()) {
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getScheduleInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 判断任务分配方式：1队列 2平均
                if (this.taskConfig.getAssignmentType() == 1) {
                    createByQueue();
                } else {
                    createByAverage();
                }
                // 线程结束
                sleep(this.performanceConfig.getThread().getScheduleInterval(), start, StatusEnums.NORMAL);
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
        // 结束线程池中所有任务
        this.threadPoolExecutor.shutdownNow();
        // 线程结束
        logger.info(this.name + "#Thread completed and exited#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    private void createByQueue() {
        // 为防止遍历过程中的更改引发异常，将全局变量本地化
        LinkedHashMap<String, InfluxdbBucketEntity> bucketMap = new LinkedHashMap<>();
        bucketMap.putAll(BucketCache.bucketMap);
        // 读取内存中Bucket子线程
        bucketMap.forEach((key, value) -> {
            // 队列方式，一直处理一个
            while (BucketCache.getBucketDataThreadQueueSize(key) > 0 && this.threadPoolExecutor.getQueue().size() < this.performanceConfig.getMaxThread()) {
                // 取出一个子线程
                BucketDataThread bucketDataThread = BucketCache.getBucketDataThread(key);
                // 正常则启动
                if (bucketDataThread != null) {
                    if (!LocalConfig.fetchFilterSet.contains(bucketDataThread.getKey())) {
                        this.threadPoolExecutor.execute(bucketDataThread);
                    } else {
                        logger.info(this.name + "#Ignore Read Data Task: {}", bucketDataThread);
                    }
                }
            }
        });
    }

    private void createByAverage() {
        // 为防止遍历过程中的更改引发异常，将全局变量本地化
        LinkedHashMap<String, InfluxdbBucketEntity> bucketMap = new LinkedHashMap<>();
        bucketMap.putAll(BucketCache.bucketMap);
        // 读取内存中Bucket子线程
        bucketMap.forEach((key, value) -> {
            // 平均方式，每个处理一个
            if (this.threadPoolExecutor.getPoolSize() < this.performanceConfig.getMaxThread()) {
                // 取出一个子线程
                BucketDataThread bucketDataThread = BucketCache.getBucketDataThread(key);
                // 正常则启动
                if (bucketDataThread != null) {
                    this.threadPoolExecutor.execute(bucketDataThread);
                }
            }
        });
    }
}

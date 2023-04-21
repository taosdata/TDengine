package com.taosdata;

import com.taosdata.caches.BucketCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.InfluxdbConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.config.dto.BucketConfig;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.NettyClient;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.service.InfluxdbService;
import com.taosdata.threads.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * 预加载
 *
 * @author ZYP
 */
@Component
public class PreLoading implements CommandLineRunner {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskConfig taskConfig;

    @Resource
    private PerformanceConfig performanceConfig;

    @Resource
    private InfluxdbConfig influxdbConfig;

    @Resource
    private NettyClientConfig nettyClientConfig;

    @Resource
    private NettyClient nettyClient;

    @Resource
    private InfluxdbService influxdbService;

    @Override
    public void run(String... args) {
        /** 监控信息及系统初始化 */
        try {
            // 设置启动时间
            StatusCache.setStartTime(new Date());
            // 加载中状态
            StatusCache.setStatus(StatusEnums.LOADING.getCode());
            StatusCache.setDescription(StatusEnums.LOADING.getDesc());
            // 启动线程MonitorThread
            MonitorThread monitor = new MonitorThread();
            Thread monitorThread = new Thread(monitor);
            monitorThread.setName("MonitorThread");
            monitorThread.start();
            ThreadInfo threadInfo = new ThreadInfo();
            threadInfo.setName("MonitorThread");
            threadInfo.setStartTime(new Date());
            threadInfo.setStatus(StatusEnums.LOADING.getCode());
            threadInfo.setDescription(StatusEnums.LOADING.getDesc());
            StatusCache.noteThread(threadInfo);
            // 启动线程MessageThread
            MessageThread message = new MessageThread();
            Thread messageThread = new Thread(message);
            messageThread.setName("MessageThread");
            messageThread.start();
            threadInfo = new ThreadInfo();
            threadInfo.setName("MessageThread");
            threadInfo.setStartTime(new Date());
            threadInfo.setStatus(StatusEnums.LOADING.getCode());
            threadInfo.setDescription(StatusEnums.LOADING.getDesc());
            StatusCache.noteThread(threadInfo);
            // Influxdb信息，创建Influxdb连接并启动BucketThread线程与ScheduleThread线程
            initInfluxdb();
            // 记录Netty连接信息
            StatusCache.noteNetty(this.nettyClientConfig.getHost(), this.nettyClientConfig.getPort());
            // 增加退出信号处理方法
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // 处理退出信号
                processShutdown();
            }));
            // 状态默认正常，线程内部会再次更新
            StatusCache.setStatus(StatusEnums.NORMAL.getCode());
            StatusCache.setDescription(StatusEnums.NORMAL.getDesc());
        } catch (Exception e) {
            // 状态异常
            StatusCache.setStatus(StatusEnums.EXCEPTION.getCode());
            StatusCache.setDescription(StatusEnums.EXCEPTION.getDesc() + ": " + e.getMessage());
            // 启动失败
            System.exit(1);
        }
    }

    /**
     * 初始化influxdb及相关线程
     */
    private void initInfluxdb() {
        try {
            // 获取所有bucket
            List<InfluxdbBucketEntity> influxdbBucketEntityList = influxdbService.selectAllBuckets();
            // 跟据参数中的orgId与buckets进行过滤
            influxdbBucketEntityList.stream().filter(influxdbBucketEntity -> {
                if (StringUtils.isEmpty(influxdbConfig.getOrgId()) || influxdbBucketEntity.getOrgId().equals(influxdbConfig.getOrgId())) {
                    return true;
                }
                return false;
            }).filter(influxdbBucketEntity -> {
                if (taskConfig.getBuckets() != null && taskConfig.getBuckets().size() > 0) {
                    for (BucketConfig bucketConfig : taskConfig.getBuckets()) {
                        if (influxdbBucketEntity.getBucketName().equals(bucketConfig.getBucket())) {
                            return true;
                        }
                    }
                } else {
                    return true;
                }
                return false;
            }).forEach(influxdbBucketEntity -> {
                // 放入缓存中
                BucketCache.bucketMap.put(influxdbBucketEntity.getBucketName(), influxdbBucketEntity);
                try {
                    // 查询bucket中所有measurement信息
                    List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = influxdbService.selectAllMeasurements(influxdbBucketEntity.getBucketName());
                    // 放入缓存中
                    for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                        BucketCache.measurementMap.put(influxdbMeasurementEntity.getBucket() + ":" + influxdbMeasurementEntity.getMeasurement(), influxdbMeasurementEntity);
                    }
                    // 启动BucketThread
                    BucketThread bucket = new BucketThread(influxdbConfig.getOrgId(), influxdbBucketEntity.getBucketName());
                    Thread bucketThread = new Thread(bucket);
                    bucketThread.setName("BucketThread-" + influxdbBucketEntity.getBucketName());
                    bucketThread.start();
                    ThreadInfo threadInfo = new ThreadInfo();
                    threadInfo.setName("BucketThread-" + influxdbBucketEntity.getBucketName());
                    threadInfo.setStartTime(new Date());
                    threadInfo.setStatus(StatusEnums.LOADING.getCode());
                    threadInfo.setDescription(StatusEnums.LOADING.getDesc());
                    StatusCache.noteThread(threadInfo);
                } catch (Exception e) {
                    logger.error("初始化influxdb及相关线程过程中发生异常", e);
                }
            });
            // 启动ScheduleThread
            ScheduleThread schedule = new ScheduleThread(performanceConfig.getMaxThread());
            Thread scheduleThread = new Thread(schedule);
            scheduleThread.setName("ScheduleThread");
            scheduleThread.start();
            ThreadInfo threadInfo = new ThreadInfo();
            threadInfo.setName("ScheduleThread");
            threadInfo.setStartTime(new Date());
            threadInfo.setStatus(StatusEnums.LOADING.getCode());
            threadInfo.setDescription(StatusEnums.LOADING.getDesc());
            StatusCache.noteThread(threadInfo);
            // 启动PushPrepareThread
            PushPrepareThread pushPrepare = new PushPrepareThread();
            Thread pushPrepareThread = new Thread(pushPrepare);
            pushPrepareThread.setName("PushPrepareThread");
            pushPrepareThread.start();
            threadInfo = new ThreadInfo();
            threadInfo.setName("PushPrepareThread");
            threadInfo.setStartTime(new Date());
            threadInfo.setStatus(StatusEnums.LOADING.getCode());
            threadInfo.setDescription(StatusEnums.LOADING.getDesc());
            StatusCache.noteThread(threadInfo);
        } catch (Exception e) {
            logger.error("初始化influxdb及相关线程过程中发生异常", e);
        }
    }

    /**
     * 处理退出信号
     */
    private void processShutdown() {
        // TODO
        logger.info("系统已执行安全退出。");
    }
}

package com.taosdata;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.InfluxdbConfig;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.service.InfluxdbService;
import com.taosdata.threads.*;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.FileUtils;
import com.taosdata.utils.influxdb.InfluxdbPoolAutoConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Component;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;

import javax.annotation.Resource;
import java.util.*;

/**
 * 预加载
 *
 * @author ZYP
 */
@Component
public class PreLoading implements CommandLineRunner {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    // private final String RECORD_FILE_RELATIVEPATH = "statics/temp";
    private final String RECORD_FILE_FETCH = "record_fetch";
    private final String RECORD_FILE_QUEUE = "record_queue";
    private final String RECORD_DELIMITER = "__;__";

    @Resource
    private TaskConfig taskConfig;

    @Resource
    private PerformanceConfig performanceConfig;

    @Resource
    private InfluxdbConfig influxdbConfig;

    @Resource
    private NettyClientConfig nettyClientConfig;

    @Resource
    private InfluxdbPoolAutoConfig influxdbPool;

    @Resource
    private InfluxdbService influxdbService;

    @Resource
    private GitProperties gitProperties;

    @Override
    public void run(String... args) {
        System.err.println("InfluxDB Connector version: 1.0.0");
        System.err.println("InfluxDB Connector commit: " + gitProperties.getCommitId());
        System.err.println("InfluxDB Connector build time: " + gitProperties.getInstant("build.time"));
        /** 监控信息及系统初始化 */
        try {
            // 设置启动时间
            StatusCache.setStartTime(new Date());
            // 加载中状态
            StatusCache.setStatus(StatusEnums.LOADING.getCode());
            StatusCache.setDescription(StatusEnums.LOADING.getDesc());
            // 判断是否存在参数且参数是否为-v
            if (args == null || args.length == 0) {
                logger.info("启动参数错误，启动失败");
                System.exit(1);
            } else if ("-v".equals(args[0].trim().toLowerCase()) || "-version".equals(args[0].trim().toLowerCase())) {
                System.exit(0);
            } else if ("-fetch".equals(args[0].trim().toLowerCase()) && args.length >= 3) {
                // 获取连接参数
                String url = args[1];
                String token = args[2];
                // 查询并输出查询结果
                System.out.println(influxdbService.fetchSchemaInfo(url, token));
                System.exit(0);
            } else {
                // 加载toml配置文件，覆盖默认配置，第一个参数是外部配置文件路径，配置不正确则默认退出
                loadToml(args[0].trim());
            }
            // 创建influxdb连接池
            this.influxdbPool.createInluxdbClientPool();
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
            // 处理工作模式：普通、恢复
            initMode();
            // Influxdb信息，创建Influxdb连接并启动BucketThread、ScheduleThread与PushPrepareThread线程
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
            logger.error("系统启动过程中发生异常，启动失败", e);
            // 状态异常
            StatusCache.setStatus(StatusEnums.EXCEPTION.getCode());
            StatusCache.setDescription(StatusEnums.EXCEPTION.getDesc() + ": " + e.getMessage());
            // 启动失败
            System.exit(1);
        }
    }

    /**
     * 加载toml配置文件
     */
    private void loadToml(String externalConfigFile) {
        try {
            // 读取外部toml文件
            String tomlConfig = FileUtils.readAbsoluteFile(externalConfigFile);
            // 解析配置内容
            TomlParseResult tomlParseResult = Toml.parse(tomlConfig);
            // 逐项替换默认配置
            this.influxdbConfig.setUrl((String) tomlParseResult.get("influx.url"));
            this.influxdbConfig.setToken((String) tomlParseResult.get("influx.token"));
            this.influxdbConfig.setOrgId((String) tomlParseResult.get("influx.orgId"));
            this.nettyClientConfig.setHost((String) tomlParseResult.get("taosx.host"));
            this.nettyClientConfig.setPort(((Long) tomlParseResult.get("taosx.port")).intValue());
            this.taskConfig.setMode((String) tomlParseResult.get("task.mode"));
            this.taskConfig.setBuckets(Arrays.asList((String) tomlParseResult.get("task.bucket")));
            Set<String> measurements = new HashSet<>();
            TomlArray tomlArray = tomlParseResult.getArrayOrEmpty("task.measurements");
            for (int i = 0; i < tomlArray.size(); i++) {
                measurements.add((String) tomlArray.get(i));
            }
            this.taskConfig.setMeasurements(measurements);
            this.taskConfig.setBeginTime((String) tomlParseResult.get("task.beginTime"));
            this.taskConfig.setEndTime((String) tomlParseResult.get("task.endTime"));
        } catch (Exception e) {
            logger.error("加载Toml文件过程中发生异常，启动失败", e);
            // 状态异常
            StatusCache.setStatus(StatusEnums.EXCEPTION.getCode());
            StatusCache.setDescription(StatusEnums.EXCEPTION.getDesc() + ": " + e.getMessage());
            // 启动失败
            System.exit(1);
        }
    }

    /**
     * 处理工作模式：普通、恢复
     */
    private void initMode() {
        // 断点续传，需要本地的“读取记录”与“内存队列持久化”两个文件
        if ("resume".equals(this.taskConfig.getMode())) {
            try {
                // 操作系统临时目录
                String temporaryPath = System.getProperty("java.io.tmpdir");
                // 读取“读取记录”与“内存队列持久化”
                String fetchRecords = FileUtils.readAbsoluteFile(temporaryPath, RECORD_FILE_FETCH);
                String queueRecords = FileUtils.readAbsoluteFile(temporaryPath, RECORD_FILE_QUEUE);
                // 将fetchRecords放入过滤集合
                if (StringUtils.isNotEmpty(fetchRecords)) {
                    LocalConfig.fetchFilterSet.addAll(Arrays.asList(StringUtils.splitByWholeSeparator(fetchRecords, RECORD_DELIMITER)));
                }
                // 将queueRecords写入内存队列
                if (StringUtils.isNotEmpty(queueRecords)) {
                    String[] records = StringUtils.splitByWholeSeparator(queueRecords, RECORD_DELIMITER);
                    // 遍历转化并写入内存队列
                    for (String record : records) {
                        if (StringUtils.isNotEmpty(record)) {
                            BucketDataCache.addBucketData(JSONObject.parseObject(record, InfluxdbBucketDataEntity.class));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("读取断点失败，将按照普通模式执行任务", e);
            }
        }
    }

    /**
     * 初始化influxdb及相关线程
     */
    private void initInfluxdb() {
        try {
            // 记录Influxdb信息
            StatusCache.noteInfluxdb(influxdbConfig.getUrl());
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
                    for (String bucket : taskConfig.getBuckets()) {
                        if (influxdbBucketEntity.getBucketName().equals(bucket)) {
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
            // 没有bucket则报错退出
            if (BucketCache.bucketMap.size() == 0) {
                // bucket错误
                logger.error("The application will exit soon: bucket not found");
                System.exit(104);
            }
            // 估算任务量
            StatisticCache.totalReadTaskEstimated = estimateTaskAmount();
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
     * 预估任务数量
     *
     * @return
     */
    private int estimateTaskAmount() {
        try {
            // Measurement数量
            int measurementAmount = BucketCache.measurementMap.size();
            // 任务开始与结束时间
            String beginTime = taskConfig.getBeginTime();
            String endTime = taskConfig.getEndTime();
            // 验证格式
            if (StringUtils.isEmpty(beginTime)) {
                throw new Exception("parameter beginTime configuration error.");
            } else if (beginTime.matches(DateUtils.PATTERN_YMD)) {
                beginTime += " 00:00:00";
            } else if (!beginTime.matches(DateUtils.PATTERN_YMDHMS)) {
                throw new Exception("parameter beginTime configuration error.");
            }
            if (StringUtils.isEmpty(endTime)) {
                endTime = DateUtils.getTime(DateUtils.DATE_FORMAT_15, TimeZone.getTimeZone("GMT"));
            } else if (endTime.matches(DateUtils.PATTERN_YMD)) {
                endTime += " 23:59:59";
            } else if (!endTime.matches(DateUtils.PATTERN_YMDHMS)) {
                throw new Exception("parameter endTime configuration error.");
            }
            Date begin = DateUtils.stringToDate(beginTime, DateUtils.DATE_FORMAT_15);
            Date end = DateUtils.stringToDate(endTime, DateUtils.DATE_FORMAT_15);
            // 相差毫秒数
            long diff = end.getTime() - begin.getTime();
            // 根据查询窗口类型计算
            String readWindow = performanceConfig.getReadWindow().toLowerCase();
            switch (readWindow) {
                case "d":
                    return (int) (Math.ceil(diff / (24 * 60 * 60 * 1000) * measurementAmount));
                case "h":
                    return (int) (Math.ceil(diff / (60 * 60 * 1000) * measurementAmount));
                case "m":
                default:
                    return (int) (Math.ceil(diff / (60 * 1000) * measurementAmount));
            }
        } catch (Exception e) {
            // 不处理异常，直接返回-1
            return -1;
        }
    }

    /**
     * 处理退出信号
     */
    private void processShutdown() {
        // 停止BucketThread、ScheduleThread线程（在ScheduleThread中停止所有BucketDataThread）
        LocalConfig.isRunBucketThread = false;
        LocalConfig.isRunScheduleThread = false;
        // 停止PushPrepareThread线程（在PushPrepareThread中停止所有PushThread，并且把所有数据写回主队列）
        LocalConfig.isRunPushPrepareThread = false;
        // 等待系统清理完成
        try {
            // 先等待2秒
            Thread.sleep(2000L);
            // 当前内存队列大小
            int fetchRecordsSize = StatisticCache.completedTaskSet.size();
            int queueRecordsSize = BucketDataCache.getBucketDataQueueSize();
            // 再等待2秒
            Thread.sleep(2000L);
            // 判断是否变化
            if (fetchRecordsSize != StatisticCache.completedTaskSet.size() || queueRecordsSize != BucketDataCache.getBucketDataQueueSize()) {
                logger.error("系统安全退出时内存数据发生变化");
            }
        } catch (Exception e) {
            logger.error("系统安全退出时判断数据变化失败", e);
        }
        // 操作系统临时目录
        String temporaryPath = System.getProperty("java.io.tmpdir");
        // 将数据读取记录写入文件
        try {
            String fetchRecords = StringUtils.join(StatisticCache.completedTaskSet.toArray(), RECORD_DELIMITER);
            FileUtils.writeAbsoluteFile(temporaryPath, RECORD_FILE_FETCH, fetchRecords);
        } catch (Exception e) {
            logger.error("系统安全退出时保存读取记录失败", e);
        }
        // 将内存队列写入文件
        try {
            StringBuffer sb = new StringBuffer();
            // 内存队列所有内容
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = BucketDataCache.getBucketData(BucketDataCache.getBucketDataQueueSize());
            // 遍历拼装内容
            for (InfluxdbBucketDataEntity influxdbBucketDataEntity : influxdbBucketDataEntityList) {
                sb.append(influxdbBucketDataEntity.toString() + RECORD_DELIMITER);
            }
            FileUtils.writeAbsoluteFile(temporaryPath, RECORD_FILE_QUEUE, sb.toString());
        } catch (Exception e) {
            logger.error("系统安全退出时保存读取记录失败", e);
        }
        logger.info("系统已执行安全退出。");
    }
}

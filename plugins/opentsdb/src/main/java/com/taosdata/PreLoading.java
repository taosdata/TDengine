package com.taosdata;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.caches.MetricCache;
import com.taosdata.caches.MetricDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.OpentsdbConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.entity.OpentsdbDataEntity;
import com.taosdata.model.entity.OpentsdbMetricEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.service.OpentsdbService;
import com.taosdata.threads.*;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.FileUtils;
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

    private final String RECORD_FILE_FETCH = "record_fetch";
    private final String RECORD_FILE_QUEUE = "record_queue";
    private final String RECORD_DELIMITER = "__;__";

    @Resource
    private TaskConfig taskConfig;

    @Resource
    private PerformanceConfig performanceConfig;

    @Resource
    private OpentsdbConfig opentsdbConfig;

    @Resource
    private NettyClientConfig nettyClientConfig;

    @Resource
    private OpentsdbService opentsdbService;

    @Resource
    private GitProperties gitProperties;

    @Override
    public void run(String... args) {
        System.err.println("OpenTSDB Connector version: 1.0.0");
        System.err.println("OpenTSDB Connector commit: " + gitProperties.getCommitId());
        System.err.println("OpenTSDB Connector build time: " + gitProperties.getInstant("build.time"));
        /** 监控信息及系统初始化 */
        try {
            // 设置启动时间
            StatusCache.setStartTime(new Date());
            // 加载中状态
            StatusCache.setStatus(StatusEnums.LOADING.getCode());
            StatusCache.setDescription(StatusEnums.LOADING.getDesc());
            // 判断是否存在参数且参数是否为-v
            if (args == null || args.length == 0) {
                logger.info("Parameters error, startup failed.");
                System.exit(1);
            } else if ("-v".equals(args[0].trim().toLowerCase()) || "-version".equals(args[0].trim().toLowerCase())) {
                System.exit(0);
            } else if ("-fetch".equals(args[0].trim().toLowerCase()) && args.length >= 2) {
                // 获取连接参数
                String url = args[1];
                // 查询所有metric
                System.out.println(opentsdbService.fetchMetricList(url));
                System.exit(0);
            } else {
                // 加载toml配置文件，覆盖默认配置，第一个参数是外部配置文件路径，配置不正确则默认退出
                loadToml(args[0].trim());
            }
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
            // OpenTSDB信息，启动MetricThread、ScheduleThread与PushPrepareThread线程
            initOpentsdb();
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
            logger.error("An exception occurred during the system startup process and the startup failed.", e);
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
            this.opentsdbConfig.setUrl(tomlParseResult.getString("opents.url", String::new));
            // this.opentsdbConfig.setApiMetrics(tomlParseResult.getString("opents.apiMetrics", String::new));
            // this.opentsdbConfig.setApiData(tomlParseResult.getString("opents.apiData", String::new));
            this.nettyClientConfig.setHost(tomlParseResult.getString("taosx.host", String::new));
            this.nettyClientConfig.setPort((int) tomlParseResult.getLong("taosx.port", () -> 0L));
            this.taskConfig.setMode(tomlParseResult.getString("task.mode", String::new));
            Set<String> metrics = new HashSet<>();
            TomlArray tomlArray = tomlParseResult.getArrayOrEmpty("task.metrics");
            for (int i = 0; i < tomlArray.size(); i++) {
                metrics.add((String) tomlArray.get(i));
            }
            this.taskConfig.setMetrics(metrics);
            this.taskConfig.setBeginTime(tomlParseResult.getString("task.beginTime", String::new));
            this.taskConfig.setEndTime(tomlParseResult.getString("task.endTime", String::new));
            // 判断时间配置，错误则退出
            if (StringUtils.isEmpty(this.taskConfig.getBeginTime()) || !this.taskConfig.getBeginTime().matches(DateUtils.PATTERN_YMDHMS_TZ)) {
                throw new Exception("parameter beginTime configuration error.");
            }
            if (StringUtils.isNotEmpty(this.taskConfig.getEndTime()) && !this.taskConfig.getEndTime().matches(DateUtils.PATTERN_YMDHMS_TZ)) {
                throw new Exception("parameter endTime configuration error.");
            }
            this.performanceConfig.setReadWindow(tomlParseResult.getString("performance.readWindow", String::new));
            // 如果设置了性能参数，则覆盖默认值
            if (tomlParseResult.getLong("performance.tolerance") != null) {
                this.performanceConfig.setTolerance(tomlParseResult.getLong("performance.tolerance").intValue());
            }
            if (tomlParseResult.getLong("performance.limitConnect") != null) {
                this.performanceConfig.setLimitConnect(tomlParseResult.getLong("performance.limitConnect").intValue());
            }
            if (tomlParseResult.getLong("performance.limitSpeed") != null) {
                this.performanceConfig.setLimitSpeed(tomlParseResult.getLong("performance.limitSpeed").intValue());
            }
            if (tomlParseResult.getLong("performance.queueSizeD") != null) {
                this.performanceConfig.setQueueSizeD(tomlParseResult.getLong("performance.queueSizeD").longValue());
            }
        } catch (Exception e) {
            logger.error("An exception occurred during the loading of the Toml file, causing startup failure.", e);
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
                            MetricDataCache.addMetricData(JSONObject.parseObject(record, OpentsdbDataEntity.class));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to read breakpoint, task will be executed in normal mode.", e);
            }
        }
    }

    /**
     * 初始化OpenTSDB及相关线程
     */
    private void initOpentsdb() {
        try {
            // 获取所有metric
            JSONArray metricArray = opentsdbService.fetchMetricList(opentsdbConfig.getUrl());
            // 放入缓存中
            for (Object metric : metricArray) {
                OpentsdbMetricEntity opentsdbMetricEntity = new OpentsdbMetricEntity();
                opentsdbMetricEntity.setMetric(metric.toString());
                opentsdbMetricEntity.setTagSet(new HashSet<>());
                MetricCache.metricMap.put(metric.toString(), opentsdbMetricEntity);
            }
            // 启动MetricThread
            MetricThread metric = new MetricThread();
            Thread metricThread = new Thread(metric);
            metricThread.setName("MetricThread");
            metricThread.start();
            ThreadInfo threadInfo = new ThreadInfo();
            threadInfo.setName("MetricThread");
            threadInfo.setStartTime(new Date());
            threadInfo.setStatus(StatusEnums.LOADING.getCode());
            threadInfo.setDescription(StatusEnums.LOADING.getDesc());
            StatusCache.noteThread(threadInfo);
            // 估算任务量
            StatisticCache.totalReadTaskEstimated = estimateTaskAmount();
            // 启动ScheduleThread
            ScheduleThread schedule = new ScheduleThread(performanceConfig.getMaxThread());
            Thread scheduleThread = new Thread(schedule);
            scheduleThread.setName("ScheduleThread");
            scheduleThread.start();
            threadInfo = new ThreadInfo();
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
            logger.error("An exception occurred during the initialization of OpenTSDB and related threads.", e);
        }
    }

    /**
     * 预估任务数量
     *
     * @return
     */
    private int estimateTaskAmount() {
        try {
            // Metric数量
            int metricAmount = MetricCache.metricMap.size();
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
                    return (int) (Math.ceil(diff / (24 * 60 * 60 * 1000) * metricAmount));
                case "h":
                    return (int) (Math.ceil(diff / (60 * 60 * 1000) * metricAmount));
                case "m":
                default:
                    return (int) (Math.ceil(diff / (60 * 1000) * metricAmount));
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
        // 停止MetricThread、ScheduleThread线程（在ScheduleThread中停止所有MetricDataThread）
        LocalConfig.isRunMetricThread = false;
        LocalConfig.isRunScheduleThread = false;
        // 停止PushPrepareThread线程（在PushPrepareThread中停止所有PushThread，并且把所有数据写回主队列）
        LocalConfig.isRunPushPrepareThread = false;
        // 等待系统清理完成
        try {
            // 先等待2秒
            Thread.sleep(2000L);
            // 当前内存队列大小
            int fetchRecordsSize = StatisticCache.completedTaskSet.size();
            int queueRecordsSize = MetricDataCache.getMetricDataQueueSize();
            // 再等待2秒
            Thread.sleep(2000L);
            // 判断是否变化
            if (fetchRecordsSize != StatisticCache.completedTaskSet.size() || queueRecordsSize != MetricDataCache.getMetricDataQueueSize()) {
                logger.error("Memory data changes during system security exit.");
            }
        } catch (Exception e) {
            logger.error("Failed to determine data changes during system security exit.", e);
        }
        // 操作系统临时目录
        String temporaryPath = System.getProperty("java.io.tmpdir");
        // 将数据读取记录写入文件
        try {
            String fetchRecords = StringUtils.join(StatisticCache.completedTaskSet.toArray(), RECORD_DELIMITER);
            FileUtils.writeAbsoluteFile(temporaryPath, RECORD_FILE_FETCH, fetchRecords);
        } catch (Exception e) {
            logger.error("Failed to save read records during system security exit.", e);
        }
        // 将内存队列写入文件
        try {
            StringBuffer sb = new StringBuffer();
            // 内存队列所有内容
            List<OpentsdbDataEntity> opentsdbDataEntityList = MetricDataCache.getMetricData(MetricDataCache.getMetricDataQueueSize());
            // 遍历拼装内容
            for (OpentsdbDataEntity opentsdbDataEntity : opentsdbDataEntityList) {
                sb.append(opentsdbDataEntity.toString() + RECORD_DELIMITER);
            }
            FileUtils.writeAbsoluteFile(temporaryPath, RECORD_FILE_QUEUE, sb.toString());
        } catch (Exception e) {
            logger.error("Failed to save read records during system security exit.", e);
        }
        logger.info("The system has executed a secure exit.");
    }
}

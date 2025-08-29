package com.taosdata;

import com.alibaba.fastjson.JSONObject;
import com.influxdb.client.JSON;
import com.influxdb.client.domain.HealthCheck;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.InfluxdbConfig;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.service.InfluxdbService;
import com.taosdata.threads.*;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.FileUtils;
import com.taosdata.utils.HttpUtils;
import com.taosdata.utils.influxdb.InfluxdbPoolAutoConfig;
import com.taosdata.utils.influxdbV1.InfluxdbV1PoolAutoConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Component;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

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
    private InfluxdbV1PoolAutoConfig influxdbV1Pool;

    @Resource
    private InfluxdbService influxdbService;

    @Resource
    private GitProperties gitProperties;

    @Override
    public void run(String... args) {
        System.err.println("InfluxDB Connector version: 1.0.0");
        if (gitProperties != null) {
            System.err.println("InfluxDB Connector commit: " + gitProperties.getCommitId());
            System.err.println("InfluxDB Connector build time: " + gitProperties.getInstant("build.time"));
        }
        /** 监控信息及系统初始化 */
        try {
            // 设置启动时间
            StatusCache.setStartTime(new Date());
            // 加载中状态
            StatusCache.setStatus(StatusEnums.LOADING.getCode());
            StatusCache.setDescription(StatusEnums.LOADING.getDesc());
            // 判断是否存在参数且参数是否为-v
            if (args == null || args.length == 0) {
                logger.error("Parameters error, startup failed.");
                System.exit(1);
            } else if ("-v".equals(args[0].trim().toLowerCase()) || "-version".equals(args[0].trim().toLowerCase())) {
                System.exit(0);
            } else if ("-fetch".equals(args[0].trim().toLowerCase())) {
                // 获取并判断版本
                if (args[1].matches("1.*") && args.length >= 5) {
                    // 获取连接参数
                    String url = args[2];
                    String username = args[3];
                    String password = args[4];
                    // 查询并输出查询结果
                    System.out.println(influxdbService.fetchSchemaInfoV1(url, username, password));
                    System.exit(0);
                } else if (args[1].matches("2.*") && args.length >= 4) {
                    // 获取连接参数
                    String url = args[2];
                    String token = args[3];
                    String orgId = args.length >= 5 ? args[4] : "";
                    // 查询并输出查询结果
                    System.out.println(influxdbService.fetchSchemaInfo(url, token, orgId));
                    System.exit(0);
                } else {
                    logger.error("Parameters error, query failed.");
                    System.exit(1);
                }
            } else if ("-check".equals(args[0].trim().toLowerCase())) {
                // 获取并判断版本
                if (args[1].matches("1.*") && args.length >= 5) {
                    // 获取连接参数
                    String url = args[2];
                    String username = args[3];
                    String password = args[4];
                    // 查询结果，仅用于验证连通性
                    influxdbService.fetchSchemaInfoV1(url, username, password);
                } else if (args[1].matches("2.*") && args.length >= 4) {
                    // 获取连接参数
                    String url = args[2];
                    String token = args[3];
                    String orgId = args.length >= 5 ? args[4] : "";
                    // 查询结果，仅用于验证连通性
                    influxdbService.fetchSchemaInfo(url, token, orgId);
                } else {
                    logger.error("Parameters error, query failed.");
                    System.exit(1);
                }
                // 检查连通性（代码到这里，说明参数正确，可以直接取url）
                JSONObject result = getInfluxdbVersion(args[2]);
                if (result.getBooleanValue("valid")) {
                    System.out.println(result.toJSONString());
                    System.exit(0);
                } else {
                    System.exit(3);
                }
            } else {
                // 加载toml配置文件，覆盖默认配置，第一个参数是外部配置文件路径，配置不正确则默认退出
                loadToml(args[0].trim());
            }
            // 打印主要性能参数, 遇到大列场景主要调整这3个参数，比如689列的
            logger.info("query limit: PERFORMANCE_THREAD_READ_BUCKET_BATCH={}", this.performanceConfig.getThread().getReadBucketBatch());
            logger.info("data queue: PERFORMANCE_QUEUE_SIZE_D={}", this.performanceConfig.getQueueSizeD());
            logger.info("send batch: PERFORMANCE_LIMIT_BATCH={}", this.performanceConfig.getLimitBatch());
            logger.info("parallel read: TASK_ASSIGNMENT_TYPE={}", this.taskConfig.getAssignmentType());
            logger.info("first level queue read limit: PERFORMANCE_THREAD_READ_BUCKET_DATA_BATCH={}", this.performanceConfig.getThread().getReadBucketDataBatch());
            logger.info("limit speed: PERFORMANCE_LIMIT_SPEED={}", this.performanceConfig.getLimitSpeed());
            // 创建influxdb连接池
            if (StringUtils.isEmpty(this.influxdbConfig.getVersion()) ||
                    this.influxdbConfig.getVersion().matches("2.*")) {
                this.influxdbPool.createInluxdbClientPool();
            } else {
                this.influxdbV1Pool.createInluxdbV1ClientPool();
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
            // initMode();
            // 检查连通性
            if (!checkInfluxdb()) {
                logger.error("Parameters error, failed to check the connectivity of InfluxDB.");
                System.exit(3);
            }
            // Influxdb信息，创建Influxdb连接并启动BucketThread、ScheduleThread与PushPrepareThread线程
            initInfluxdb();
            // 记录Netty连接信息
            StatusCache.noteNetty(this.nettyClientConfig.getHost(), this.nettyClientConfig.getPort());
            // 增加退出信号处理方法
            /*Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // 处理退出信号
                processShutdown();
            }));*/
            // 状态默认正常，线程内部会再次更新
            StatusCache.setStatus(StatusEnums.NORMAL.getCode());
            StatusCache.setDescription(StatusEnums.NORMAL.getDesc());
        } catch (Exception e) {
            logger.error("An exception occurred during the system startup process and the startup failed.", e);
            // 状态异常
            StatusCache.setStatus(StatusEnums.EXCEPTION.getCode());
            StatusCache.setDescription(StatusEnums.EXCEPTION.getDesc() + ": " + e.getMessage());
            // 启动失败
            System.exit(9);
        }
    }

    /**
     * 加载toml配置文件
     */
    private void loadToml(String externalConfigFile) {
        try {
            // 读取外部toml文件
            String tomlConfig = FileUtils.readAbsoluteFile(externalConfigFile);
            /** 输出配置（处理token与password） START */
            String printConfig = tomlConfig;
            Pattern tokenPattern = Pattern.compile("^token.*\n?$", Pattern.MULTILINE);
            Pattern passwordPattern = Pattern.compile("^password.*\n?$", Pattern.MULTILINE);
            printConfig = tokenPattern.matcher(printConfig).replaceAll("token = *");
            printConfig = passwordPattern.matcher(printConfig).replaceAll("password = *");
            System.err.println(printConfig);
            /** 输出配置（处理token与password） END */
            // 解析配置内容
            TomlParseResult tomlParseResult = Toml.parse(tomlConfig);
            // 逐项替换默认配置
            this.influxdbConfig.setUrl(tomlParseResult.getString("influx.url", String::new));
            this.influxdbConfig.setVersion(tomlParseResult.getString("influx.version", String::new));
            this.influxdbConfig.setUsername(tomlParseResult.getString("influx.username", String::new));
            this.influxdbConfig.setPassword(tomlParseResult.getString("influx.password", String::new));
            this.influxdbConfig.setToken(tomlParseResult.getString("influx.token", String::new));
            this.influxdbConfig.setOrgId(tomlParseResult.getString("influx.orgId", String::new));
            this.influxdbConfig.setAddDbrp(tomlParseResult.getBoolean("influx.addDbrp", () -> false));
            this.nettyClientConfig.setHost(tomlParseResult.getString("taosx.host", String::new));
            this.nettyClientConfig.setPort((int) tomlParseResult.getLong("taosx.port", () -> 0L));
            this.taskConfig.setMode(tomlParseResult.getString("task.mode", String::new));
            this.taskConfig.setBuckets(Arrays.asList(tomlParseResult.getString("task.bucket", String::new)));
            if (tomlParseResult.getLong("task.assignmentType") != null) {
                this.taskConfig.setAssignmentType(tomlParseResult.getLong("task.assignmentType").intValue());
            }
            Set<String> measurements = new HashSet<>();
            TomlArray tomlArray = tomlParseResult.getArrayOrEmpty("task.measurements");
            for (int i = 0; i < tomlArray.size(); i++) {
                measurements.add((String) tomlArray.get(i));
            }
            this.taskConfig.setMeasurements(measurements);
            this.taskConfig.setBeginTime(tomlParseResult.getString("task.beginTime", String::new));
            this.taskConfig.setEndTime(tomlParseResult.getString("task.endTime", String::new));
            // 判断时间配置，错误则退出
            if (StringUtils.isEmpty(this.taskConfig.getBeginTime()) ||
                    !this.taskConfig.getBeginTime().matches(DateUtils.PATTERN_YMDHMS_TZ)) {
                throw new Exception("parameter beginTime configuration error.");
            }
            if (StringUtils.isNotEmpty(this.taskConfig.getEndTime()) &&
                    !this.taskConfig.getEndTime().matches(DateUtils.PATTERN_YMDHMS_TZ)) {
                throw new Exception("parameter endTime configuration error.");
            }
            String breakpoints = tomlParseResult.getString("task.breakpoints", String::new);
            // 存在断点信息则解析
            if (StringUtils.isNotEmpty(breakpoints)) {
                this.taskConfig.setBreakpoint(parseBreakpoint(breakpoints));
            }
            // 日志级别error/warn/info/debug/trace，配置错误将会设置为error级别
            if (StringUtils.isNotEmpty(tomlParseResult.getString("task.logLevel", String::new))) {
                this.taskConfig.setLogLevel(tomlParseResult.getString("task.logLevel", String::new));
            }
            LoggerContext loggerContext = LoggerContext.getContext(false);
            LoggerConfig loggerConfig = loggerContext.getConfiguration().getRootLogger();
            loggerConfig.setLevel(Level.getLevel(this.taskConfig.getLogLevel().toUpperCase()));
            loggerContext.updateLoggers();
            // 如果设置了性能参数，则覆盖默认值
            if (tomlParseResult.getLong("performance.readWindow") != null) {
                this.performanceConfig.setReadWindow(tomlParseResult.getLong("performance.readWindow").intValue());
            }
            if (tomlParseResult.getLong("performance.delay") != null) {
                this.performanceConfig.setDelay(tomlParseResult.getLong("performance.delay").intValue());
            }
            if (tomlParseResult.getLong("performance.maxThread") != null) {
                this.performanceConfig.setMaxThread(tomlParseResult.getLong("performance.maxThread").intValue());
            }
            if (tomlParseResult.getLong("performance.queueSizeT") != null) {
                this.performanceConfig.setQueueSizeT(tomlParseResult.getLong("performance.queueSizeT").longValue());
            }
            if (tomlParseResult.getLong("performance.rowsPerRead") != null) {
                this.performanceConfig.getThread().setReadBucketBatch(tomlParseResult.getLong("performance.rowsPerRead").longValue());
            }
            if (tomlParseResult.getLong("performance.queueSizeD") != null) {
                this.performanceConfig.setQueueSizeD(tomlParseResult.getLong("performance.queueSizeD").longValue());
            }
            if (tomlParseResult.getLong("performance.limitBatch") != null) {
                this.performanceConfig.setLimitBatch(tomlParseResult.getLong("performance.limitBatch").intValue());
            }
            if (tomlParseResult.getLong("performance.limitSpeed") != null) {
                this.performanceConfig.setLimitSpeed(tomlParseResult.getLong("performance.limitSpeed").intValue());
            }
        } catch (Exception e) {
            logger.error("An exception occurred during the loading of the Toml file, causing startup failure.", e);
            // 状态异常
            StatusCache.setStatus(StatusEnums.EXCEPTION.getCode());
            StatusCache.setDescription(StatusEnums.EXCEPTION.getDesc() + ": " + e.getMessage());
            // 启动失败
            System.exit(2);
        }
    }

    /**
     * 解析断点信息，格式为measurement1:timestamp&measurement2:timestamp&...
     *
     * @param breakpoints
     * @return
     */
    private Map<String, Long> parseBreakpoint(String breakpoints) {
        Map<String, Long> breakpointMap = new HashMap<>();
        try {
            // 按 & 分割
            String[] measurementInfoArr = breakpoints.split("&");
            // 遍历封装map
            for (String measurementInfo : measurementInfoArr) {
                // 按 : 分割
                String[] arr = measurementInfo.split(":");
                // measurement与timestamp均不为空才赋值
                if (StringUtils.isNotEmpty(arr[0]) && StringUtils.isNotEmpty(arr[1])) {
                    breakpointMap.put(arr[0], Long.parseLong(arr[1]));
                }
            }
        } catch (Exception e) {
            logger.error("An exception occurred during the parsing of breakpoints, breakpoints={}", breakpoints, e);
        }
        return breakpointMap;
    }

    /**
     * 处理工作模式：普通、恢复
     */
    /*private void initMode() {
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
                logger.error("Failed to read breakpoint, task will be executed in normal mode.", e);
            }
        }
    }*/

    /**
     * 检查influxdb连通性
     */
    private boolean checkInfluxdb() {
        // 本地部署的健康检查接口
        String healthUrl = "health";
        // 云服务的接口列表（因为它是公开访问的）
        String cloudApi = "api/v2";

        // http://host:port/ping
        String ping = "ping";

        // 拼接请求url
        if (influxdbConfig.getUrl().endsWith("/")) {
            healthUrl = influxdbConfig.getUrl() + healthUrl;
            cloudApi = influxdbConfig.getUrl() + cloudApi;
            ping = influxdbConfig.getUrl() + ping;
        } else {
            healthUrl = influxdbConfig.getUrl() + "/" + healthUrl;
            cloudApi = influxdbConfig.getUrl() + "/" + cloudApi;
            ping = influxdbConfig.getUrl() + "/" + ping;
        }
        try {
            // 一般情况下使用health接口获取服务状态
            String result = HttpUtils.sendGet(healthUrl, "");
            // 解析JSON结果
            JSONObject object = JSONObject.parseObject(result);
            // 判断数据库状态是否正常
            return HealthCheck.StatusEnum.PASS.getValue().equals(object.getString("status"));
        } catch (Exception e) {
            try {
                // 判断它是否是云服务
                String result = HttpUtils.sendGet(cloudApi, "");
                // 解析JSON结果
                JSONObject object = JSONObject.parseObject(result);
                // 判断是否包含authorizations（随便找一个，只需要证明接口访问正常）
                if (object.containsKey("authorizations")) {
                    LocalConfig.isInfluxDBCloud = true;
                    return true;
                }
            } catch (Exception ex) {
                try {
                    Map<String, String> res = HttpUtils.sendGetHeaders(ping, "");
                    logger.info("InfluxDB ping response:");
                    for (Map.Entry<String, String> entry : res.entrySet()) {
                        logger.info("{}: {}", entry.getKey(), entry.getValue());
                    }
                    if (res.containsKey("X-Influxdb-Build") && res.containsKey("X-Influxdb-Version")) {
                        return true;
                    }
                } catch (Exception exc) {
                    logger.error("failed to check InfluxDB connectivity, cause: {}", exc.getCause().toString());
                }

            }
        }
        return false;
    }

    /**
     * 获取influxdb版本信息
     *
     * @param url
     * @return
     */
    private JSONObject getInfluxdbVersion(String url) {
        JSONObject result = new JSONObject();
        try {
            // 建立http客户端
            CloseableHttpClient httpClient = HttpClients.createDefault();
            // 创建连接
            URIBuilder builder = new URIBuilder(url);
            // 建立httpGet
            HttpGet httpGet = new HttpGet(builder.build());
            // 获取响应
            CloseableHttpResponse response = httpClient.execute(httpGet);
            // 从header中获取版本信息
            String version = "";
            if (response.getHeaders("x-influxdb-build").length > 0) {
                version += response.getHeaders("x-influxdb-build")[0].getValue() + " ";
            }
            if (response.getHeaders("x-influxdb-version").length > 0) {
                version += response.getHeaders("x-influxdb-version")[0].getValue();
            }
            // 封装数据
            result.put("valid", true);
            result.put("support", true);
            result.put("version", version.trim());
            result.put("message", "Your data source is available, its version is " + version.trim() +
                    ", which is supported, you can proceed to transfer your data to TDengine.");
        } catch (Exception e) {
            result.put("valid", false);
            result.put("support", false);
            result.put("version", "");
            result.put("message", e.getMessage());
        }
        return result;
    }

    /**
     * 初始化influxdb及相关线程
     */
    private void initInfluxdb() {
        try {
            // 记录Influxdb信息
            StatusCache.noteInfluxdb(influxdbConfig.getUrl());
            // 获取所有bucket
            List<InfluxdbBucketEntity> influxdbBucketEntityList = influxdbService.selectAllBuckets(
                    influxdbConfig.getOrgId());
            // 跟据参数中的orgId与buckets进行过滤
            influxdbBucketEntityList.stream().filter(influxdbBucketEntity -> {
                if (StringUtils.isEmpty(influxdbConfig.getOrgId()) ||
                        influxdbBucketEntity.getOrgId().equals(influxdbConfig.getOrgId())) {
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
                    // 创建DBRP
                    if (this.influxdbConfig.isAddDbrp()) {
                        createDbrp(influxdbBucketEntity);
                    }
                    // 查询bucket中所有measurement信息
                    List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = influxdbService.selectAllMeasurements(
                            influxdbBucketEntity.getBucketName());
                    // 放入缓存中
                    for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                        BucketCache.measurementMap.put(
                                BucketCache.generateBucketDataThreadKey(influxdbMeasurementEntity.getBucket(),
                                        influxdbMeasurementEntity.getMeasurement()), influxdbMeasurementEntity);
                    }
                    // 通过first函数获取每个measurement的最早时间戳
                    for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                        try {
                            Instant firstTimestamp = influxdbService.getFirstTimestampInRange(influxdbConfig.getOrgId(),
                                    influxdbMeasurementEntity.getBucket(), influxdbMeasurementEntity.getMeasurement(),
                                    taskConfig.getBeginTime());
                            // 写入内存中
                            BucketCache.measurementFirstTimestampMap.put(
                                    BucketCache.generateBucketDataThreadKey(influxdbMeasurementEntity.getBucket(),
                                            influxdbMeasurementEntity.getMeasurement()), firstTimestamp);
                            logger.info("Auto update startTime: bucket: {}, measurement: {}, first: {}",
                                    influxdbMeasurementEntity.getBucket(), influxdbMeasurementEntity.getMeasurement(),
                                    firstTimestamp);
                        } catch (Exception e) {
                            logger.error("An exception occurred during getting first  timestamp", e);
                        }
                    }
                    // 启动BucketThread
                    BucketThread bucket = new BucketThread(influxdbConfig.getOrgId(),
                            influxdbBucketEntity.getBucketName());
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
                    logger.error("An exception occurred during the initialization of InfluxDB and related threads.", e);
                }
            });
            // 没有bucket则报错退出
            if (BucketCache.bucketMap.size() == 0) {
                // bucket错误
                logger.error("The application will exit soon: bucket not found.");
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
            logger.error("An exception occurred during the initialization of InfluxDB and related threads.", e);
        }
    }

    /**
     * 创建DBRP
     *
     * @param influxdbBucketEntity
     */
    private void createDbrp(InfluxdbBucketEntity influxdbBucketEntity) {
        // 请求地址
        String url = this.influxdbConfig.getUrl();
        if (url.endsWith("/")) {
            url += "api/v2/dbrps";
        } else {
            url += "/api/v2/dbrps";
        }
        // 请求参数
        JSONObject params = new JSONObject();
        params.put("bucketID", influxdbBucketEntity.getBucketId());
        params.put("database", influxdbBucketEntity.getBucketName());
        params.put("default", true);
        params.put("orgID", influxdbBucketEntity.getOrgId());
        params.put("retention_policy", "autogen");
        try {
            // 建立http客户端
            CloseableHttpClient httpClient = HttpClients.createDefault();
            // 建立httpPost
            HttpPost httpPost = new HttpPost(url);
            // 解决中文乱码问题
            StringEntity entity = new StringEntity(params.toJSONString(), "utf-8");
            // 设置编码格式
            entity.setContentEncoding("UTF-8");
            // 设置参数类型为json
            entity.setContentType("application/json");
            // 添加参数
            httpPost.setEntity(entity);
            // 添加token
            httpPost.addHeader("Authorization", "Token " + this.influxdbConfig.getToken());
            // 只执行不获取响应
            httpClient.execute(httpPost);
        } catch (Exception e) {
            logger.error("添加DBRP过程中发生异常，exception={}", e.getMessage());
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
            // 根据查询窗口长度计算（单位：分钟）
            int readWindow = performanceConfig.getReadWindow();
            return (int) (Math.ceil(diff / (readWindow * 60 * 1000) * measurementAmount));
        } catch (Exception e) {
            // 不处理异常，直接返回-1
            return -1;
        }
    }

    /**
     * 处理退出信号
     */
    /*private void processShutdown() {
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
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = BucketDataCache.getBucketData(BucketDataCache.getBucketDataQueueSize());
            // 遍历拼装内容
            for (InfluxdbBucketDataEntity influxdbBucketDataEntity : influxdbBucketDataEntityList) {
                sb.append(influxdbBucketDataEntity.toString() + RECORD_DELIMITER);
            }
            FileUtils.writeAbsoluteFile(temporaryPath, RECORD_FILE_QUEUE, sb.toString());
        } catch (Exception e) {
            logger.error("Failed to save read records during system security exit.", e);
        }
        logger.info("The system has executed a secure exit.");
    }*/
}

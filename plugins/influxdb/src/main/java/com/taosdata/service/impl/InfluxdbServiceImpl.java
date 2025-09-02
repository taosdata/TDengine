package com.taosdata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.influxdb.client.BucketsQuery;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.InfluxQLQuery;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.influxdb.query.InfluxQLQueryResult;
import com.taosdata.caches.BucketCache;
import com.taosdata.config.InfluxdbConfig;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.ResEnums;
import com.taosdata.service.InfluxdbService;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.exception.ArtificialException;
import com.taosdata.utils.influxdb.InfluxdbPoolAutoConfig;
import com.taosdata.utils.influxdbV1.InfluxdbV1PoolAutoConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Influxdb数据库操作服务实现类
 *
 * @author ZYP
 */
@Service
public class InfluxdbServiceImpl implements InfluxdbService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final String SEP = "\001";
    public static final String COMMA = ",";
    public static final String ESCAPE_COMMA = "\\,";
    public static final String EQUAL = "=";
    public static final String ESCAPE_EQUAL = "\\=";

    @Resource
    InfluxdbPoolAutoConfig influxdbPool;

    @Resource
    InfluxdbV1PoolAutoConfig influxdbV1Pool;

    @Resource
    private InfluxdbConfig influxdbConfig;

    @Resource
    private PerformanceConfig performanceConfig;

    /**
     * 单次连接，查询指定influxdb中schema信息
     *
     * @param url
     * @param token
     * @param orgId
     * @return
     * @throws ArtificialException
     */
    @Override
    public JSONObject fetchSchemaInfo(String url, String token, String orgId) throws ArtificialException {
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 使用url与token建立连接
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
            // 返回结果
            JSONObject bucketJson = new JSONObject(new LinkedHashMap<>());
            // 获取所有bucket列表
            List<Bucket> buckets = getBucketsV2(influxDBClient, orgId);
            // 按bucket名称排序
            buckets.sort(Comparator.comparing(Bucket::getName));
            // 遍历封装
            for (Bucket bucket : buckets) {
                // 查询所有measurement
                Set<String> measurements = getMeasurementsV2(influxDBClient, bucket.getName());
                // 封装JsonArray
                JSONArray jsonArray = new JSONArray(measurements.stream().sorted().collect(Collectors.toList()));
                // 放入结果集中
                bucketJson.put(bucket.getName(), jsonArray);
            }
            return bucketJson;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            if (influxDBClient != null) {
                influxDBClient.close();
            }
        }
    }

    /**
     * 单次连接，查询指定influxdb中schema信息，适用于v1.7/1.8
     *
     * @param url
     * @param username
     * @param password
     * @return
     * @throws ArtificialException
     */
    @Override
    public JSONObject fetchSchemaInfoV1(String url, String username, String password) throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 使用url与token建立连接
            influxDB = InfluxDBFactory.connect(url, username, password);
            // 返回结果
            JSONObject bucketJson = new JSONObject();
            // 获取所有bucket列表
            Set<String> buckets = getBucketsV1(influxDB);
            // 转化为list并排序
            List<String> bucketList = buckets.stream().sorted().collect(Collectors.toList());
            // 遍历封装
            for (String bucket : bucketList) {
                // 查询所有measurement
                Set<String> measurements = getMeasurementsV1(influxDB, bucket);
                // 封装JsonArray
                JSONArray jsonArray = new JSONArray(measurements.stream().sorted().collect(Collectors.toList()));
                // 放入结果集中
                bucketJson.put(bucket, jsonArray);
            }
            return bucketJson;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            if (influxDB != null) {
                influxDB.close();
            }
        }
    }

    /**
     * 获取influxdb中所有bucket
     *
     * @param orgId
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<InfluxdbBucketEntity> selectAllBuckets(String orgId) throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8":
                return selectAllBucketsV1();
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            if (LocalConfig.isInfluxDBCloud) {
                // 使用url与token建立连接
                influxDBClient = InfluxDBClientFactory.create(influxdbConfig.getUrl(), influxdbConfig.getToken().toCharArray());
            } else {
                // 连接池中获取客户端
                influxDBClient = influxdbPool.getPool().borrowObject();
            }
            // 返回列表
            List<InfluxdbBucketEntity> influxdbBucketEntityList = new ArrayList<>();
            // 获取所有bucket列表
            List<Bucket> bucketList = getBucketsV2(influxDBClient, orgId);
            // 遍历组装
            for (Bucket bucket : bucketList) {
                InfluxdbBucketEntity influxdbBucketEntity = new InfluxdbBucketEntity();
                influxdbBucketEntity.setBucketId(bucket.getId());
                influxdbBucketEntity.setBucketType(bucket.getType().getValue());
                influxdbBucketEntity.setBucketName(bucket.getName());
                influxdbBucketEntity.setBucketDescription(bucket.getDescription());
                influxdbBucketEntity.setOrgId(bucket.getOrgID());
                influxdbBucketEntity.setCreateTime(DateUtils.fromOffsetDateTime(bucket.getCreatedAt()));
                influxdbBucketEntity.setUpdateTime(DateUtils.fromOffsetDateTime(bucket.getUpdatedAt()));
                // 放入列表
                influxdbBucketEntityList.add(influxdbBucketEntity);
            }
            return influxdbBucketEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDBClient(influxDBClient);
        }
    }

    /**
     * 获取指定bucket中所有measurement
     *
     * @param bucket
     * @return
     */
    @Override
    public List<InfluxdbMeasurementEntity> selectAllMeasurements(String bucket) throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8":
                return selectAllMeasurementsV1(bucket);
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            if (LocalConfig.isInfluxDBCloud) {
                // 使用url与token建立连接
                influxDBClient = InfluxDBClientFactory.create(influxdbConfig.getUrl(), influxdbConfig.getToken().toCharArray());
            } else {
                // 连接池中获取客户端
                influxDBClient = influxdbPool.getPool().borrowObject();
            }
            // 返回结果
            List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = new ArrayList<>();
            // 查询所有measurement
            Set<String> measurements = getMeasurementsV2(influxDBClient, bucket);
            // 遍历封装
            for (String measurement : measurements) {
                InfluxdbMeasurementEntity influxdbMeasurementEntity = new InfluxdbMeasurementEntity();
                influxdbMeasurementEntity.setBucket(bucket);
                influxdbMeasurementEntity.setMeasurement(measurement);
                influxdbMeasurementEntity.setFieldMap(new HashMap<>());
                influxdbMeasurementEntity.setTagSet(new HashSet<>());
                influxdbMeasurementEntityList.add(influxdbMeasurementEntity);
            }
            // 遍历measurement列表
            for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                // 查询所有field
                try {
                    influxdbMeasurementEntity.getFieldMap().putAll(getFieldsV2(influxDBClient, bucket, influxdbMeasurementEntity.getMeasurement()));
                } catch (Exception e) {
                    handlerException(e);
                }
                // 查询所有tag
                try {
                    influxdbMeasurementEntity.getTagSet().addAll(getTagsV2(influxDBClient, bucket, influxdbMeasurementEntity.getMeasurement()));
                } catch (Exception e) {
                    handlerException(e);
                }
            }
            return influxdbMeasurementEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDBClient(influxDBClient);
        }
    }

    /**
     * 获取指定bucket、measurement的所有字段
     *
     * @param bucket
     * @param measurement
     * @return
     * @throws ArtificialException
     */
    @Override
    public Map<String, String> selectAllFields(String bucket, String measurement) throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8": {
                // influxdb客户端
                InfluxDB influxDB = null;
                try {
                    // 连接池中获取客户端
                    influxDB = influxdbV1Pool.getPool().borrowObject();
                    // 返回查询结果
                    return getFieldsV1(influxDB, bucket, measurement);
                } catch (Exception e) {
                    handlerException(e);
                    throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
                } finally {
                    closeInfluxDB(influxDB);
                }
            }
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            if (LocalConfig.isInfluxDBCloud) {
                // 使用url与token建立连接
                influxDBClient = InfluxDBClientFactory.create(influxdbConfig.getUrl(), influxdbConfig.getToken().toCharArray());
            } else {
                // 连接池中获取客户端
                influxDBClient = influxdbPool.getPool().borrowObject();
            }
            // 返回查询结果
            return getFieldsV2(influxDBClient, bucket, measurement);
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDBClient(influxDBClient);
        }
    }

    /**
     * 获取指定bucket、measurement与时间段内的第一个时间戳
     *
     * @param orgId
     * @param bucket
     * @param measurement
     * @param startTime
     * @return
     * @throws ArtificialException
     */
    @Override
    public Instant getFirstTimestampInRange(String orgId, String bucket, String measurement, String startTime) throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8":
                return getFirstTimestampInRangeV1(bucket, measurement, startTime);
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            if (LocalConfig.isInfluxDBCloud) {
                // 使用url与token建立连接
                influxDBClient = InfluxDBClientFactory.create(influxdbConfig.getUrl(), influxdbConfig.getToken().toCharArray());
            } else {
                // 连接池中获取客户端
                influxDBClient = influxdbPool.getPool().borrowObject();
            }
            // 查询语句
            String sql = "from(bucket: \"" + bucket + "\")" +
                    "|> range(start: " + startTime + ")" +
                    "|> filter(fn: (r) => r._measurement == \"" + measurement + "\")" +
                    "|> first()";
            // 执行查询
            List<FluxTable> tables = influxDBClient.getQueryApi().query(sql, orgId);
            // 遍历结果集进行封装
            for (FluxTable fluxTable : tables) {
                // 结果空则跳过
                if (fluxTable == null || fluxTable.getRecords() == null) {
                    continue;
                }
                // 记录
                for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                    // 结果空则跳过
                    if (fluxRecord == null || fluxRecord.getValues() == null) {
                        continue;
                    }
                    // 获取字段及对应值
                    Map<String, Object> map = fluxRecord.getValues();
                    // 返回结果
                    return (Instant) map.getOrDefault("_time", null);
                }
            }
            return null;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDBClient(influxDBClient);
        }
    }

    /**
     * 获取influxdb中指定bucket、measurement与时间段的数据
     *
     * @param orgId
     * @param bucket
     * @param measurement
     * @param field
     * @param startTime
     * @param stopTime
     * @param batch
     * @param offset
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<InfluxdbBucketDataEntity> selectBucketData(String orgId, String bucket, String measurement, String field, String startTime, String stopTime, long batch, long offset) throws ArtificialException {
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            if (LocalConfig.isInfluxDBCloud) {
                // 使用url与token建立连接
                influxDBClient = InfluxDBClientFactory.create(influxdbConfig.getUrl(), influxdbConfig.getToken().toCharArray());
            } else {
                // 连接池中获取客户端
                influxDBClient = influxdbPool.getPool().borrowObject();
            }
            // 返回列表
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
            // 根据bucket与measurement获取内存中的表结构
            InfluxdbMeasurementEntity influxdbMeasurementEntity = BucketCache.measurementMap.get(BucketCache.generateBucketDataThreadKey(bucket, measurement));
            // 查询语句
            String sql = "from(bucket: \"" + bucket + "\")" +
                    "|> range(start: " + startTime + ", stop: " + stopTime + ")" +
                    "|> filter(fn: (r) => r._measurement == \"" + measurement + "\" and r._field == \"" + field + "\")" +
                    "|> limit(n: " + batch + ", offset: " + offset + ")";
            // 执行查询
            List<FluxTable> tables = influxDBClient.getQueryApi().query(sql, orgId);
            // 子表集合
            Set<String> subtableSet = new HashSet<>();
            // 遍历结果集进行封装
            for (FluxTable fluxTable : tables) {
                // 结果空则跳过
                if (fluxTable == null || fluxTable.getRecords() == null) {
                    continue;
                }
                // 记录
                for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                    // 结果空则跳过
                    if (fluxRecord == null || fluxRecord.getValues() == null) {
                        continue;
                    }
                    InfluxdbBucketDataEntity influxdbBucketDataEntity = new InfluxdbBucketDataEntity();
                    influxdbBucketDataEntity.setTags(new HashMap<>());
                    // 获取字段及对应值
                    Map<String, Object> map = fluxRecord.getValues();
                    map.forEach((key, value) -> {
                        if ("result".equalsIgnoreCase(key) || "_start".equalsIgnoreCase(key) || "_stop".equalsIgnoreCase(key)) {
                            // 忽略
                        } else if ("_measurement".equalsIgnoreCase(key)) {
                            influxdbBucketDataEntity.setMeasurement(String.valueOf(value));
                        } else if ("table".equalsIgnoreCase(key)) {
                            influxdbBucketDataEntity.setTable(String.valueOf(value));
                            subtableSet.add(String.valueOf(value));
                        } else if ("_time".equalsIgnoreCase(key)) {
                            influxdbBucketDataEntity.setTime((Instant) value);
                        } else if ("_field".equalsIgnoreCase(key)) {
                            influxdbBucketDataEntity.setField(String.valueOf(value));
                        } else if ("_value".equalsIgnoreCase(key)) {
                            influxdbBucketDataEntity.setValue(value);
                        } else {
                            influxdbBucketDataEntity.getTags().put(key, value);
                        }
                    });
                    // 如果存在新增字段，需要更新缓存
                    if (!influxdbMeasurementEntity.getFieldMap().containsKey(influxdbBucketDataEntity.getField())) {
                        // 获取新的字段列表
                        Map<String, String> fieldMap = getFieldsV2(influxDBClient, bucket, measurement);
                        // 更新缓存
                        influxdbMeasurementEntity.getFieldMap().putAll(fieldMap);
                    }
                    // 设置表结构
                    influxdbBucketDataEntity.setInfluxdbMeasurementEntity(influxdbMeasurementEntity);
                    // 放入列表
                    influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
                }
            }
            // 更新measurement查询限制
            BucketCache.updateQueryLimit(BucketCache.generateBucketDataThreadKey(bucket, measurement), subtableSet.size(), performanceConfig.getQueueSizeD(), performanceConfig.getThread().getReadBucketBatch());
            return influxdbBucketDataEntityList;
        } catch (Exception e) {
            handlerException(e);
            // 异常后更新measurement查询限制
            BucketCache.updateQueryLimit(BucketCache.generateBucketDataThreadKey(bucket, measurement), Integer.MAX_VALUE, performanceConfig.getQueueSizeD(), performanceConfig.getThread().getReadBucketBatch());
            logger.error("update query limit from {} to {}", batch, BucketCache.getQueryLimit(BucketCache.generateBucketDataThreadKey(bucket, measurement)));
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDBClient(influxDBClient);
        }
    }

    /**
     * @return 具体的 influxdb 的版本
     */
    @Override
    public String getInfluxdbVersion() {
        return this.influxdbConfig.getVersion();
    }

    /**
     * 异常处理
     *
     * @param e
     */
    private void handlerException(Exception e) {
        String errMsg = e.getMessage();
        if (StringUtils.isNotEmpty(errMsg) && (errMsg.contains("Failed to connect") || errMsg.contains("Unable to validate object") || errMsg.contains("connect timed out") || errMsg.contains("No route to host"))) {
            // url错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(101);
        } else if (StringUtils.isNotEmpty(errMsg) && errMsg.contains("unauthorized access")) {
            // token错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(102);
        } else if (StringUtils.isNotEmpty(errMsg) && errMsg.contains("organization not found")) {
            // organization错误
            logger.error("The application will exit soon: {}", e.getMessage());
            System.exit(103);
        }
    }

    /**
     * 获取influxdb中所有bucket，适用于v1.7/1.8
     *
     * @return
     * @throws ArtificialException
     */
    private List<InfluxdbBucketEntity> selectAllBucketsV1() throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 连接池中获取客户端
            influxDB = influxdbV1Pool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketEntity> influxdbBucketEntityList = new ArrayList<>();
            // 获取所有bucket列表
            Set<String> buckets = getBucketsV1(influxDB);
            // 遍历组装
            for (String bucket : buckets) {
                InfluxdbBucketEntity influxdbBucketEntity = new InfluxdbBucketEntity();
                influxdbBucketEntity.setBucketName(bucket);
                // 在1.x版本中不关心orgId，此处直接使用配置中的，以免被过滤
                influxdbBucketEntity.setOrgId(influxdbConfig.getOrgId());
                // 放入列表
                influxdbBucketEntityList.add(influxdbBucketEntity);
            }
            return influxdbBucketEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDB(influxDB);
        }
    }

    /**
     * 获取指定bucket中所有measurement，适用于v1.7/1.8
     *
     * @param bucket
     * @return
     */
    private List<InfluxdbMeasurementEntity> selectAllMeasurementsV1(String bucket) throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 连接池中获取客户端
            influxDB = influxdbV1Pool.getPool().borrowObject();
            // 返回结果
            List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = new ArrayList<>();
            // 查询所有measurement
            Set<String> measurements = getMeasurementsV1(influxDB, bucket);
            // 遍历封装
            for (String measurement : measurements) {
                InfluxdbMeasurementEntity influxdbMeasurementEntity = new InfluxdbMeasurementEntity();
                influxdbMeasurementEntity.setBucket(bucket);
                influxdbMeasurementEntity.setMeasurement(measurement);
                influxdbMeasurementEntity.setFieldMap(new HashMap<>());
                influxdbMeasurementEntity.setTagSet(new HashSet<>());
                influxdbMeasurementEntityList.add(influxdbMeasurementEntity);
            }
            // 遍历measurement列表
            for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                // 查询所有field
                try {
                    influxdbMeasurementEntity.getFieldMap().putAll(getFieldsV1(influxDB, bucket, influxdbMeasurementEntity.getMeasurement()));
                } catch (Exception e) {
                    handlerException(e);
                }
                // 查询所有tag
                try {
                    influxdbMeasurementEntity.getTagSet().addAll(getTagsV1(influxDB, bucket, influxdbMeasurementEntity.getMeasurement()));
                } catch (Exception e) {
                    handlerException(e);
                }
            }
            return influxdbMeasurementEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDB(influxDB);
        }
    }

    /**
     * 获取指定bucket、measurement与时间段内的第一个时间戳，适用于v1.7/1.8
     *
     * @param bucket
     * @param measurement
     * @param startTime
     * @return
     * @throws ArtificialException
     */
    private Instant getFirstTimestampInRangeV1(String bucket, String measurement, String startTime) throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 连接池中获取客户端
            influxDB = influxdbV1Pool.getPool().borrowObject();
            // 查询语句
            String sql = "select first() from \"" + measurement + "\" where time >= '" + startTime + "'";
            // 执行查询
            QueryResult queryResult = influxDB.query(new Query(sql, bucket));
            // 结果空则返回空
            if (queryResult == null || queryResult.getResults() == null) {
                return null;
            }
            // 遍历结果集进行封装
            for (QueryResult.Result result : queryResult.getResults()) {
                // 结果空则跳过
                if (result == null || result.getSeries() == null) {
                    continue;
                }
                // 记录
                for (QueryResult.Series series : result.getSeries()) {
                    // 结果空则跳过
                    if (series == null) {
                        continue;
                    }
                    // 获取字段与对应的值
                    List<String> columns = series.getColumns() != null ? series.getColumns() : new ArrayList<>();
                    List<List<Object>> values = series.getValues() != null ? series.getValues() : new ArrayList<>();
                    // 遍历并按照v2.7格式封装
                    for (List<Object> record : values) {
                        // 结果空则跳过
                        if (record == null || record.size() == 0) {
                            continue;
                        }
                        for (int i = 0; i < record.size(); i++) {
                            // 取对应的列名
                            String column = columns.size() > i ? columns.get(i) : "";
                            // 获取时间戳
                            if ("time".equalsIgnoreCase(column)) {
                                return Instant.parse(record.get(i).toString());
                            }
                        }
                    }
                }
            }
            return null;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDB(influxDB);
        }
    }

    /**
     * 获取 tag set
     * @param bucket
     * @param measurement
     * @return
     * @throws ArtificialException
     */
    public List<List<Pair<String, String>>> getTagSet(String bucket, String measurement) throws ArtificialException {
        InfluxDB influxDB = null;
        try {
            influxDB = influxdbV1Pool.getPool().borrowObject();
            return getTagSetV1(influxDB, bucket, measurement);
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDB(influxDB);
        }
    }

    /**
     * 获取influxdb中指定bucket、measurement与时间段的数据，适用于v1.7/1.8
     *
     * @param bucket
     * @param measurement
     * @param tagCondition
     * @param startTime
     * @param stopTime
     * @param batch
     * @param offset
     * @return
     * @throws ArtificialException
     */
    public List<InfluxdbBucketDataEntity> selectBucketDataV1(String bucket, String measurement, String tagCondition, String startTime, String stopTime, long batch, long offset) throws ArtificialException {
//        String sql = "select * from \"" + measurement + "\" where time >= '" + startTime + "' and time <= '" + stopTime + "' limit " + batch + " offset " + offset;
        String sql = "select * from \"" + measurement + "\" where time >= '" + startTime + "' and time <= '" + stopTime + "' and " + tagCondition + " limit " + batch + " offset " + offset;
        return selectBucketDataV1(bucket, measurement, sql);
    }

    /**
     * 获取influxdb中指定bucket、measurement与时间段的数据，适用于v1.7/1.8
     *
     * @param bucket
     * @param measurement
     * @param sql
     * @return
     * @throws ArtificialException
     */
    private List<InfluxdbBucketDataEntity> selectBucketDataV1(String bucket, String measurement, String sql) throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 连接池中获取客户端
            influxDB = influxdbV1Pool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
            // 根据bucket与measurement获取内存中的表结构
            InfluxdbMeasurementEntity influxdbMeasurementEntity = BucketCache.measurementMap.get(BucketCache.generateBucketDataThreadKey(bucket, measurement));

            logger.info("influxdb 1.x send query sql: {}", sql);
            long sTime = System.currentTimeMillis();
            // 执行查询
            QueryResult queryResult = influxDB.query(new Query(sql, bucket));
            // 结果空则返回空列表
            if (queryResult == null || queryResult.getResults() == null) {
                return influxdbBucketDataEntityList;
            }
            // 遍历结果集进行封装
            for (QueryResult.Result result : queryResult.getResults()) {
                // 结果空则跳过
                if (result == null || result.getSeries() == null) {
                    continue;
                }
                // 记录
                for (QueryResult.Series series : result.getSeries()) {
                    // 结果空则跳过
                    if (series == null) {
                        continue;
                    }
                    InfluxdbBucketDataEntity influxdbBucketDataEntity = new InfluxdbBucketDataEntity();
                    influxdbBucketDataEntity.setTags(new HashMap<>());
                    // 获取字段与对应的值
                    List<String> columns = series.getColumns() != null ? series.getColumns() : new ArrayList<>();
                    List<List<Object>> values = series.getValues() != null ? series.getValues() : new ArrayList<>();
                    // 遍历并按照v2.7格式封装
                    for (List<Object> record : values) {
                        // 结果空则跳过
                        if (record == null || record.size() == 0) {
                            continue;
                        }
                        // 首先封装公共部分
                        influxdbBucketDataEntity.setInfluxdbMeasurementEntity(BucketCache.measurementMap.get(BucketCache.generateBucketDataThreadKey(bucket, measurement)));
                        influxdbBucketDataEntity.setMeasurement(measurement);
                        influxdbBucketDataEntity.setTable("");
                        for (int i = 0; i < record.size(); i++) {
                            // 取对应的列名
                            String column = columns.size() > i ? columns.get(i) : "";
                            // 设置tag
                            if ("time".equalsIgnoreCase(column)) {
                                influxdbBucketDataEntity.setTime(Instant.parse(record.get(i).toString()));
                            } else if (influxdbMeasurementEntity.getTagSet().contains(column)) {
                                influxdbBucketDataEntity.getTags().put(column, record.get(i));
                            }
                        }
                        // 然后每个column一条记录
                        for (int i = 0; i < record.size(); i++) {
                            // 取对应的列名
                            String column = columns.size() >= i + 1 ? columns.get(i) : "";
                            // 判断time、col、tag
                            if (StringUtils.isEmpty(column) || "time".equalsIgnoreCase(column) || influxdbMeasurementEntity.getTagSet().contains(column)) {
                                // 忽略
                            } else if (influxdbMeasurementEntity.getFieldMap().containsKey(column)) {
                                InfluxdbBucketDataEntity data = influxdbBucketDataEntity.clone();
                                data.setField(column);
                                data.setValue(record.get(i));
                                // 放入列表
                                influxdbBucketDataEntityList.add(data);
                            } else {
                                // TODO 默认放入col中
                                InfluxdbBucketDataEntity data = influxdbBucketDataEntity.clone();
                                data.setField(column);
                                data.setValue(record.get(i));
                                // 放入列表
                                influxdbBucketDataEntityList.add(data);
                            }
                        }
                    }
                }
            }
            // 获取 limit
            BucketCache.updateQueryLimit(BucketCache.generateBucketDataThreadKey(bucket, measurement), 1, performanceConfig.getThread().getReadBucketBatch(), Long.MAX_VALUE);
            double costTime = (System.currentTimeMillis() - sTime) / 1000.0;
            logger.info("influxdb 1.x exec sql: {}, result point size: {}, time cost {} s", sql, influxdbBucketDataEntityList.size(), costTime);
            return influxdbBucketDataEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            closeInfluxDB(influxDB);
        }
    }

    /**
     * 获取bucket列表，适用于v1.7/1.8
     *
     * @param influxDB
     * @return
     */
    private Set<String> getBucketsV1(InfluxDB influxDB) {
        // 返回结果
        Set<String> bucketSet = new HashSet<>();
        // 获取所有bucket
        QueryResult queryResult = influxDB.query(new Query("show databases"));
        // 结果空则返回空列表
        if (queryResult == null || queryResult.getResults() == null) {
            return bucketSet;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() == 0) {
                        continue;
                    }
                    // 查询measurement并放入结果集
                    bucketSet.add(record.get(0).toString());
                }
            }
        }
        return bucketSet;
    }

    /**
     * 获取bucket列表，适用于v2.0-2.7
     *
     * @param influxDBClient
     * @return
     */
    private List<Bucket> getBucketsV2(InfluxDBClient influxDBClient, String orgId) {
        // 所有bucket列表
        List<Bucket> bucketAll = new ArrayList<>();
        // 循环分页查询
        while (true) {
            // 查询参数
            BucketsQuery bucketsQuery = new BucketsQuery();
            bucketsQuery.setOrgID(orgId);
            bucketsQuery.setOffset(bucketAll.size());
            bucketsQuery.setLimit(100);
            // 获取bucket列表
            List<Bucket> buckets = influxDBClient.getBucketsApi().findBuckets(bucketsQuery);
            // 判断是否取到
            if (buckets == null || buckets.size() == 0) {
                break;
            } else {
                bucketAll.addAll(buckets);
                // InfluxDB接口存在缺陷，当token仅拥有少量bucket权限时，可多次查到重复数据，为避免这一情况，结果集小于limit时直接结束
                if (buckets.size() < 100) {
                    break;
                }
            }
        }
        return bucketAll;
    }

    /**
     * 获取measurement列表，适用于v1.7/1.8
     *
     * @param influxDB
     * @param bucket
     * @return
     */
    private Set<String> getMeasurementsV1(InfluxDB influxDB, String bucket) {
        // 返回结果
        Set<String> measurementSet = new HashSet<>();
        // 查询所有measurement
        QueryResult queryResultMeasurement = influxDB.query(new Query("show measurements", bucket));
        // 结果空则返回空列表
        if (queryResultMeasurement == null) {
            // 将空array放入结果集
            return measurementSet;
        }
        // 遍历封装
        for (QueryResult.Result result : queryResultMeasurement.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() == 0) {
                        continue;
                    }
                    measurementSet.add(record.get(0).toString());
                }
            }
        }
        return measurementSet;
    }

    /**
     * 获取measurement列表，适用于v2.0-2.7
     *
     * @param influxDBClient
     * @param bucket
     * @return
     */
    private Set<String> getMeasurementsV2(InfluxDBClient influxDBClient, String bucket) {
        // 返回结果
        Set<String> measurementSet = new HashSet<>();
        // 查询所有measurement
        InfluxQLQuery showMeasurementSql = new InfluxQLQuery("show measurements", bucket);
        InfluxQLQueryResult showMeasurementResult = influxDBClient.getInfluxQLQueryApi().query(showMeasurementSql);
        // 结果空则返回空列表
        if (showMeasurementResult == null || showMeasurementResult.getResults() == null) {
            // 将空array放入结果集
            return measurementSet;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showMeasurementResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.getValues() == null || record.getValues().length == 0) {
                        continue;
                    }
                    measurementSet.add(record.getValues()[0].toString());
                }
            }
        }
        return measurementSet;
    }

    /**
     * 查询field列表，适用于v1.7/1.8
     *
     * @param influxDB
     * @param bucket
     * @param measurement
     * @return
     */
    private Map<String, String> getFieldsV1(InfluxDB influxDB, String bucket, String measurement) {
        // 返回结果
        Map<String, String> fieldMap = new HashMap<>();
        // 查询所有field
        QueryResult queryResult = influxDB.query(new Query("show field keys from \"" + measurement + "\"", bucket));
        // 结果空则返回空map
        if (queryResult == null || queryResult.getResults() == null) {
            return fieldMap;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() < 2) {
                        continue;
                    }
                    fieldMap.put(record.get(0).toString(), record.get(1).toString());
                }
            }
        }
        return fieldMap;
    }

    /**
     * 查询field列表，适用于v2.0-2.7
     *
     * @param influxDBClient
     * @param bucket
     * @param measurement
     * @return
     */
    private Map<String, String> getFieldsV2(InfluxDBClient influxDBClient, String bucket, String measurement) {
        // 返回结果
        Map<String, String> fieldMap = new HashMap<>();
        // 查询所有field
        InfluxQLQuery showFieldSql = new InfluxQLQuery("show field keys from \"" + measurement + "\"", bucket);
        InfluxQLQueryResult showFieldResult = influxDBClient.getInfluxQLQueryApi().query(showFieldSql);
        // 结果空则返回空map
        if (showFieldResult == null || showFieldResult.getResults() == null) {
            return fieldMap;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showFieldResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.getValues() == null || record.getValues().length < 2) {
                        continue;
                    }
                    fieldMap.put(record.getValues()[0].toString(), record.getValues()[1].toString());
                }
            }
        }
        return fieldMap;
    }

    /**
     * 查询tag列表，适用于v1.7/1.8
     *
     * @param influxDB
     * @param bucket
     * @param measurement
     * @return
     */
    private Set<String> getTagsV1(InfluxDB influxDB, String bucket, String measurement) {
        // 返回结果
        Set<String> tagSet = new HashSet<>();
        // 查询所有tag
        QueryResult queryResult = influxDB.query(new Query("show tag keys from \"" + measurement + "\"", bucket));
        // 结果空则返回空set
        if (queryResult == null || queryResult.getResults() == null) {
            return tagSet;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() == 0) {
                        continue;
                    }
                    tagSet.add(record.get(0).toString());
                }
            }
        }
        return tagSet;
    }

    /**
     * 查询tag列表值，适用于v1.7/1.8
     *
     * @param influxDB
     * @param bucket
     * @param measurement
     * @return
     */
    private List<List<Pair<String, String>>> getTagSetV1(InfluxDB influxDB, String bucket, String measurement) {
        // 返回结果
        List<List<Pair<String, String>>> tagValues = new ArrayList<>();
        // 查询所有tag
        QueryResult queryResult = influxDB.query(new Query("show series from \"" + measurement + "\"", bucket));
        // 结果空则返回空set
        if (queryResult == null || queryResult.getResults() == null) {
            return tagValues;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() == 0) {
                        continue;
                    }
                    List<Pair<String, String>> tags = new ArrayList<>();
                    for (int i = 0; i < record.size(); i++) {
                        String line = record.get(i).toString();
                        if (line.contains(ESCAPE_COMMA)) {
                            line = line.replace(ESCAPE_COMMA, SEP);
                        }
                        String[] kvs = line.split(COMMA);
                        // 第一个元素是 measurement
                        for (int j = 1; j < kvs.length; j++) {
                            String kv = kvs[j];
                            kv = kv.replace(SEP, ESCAPE_COMMA);
                            kv = kv.replace(ESCAPE_EQUAL, SEP);
                            String[] pair = kv.split(EQUAL);
                            if (pair.length != 2) {
                                kv = kv.replace(SEP, ESCAPE_EQUAL);
                                logger.error("show series get abnormal pair: {}", kv);
                                continue;
                            }
                            String tag = pair[0].replace(SEP, ESCAPE_EQUAL);
                            String val = pair[1].replace(SEP, ESCAPE_EQUAL);
                            // 在后面 sql 中，需去掉转移符号 \
                            val = val.replace(ESCAPE_EQUAL, EQUAL);
                            val = val.replace(ESCAPE_COMMA, COMMA);
                            tags.add(Pair.of(tag, val));
                        }
                    }
                    if (!tags.isEmpty()) {
                        tagValues.add(tags);
                    }
                }
            }
        }
        return tagValues;
    }

    /**
     * 查询tag列表，适用于v2.0-2.7
     *
     * @param influxDBClient
     * @param bucket
     * @param measurement
     * @return
     */
    private Set<String> getTagsV2(InfluxDBClient influxDBClient, String bucket, String measurement) {
        // 返回结果
        Set<String> tagSet = new HashSet<>();
        // 查询所有tag
        InfluxQLQuery showTagSql = new InfluxQLQuery("show tag keys from \"" + measurement + "\"", bucket);
        InfluxQLQueryResult showTagResult = influxDBClient.getInfluxQLQueryApi().query(showTagSql);
        // 结果空则返回空set
        if (showTagResult == null || showTagResult.getResults() == null) {
            return tagSet;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showTagResult.getResults()) {
            // 空则跳过并继续
            if (result == null || result.getSeries() == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null || series.getValues() == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.getValues() == null || record.getValues().length == 0) {
                        continue;
                    }
                    tagSet.add(record.getValues()[0].toString());
                }
            }
        }
        return tagSet;
    }

    /**
     * 回收 InfluxDB v1.x 连接
     *
     * @param influxDB
     */
    private void closeInfluxDB(InfluxDB influxDB) {
        try {
            if (influxDB != null) {
                influxdbV1Pool.getPool().returnObject(influxDB);
            }
        } catch (Exception e) {
            logger.error("An exception occurred during the recycling of connection", e);
        }
    }

    /**
     * 回收 InfluxDB v2.x 连接
     *
     * @param influxDBClient
     */
    private void closeInfluxDBClient(InfluxDBClient influxDBClient) {
        try {
            if (influxDBClient != null) {
                if (LocalConfig.isInfluxDBCloud) {
                    influxDBClient.close();
                } else {
                    influxdbPool.getPool().returnObject(influxDBClient);
                }
            }
        } catch (Exception e) {
            logger.error("An exception occurred during the recycling of connection", e);
        }
    }
}

package com.taosdata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.InfluxQLQuery;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.influxdb.query.InfluxQLQueryResult;
import com.taosdata.caches.BucketCache;
import com.taosdata.config.InfluxdbConfig;
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

/**
 * Influxdb数据库操作服务实现类
 *
 * @author ZYP
 */
@Service
public class InfluxdbServiceImpl implements InfluxdbService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    InfluxdbPoolAutoConfig influxdbPool;

    @Resource
    InfluxdbV1PoolAutoConfig influxdbV1Pool;

    @Resource
    private InfluxdbConfig influxdbConfig;

    /**
     * 单次连接，查询指定influxdb中schema信息
     *
     * @param url
     * @param token
     * @return
     * @throws ArtificialException
     */
    @Override
    public JSONObject fetchSchemaInfo(String url, String token) throws ArtificialException {
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 使用url与token建立连接
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
            // 返回结果
            JSONObject bucketJson = new JSONObject();
            // 获取所有bucket列表
            List<Bucket> buckets = getBucketsV2(influxDBClient);
            // 遍历封装
            for (Bucket bucket : buckets) {
                // 查询所有measurement
                Set<String> measurements = getMeasurementsV2(influxDBClient, bucket.getName());
                // 封装JsonArray
                JSONArray jsonArray = new JSONArray(Arrays.asList(measurements));
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
            // 遍历封装
            for (String bucket : buckets) {
                // 查询所有measurement
                Set<String> measurements = getMeasurementsV1(influxDB, bucket);
                // 封装JsonArray
                JSONArray jsonArray = new JSONArray(Arrays.asList(measurements));
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
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<InfluxdbBucketEntity> selectAllBuckets() throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8":
                return selectAllBucketsV1();
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketEntity> influxdbBucketEntityList = new ArrayList<>();
            // 获取所有bucket列表
            List<Bucket> bucketList = getBucketsV2(influxDBClient);
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
            if (influxDBClient != null) {
                influxdbPool.getPool().returnObject(influxDBClient);
            }
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
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
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
            if (influxDBClient != null) {
                influxdbPool.getPool().returnObject(influxDBClient);
            }
        }
    }

    /**
     * 获取influxdb中指定bucket、measurement与时间段的数据
     *
     * @param orgId
     * @param bucket
     * @param measurement
     * @param startTime
     * @param stopTime
     * @param batch
     * @param offset
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<InfluxdbBucketDataEntity> selectBucketData(String orgId, String bucket, String measurement, String startTime, String stopTime, long batch, long offset) throws ArtificialException {
        switch (influxdbConfig.getVersion()) {
            case "1.7":
            case "1.8":
                return selectBucketDataV1(bucket, measurement, startTime, stopTime, batch, offset);
        }
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
            // 根据bucket与measurement获取内存中的表结构
            InfluxdbMeasurementEntity influxdbMeasurementEntity = BucketCache.measurementMap.get(bucket + ":" + measurement);
            // 查询语句
            String sql = "from(bucket: \"" + bucket + "\")" +
                    "|> range(start: " + startTime + ", stop: " + stopTime + ")" +
                    "|> filter(fn: (r) => r._measurement == \"" + measurement + "\")" +
                    "|> limit(n: " + batch + ", offset: " + offset + ")";
            // 执行查询
            List<FluxTable> tables = influxDBClient.getQueryApi().query(sql, orgId);
            // 遍历结果集进行封装
            for (FluxTable fluxTable : tables) {
                // 记录
                for (FluxRecord fluxRecord : fluxTable.getRecords()) {
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
                    // 设置表结构
                    influxdbBucketDataEntity.setInfluxdbMeasurementEntity(influxdbMeasurementEntity);
                    // 放入列表
                    influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
                }
            }
            return influxdbBucketDataEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            if (influxDBClient != null) {
                influxdbPool.getPool().returnObject(influxDBClient);
            }
        }
    }

    /**
     * 异常处理
     *
     * @param e
     */
    private void handlerException(Exception e) {
        String errMsg = e.getMessage();
        if (StringUtils.isNotEmpty(errMsg) && (errMsg.contains("Failed to connect") || errMsg.contains("Unable to validate object"))) {
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
            if (influxDB != null) {
                influxdbV1Pool.getPool().returnObject(influxDB);
            }
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
            if (influxDB != null) {
                influxdbV1Pool.getPool().returnObject(influxDB);
            }
        }
    }

    /**
     * 获取influxdb中指定bucket、measurement与时间段的数据，适用于v1.7/1.8
     *
     * @param bucket
     * @param measurement
     * @param startTime
     * @param stopTime
     * @param batch
     * @param offset
     * @return
     * @throws ArtificialException
     */
    private List<InfluxdbBucketDataEntity> selectBucketDataV1(String bucket, String measurement, String startTime, String stopTime, long batch, long offset) throws ArtificialException {
        // influxdb客户端
        InfluxDB influxDB = null;
        try {
            // 连接池中获取客户端
            influxDB = influxdbV1Pool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
            // 根据bucket与measurement获取内存中的表结构
            InfluxdbMeasurementEntity influxdbMeasurementEntity = BucketCache.measurementMap.get(bucket + ":" + measurement);
            // 查询语句
            String sql = "select * from " + measurement + " where time >= '" + startTime + "' and time <= '" + stopTime + "' limit " + batch + " offset " + offset;
            // 执行查询
            QueryResult queryResult = influxDB.query(new Query(sql, bucket));
            // 结果空则返回空列表
            if (queryResult == null) {
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
                        // 首先封装公共部分
                        influxdbBucketDataEntity.setInfluxdbMeasurementEntity(BucketCache.measurementMap.get(bucket + ":" + measurement));
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
            return influxdbBucketDataEntityList;
        } catch (Exception e) {
            handlerException(e);
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            if (influxDB != null) {
                influxdbV1Pool.getPool().returnObject(influxDB);
            }
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
        if (queryResult == null) {
            return bucketSet;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
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
    private List<Bucket> getBucketsV2(InfluxDBClient influxDBClient) {
        // 获取所有bucket列表
        List<Bucket> buckets = influxDBClient.getBucketsApi().findBuckets();
        // 返回非空结果
        return buckets != null ? buckets : new ArrayList<>();
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
            if (result == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
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
        if (showMeasurementResult == null) {
            // 将空array放入结果集
            return measurementSet;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showMeasurementResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null) {
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
        QueryResult queryResult = influxDB.query(new Query("show field keys from " + measurement, bucket));
        // 结果空则返回空map
        if (queryResult == null) {
            return fieldMap;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null || record.size() == 0) {
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
        InfluxQLQuery showFieldSql = new InfluxQLQuery("show field keys from " + measurement, bucket);
        InfluxQLQueryResult showFieldResult = influxDBClient.getInfluxQLQueryApi().query(showFieldSql);
        // 结果空则返回空map
        if (showFieldResult == null) {
            return fieldMap;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showFieldResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null) {
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
        QueryResult queryResult = influxDB.query(new Query("show tag keys from " + measurement, bucket));
        // 结果空则返回空set
        if (queryResult == null) {
            return tagSet;
        }
        for (QueryResult.Result result : queryResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (QueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
                    continue;
                }
                for (List<Object> record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null) {
                        continue;
                    }
                    tagSet.add(record.get(0).toString());
                }
            }
        }
        return tagSet;
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
        InfluxQLQuery showTagSql = new InfluxQLQuery("show tag keys from " + measurement, bucket);
        InfluxQLQueryResult showTagResult = influxDBClient.getInfluxQLQueryApi().query(showTagSql);
        // 结果空则返回空set
        if (showTagResult == null) {
            return tagSet;
        }
        // 遍历封装
        for (InfluxQLQueryResult.Result result : showTagResult.getResults()) {
            // 空则跳过并继续
            if (result == null) {
                continue;
            }
            for (InfluxQLQueryResult.Series series : result.getSeries()) {
                // 空则跳过并继续
                if (series == null) {
                    continue;
                }
                for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                    // 空则跳过并继续
                    if (record == null) {
                        continue;
                    }
                    tagSet.add(record.getValues()[0].toString());
                }
            }
        }
        return tagSet;
    }
}

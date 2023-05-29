package com.taosdata.service.impl;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.InfluxQLQuery;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.influxdb.query.InfluxQLQueryResult;
import com.taosdata.caches.BucketCache;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.ResEnums;
import com.taosdata.service.InfluxdbService;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.exception.ArtificialException;
import com.taosdata.utils.influxdb.InfluxdbPoolAutoConfig;
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

    @Resource
    InfluxdbPoolAutoConfig influxdbPool;

    /**
     * 获取influxdb中所有bucket
     *
     * @return
     * @throws ArtificialException
     */
    @Override
    public List<InfluxdbBucketEntity> selectAllBuckets() throws ArtificialException {
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketEntity> influxdbBucketEntityList = new ArrayList<>();
            // 获取所有bucket列表
            List<Bucket> bucketList = influxDBClient.getBucketsApi().findBuckets();
            // 判断队列是否空
            if (bucketList != null && bucketList.size() > 0) {
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
            }
            return influxdbBucketEntityList;
        } catch (Exception e) {
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), new Exception());
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
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
            // 返回结果
            List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = new ArrayList<>();
            // TODO 好像只支持token方式认证，所以在创建连接时屏蔽掉了username/password方式
            // 查询所有measurement
            try {
                InfluxQLQuery showMeasurementSql = new InfluxQLQuery("show measurements", bucket);
                InfluxQLQueryResult showMeasurementResult = influxDBClient.getInfluxQLQueryApi().query(showMeasurementSql);
                for (InfluxQLQueryResult.Result result : showMeasurementResult.getResults()) {
                    for (InfluxQLQueryResult.Series series : result.getSeries()) {
                        for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                            InfluxdbMeasurementEntity influxdbMeasurementEntity = new InfluxdbMeasurementEntity();
                            influxdbMeasurementEntity.setBucket(bucket);
                            influxdbMeasurementEntity.setMeasurement(record.getValues()[0].toString());
                            influxdbMeasurementEntity.setFieldMap(new HashMap<>());
                            influxdbMeasurementEntity.setTagSet(new HashSet<>());
                            influxdbMeasurementEntityList.add(influxdbMeasurementEntity);
                        }
                    }
                }
            } catch (Exception e) {
                // 忽略
            }
            // 遍历measurement列表
            for (InfluxdbMeasurementEntity influxdbMeasurementEntity : influxdbMeasurementEntityList) {
                // 查询所有field
                try {
                    InfluxQLQuery showFieldSql = new InfluxQLQuery("show field keys from " + influxdbMeasurementEntity.getMeasurement(), bucket);
                    InfluxQLQueryResult showFieldResult = influxDBClient.getInfluxQLQueryApi().query(showFieldSql);
                    for (InfluxQLQueryResult.Result result : showFieldResult.getResults()) {
                        for (InfluxQLQueryResult.Series series : result.getSeries()) {
                            for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                                influxdbMeasurementEntity.getFieldMap().put(record.getValues()[0].toString(), record.getValues()[1].toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    // 忽略
                }
                // 查询所有tag
                try {
                    InfluxQLQuery showTagSql = new InfluxQLQuery("show tag keys from " + influxdbMeasurementEntity.getMeasurement(), bucket);
                    InfluxQLQueryResult showTagResult = influxDBClient.getInfluxQLQueryApi().query(showTagSql);
                    for (InfluxQLQueryResult.Result result : showTagResult.getResults()) {
                        for (InfluxQLQueryResult.Series series : result.getSeries()) {
                            for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                                influxdbMeasurementEntity.getTagSet().add(record.getValues()[0].toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    // 忽略
                }
            }
            return influxdbMeasurementEntityList;
        } catch (Exception e) {
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
        // influxdb客户端
        InfluxDBClient influxDBClient = null;
        try {
            // 连接池中获取客户端
            influxDBClient = influxdbPool.getPool().borrowObject();
            // 返回列表
            List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
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
                    // 根据measurement确定表结构
                    influxdbBucketDataEntity.setInfluxdbMeasurementEntity(BucketCache.measurementMap.get(bucket + ":" + influxdbBucketDataEntity.getMeasurement()));
                    // 放入列表
                    influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
                }
            }
            return influxdbBucketDataEntityList;
        } catch (Exception e) {
            throw new ArtificialException(ResEnums.ERR_DATABASE.getCode(), ResEnums.ERR_DATABASE.getMsg(), e);
        } finally {
            if (influxDBClient != null) {
                influxdbPool.getPool().returnObject(influxDBClient);
            }
        }
    }
}

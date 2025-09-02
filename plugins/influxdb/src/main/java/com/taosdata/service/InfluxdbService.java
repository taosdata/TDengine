package com.taosdata.service;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.utils.exception.ArtificialException;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Influxdb数据库操作服务类
 *
 * @author ZYP
 */
public interface InfluxdbService {

    /**
     * 单次连接，查询指定influxdb中schema信息
     *
     * @param url
     * @param token
     * @param orgId
     * @return
     * @throws ArtificialException
     */
    JSONObject fetchSchemaInfo(String url, String token, String orgId) throws ArtificialException;

    /**
     * 单次连接，查询指定influxdb中schema信息，适用于v1.7/1.8
     *
     * @param url
     * @param username
     * @param password
     * @return
     * @throws ArtificialException
     */
    JSONObject fetchSchemaInfoV1(String url, String username, String password) throws ArtificialException;

    /**
     * 获取influxdb中所有bucket
     *
     * @param orgId
     * @return
     * @throws ArtificialException
     */
    List<InfluxdbBucketEntity> selectAllBuckets(String orgId) throws ArtificialException;

    /**
     * 获取指定bucket中所有measurement
     *
     * @param bucket
     * @return
     */
    List<InfluxdbMeasurementEntity> selectAllMeasurements(String bucket) throws ArtificialException;

    /**
     * 获取指定bucket、measurement的所有字段
     *
     * @param bucket
     * @param measurement
     * @return
     * @throws ArtificialException
     */
    Map<String, String> selectAllFields(String bucket, String measurement) throws ArtificialException;

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
    Instant getFirstTimestampInRange(String orgId, String bucket, String measurement, String startTime) throws ArtificialException;

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
    List<InfluxdbBucketDataEntity> selectBucketData(String orgId, String bucket, String measurement, String field, String startTime, String stopTime, long batch, long offset) throws ArtificialException;

    /**
     * @return 具体的 influxdb 的版本
     */
    String getInfluxdbVersion();

    /**
     * 获取 tag set
     * @param bucket
     * @param measurement
     * @return
     * @throws ArtificialException
     */
    List<List<Pair<String, String>>> getTagSet(String bucket, String measurement) throws ArtificialException;

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
    List<InfluxdbBucketDataEntity> selectBucketDataV1(String bucket, String measurement, String tagCondition, String startTime, String stopTime, long batch, long offset) throws ArtificialException;
}

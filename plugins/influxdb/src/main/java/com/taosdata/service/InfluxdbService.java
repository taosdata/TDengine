package com.taosdata.service;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbBucketEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.utils.exception.ArtificialException;

import java.util.List;

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
     * @return
     * @throws ArtificialException
     */
    JSONObject fetchSchemaInfo(String url, String token) throws ArtificialException;

    /**
     * 获取influxdb中所有bucket
     *
     * @return
     * @throws ArtificialException
     */
    List<InfluxdbBucketEntity> selectAllBuckets() throws ArtificialException;

    /**
     * 获取指定bucket中所有measurement
     *
     * @param bucket
     * @return
     */
    List<InfluxdbMeasurementEntity> selectAllMeasurements(String bucket) throws ArtificialException;

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
    List<InfluxdbBucketDataEntity> selectBucketData(String orgId, String bucket, String measurement, String startTime, String stopTime, long batch, long offset) throws ArtificialException;
}

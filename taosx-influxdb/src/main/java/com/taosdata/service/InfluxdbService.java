package com.taosdata.service;

import com.influxdb.client.InfluxDBClient;
import com.taosdata.model.dto.init.InfluxdbConnectionParam;
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
     * 获取influxdb数据库连接
     *
     * @param param
     * @return
     * @throws ArtificialException
     */
    InfluxDBClient getInfluxDBClient(InfluxdbConnectionParam param) throws ArtificialException;

    /**
     * 获取influxdb中所有bucket
     *
     * @param influxDBClient
     * @return
     * @throws ArtificialException
     */
    List<InfluxdbBucketEntity> selectAllBuckets(InfluxDBClient influxDBClient) throws ArtificialException;

    /**
     * 获取指定bucket中所有measurement
     *
     * @param influxDBClient
     * @param bucket
     * @return
     * @throws ArtificialException
     */
    List<InfluxdbMeasurementEntity> selectAllMeasurements(InfluxDBClient influxDBClient, String bucket);

    /**
     * 获取influxdb中指定bucket与时间段的数据
     *
     * @param influxDBClient
     * @param orgId
     * @param bucket
     * @param startTime
     * @param stopTime
     * @param batch
     * @param offset
     * @return
     * @throws ArtificialException
     */
    List<InfluxdbBucketDataEntity> selectBucketData(InfluxDBClient influxDBClient, String orgId, String bucket, String startTime, String stopTime, long batch, long offset) throws ArtificialException;
}

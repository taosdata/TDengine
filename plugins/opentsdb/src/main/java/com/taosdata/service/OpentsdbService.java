package com.taosdata.service;

import com.alibaba.fastjson.JSONArray;
import com.taosdata.model.entity.OpentsdbDataEntity;
import com.taosdata.utils.exception.ArtificialException;

import javax.annotation.Nullable;
import java.util.List;

/**
 * OpenTSDB数据库操作服务类（OpenTSDB目前无身份验证和访问控制系统）
 *
 * @author ZYP
 */
public interface OpentsdbService {

    /**
     * 获取所有metric列表
     *
     * @param url
     * @return
     * @throws ArtificialException
     */
    JSONArray fetchMetricList(@Nullable String url) throws ArtificialException;

    /**
     * 获取opentsdb中指定metric与时间段的数据
     *
     * @param url
     * @param metric
     * @param startTime
     * @param stopTime
     * @return
     * @throws ArtificialException
     */
    List<OpentsdbDataEntity> fetchData(@Nullable String url, String metric, String startTime, String stopTime) throws ArtificialException;
}

package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.Map;
import java.util.Set;

/**
 * influxdb measurement实体类
 *
 * @author ZYP
 */
@Data
public class InfluxdbMeasurementEntity {

    private String bucket;

    private String measurement;
    private Map<String, String> fieldMap;
    private Set<String> tagSet;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

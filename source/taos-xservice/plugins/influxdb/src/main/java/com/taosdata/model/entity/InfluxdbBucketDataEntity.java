package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * influxdb data实体类
 *
 * @author ZYP
 */
@Getter
@Setter
public class InfluxdbBucketDataEntity implements Cloneable {

    private InfluxdbMeasurementEntity influxdbMeasurementEntity;

    private String measurement;
    private String table;
    private Instant time;
    private String field;
    private Object value;
    //use TreeMap
    private Map<String, Object> tags;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }

    @Override
    public InfluxdbBucketDataEntity clone() throws CloneNotSupportedException {
        InfluxdbBucketDataEntity clone = (InfluxdbBucketDataEntity) super.clone();
        clone.tags = new TreeMap<>();
        clone.tags.putAll(this.tags);
        return clone;
    }
}

package com.taosdata.model.entity;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import java.util.Map;
import java.util.TreeSet;

/**
 * influxdb measurement实体类
 *
 * @author ZYP
 */
@Getter
@Setter
public class InfluxdbMeasurementEntity {

    private String bucket;

    private String measurement;
    private Map<String, String> fieldMap;
    private TreeSet<String> tagSet;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

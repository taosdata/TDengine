package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.Set;

/**
 * opentsdb metric实体类
 *
 * @author ZYP
 */
@Data
public class OpentsdbMetricEntity {

    private String metric;
    private Set<String> tagSet;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

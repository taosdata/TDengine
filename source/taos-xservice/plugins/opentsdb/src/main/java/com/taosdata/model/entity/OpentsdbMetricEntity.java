package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

/**
 * opentsdb metric实体类
 *
 * @author ZYP
 */
@Getter
@Setter
public class OpentsdbMetricEntity {

    private String metric;
    private Set<String> tagSet;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

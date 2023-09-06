package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * opentsdb data实体类
 *
 * @author ZYP
 */
@Data
public class OpentsdbDataEntity implements Cloneable {

    private OpentsdbMetricEntity opentsdbMetricEntity;

    private String metric;
    private String table;
    private Map<String, String> aggregateTags;
    private Map<String, Object> tags;
    private List<OpentsdbDataPointEntity> dps;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }

    @Override
    public OpentsdbDataEntity clone() throws CloneNotSupportedException {
        OpentsdbDataEntity clone = (OpentsdbDataEntity) super.clone();
        clone.aggregateTags = new HashMap<>();
        clone.aggregateTags.putAll(this.aggregateTags);
        clone.tags = new HashMap<>();
        clone.tags.putAll(this.tags);
        return clone;
    }
}

package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * opentsdb data实体类
 *
 * @author ZYP
 */
@Data
public class OpentsdbDataPointEntity {

    private long timestamp;
    private Object value;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

/**
 * opentsdb data实体类
 *
 * @author ZYP
 */
@Getter
@Setter
public class OpentsdbDataPointEntity {

    private long timestamp;
    private Object value;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

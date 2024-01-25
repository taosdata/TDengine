package com.taosdata.model.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * influxdb bucket实体类
 *
 * @author ZYP
 */
@Getter
@Setter
public class InfluxdbBucketEntity {

    private String bucketId;
    private String bucketType;
    private String bucketName;
    private String bucketDescription;
    private String orgId;
    private Date createTime;
    private Date updateTime;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

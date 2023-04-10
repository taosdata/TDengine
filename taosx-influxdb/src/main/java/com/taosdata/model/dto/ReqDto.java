package com.taosdata.model.dto;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * 请求实体类
 *
 * @author ZYP
 */
@Data
public class ReqDto {

    /**
     * 账号
     */
    private String userid;

    /**
     * 签名
     */
    private String sign;

    /**
     * 时间戳
     */
    private String timestamp;

    /**
     * 请求内容
     */
    private RequestDto data;

    /**
     * 源ip
     */
    private String ip;

    /**
     * 接收时间
     */
    private String recvTime;

    /**
     * 开始时间
     */
    private long start;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

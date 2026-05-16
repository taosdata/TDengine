package com.taosdata.netty.model.dto;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * 消息结构体
 *
 * @author ZYP
 */
@Data
public class MessageDto {

    /**
     * 版本号
     */
    private byte version;

    /**
     * 消息类型
     */
    private byte msgType;

    /**
     * 序列号
     */
    private long seq;

    /**
     * 消息体
     */
    private byte[] body;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

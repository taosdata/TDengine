package com.taosdata.netty.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 消息内容结构体
 *
 * @author ZYP
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MessageBodyInfluxdbDto extends MessageBodyDto {

    /**
     * 请求参数
     */
    /**
     * 数据库地址
     */
    private String url;

    /**
     * 鉴权类型：1 账号密码、2 TOKEN
     */
    private String authType;

    /**
     * 数据库用户
     */
    private String username;

    /**
     * 数据库密码
     */
    private String password;

    /**
     * 令牌
     */
    private String token;

    /**
     * 机构ID
     */
    private String orgId;

    /**
     * bucket数组
     */
    private String[] buckets;

    /**
     * 用于实体类映射
     */
    public static final String TYPE = "Influxdb";

    public MessageBodyInfluxdbDto() {
        setType(TYPE);
    }
}

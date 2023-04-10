package com.taosdata.model.dto.init;

import lombok.Data;

/**
 * 检查源数据库连接
 *
 * @author ZYP
 */
@Data
public class InfluxdbConnectionParam {

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
}

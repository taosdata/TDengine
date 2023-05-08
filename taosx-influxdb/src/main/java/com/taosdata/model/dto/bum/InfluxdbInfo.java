package com.taosdata.model.dto.bum;

import lombok.Data;

/**
 * Influxdb信息
 *
 * @author ZYP
 */
@Data
public class InfluxdbInfo {

    /**
     * 服务地址
     */
    private String serverAddr;

    /**
     * 创建的对象总数
     */
    private long createdCount;

    /**
     * 销毁的对象总数
     */
    private long destroyedCount;

    /**
     * 借用的对象总数
     */
    private long borrowedCount;

    /**
     * 归还的对象总数
     */
    private long returnedCount;

    /**
     * 状态
     */
    private int status;

    /**
     * 描述
     */
    private String description;
}

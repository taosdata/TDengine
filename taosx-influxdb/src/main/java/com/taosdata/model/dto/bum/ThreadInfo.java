package com.taosdata.model.dto.bum;

import lombok.Data;

import java.util.Date;

/**
 * 线程信息
 *
 * @author ZYP
 */
@Data
public class ThreadInfo {

    /**
     * 线程名
     */
    private String name;

    /**
     * 启动时间
     */
    private Date startTime;

    /**
     * 上次执行时间
     */
    private Date lastTime;

    /**
     * 上次执行耗时
     */
    private long lastTake;

    /**
     * 状态
     */
    private int status;

    /**
     * 描述
     */
    private String description;
}

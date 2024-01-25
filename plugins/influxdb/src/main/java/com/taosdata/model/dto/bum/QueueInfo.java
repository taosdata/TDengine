package com.taosdata.model.dto.bum;

import lombok.Getter;
import lombok.Setter;

/**
 * 内存队列信息
 *
 * @author ZYP
 */
@Getter
@Setter
public class QueueInfo {

    /**
     * 队列名
     */
    private String name;

    /**
     * 配置大小
     */
    private long limit;

    /**
     * 当前大小
     */
    private long length;
}

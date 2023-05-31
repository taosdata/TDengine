package com.taosdata.model.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 数据请求
 *
 * @author ZYP
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class DataQuery<T> extends RequestDto {

    /**
     * 请求参数
     */
    private T param;

    /**
     * 用于实体类映射
     */
    public static final String SERVICE = "DataQuery";

    public DataQuery() {
        setService(SERVICE);
    }
}

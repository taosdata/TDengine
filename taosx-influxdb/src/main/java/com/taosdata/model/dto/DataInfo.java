package com.taosdata.model.dto;

import lombok.Data;

/**
 * 数据信息
 *
 * @author ZYP
 */
@Data
public class DataInfo<T> extends ResponseDto {

    /**
     * 结果数据
     */
    private T data;

    public DataInfo(T data) {
        this.data = data;
    }
}

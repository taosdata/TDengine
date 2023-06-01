package com.taosdata.model.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

/**
 * 请求内容实体类，用于构建请求实体类，实现实体类反射
 *
 * @author ZYP
 */
@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "service")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(value = DataQuery.class, name = DataQuery.SERVICE)
})
public abstract class RequestDto {

    /**
     * 服务
     */
    private String service;
}

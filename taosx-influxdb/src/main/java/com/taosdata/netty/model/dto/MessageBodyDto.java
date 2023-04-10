package com.taosdata.netty.model.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

/**
 * 消息内容结构体
 *
 * @author ZYP
 */
@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(value = MessageBodyInfluxdbDto.class, name = MessageBodyInfluxdbDto.TYPE)
})
public abstract class MessageBodyDto {

    private String type;
}

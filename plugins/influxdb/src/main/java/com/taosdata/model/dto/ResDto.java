package com.taosdata.model.dto;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * 响应实体类
 *
 * @author ZYP
 */
@Data
public class ResDto {

    /**
     * 响应代码
     */
    private String code;

    /**
     * 响应描述
     */
    private String msg;

    /**
     * 响应数据
     */
    private ResponseDto data;

    /**
     * 耗时
     */
    private long usetime;

    @Override
    public String toString() {
        Object json = JSONObject.toJSON(this);
        return json.toString();
    }
}

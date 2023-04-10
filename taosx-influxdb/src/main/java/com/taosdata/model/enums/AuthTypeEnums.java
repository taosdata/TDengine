package com.taosdata.model.enums;

/**
 * 校验类型枚举类
 *
 * @author ZYP
 */
public enum AuthTypeEnums {

    PASSWORD("1", "密码"), TOKEN("2", "令牌");

    /**
     * 字段内容
     */
    private String code;

    /**
     * 字段描述
     */
    private String msg;

    AuthTypeEnums(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getCode() {
        return this.code;
    }

    public String getMsg() {
        return this.msg;
    }
}

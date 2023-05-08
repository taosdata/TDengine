package com.taosdata.model.enums;

/**
 * 响应枚举类
 *
 * @author ZYP
 */
public enum ResEnums {

    /**
     * 成功
     */
    SUCCESS("200", "成功"),

    /**
     * 失败
     */
    FAILED("201", "失败"),

    /**
     * 错误
     */
    ERR_PARAM("301", "参数转换错误"),
    ERR_DATABASE("302", "数据库错误"),

    /**
     * 异常
     */
    EXCEPTION("500", "系统异常");

    /**
     * 响应代码
     */
    private String code;

    /**
     * 响应内容
     */
    private String msg;

    ResEnums(String code, String msg) {
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

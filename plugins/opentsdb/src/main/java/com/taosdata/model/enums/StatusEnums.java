package com.taosdata.model.enums;

/**
 * 状态枚举类
 *
 * @author ZYP
 */
public enum StatusEnums {

    /**
     * 未知
     */
    UNKNOWN(0, "unknown"),

    /**
     * 正常
     */
    NORMAL(1, "normal"),

    /**
     * 失败
     */
    FAILED(2, "failed"),

    /**
     * 高负载
     */
    HIGH_LOAD(3, "high-load"),

    /**
     * 高延迟
     */
    HIGH_DELAY(4, "high-delay"),

    /**
     * 加载中
     */
    LOADING(8, "loading"),

    /**
     * 系统异常
     */
    EXCEPTION(9, "exception");

    /**
     * 状态代码
     */
    private int code;

    /**
     * 状态描述
     */
    private String desc;

    StatusEnums(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return this.code;
    }

    public String getDesc() {
        return this.desc;
    }
}

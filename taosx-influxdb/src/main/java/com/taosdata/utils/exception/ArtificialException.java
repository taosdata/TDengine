package com.taosdata.utils.exception;

import lombok.Data;

/**
 * 自定义异常
 *
 * @author ZYP
 */
@Data
public class ArtificialException extends Exception {

    /**
     * 错误代码
     */
    private String code;

    /**
     * 错误描述
     */
    private String msg;

    /**
     * 原始异常
     */
    private Exception e;

    public ArtificialException(String code, String msg, Exception e) {
        this.code = code;
        this.msg = msg;
        this.e = e;
    }

    @Override
    public String getMessage() {
        return String.format("{code: %s, msg: %s, e: %s}", this.code, this.msg, this.e.getMessage());
    }
}

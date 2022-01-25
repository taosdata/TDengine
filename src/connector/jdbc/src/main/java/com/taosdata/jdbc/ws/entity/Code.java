package com.taosdata.jdbc.ws.entity;

/**
 * response message info
 */
public enum Code {
    SUCCESS(0, "success"),

    ;

    private final int code;
    private final String message;

    Code(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static Code of(int code) {
        for (Code value : Code.values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}

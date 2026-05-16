package com.taosdata.jdbc.enums;

public enum TmqMessageType {
    TMQ_RES_INVALID(-1),

    TMQ_RES_DATA(1),

    TMQ_RES_TABLE_META(2),

    TMQ_RES_METADATA(3),
    ;

    final int code;

    TmqMessageType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}

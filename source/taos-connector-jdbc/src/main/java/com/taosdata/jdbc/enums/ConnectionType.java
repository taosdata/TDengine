package com.taosdata.jdbc.enums;

public enum ConnectionType {
    // jni
    JNI("jni"),
    // http
    HTTP("http"),

    // websocket
    WS("ws"),
    WEBSOCKET("websocket"),
    ;

    final String type;

    ConnectionType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}

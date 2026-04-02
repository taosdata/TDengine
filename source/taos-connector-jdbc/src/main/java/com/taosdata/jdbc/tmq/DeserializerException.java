package com.taosdata.jdbc.tmq;

public class DeserializerException extends RuntimeException {

    public DeserializerException(String message, Exception e) {
        super(message, e);
    }
}

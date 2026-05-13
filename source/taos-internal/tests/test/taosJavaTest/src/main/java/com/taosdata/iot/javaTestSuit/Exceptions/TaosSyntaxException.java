package com.taosdata.iot.javaTestSuit.Exceptions;

public class TaosSyntaxException extends RuntimeException {

    public TaosSyntaxException(String reason) {
        super(reason);
    }

    public TaosSyntaxException() {
        super();
    }
}

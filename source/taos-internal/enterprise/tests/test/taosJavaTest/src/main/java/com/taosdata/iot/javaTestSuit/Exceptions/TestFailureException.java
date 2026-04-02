package com.taosdata.iot.javaTestSuit.Exceptions;

public class TestFailureException extends RuntimeException {

    public TestFailureException(String reason) {
        super(reason);
    }
}

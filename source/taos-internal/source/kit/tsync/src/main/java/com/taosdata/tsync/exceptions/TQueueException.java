package com.taosdata.tsync.exceptions;

public class TQueueException extends Exception {

    public TQueueException(String message) {
        super(message);
    }

    public TQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}

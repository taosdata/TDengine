package com.taosdata.iot.clients.exceptions;

/**
 * General exception for errors raised during a subscribe/consume process.
 */
public class TaosRuntimeException extends RuntimeException {
    public TaosRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaosRuntimeException(String message) {
        super(message);
    }

    public TaosRuntimeException(Throwable cause) {
        super(cause);
    }

    public TaosRuntimeException() {
        super();
    }
}

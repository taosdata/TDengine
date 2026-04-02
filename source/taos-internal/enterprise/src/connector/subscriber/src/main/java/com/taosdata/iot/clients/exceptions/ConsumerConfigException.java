package com.taosdata.iot.clients.exceptions;

/**
 * This exception is raised when a TaosConsumer initialization has an invalid configuration property.
 */
public class ConsumerConfigException extends TaosRuntimeException {

    public ConsumerConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumerConfigException(String message) {
        super(message);
    }

    public ConsumerConfigException(Throwable cause) {
        super(cause);
    }

    public ConsumerConfigException() {
        super();
    }
}

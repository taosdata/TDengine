package com.taosdata.iot.clients.exceptions;

/**
 * This exception is raised when a TaosConsumer.poll() is executed with an invalid subscription.
 */
public class InvalidSubscriptionException extends TaosRuntimeException{
    public InvalidSubscriptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSubscriptionException(String message) {
        super(message);
    }

    public InvalidSubscriptionException(Throwable cause) {
        super(cause);
    }

    public InvalidSubscriptionException() {
        super();
    }
}

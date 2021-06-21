package com.taosdata.tsync.entity;

import java.util.UUID;
import java.util.concurrent.Callable;

public class CallableTask<T> {
    private final UUID id;
    private final Callable<T> callable;

    public CallableTask(Callable<T> runnable) {
        this.id = UUID.randomUUID();
        this.callable = runnable;
    }

    public UUID getId() {
        return id;
    }

    public Callable<T> getCallable() {
        return callable;
    }
}

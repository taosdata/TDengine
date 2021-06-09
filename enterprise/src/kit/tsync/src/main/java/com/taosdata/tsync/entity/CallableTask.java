package com.taosdata.tsync.entity;

import java.util.concurrent.Callable;

public class CallableTask<T> {
    private final int id;
    private final Callable<T> callable;

    public CallableTask(int id, Callable<T> runnable) {
        this.id = id;
        this.callable = runnable;
    }

    public int getId() {
        return id;
    }

    public Callable<T> getCallable() {
        return callable;
    }
}

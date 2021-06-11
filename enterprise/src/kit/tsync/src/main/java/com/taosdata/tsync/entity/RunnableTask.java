package com.taosdata.tsync.entity;

public class RunnableTask {
    private final int id;
    private final Runnable runnable;

    public RunnableTask(int id, Runnable runnable) {
        this.id = id;
        this.runnable = runnable;
    }

    public int getId() {
        return this.id;
    }

    public Runnable getRunnable() {
        return this.runnable;
    }
}
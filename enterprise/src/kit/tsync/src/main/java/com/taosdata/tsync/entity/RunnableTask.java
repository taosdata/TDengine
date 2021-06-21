package com.taosdata.tsync.entity;

import java.util.UUID;

public class RunnableTask {
    private final UUID id;
    private final Runnable runnable;

    public RunnableTask( Runnable runnable) {
        this.id = UUID.randomUUID();
        this.runnable = runnable;
    }

    public UUID getId() {
        return this.id;
    }

    public Runnable getRunnable() {
        return this.runnable;
    }
}
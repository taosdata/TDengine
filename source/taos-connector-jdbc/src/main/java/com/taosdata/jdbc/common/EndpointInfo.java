package com.taosdata.jdbc.common;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EndpointInfo {
    private final AtomicBoolean online;
    private final AtomicInteger connectCount;
    public EndpointInfo() {
        this.online = new AtomicBoolean(true);
        this.connectCount = new AtomicInteger(0);
    }

    public boolean isOnline() {
        return online.get();
    }

    public int getConnectCount() {
        return connectCount.get();
    }

    public void setOnline() {
        this.online.set(true);
    }

    public boolean setOffline() {
        return online.compareAndSet(true, false);
    }

    public void incrementConnectCount() {
        this.connectCount.incrementAndGet();
    }

    public void decrementConnectCount() {
        this.connectCount.decrementAndGet();
    }

}

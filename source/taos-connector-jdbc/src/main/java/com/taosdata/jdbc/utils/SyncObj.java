package com.taosdata.jdbc.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SyncObj {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    public void signal() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings({"java:S2274","java:S899"})
    public void await() throws InterruptedException {
        lock.lock();
        try {
            condition.await(10, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }
}
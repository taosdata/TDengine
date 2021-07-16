package com.taosdata.tsync.service;

@FunctionalInterface
public interface Stoppable {
    void shutdown();
}

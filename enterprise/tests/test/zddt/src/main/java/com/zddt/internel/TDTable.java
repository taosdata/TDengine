package com.zddt.internel;

public class TDTable {
    public int threadIndex;
    public long lastTimestamp;

    public TDTable(int threadIndex, long lastTimestamp) {
        this.threadIndex = threadIndex;
        this.lastTimestamp = lastTimestamp;
    }
}

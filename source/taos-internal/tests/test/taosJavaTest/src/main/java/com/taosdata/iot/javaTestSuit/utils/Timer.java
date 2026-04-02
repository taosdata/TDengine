package com.taosdata.iot.javaTestSuit.utils;

import java.math.BigDecimal;

public class Timer {

    private long start = 0;
    private long stop = 0;
    private long time = 0;

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return stop;
    }

    public long getTimeInNanoSeconds() {
        return time;
    }

    public void start() {
        start = System.nanoTime();
    }

    public void stop() {
        stop = System.nanoTime();
        time += (stop - start);
    }

    public void reset() {
        start = System.nanoTime();
        stop = start;
        time = 0;
    }

    public void printTimeInSeconds() {

        System.out.printf("Time used: %fs\n", getTimeInSeconds());
    }

    public BigDecimal getTimeInSeconds() {
        BigDecimal second = BigDecimal.valueOf(time).divide(BigDecimal.valueOf(1e9));
        return second;
    }
}

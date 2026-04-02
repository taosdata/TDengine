package com.taosdata.tsync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PrintCountRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PrintCountRunnableTask.class);

    private final Countable countable;
    private final long interval;

    private volatile boolean isShutdown;

    public PrintCountRunnableTask(Countable countable, long interval) {
        this.countable = countable;
        this.interval = interval;
    }

    @Override
    public void run() {
        logger.info("start count printing");
        while (!isShutdown) {
            long start = countable.getCount();
            try {
                TimeUnit.MILLISECONDS.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long end = countable.getCount();
            logger.info("count from: " + start + ", to: " + end + ", speed: " + ((end - start) / (interval / 1000.0)) + " count/s.");
        }
        logger.info("stop count printing");
    }

    public void shutdown() {
        this.isShutdown = true;
    }
}

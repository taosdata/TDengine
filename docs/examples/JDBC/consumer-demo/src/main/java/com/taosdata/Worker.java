package com.taosdata;

import com.google.common.util.concurrent.RateLimiter;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

public class Worker implements Runnable {

    int sleepTime;
    int rate;

    ForkJoinPool pool = new ForkJoinPool();
    Semaphore semaphore;

    TaosConsumer<Bean> consumer;

    public Worker(Properties prop, Config config) throws SQLException {
        consumer = new TaosConsumer<>(prop);
        consumer.subscribe(Collections.singletonList(Config.TOPIC));
        semaphore = new Semaphore(config.getProcessCapacity());
        sleepTime = config.getPollSleep();
        rate = config.getRate();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                // Control request rate
                if (semaphore.tryAcquire()) {
                    ConsumerRecords<Bean> records = consumer.poll(Duration.ofMillis(sleepTime));
                    pool.submit(() -> {
                        RateLimiter limiter = RateLimiter.create(rate);
                        try {
                            for (ConsumerRecord<Bean> record : records) {
                                // Traffic control
                                limiter.acquire();
                                // Business data processing
                                System.out.println("[" + LocalDateTime.now() + "] Thread id:"
                                        + Thread.currentThread().getId() + " -> " + record.value());
                            }
                        } finally {
                            semaphore.release();
                        }
                    });
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

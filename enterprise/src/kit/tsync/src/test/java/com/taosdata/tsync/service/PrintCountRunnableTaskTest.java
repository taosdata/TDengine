package com.taosdata.tsync.service;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PrintCountRunnableTaskTest {

    @Test
    public void test() throws InterruptedException {

        MyCountable myCountable = new MyCountable();
        new Thread(myCountable).start();
        PrintCountRunnableTask printCountRunnableTask = new PrintCountRunnableTask(myCountable, 1000);

        Thread thread = new Thread(printCountRunnableTask);
        thread.start();

        TimeUnit.SECONDS.sleep(10);

        printCountRunnableTask.shutdown();
        thread.join();
    }

    private class MyCountable implements Runnable, Countable {
        private AtomicLong count = new AtomicLong(0);

        @Override
        public long getCount() {
            return count.get();
        }

        @Override
        public void run() {
            while (true) {
                count.getAndIncrement();
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
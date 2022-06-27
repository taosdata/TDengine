package com.taos.example.highvolume;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

// ANCHOR: main
public class FastWriteExample {

    final static int readTaskCount = 2;
    final static int writeTaskCount = 4;
    final static int taskQueueCapacity = 1000;
    final static List<BlockingQueue> taskQueues = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new LinkedBlockingDeque<>(taskQueueCapacity);
            WriteTask task = new WriteTask(queue);
            Thread t = new Thread(task);
            t.start();
        }

        // create reading tasks and start reading threads
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(taskQueues);
            Thread t = new Thread(task);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}

// ANCHOR_END: main
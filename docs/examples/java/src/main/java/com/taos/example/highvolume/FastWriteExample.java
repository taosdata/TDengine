package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

// ANCHOR: main
public class FastWriteExample {
    final static Logger logger = LoggerFactory.getLogger(FastWriteExample.class);

    final static int readTaskCount = 1;
    final static int writeTaskCount = 1;
    final static int taskQueueCapacity = 1000;
    final static List<BlockingQueue<String>> taskQueues = new ArrayList<>();
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();

    public static void stopAll() {
        logger.info("stopAll");
        readTasks.forEach(task -> task.stop());
        writeTasks.forEach(task -> task.stop());
    }

    public static void main(String[] args) throws InterruptedException {
        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(taskQueueCapacity);
            taskQueues.add(queue);
            WriteTask task = new WriteTask(queue);
            Thread t = new Thread(task);
            t.setName("WriteThread-" + i);
            t.start();
        }

        // create reading tasks and start reading threads
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, taskQueues);
            Thread t = new Thread(task);
            t.setName("ReadThread-" + i);
            t.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::stopAll));
    }
}

// ANCHOR_END: main
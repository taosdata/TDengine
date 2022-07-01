package com.taos.example.highvolume;

import java.util.ArrayList;
import java.util.List;

// ANCHOR: main
public class FastWriteExample {

    final static int readTaskCount = 1;
    final static int writeTaskCount = 4;
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        // Create writing tasks.
        // Every writing task contains a work thread and a task queue.
        for (int i = 0; i < writeTaskCount; ++i) {
            WriteTask task = new WriteTask();
            task.start();
            writeTasks.add(new WriteTask());
        }

        // Create reading tasks.
        // Every reading task contains a work thread and a reference to each writing task.
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, writeTasks);
            task.start();
            readTasks.add(task);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}

// ANCHOR_END: main
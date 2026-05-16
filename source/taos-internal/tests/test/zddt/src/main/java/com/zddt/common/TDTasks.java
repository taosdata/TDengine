package com.zddt.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDTasks {
    private static TDTasksThread[] threads = null;
    private static ExecutorService executorService = null;
    private static int curTaskIndex = 0;

    public static void run() {
        initThreads();
        runThreads();
        waitThreads();
    }

    private static void initThreads() {
        threads = new TDTasksThread[TDConfig.jdbcThreadNum];
        for (int threadIndex = 0; threadIndex < TDConfig.jdbcThreadNum; ++threadIndex) {
            TDTasksThread thread = new TDTasksThread(threadIndex);
            threads[threadIndex] = thread;
        }
    }

    private static void runThreads() {
        TDLog.print(String.format("%d tasks %d threads start to run", TDConfig.jdbcSqls.size(), threads.length));
        executorService = Executors.newFixedThreadPool(threads.length);
        for (TDTasksThread thread : threads) {
            executorService.execute(thread);
        }
    }

    private static void waitThreads() {
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        TDLog.print(String.format("%d tasks %d threads run finished", TDConfig.jdbcSqls.size(), threads.length));
    }

    public static synchronized int getNextTaskIndex() {
        if (curTaskIndex == TDConfig.jdbcSqls.size()) {
            return -1;
        } else {
            curTaskIndex++;
            return curTaskIndex - 1;
        }
    }
}

class TDTasksThread implements Runnable {
    private int threadIndex;

    public TDTasksThread(int threadIndex) {
        this.threadIndex = threadIndex;
    }

    public void run() {
        int taskIndex = TDTasks.getNextTaskIndex();
        if (taskIndex != -1) {
            TDTask task = new TDTask(taskIndex, TDConfig.jdbcSqls.get(taskIndex));
            task.run();
        }
    }
}

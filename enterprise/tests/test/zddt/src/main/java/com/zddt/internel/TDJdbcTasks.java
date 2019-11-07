package com.zddt.internel;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDJdbcTasks {
    private static TDJdbcTaskThread[] taskThreads = null;
    private static ExecutorService executorService = null;
    private static int curTaskIndex = 0;

    public static void run() {
        initThreads();
        runThreads();
        waitThreads();
    }

    private static void initThreads() {
        taskThreads = new TDJdbcTaskThread[TDConfig.jdbcThreadNum];
        for (int taskThreadIndex = 0; taskThreadIndex < TDConfig.jdbcThreadNum; ++taskThreadIndex) {
            TDJdbcTaskThread thread = new TDJdbcTaskThread(taskThreadIndex);
            taskThreads[taskThreadIndex] = thread;
        }
    }

    private static void runThreads() {
        TDLog.print(String.format("jdbc tasks:%d taskThreads:%d start to run", TDConfig.jdbcSqls.size(), taskThreads.length));
        executorService = Executors.newFixedThreadPool(taskThreads.length);
        for (TDJdbcTaskThread thread : taskThreads) {
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
        TDLog.print(String.format("jdbc tasks:%d taskThreads:%d run finished", TDConfig.jdbcSqls.size(), taskThreads.length));
    }

    public static synchronized int getNextDsIndex() {
        if (curTaskIndex >= TDConfig.jdbcSqls.size()) {
            return -1;
        } else {
            curTaskIndex++;
            return curTaskIndex - 1;
        }
    }
}

class TDJdbcTaskThread implements Runnable {
    private int taskThreadIndex;
    TDJdbcTaskThread(int taskThreadIndex) {
        this.taskThreadIndex = taskThreadIndex;
    }
    public void run() {
        TDLog.print(String.format("taskThreadIndex:%d start to run", this.taskThreadIndex));

        while (true) {
            int taskIndex = TDJdbcTasks.getNextDsIndex();
            if (taskIndex == -1) {
                break;
            }
            TDLog.print(String.format("taskThreadIndex:%d task:%d start to run", taskThreadIndex, taskIndex));
            TDJdbcTask task = new TDJdbcTask(taskIndex, TDConfig.jdbcSqls.get(taskIndex));
            TDInsertThreads insertThreads = new TDInsertThreads(taskThreadIndex, task);
            insertThreads.run();
            TDLogDb.recordTask(task);
            TDLog.print(String.format("taskThreadIndex:%d task:%d run finished", taskThreadIndex, taskIndex));
        }

        TDLog.print(String.format("taskThreadIndex:%d run finished", this.taskThreadIndex));
    }
}

class TDJdbcTask extends TDTask {
    private static Connection jdbcConnection = null;

    public TDJdbcTask(int taskIndex, String sql) {
        this.taskIndex = taskIndex;
        this.taskName = sql;
    }

    public boolean init() {
        return true;
    }

    public TDLine getNextLine() throws Exception {
        return null;
    }

    public void close() { }
}

package com.zddt.internel;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDInsertThreads {
    private int insertedRows = 0;
    private static Connection connection = null;
    private static TDInsertThread[] threads = null;
    private ExecutorService executorService;
    private TDTask task;
    private int taskThreadIndex;

    public synchronized void addInsertRows(int insertedRows) {
        this.insertedRows += insertedRows;
    }

    public TDInsertThreads(int taskThreadIndex, TDTask task) {
        this.taskThreadIndex = taskThreadIndex;
        this.task = task;
        this.initThreads();
    }

    private void initThreads() {
        this.threads = new TDInsertThread[TDConfig.threadNum];
        for (int threadIndex = 0; threadIndex < threads.length; ++threadIndex) {
            TDInsertThread thread = new TDInsertThread(taskThreadIndex, threadIndex, task);
            this.threads[threadIndex] = thread;
        }
    }

    private void runThreads() {
        TDLog.print(String.format("task:%d, threads:%d start to run", this.task.getTaskIndex(), threads.length));
        executorService = Executors.newFixedThreadPool(this.threads.length);
        for (TDInsertThread thread : this.threads) {
            executorService.execute(thread);
        }
    }

    private void waitThreads() {
        for (TDInsertThread thread : this.threads) {
            thread.setStopFlag();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        TDLog.print(String.format("task:%d, threads:%d run finished", this.task.getTaskIndex(), threads.length));
    }

    public boolean run() {
        if (!task.init()) {
            TDLog.print(String.format("task:%d, init failed", task.getTaskIndex()));
        }

        this.runThreads();

        try {
            TDLine line = null;
            while ((line = task.getNextLine()) != null) {
                TDTable tb = TDDataDb.getTbThread(line);
                threads[tb.threadIndex].addLine(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("task:%d, processing failed, error:%s", this.task.getTaskIndex(), e.getMessage()));
            return false;
        } finally {
            this.waitThreads();
            TDLogDb.recordTask(task);
            task.close();
        }

        return true;
    }
}

class TDInsertThread implements Runnable {
    private int threadIndex;
    private boolean stopFlag;
    private TDLine[] lines;
    private int maxLines;
    private int readPos;
    private int writePos;
    private int insertedRows;
    private TDTask task;
    private TDConnection connection = null;

    public TDInsertThread(int taskThreadIndex, int threadIndex, TDTask task) {
        this.threadIndex = threadIndex;
        this.task = task;
        this.maxLines = TDConfig.cacheRows;
        this.lines = new TDLine[this.maxLines];
        this.readPos = 0;
        this.writePos = 0;
        this.stopFlag = false;
        this.insertedRows = 0;
        this.connection = TDDataDb.getConnection(taskThreadIndex, threadIndex);
    }

    public void setStopFlag() {
        stopFlag = true;
    }

    public void addLine(TDLine line) {
        while (writePos - readPos >= maxLines) {
            TDUtil.sleepMs(100);
            continue;
        }
        lines[writePos % maxLines] = line;
        writePos++;
    }

    public void run() {
        TDLog.print(String.format("task:%d, thread:%d start to run", task.getTaskIndex(), threadIndex));
        while (true) {
            if (readPos >= writePos) {
                if (stopFlag) {
                    break;
                } else {
                    TDUtil.sleepMs(100);
                    continue;
                }
            }

            doInsert();
        }

        task.addInsertRows(insertedRows);
        TDLog.print(String.format("task:%d, thread:%d run finished", task.getTaskIndex(), threadIndex));
    }

    private void doInsert() {
        StringBuilder sqlBuffer = new StringBuilder();
        sqlBuffer.append("import into");
        int batchSize = 0;

        for (int l = readPos; l < writePos; ++l) {
            TDLine csvLine = lines[l % maxLines];
            sqlBuffer.append(' ').append(TDConfig.datadbName).append('.').append(TDConfig.tablePrefix).append(csvLine.tableName)
                    .append(" values(").append(csvLine.timestamp);
            for (int i = 1; i < TDConfig.fields.length; ++i) {
                TDField field = TDConfig.fields[i];
                sqlBuffer.append(',');
                if (field.isTypeBinary) {
                    sqlBuffer.append('\'');
                }
                for (int col : field.columns) {
                    String colStr = csvLine.cols[col];
                    if (colStr == null) {
                        sqlBuffer.append("NULL");
                    } else if (colStr.length() == 0) {
                        sqlBuffer.append("NULL");
                    } else {
                        sqlBuffer.append(colStr);
                    }
                }
                if (field.isTypeBinary) {
                    sqlBuffer.append('\'');
                }
            }
            sqlBuffer.append(')');
            batchSize++;

            if (batchSize >= TDConfig.batchSize) {
                break;
            }
            if (sqlBuffer.toString().length() > 60000) {
                break;
            }
        }

        String processingSql = sqlBuffer.toString();
        int affectRows = executeInsertSql(processingSql);

        if (affectRows == batchSize) {
            insertedRows += affectRows;
        } else {
           TDLog.print(String.format("task:%d, thread:%d inserted:%d batch:%d in [%d, %d) but affectRows:%d, need insert one by one"
                        , task.getTaskIndex(), threadIndex, insertedRows, batchSize, readPos, readPos+batchSize, affectRows));

            for (long i = readPos; i < readPos+batchSize; ++i) {
                TDLine line = lines[(int)(i % maxLines)];
                String selectSql = line.getSelectSql();
                String insertSql = line.getInsertSql();
                int count = connection.executeQueryCount(selectSql);
                if (count == 1) {
                    insertedRows++;
                    TDLog.trace(String.format("task:%d, thread:%d inserted:%d pos:%d line:%d already exist, sql:%s"
                            , task.getTaskIndex(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                } else {
                    TDLog.trace(String.format("task:%d, thread:%d inserted:%d pos:%d line:%d not exist, sql:%s"
                            , task.getTaskIndex(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                    affectRows = executeInsertSql(insertSql);
                    if (affectRows == 1) {
                        insertedRows++;
                        TDLog.trace(String.format("task:%d, thread:%d inserted:%d pos:%d line:%d not exist, insert success, affectRows:%d sql:%s"
                                , task.getTaskIndex(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                    } else {
                        TDLog.error(String.format("task:%d, thread:%d inserted:%d pos:%d line:%d not exist, insert failed,affectRows:%d sql:%s"
                                , task.getTaskIndex(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                        TDLogDb.recordLine(task, line.lineIndex, connection.getErrorCode(), insertSql);
                    }
                }
            } // end for
        }

        readPos += batchSize;
    }

    private int executeInsertSql(String sql) {
        if (connection.executeUpdate(sql)) {
            return connection.getAffectrows();
        } else {
            return 0;
        }
    }
}


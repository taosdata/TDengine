package com.zddt.common;

import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDTask {
    private int taskIndex;
    private String taskSql;
    private int fetchedRows = 0;
    private int insertedRows = 0;
    private float fetchedTimeTs = 0;
    private float insertTimeTs = 0;
    private float createtbTimeTs = 0;
    private long beginTs = 0;
    private static Connection connection = null;
    private static TDTaskThread[] threads = null;
    private ExecutorService executorService;

    public int getTaskIndex() {
        return this.taskIndex;
    }

    public String getTaskSql() {
        return this.taskSql;
    }

    public int getFetchedRows() {
        return this.fetchedRows;
    }

    public int getInsertedRows() {
        return this.insertedRows;
    }

    public float getFetchedTimeSec() {
        return this.fetchedTimeTs;
    }

    public float getInsertTimeSec() {
        return this.insertTimeTs;
    }

    public float getCreatetbTimeSec() {
        return this.createtbTimeTs;
    }

    public void addInsertRows(int insertedRows) {
        this.insertedRows += insertedRows;
    }

    public void addInsertTimeSec(float insertTimeTs) {
        this.insertTimeTs += insertTimeTs;
    }

    public void addCreatetbTimeSec(float createtbTimeTs) {
        this.createtbTimeTs = createtbTimeTs;
    }

    public TDTask(int taskIndex, String taskSql) {
        this.taskIndex = taskIndex;
        this.taskSql = taskSql;
        this.initBeginTs();
        this.initThreads();
    }

    private void initThreads() {
        this.threads = new TDTaskThread[TDConfig.jdbcSubThreadNum];
        for (int threadIndex = 0; threadIndex < TDConfig.jdbcSubThreadNum; ++threadIndex) {
            TDTaskThread thread = new TDTaskThread(threadIndex);
            this.threads[threadIndex] = thread;
        }
    }

    private void runThreads() {
        TDLog.print(String.format("task:%d %d threads start to run, sql:%s", this.taskIndex, threads.length, taskSql));
        executorService = Executors.newFixedThreadPool(this.threads.length);
        for (TDTaskThread thread : this.threads) {
            executorService.execute(thread);
        }
    }

    private void waitThreads() {
        for (TDTaskThread thread : this.threads) {
            thread.setStopFlag();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        TDLog.print(String.format("task:%d %d threads run finished, sql:%s", this.taskIndex, threads.length, taskSql));
    }

    private void initBeginTs() {
        if (TDConfig.datadbMicroSecond) {
            beginTs = TDUtil.getTimeStampUs();
        } else {
            beginTs = TDUtil.getTimeStampMs();
        }
        while (beginTs <= TDConfig.fileBeginTimestamp) {
            TDUtil.sleepMs(100);
            if (TDConfig.datadbMicroSecond) {
                beginTs = TDUtil.getTimeStampUs();
            } else {
                beginTs = TDUtil.getTimeStampMs();
            }
        }
    }

    public boolean run() {
        this.runThreads();

        Statement stmt;
        ResultSet resSet = null;
        try {
//            stmt = connection.createStatement();
//            resSet = stmt.executeQuery(taskSql);
//            if (resSet == null) {
//                TDLog.error(String.format("failed to execute sql:%s", taskSql));
//                return false;
//            }
//
//            ResultSetMetaData metaData = resSet.getMetaData();
//            if (metaData == null) {
//                TDLog.error(String.format("no result set, sql:%s", taskSql));
//                return false;
//            }

            ArrayList<TDField> fields = new ArrayList<TDField>();
            ArrayList<TDField> tags = new ArrayList<TDField>();
            fields.add(new TDField("ts", "timestamp"));
            fields.add(new TDField("i", "int"));
            fields.add(new TDField("b", "binary", 20));
            tags.add(new TDField("t1", "int"));
            tags.add(new TDField("t2", "binary", 20));

            TDConfig.setSchema(fields, tags);
            if (!TDDataDb.createStb()) {
                TDLog.error(String.format("failed to create stable from jdbc sql:%s", taskSql));
                return false;
            }

            //            while (resSet.next()) {
//                TDLine line = null;
//                String tableName = "";
//                int threadIndex = TDDataDb.createTb(tableName);
//                threads[threadIndex].addLine(line);
//            }

            for (int i = 0; i < 100; ++i) {
                TDLine line = null;
                String tableName = "";
                int threadIndex = TDDataDb.createTb(tableName);
                threads[threadIndex].addLine(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            //TDLog.error(String.format("query failed, task:%d, sql:%s, code:%d, error:%s, fetchedRows:%d", taskIndex, taskSql, e.getErrorCode(), e.getErrorCode(), getFetchedRows()));
            return false;
        } finally {
            try {
                resSet.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.waitThreads();
        return true;
    }
}

class TDTaskThread implements Runnable {
    private TDCsv csv;
    private int threadIndex;
    private int lineBegin;
    private int lineEnd;
    private int insertedRows;
    private TDConnection connection = null;

    public int getLineEnd() {
        return this.lineEnd;
    }

    public int getLineBegin() {
        return this.lineBegin;
    }

    public int getThreadIndex() {
        return this.threadIndex;
    }

    public int getInsertedRows() {
        return this.insertedRows;
    }

    public TDTaskThread(int threadIndex) {

    }

    public void addLine(TDLine line) {

    }

    public void setStopFlag() {

    }

    public void init(int threadIndex) {
        this.threadIndex = threadIndex;
        this.csv = csv;
        this.lineBegin = lineBegin;
        this.lineEnd = lineEnd;
        this.connection = TDDataDb.getConnection(threadIndex);
        if (lineEnd < lineBegin) {
            lineEnd = lineBegin;
        }

        TDLog.print(String.format("file:%s thread:%d init, lineBegin:%d, lineEnd:%d", csv.getFileName(), threadIndex, lineBegin, lineEnd));
    }

    public void run() {
        if (lineEnd <= lineBegin) {
            return;
        }
        TDLog.print(String.format("file:%s thread:%d is running", csv.getFileName(), threadIndex));

        insertedRows = 0;
        int processingBegin = lineBegin;
        while (processingBegin < lineEnd) {
            Object ret[] = csv.getProcessingSql(processingBegin, lineEnd);
            String processingSql = (String) ret[0];
            int processingEnd = (Integer) ret[1];
            int processingRows = processingEnd - processingBegin;
            int affectRows = executeInsertSql(processingSql);

            if (affectRows == processingRows) {
                insertedRows += affectRows;
            } else {
                TDLog.print(String.format("file:%s thread:%d inserted:%d rows:%d in [%d, %d) but affectRows:%d, need insert one by one"
                        , csv.getFileName(), threadIndex, insertedRows, processingRows, processingBegin, processingEnd, affectRows));

                for (int i = processingBegin; i < processingEnd; ++i) {
                    TDCsvLine line = csv.lines.get(i);
                    String selectSql = line.getSelectSql();
                    String insertSql = line.getInsertSql();
                    int count = connection.executeQueryCount(selectSql);
                    if (count == 1) {
                        insertedRows++;
                        TDLog.trace(String.format("file:%s thread:%d inserted:%d pos:%d line:%d already exist, sql:%s"
                                , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                    } else {
                        TDLog.trace(String.format("file:%s thread:%d inserted:%d pos:%d line:%d not exist, sql:%s"
                                , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                        affectRows = executeInsertSql(insertSql);
                        if (affectRows == 1) {
                            insertedRows++;
                            TDLog.trace(String.format("file:%s thread:%d inserted:%d pos:%d line:%d not exist, insert success, affectRows:%d sql:%s"
                                    , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                        } else {
                            TDLog.error(String.format("file:%s thread:%d inserted:%d pos:%d line:%d not exist, insert failed,affectRows:%d sql:%s"
                                    , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                            TDLogDb.recordLine(threadIndex, csv.getFileName()
                                    , line.tableName, line.lineIndex, line.timestamp, connection.getErrorCode());
                        }
                    }
                } // end for
            }

            processingBegin = processingEnd;
        }

        TDLog.print(String.format("file:%s thread:%d process successfully, expectInsertRows:%d, insertedRows:%d"
                , csv.getFileName(), threadIndex, lineEnd - lineBegin, insertedRows));
    }

    private int executeInsertSql(String sql) {
        if (connection.executeUpdate(sql)) {
            return connection.getAffectrows();
        } else {
            return 0;
        }
    }
}

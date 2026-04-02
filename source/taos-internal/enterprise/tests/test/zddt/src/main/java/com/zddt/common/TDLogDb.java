package com.zddt.common;

import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDLogDb {
    private static TDConnection connection = null;
    private static String jdbcSqlTableName = "jdbctask";
    private static String jdbcLineTableName = "jdbcsql";
    private static long lastTs = 0;

    public static boolean init() {
        if (!TDConfig.logdbRecord) {
            return true;
        }

        connection = new TDConnection(TDConfig.host, TDConfig.user, TDConfig.password);
        if (!connection.connect()) {
            TDLog.print(String.format("logdb connect to tdengine failed, user:%s, password:%s, host:%s, code:%d, reason:%s", TDConfig.user, TDConfig.password, TDConfig.host));
            return false;
        }


        TDLog.print("logdb connect to tdengine success");
        return createSchema();
    }

    public static void close() {
        if (!TDConfig.logdbRecord) {
            return;
        }

        if (connection != null)
            connection.close();
        TDLog.print("logdb connection is closed");
    }

    public static boolean createSchema() {
        if (!TDConfig.logdbRecord) {
            return true;
        }

        String sql = String.format("create database if not exists %s replica %d days %d keep %d rows %d cache %d ablocks %f tblocks %d tables %d precision us"
                , TDConfig.logdbName
                , TDConfig.logdbReplica
                , TDConfig.logdbDays
                , TDConfig.logdbKeep
                , TDConfig.logdbRows
                , TDConfig.logdbCache
                , TDConfig.logdbAblocks
                , TDConfig.logdbTblocks
                , TDConfig.logdbTables);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create database:%s, code:%d, error:%s, sql:%s", TDConfig.logdbName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create database:%s finished", TDConfig.logdbName));
        }

        sql = String.format("use %s", TDConfig.logdbName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to use database:%s, code:%d, error:%s, sql:%s", TDConfig.logdbName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        }

        jdbcSqlTableName = String.format("%s.%stask", TDConfig.logdbName, TDConfig.logdbTablePrefix);
        sql = String.format(
                "create table if not exists %s (ts timestamp, taskIndex int, fetchedRows int, insertedRows int, failedRows int, fetchTime float, createtbTime float, insertTime float, totalTime float, taskSql binary(3000))"
                , jdbcSqlTableName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create table:%s, code:%d, error:%s, sql:%s", jdbcSqlTableName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create tables:%s finished", jdbcSqlTableName));
        }

        jdbcLineTableName = String.format("%s.%sline", TDConfig.logdbName, TDConfig.logdbTablePrefix);
        sql = String.format(
                "create table if not exists %s (ts timestamp, taskIndex int, lineIndex int, errorCode int, failedSql binary(3000))"
                , jdbcLineTableName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create table:%s, code:%d, error:%s, sql:%s", jdbcLineTableName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create tables:%s finished", jdbcLineTableName));
        }

        return true;
    }

    public static synchronized void recordTask(TDJdbc jdbc) {
        long lastTs = 0;
        long ts = TDUtil.getTimeStampUs();
        if (lastTs == ts) {
            TDUtil.sleepMs(100);
            ts = TDUtil.getTimeStampUs();
        }
        lastTs = ts;

        String sql = String.format("insert into %s values(%d,%d,%d,%d,%d,%f,%f,%f,%f,\"%s\")"
                , jdbcSqlTableName
                , ts
                , jdbc.getTaskIndex()
                , jdbc.getFetchedRows(), jdbc.getInsertedRows(), jdbc.getFetchedRows() - jdbc.getInsertedRows()
                , jdbc.getFetchedTimeSec(), jdbc.getCreatetbTimeSec(), jdbc.getInsertTimeSec()
                , jdbc.getFetchedTimeSec() + jdbc.getCreatetbTimeSec() + jdbc.getInsertTimeSec()
                , jdbc.getTaskSql()
        );

        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("sql:%d failed to record task log, code:%d, reason:%s, sql:%s"
                    , jdbc.getTaskIndex(), connection.getErrorCode(), connection.getErrorStr(), sql));
        } else {
            TDLog.trace(String.format("sql:%d record log, affectRows:%d, sql:%s"
                    , jdbc.getTaskIndex(), connection.getAffectrows(), sql));
        }
    }

    public static synchronized void recordLine(int taskIndex, int lineIndex, int errCode, String failedSql) {

        long lastTs = 0;
        long ts = TDUtil.getTimeStampUs();
        if (lastTs == ts) {
            TDUtil.sleepMs(100);
            ts = TDUtil.getTimeStampUs();
        }
        lastTs = ts;

        String sql = String.format("insert into %s values(%d,%d,%d,%d,\"%s\")"
                , jdbcLineTableName
                , ts
                , taskIndex
                , lineIndex
                , errCode
                , failedSql
        );

        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("sql:%d failed to record line log, code:%d, reason:%s, sql:%s"
                    , taskIndex, connection.getErrorCode(), connection.getErrorStr(), sql));
        } else {
            TDLog.error(String.format("sql:%d record line log, affectRows:%d, sql:%s"
                    , taskIndex, connection.getAffectrows(), sql));
        }
    }

    public static class TDJdbc {
        private int taskIndex;
        private String taskSql;
        private int fetchedRows = 0;
        private int insertedRows = 0;
        private float fetchedTimeTs = 0;
        private float insertTimeTs = 0;
        private float createtbTimeTs = 0;
        public ArrayList<TDLine> lines = new ArrayList<TDLine>();
        public long beginTs = 0;
        private int writePartition = 1;
        private static Connection connection = null;
        private static TDUtil.TDJdbcThread[] threads = null;
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

        public TDJdbc(int taskIndex, String taskSql) {
            this.taskIndex = taskIndex;
            this.taskSql = taskSql;
            this.initBeginTs();
            this.initThreads();
        }

        private void initThreads() {
            this.threads = new TDUtil.TDJdbcThread[TDConfig.jdbcSubThreadNum];
            for (int threadIndex = 0; threadIndex < TDConfig.jdbcSubThreadNum; ++threadIndex) {
                TDUtil.TDJdbcThread thread = new TDUtil.TDJdbcThread(threadIndex);
                this.threads[threadIndex] = thread;
            }
        }

        private void runAllThreads() {
            TDLog.print(String.format("task:%d threads start to run", this.taskIndex));
            executorService = Executors.newFixedThreadPool(this.threads.length);
            for (TDUtil.TDJdbcThread thread : this.threads) {
                executorService.execute(thread);
            }
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

        public boolean read() {
            this.runAllThreads();
            Statement stmt;
            ResultSet resSet = null;
            try {
                stmt = connection.createStatement();
                resSet = stmt.executeQuery(taskSql);
                if (resSet == null) {
                    TDLog.error(String.format("failed to execute sql:%s", taskSql));
                    return false;
                }

                ResultSetMetaData metaData = resSet.getMetaData();
                if (metaData == null) {
                    TDLog.error(String.format("no result set, sql:%s", taskSql));
                    return false;
                }

                if (!TDDataDb.createStb()) {
                    TDLog.error(String.format("failed to create stable from jdbc sql:%s", taskSql));
                    return false;
                }

                while (resSet.next()) {
                    TDLine line = null;
                    String tableName = "";
                    int threadIndex = TDDataDb.createTb(tableName);
                    threads[threadIndex].addLine(line);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                TDLog.error(String.format("query failed, task:%d, sql:%s, code:%d, error:%s, fetchedRows:%d", taskIndex, taskSql, e.getErrorCode(), e.getErrorCode(), getFetchedRows()));
                return false;
            } finally {
                try {
                    resSet.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
            }
            TDLog.print(String.format("task:%d threads run finished", this.taskIndex));
            return true;
        }

    }
}
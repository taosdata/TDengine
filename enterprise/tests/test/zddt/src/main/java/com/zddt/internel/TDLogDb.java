package com.zddt.internel;

public class TDLogDb {
    private static TDConnection connection = null;
    private static String csvFileTableName = "csvfile";
    private static String csvLineTableName = "csvline";
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

        csvFileTableName = String.format("%s.%stask", TDConfig.logdbName, TDConfig.logdbTablePrefix);
        sql = String.format(
                "create table if not exists %s (ts timestamp, taskIndex int, fetchedRows int, insertedRows int, failedRows int, insertTime float, taskName binary(2048))"
                , csvFileTableName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create table:%s, code:%d, error:%s, sql:%s", csvFileTableName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create tables:%s finished", csvFileTableName));
        }

        csvLineTableName = String.format("%s.%sline", TDConfig.logdbName, TDConfig.logdbTablePrefix);
        sql = String.format(
                "create table if not exists %s (ts timestamp, taskIndex int, lineIndex int, errorCode int, failedSql binary(128))"
                , csvLineTableName);
        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("failed to create table:%s, code:%d, error:%s, sql:%s", csvLineTableName, connection.getErrorCode(), connection.getErrorStr(), sql));
            return false;
        } else {
            TDLog.print(String.format("create tables:%s finished", csvLineTableName));
        }

        return true;
    }

    public static synchronized void recordTask(TDTask task) {
        long lastTs = 0;
        long ts = TDUtil.getTimeStampUs();
        if (lastTs == ts) {
            TDUtil.sleepMs(100);
            ts = TDUtil.getTimeStampUs();
        }
        lastTs = ts;

        String sql = String.format("insert into %s values(%d,%d,%d,%d,%d,%f,\"%s\")"
                , csvFileTableName
                , ts
                , task.getTaskIndex()
                , task.getFetchedRows(), task.getInsertedRows(), task.getFetchedRows() - task.getInsertedRows()
                , task.getFetchedTimeSec()
                , task.getTaskName()
        );

        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("task:%d, failed to record task log, code:%d, reason:%s"
                    , task.getTaskIndex(), connection.getErrorCode(), connection.getErrorStr(), sql));
        } else {
            TDLog.trace(String.format("task:%d, record log, affectRows:%d, sql:%s"
                    , task.getTaskIndex(), connection.getAffectrows(), sql));
        }
    }

    public static synchronized void recordLine(TDTask task, int lineIndex, int errCode, String failedSql) {

        long lastTs = 0;
        long ts = TDUtil.getTimeStampUs();
        if (lastTs == ts) {
            TDUtil.sleepMs(100);
            ts = TDUtil.getTimeStampUs();
        }
        lastTs = ts;

        String sql = String.format("insert into %s values(%d,%d,%d,%d,\"%s\")"
                , csvLineTableName
                , ts
                , task.getTaskIndex()
                , lineIndex
                , errCode
                , failedSql
        );

        if (!connection.executeUpdate(sql)) {
            TDLog.error(String.format("task:%d, failed to record line log, code:%d, reason:%s, sql:%s"
                    , task.getTaskIndex(), connection.getErrorCode(), connection.getErrorStr(), sql));
        } else {
            TDLog.error(String.format("task:%d, record line log, affectRows:%d, sql:%s"
                    , task.getTaskIndex(), connection.getAffectrows(), sql));
        }
    }
}
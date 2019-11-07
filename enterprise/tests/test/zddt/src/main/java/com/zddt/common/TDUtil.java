package com.zddt.common;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDUtil {
    public static String getTimeStringMs() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return df.format(new Date());
    }

    public static long getTimeMsFromFormat(String data, String fmt) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d;
        try {
            d = sdf.parse(data);
        } catch (Exception e) {
            //e.printStackTrace();
            return -1;
        }

        return d.getTime();
    }

    public static long getTimeMsFromYYYYMMDD(String yyyymmdd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d;
        try {
            d = sdf.parse(yyyymmdd);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }

        return d.getTime();
    }

    public static long getTimeStampUs() {
        Long cutime = System.currentTimeMillis() * 1000;
        Long nanoTime = System.nanoTime();
        return cutime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }

    public static long getTimeStampMs() {
        return System.currentTimeMillis();
    }

    public static void sleepMs(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {

        }
    }

    public static String getBaseFileName(String fileName) {
        int begin = 0;
        int end = fileName.length();
        for (int i = end - 1; i >= 0; i--) {
            if (fileName.charAt(i) == '.') {
                end = i;
                break;
            }
        }

        for (int i = end - 1; i >= 0; i--) {
            if (fileName.charAt(i) == '\\' || fileName.charAt(i) == '/') {
                begin = i + 1;
                break;
            }
        }

        if (end < 0 || end >= fileName.length()) {
            end = fileName.length();
        }

        if (begin == end) {
            begin = end - 1;
        }

        if (begin < 0 || begin >= fileName.length()) {
            begin = 0;
        }

        String name = fileName.substring(begin, end);
        return name;
    }

    public static ArrayList<String> getAllFiles(String filepath) {
        File file = new File(filepath);
        File[] fileList = file.listFiles();
        ArrayList<String> allFiles = new ArrayList<String>();
        for (File f : fileList) {
            allFiles.add(f.getPath());
        }
        return allFiles;
    }

    public static String getAbsolutePath(String path) {
        File directory = new File("./");
        return directory.getAbsolutePath();
    }

    public static class TDJdbcLine {
    }

    public static class TDJdbcThread implements Runnable {
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

        public TDJdbcThread(int threadIndex) {

        }

        public void addLine(TDJdbcLine line) {

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

    public static class TDJdbc {
        private int taskIndex;
        private String taskSql;
        private int fetchedRows = 0;
        private int insertedRows = 0;
        private float fetchedTimeTs = 0;
        private float insertTimeTs = 0;
        private float createtbTimeTs = 0;
        public ArrayList<TDJdbcLine> lines = new ArrayList<TDJdbcLine>();
        public long beginTs = 0;
        private int writePartition = 1;
        private static Connection connection = null;
        private static TDJdbcThread[] threads = null;
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
            this.threads = new TDJdbcThread[TDConfig.jdbcSubThreadNum];
            for (int threadIndex = 0; threadIndex < TDConfig.jdbcSubThreadNum; ++threadIndex) {
                TDJdbcThread thread = new TDJdbcThread(threadIndex);
                this.threads[threadIndex] = thread;
            }
        }

        private void runAllThreads() {
            TDLog.print(String.format("task:%d threads start to run", this.taskIndex));
            executorService = Executors.newFixedThreadPool(this.threads.length);
            for (TDJdbcThread thread : this.threads) {
                executorService.execute(thread);
            }
        }

        private void initBeginTs() {
            if (TDConfig.datadbMicroSecond) {
                beginTs = getTimeStampUs();
            } else {
                beginTs = getTimeStampMs();
            }
            while (beginTs <= TDConfig.fileBeginTimestamp) {
                sleepMs(100);
                if (TDConfig.datadbMicroSecond) {
                    beginTs = getTimeStampUs();
                } else {
                    beginTs = getTimeStampMs();
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
                    TDJdbcLine line = null;
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

    public static class TDJdbcLine {
    }

    public static class TDJdbcThread implements Runnable {
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

        public TDJdbcThread(int threadIndex) {

        }

        public void addLine(TDJdbcLine line) {

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

    public static class TDJdbcThreads {
        public static void run() {

        }
    }
}

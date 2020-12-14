package com.taosdata.example.jdbcTaosdemo;

import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import com.taosdata.example.jdbcTaosdemo.task.CreateTableTask;
import com.taosdata.example.jdbcTaosdemo.task.InsertTableDatetimeTask;
import com.taosdata.example.jdbcTaosdemo.task.InsertTableTask;
import com.taosdata.example.jdbcTaosdemo.utils.ConnectionFactory;
import com.taosdata.example.jdbcTaosdemo.utils.SqlSpeller;
import com.taosdata.example.jdbcTaosdemo.utils.TimeStampUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JdbcTaosdemo {

    private static Logger logger = Logger.getLogger(JdbcTaosdemo.class);
    private final JdbcTaosdemoConfig config;
    private Connection connection;

    public JdbcTaosdemo(JdbcTaosdemoConfig config) {
        this.config = config;
    }

    public static void main(String[] args) {
        // parse config from args
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);

        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp) {
            JdbcTaosdemoConfig.printHelp();
            return;
        }
        if (config.getHost() == null) {
            JdbcTaosdemoConfig.printHelp();
            return;
        }

        JdbcTaosdemo taosdemo = new JdbcTaosdemo(config);
        // establish connection
        taosdemo.init();
        // drop database
        taosdemo.dropDatabase();
        // create database
        taosdemo.createDatabase();
        // use db
        taosdemo.useDatabase();
        // create super table
        taosdemo.createSuperTable();
        // create sub tables
        taosdemo.createTableMultiThreads();

        boolean infinite = Arrays.asList(args).contains("--infinite");
        if (infinite) {
            logger.info("!!! Infinite Insert Mode Started. !!!");
            taosdemo.insertInfinite();
        } else {
            // insert into table
            taosdemo.insertMultiThreads();
            // select from sub table
            taosdemo.selectFromTableLimit();
            taosdemo.selectCountFromTable();
            taosdemo.selectAvgMinMaxFromTable();
            // select last from
            taosdemo.selectLastFromTable();
            // select from super table
            taosdemo.selectFromSuperTableLimit();
            taosdemo.selectCountFromSuperTable();
            taosdemo.selectAvgMinMaxFromSuperTable();
            //select avg ,max from stb where tag
            taosdemo.selectAvgMinMaxFromSuperTableWhereTag();
            //select last from stb where location = ''
            taosdemo.selectLastFromSuperTableWhere();
            // select group by
            taosdemo.selectGroupBy();
            // select like
            taosdemo.selectLike();
            // select where ts >= ts<=
            taosdemo.selectLastOneHour();
            taosdemo.selectLastOneDay();
            taosdemo.selectLastOneWeek();
            taosdemo.selectLastOneMonth();
            taosdemo.selectLastOneYear();

            // drop super table
            if (config.isDeleteTable())
                taosdemo.dropSuperTable();
            taosdemo.close();
        }
    }


    /**
     * establish the connection
     */
    private void init() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            connection = ConnectionFactory.build(config);
            if (connection != null)
                logger.info("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("connection failed: " + config.getHost());
        }
    }

    /**
     * create database
     */
    private void createDatabase() {
        String sql = SqlSpeller.createDatabaseSQL(config.getDbName(), config.getKeep(), config.getDays());
        execute(sql);
    }

    /**
     * drop database
     */
    private void dropDatabase() {
        String sql = SqlSpeller.dropDatabaseSQL(config.getDbName());
        execute(sql);
    }

    /**
     * use database
     */
    private void useDatabase() {
        String sql = SqlSpeller.useDatabaseSQL(config.getDbName());
        execute(sql);
    }

    /**
     * create super table
     */
    private void createSuperTable() {
        String sql = SqlSpeller.createSuperTableSQL(config.getStbName());
        execute(sql);
    }

    /**
     * create table use super table with multi threads
     */
    private void createTableMultiThreads() {
        try {
            final int tableSize = config.getNumberOfTable() / config.getNumberOfThreads();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < config.getNumberOfThreads(); i++) {
                Thread thread = new Thread(new CreateTableTask(config, i * tableSize, tableSize), "Thread-" + i);
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            logger.info("<<< Multi Threads create table finished.");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * insert data infinitely
     */
    private void insertInfinite() {
        try {
            final long startDatetime = TimeStampUtil.datetimeToLong("2005-01-01 00:00:00.000");
            final long finishDatetime = TimeStampUtil.datetimeToLong("2030-01-01 00:00:00.000");

            final int tableSize = config.getNumberOfTable() / config.getNumberOfThreads();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < config.getNumberOfThreads(); i++) {
                Thread thread = new Thread(new InsertTableDatetimeTask(config, i * tableSize, tableSize, startDatetime, finishDatetime), "Thread-" + i);
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            logger.info("<<< Multi Threads insert table finished.");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void insertMultiThreads() {
        try {
            final int tableSize = config.getNumberOfTable() / config.getNumberOfThreads();
            final int numberOfRecordsPerTable = config.getNumberOfRecordsPerTable();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < config.getNumberOfThreads(); i++) {
                Thread thread = new Thread(new InsertTableTask(config, i * tableSize, tableSize, numberOfRecordsPerTable), "Thread-" + i);
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            logger.info("<<< Multi Threads insert table finished.");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void selectFromTableLimit() {
        String sql = SqlSpeller.selectFromTableLimitSQL(config.getDbName(), config.getTbPrefix(), 1, 10, 0);
        executeQuery(sql);
    }

    private void selectCountFromTable() {
        String sql = SqlSpeller.selectCountFromTableSQL(config.getDbName(), config.getTbPrefix(), 1);
        executeQuery(sql);
    }

    private void selectAvgMinMaxFromTable() {
        String sql = SqlSpeller.selectAvgMinMaxFromTableSQL("current", config.getDbName(), config.getTbPrefix(), 1);
        executeQuery(sql);
    }

    private void selectLastFromTable() {
        String sql = SqlSpeller.selectLastFromTableSQL(config.getDbName(), config.getTbPrefix(), 1);
        executeQuery(sql);
    }

    private void selectFromSuperTableLimit() {
        String sql = SqlSpeller.selectFromSuperTableLimitSQL(config.getDbName(), config.getStbName(), 10, 0);
        executeQuery(sql);
    }

    private void selectCountFromSuperTable() {
        String sql = SqlSpeller.selectCountFromSuperTableSQL(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectAvgMinMaxFromSuperTable() {
        String sql = SqlSpeller.selectAvgMinMaxFromSuperTableSQL("current", config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectAvgMinMaxFromSuperTableWhereTag() {
        String sql = SqlSpeller.selectAvgMinMaxFromSuperTableWhere("current", config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastFromSuperTableWhere() {
        String sql = SqlSpeller.selectLastFromSuperTableWhere("current", config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectGroupBy() {
        String sql = SqlSpeller.selectGroupBy("current", config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLike() {
        String sql = SqlSpeller.selectLike(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastOneHour() {
        String sql = SqlSpeller.selectLastOneHour(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastOneDay() {
        String sql = SqlSpeller.selectLastOneDay(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastOneWeek() {
        String sql = SqlSpeller.selectLastOneWeek(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastOneMonth() {
        String sql = SqlSpeller.selectLastOneMonth(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void selectLastOneYear() {
        String sql = SqlSpeller.selectLastOneYear(config.getDbName(), config.getStbName());
        executeQuery(sql);
    }

    private void close() {
        try {
            if (connection != null) {
                this.connection.close();
                logger.info("connection closed.");
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * drop super table
     */
    private void dropSuperTable() {
        String sql = SqlSpeller.dropSuperTableSQL(config.getDbName(), config.getStbName());
        execute(sql);
    }

    /**
     * execute sql, use this method when sql is create, alter, drop..
     */
    private void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, execute, (end - start));
        } catch (SQLException e) {
            logger.error("ERROR execute SQL ===> " + sql);
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            logger.error("ERROR execute SQL ===> " + sql);
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                sb.append(columnLabel + ": " + value + "\t");
            }
            System.out.println(sb.toString());
        }
    }

}

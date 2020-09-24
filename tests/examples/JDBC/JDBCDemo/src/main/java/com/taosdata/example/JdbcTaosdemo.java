package com.taosdata.example;

import com.taosdata.example.domain.JdbcTaosdemoConfig;
import com.taosdata.example.task.CreateTableTask;
import com.taosdata.example.task.InsertTableDatetimeTask;
import com.taosdata.example.task.InsertTableTask;
import com.taosdata.example.utils.TimeStampUtil;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class JdbcTaosdemo {

    private static Logger logger = Logger.getLogger(JdbcTaosdemo.class);
    private static AtomicLong beginTimestamp = new AtomicLong(TimeStampUtil.datetimeToLong("2005-01-01 00:00:00.000"));
    private final JdbcTaosdemoConfig config;
    private Connection connection;
    private static final String[] locations = {"Beijing", "Shanghai", "Guangzhou", "Shenzhen", "HangZhou", "Tianjin", "Wuhan", "Changsha", "Nanjing", "Xian"};
    private static Random random = new Random(System.currentTimeMillis());

    public JdbcTaosdemo(JdbcTaosdemoConfig config) {
        this.config = config;
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JDBCConnectorChecker.jar -h host [OPTION...]");
        System.out.println("-p    port                       The TCP/IP port number to use for the connection. Default is 6030");
        System.out.println("-u    user                       The TDengine user name to use when connecting to the server. Default is 'root'");
        System.out.println("-P    password                   The password to use when connecting to the server.Default is 'taosdata'");
        System.out.println("-d    database                   Destination database. Default is 'test'");
        System.out.println("-m    tablePrefix                Table prefix name. Default is 'd'");
        System.out.println("-T    num_of_threads             The number of threads. Default is 10");
        System.out.println("-t    num_of_tables              The number of tables. Default is 10000");
        System.out.println("-n    num_of_records_per_table   The number of records per table. Default is 100000");
        System.out.println("-D    delete table               Delete data methods. Default is false");
        System.out.println("--help                           Give this help list");
    }

    public static void main(String[] args) {
        JdbcTaosdemoConfig config = JdbcTaosdemoConfig.build(args);

        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp) {
            printHelp();
            return;
        }
        if (config.getHost() == null) {
            printHelp();
            return;
        }

        boolean infinite = Arrays.asList().contains("--infinite");
        JdbcTaosdemo taosdemo = new JdbcTaosdemo(config);
        taosdemo.init();
        taosdemo.dropDatabase();
        taosdemo.createDatabase();
        taosdemo.useDatabase();
        taosdemo.createSuperTable();
        taosdemo.createTableMultiThreads();
        if (infinite) {
            taosdemo.insertInfinite();
        } else {
            taosdemo.insertMultiThreads();
            taosdemo.countFromSuperTable();
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
            connection = getConnection(config);
            if (connection != null)
                logger.info("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("connection failed: " + config.getHost());
        }
    }

    public static Connection getConnection(JdbcTaosdemoConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, config.getHost());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, config.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, config.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        return DriverManager.getConnection("jdbc:TAOS://" + config.getHost() + ":" + config.getPort() + "/" + config.getDbName() + "", properties);
    }

    /**
     * create database
     */
    private void createDatabase() {
        String sql = "create database if not exists " + config.getDbName() + " keep " + config.getKeep() + " days " + config.getDays();
        execute(sql);
    }

    private void dropDatabase() {
        String sql = "drop database if exists " + config.getDbName();
        execute(sql);
    }

    /**
     * use database
     */
    private void useDatabase() {
        String sql = "use " + config.getDbName();
        execute(sql);
    }

    private void createSuperTable() {
        String sql = "create table if not exists " + config.getStbName() + "(ts timestamp, current float, voltage int, phase float) tags(location binary(64), groupId int)";
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
            logger.info(">>> Multi Threads create table finished.");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

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
            logger.info(">>> Multi Threads insert table finished.");
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
            logger.info(">>> Multi Threads insert table finished.");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static String insertSql(int tableIndex, JdbcTaosdemoConfig config) {
        float current = 10 + random.nextFloat();
        int voltage = 200 + random.nextInt(20);
        float phase = random.nextFloat();
        String sql = "insert into " + config.getDbName() + "." + config.getTbPrefix() + "" + tableIndex + " " +
                "values(" + beginTimestamp.getAndIncrement() + ", " + current + ", " + voltage + ", " + phase + ") ";
        return sql;
    }

    public static String insertSql(int tableIndex, long ts, JdbcTaosdemoConfig config) {
        float current = 10 + random.nextFloat();
        int voltage = 200 + random.nextInt(20);
        float phase = random.nextFloat();
        String sql = "insert into " + config.getDbName() + "." + config.getTbPrefix() + "" + tableIndex + " " +
                "values(" + ts + ", " + current + ", " + voltage + ", " + phase + ") ";
        return sql;
    }


    public static String createTableSql(int tableIndex, JdbcTaosdemoConfig config) {
        String location = locations[random.nextInt(locations.length)];
        return "create table d" + tableIndex + " using " + config.getDbName() + "." + config.getStbName() + " tags('" + location + "'," + tableIndex + ")";
    }

    private void countFromSuperTable() {
        String sql = "select count(*) from " + config.getDbName() + "." + config.getStbName();
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
        String sql = "drop table if exists " + config.getDbName() + "." + config.getStbName();
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
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void printSql(String sql, boolean succeed, long cost) {
        logger.info("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                sb.append(columnLabel + ": " + value + "\t");
            }
            logger.info(sb.toString());
        }
    }

}

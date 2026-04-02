package com.taosdata.iot.javaTestSuit.suit_02;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;
import com.taosdata.jdbc.TSDBDriver;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 19-4-30
 */
public class ImportAndInsertTester {

    /**
     * Params for data importing
     */

    // connection params
    private static String host = "127.0.0.1";
    private static String user = "root";
    private static String password = "taosdata";

    // schema params
    private static String[] cols = new String[]{"ts timestamp", "c1 int"}; // column defination
    private static String[] tags = new String[]{"t1 int"}; // tag defination
    private static String db = "db"; // database name
    private static String stb = "stb"; // metric name
    private static String tbPrefix = "tb"; // table name
    private static int colNum = cols.length;

    // data scale
    private static long timestep = 1; // sample time interval in milliseconds
    private static long tbNum = 10; // number of tables
    private static long dataStartTime; // historical data end time
    private static long dataEndTime; // historical data end time
    private static long totalRowsToInsertPerTable = 1;

    // retention params
    private static int replica = 1;
    private static int days = 10;
    private static int keep = 3650;
    private static int tables = 1000;
    private static int rows = 4096;
    private static int cache = 16384;
    private static double ablocks = 4;
    private static int tblocks = 512;
    private static int ctime = 3600;
    private static int clog = 0;
    private static int comp = 2;

    // insert option params
    private static ImportAndInsertTester.InsertOption insertOption;
    private static int createTableThreadNum = 1;
    private static int insertThreadNum = 1;
    private static int insertBatchSize = 10;
    private static int insertBatchNum = 100;
    private static int importThreadNum = 1;
    private static int importBatchSize = 10;
    private static int importBatchNum = 100;
    private static long sleepBetweenInserts = 0L;
    private static long sleepBetweenImports = 0L;

    // utils
//    private static ConnectionFactory connectionFactory = new ConnectionFactory();

    public ImportAndInsertTester() {
    }

    public static void main(String[] args) throws Exception{
        Timer mainTimer = new Timer();
        mainTimer.start();
        ImportAndInsertTester generator = new ImportAndInsertTester();

        // read params from json and set to environment
        generator.setParams(args[0]);

        if (totalRowsToInsertPerTable < 1) {
            System.out.println("Aborted: no row to insert!");
            return;
        }

        // set up schema and create all tables
        if (insertOption.equals(insertOption.HISTORICAL)) {
            generator.createAllTables();
        }

        // insert data
        generator.insertData();

        mainTimer.stop();
        mainTimer.printTimeInSeconds();
    }

    private enum InsertOption {
        HISTORICAL,
        REALTIME;
    }

    private class Schema {

        String cols;
        String tags;

        Schema(String cols, String tags) {
            this.cols = cols;
            this.tags = tags;
        }

        public String getCols() {
            return cols;
        }

        public void setCols(String cols) {
            this.cols = cols;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }
    }

    private void setParams(String jsonFilePath) {
        System.out.println("Reading parameters from the json file...");
        // Utilities
        JsonParser jsonParser = new JsonParser();
        Gson gson =  new Gson();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // Start to parse json config file
        try {
            JsonObject params = (JsonObject) jsonParser.parse(new FileReader(jsonFilePath));
            if (!Strings.isNullOrEmpty(params.get("host").getAsString())) {
                host = params.get("host").getAsString();
            }
            if (!Strings.isNullOrEmpty(params.get("user").getAsString())) {
                user = params.get("user").getAsString();
            }
            if (!Strings.isNullOrEmpty(params.get("password").getAsString())) {
                password = params.get("password").getAsString();
            }

            db = params.get("db").getAsString();
            stb = params.get("stb").getAsString();
            tbPrefix = params.get("tbPrefix").getAsString();
            cols = params.get("cols").getAsString().split(",");
            tags = params.get("tags").getAsString().split(",");
            timestep = params.get("timestep").getAsLong();
            tbNum = params.get("tbNum").getAsLong();
            dataStartTime = sdf.parse(params.get("dataStartTime").getAsString()).getTime();
            dataEndTime = sdf.parse(params.get("dataEndTime").getAsString()).getTime();

            replica = params.get("replica").getAsInt();
            days = params.get("days").getAsInt();
            keep = params.get("keep").getAsInt();
            tables = params.get("tables").getAsInt();
            rows = params.get("rows").getAsInt();
            cache = params.get("cache").getAsInt();
            ablocks = params.get("ablocks").getAsDouble();
            tblocks = params.get("tblocks").getAsInt();
            ctime = params.get("ctime").getAsInt();
            clog = params.get("clog").getAsInt();
            comp = params.get("comp").getAsInt();

            insertOption = gson.fromJson(params.get("insertOption"), ImportAndInsertTester.InsertOption.class);
            insertThreadNum = params.get("insertThreadNum").getAsInt();
            insertBatchSize = params.get("insertBatchSize").getAsInt();
            insertBatchNum = params.get("insertBatchNum").getAsInt();
            sleepBetweenInserts = params.get("sleepBetweenInserts").getAsLong();

        } catch (Exception e) {
            e.printStackTrace();
        }

        totalRowsToInsertPerTable = ((dataEndTime - dataStartTime) / timestep);
        insertBatchNum = (int)totalRowsToInsertPerTable / insertBatchSize;
        colNum = cols.length;

        System.out.println("Parameters are set!");

        System.out.println("=======================Paramerters========================");
        System.out.printf("host IP: \t%s\n", host);
        System.out.printf("user: \t%s\n", user);
        System.out.printf("password: \t%s\n", password);
        System.out.printf("insert option: \t%s\n", insertOption);
        System.out.printf("database name: \t%s\n", db);
        System.out.printf("stable name: \t%s\n", stb);
        System.out.printf("table prefix: \t%s\n", tbPrefix);
        System.out.printf("columns: \t%s\n", Arrays.asList(cols).toString());
        System.out.printf("tags: \t%s\n", Arrays.asList(tags).toString());
        System.out.printf("time step: \t%d ms\n", timestep);
        System.out.printf("number of tables: \t%d\n", tbNum);
        if (ImportAndInsertTester.InsertOption.HISTORICAL.equals(insertOption)) {
            System.out.printf("historical data start time: \t%s\n", new Timestamp(dataStartTime));
            System.out.printf("historical data end time: \t%s\n", new Timestamp(dataEndTime));
            System.out.printf("rows expected in each table: \t%d\n", totalRowsToInsertPerTable);
            System.out.printf("sleep time between insertions: \t%d\n", sleepBetweenInserts);
        } else {
            System.out.printf("rows expected in each table: \t%d\n", totalRowsToInsertPerTable);
        }
//        System.out.printf("replica: %d\n", replica);
//        System.out.printf("days: %d\n", days);
//        System.out.printf("keep: %d\n", keep);
//        System.out.printf("tables: %d\n", tables);
//        System.out.printf("rows: %d\n", rows);
//        System.out.printf("cache: %d\n", cache);
//        System.out.printf("ablocks: %d\n", ablocks);
//        System.out.printf("tblocks: %d\n", tblocks);
//        System.out.printf("ctime: %d\n", ctime);
//        System.out.printf("clog: %d\n", clog);
//        System.out.printf("comp: %d\n", comp);
        System.out.printf("number of threads for insert: \t%d\n", insertThreadNum);
        System.out.printf("batch size of insertion: \t%d\n", insertBatchSize);
        System.out.printf("number of batches needed for insertion: \t%d\n", insertBatchNum);
        System.out.println("==========================================================");
    }

    private void createAllTables() {
        System.out.println("Creating desired schema...");
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
        Connection connection = new ConnectionFactory().getConnection(host, properties);
        String sql = "";
        try {
            Statement stmt = connection.createStatement();

            // create db
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(db, replica, days, keep, rows, cache, ablocks, tblocks, tables, ctime, clog, comp);
            stmt.executeUpdate(sql);
            sql = "use " + db;
            stmt.executeUpdate(sql);

            // create stb
            sql = SqlGenerator.getCreateMetricSql(stb, cols, tags);
            stmt.executeUpdate(sql);

            // create tb
            String[] cities = new String[] {"'beijing'", "'shanghai'", "'hongkong'"};
            for (int i = 1; i <= tbNum; i++) {
                sql = SqlGenerator.getCreateTableUsingMetricSql(tbPrefix + i, stb, new String[]{String.valueOf(i), String.valueOf(i%5), cities[i%3]});
                stmt.executeUpdate(sql);
            }
            stmt.close();
            System.out.println("Schema is created!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Failed to close connection.\n");
            }
        }
    }

    public void insertData() {
        System.out.println("Inserting data...");
        switch (insertOption) {
            case REALTIME:
                insertRealtimeData();
                break;
            case HISTORICAL:
                insertAsHistoricalData();
                break;
        }
        System.out.println("Data insertion completed!");
        System.out.printf("Number of tables: %d\nNumber of rows in each table: %d\n", tbNum, insertBatchNum * insertBatchSize);
    }

    private void insertAsHistoricalData() {
        ExecutorService executorService = Executors.newFixedThreadPool(insertThreadNum);
        try {
            for (int i = 1; i <= insertThreadNum; i++) {
                executorService.execute(new ImportAndInsertTester.HistoricalInsertTask(i));
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {

            // wait till all threads complete their tasks

        }

        System.out.println("All thread tasks are completed!");
    }

    private void insertRealtimeData() {
        ExecutorService executorService = Executors.newFixedThreadPool(insertThreadNum);
        try {
            for (int i = 1; i <= insertThreadNum; i++) {
                executorService.execute(new ImportAndInsertTester.RealTimeInsertTask(i));
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {

            // wait till all threads complete their tasks

        }
        System.out.println("All thread tasks are completed!");
    }

    private class HistoricalInsertTask implements Runnable {

        private int threadId;

        public HistoricalInsertTask(int threadId) {
            this.threadId = threadId;
        }

        public void run() {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
            Connection connection = new ConnectionFactory().getConnection(host, properties);
            long rowsAffected = 0; // counter for total affected rows
            String threadName = Thread.currentThread().getName().replaceAll("pool-1-thread-", "thread");
//            String sql = "";
            Timer timer = new Timer();
            long tbs = tbNum / insertThreadNum; // number of tables assigned to this thread
            tbs += (threadId <= tbNum % insertThreadNum) ? 1 : 0;

            StringBuilder insertSql = new StringBuilder("insert into ");
            StringBuilder importSql = new StringBuilder("import into ");
            Random random = new Random();

            try {
                Statement stmt = connection.createStatement();
                System.out.printf("\t%s: start to batch insert %d records into %d tables...\n", threadName, insertBatchNum * insertBatchSize, tbs);
                timer.reset();
                stmt.executeUpdate("use " + db);
                String tbName = "";
                long ts0 = dataStartTime;
                long ts = 0l;
                long tbStartId = (threadId - 1) * tbs + 1;
                long tbEndId = (threadId == insertThreadNum) ? (threadId * tbs + tbNum % tbs) : (threadId * tbs);
                int count = 0;
                for (long t = tbStartId; t <= tbEndId; t++){
                    ts = ts0;
                    ts = ts + timestep;
                    tbName = tbPrefix + t; // create table name
                    insertSql.append(tbName).append(" values ");
                    importSql.append(tbName).append(" values ");
                    for (int j = 1; j <= totalRowsToInsertPerTable; j++) {
                        ts = ts + timestep;
                        insertSql.append("(").append(ts);

                        for (int k = 1; k < colNum; k++) {
                            insertSql.append(", ").append(j);
                        }
                        insertSql.append(") ");
                        count++;

                        if (count >= insertBatchSize) {
                            timer.start();
                            rowsAffected += stmt.executeUpdate(insertSql.toString());
                            importSql = new StringBuilder("import into ").append(tbName).append(" values ");
                            importSql.append("(").append(ts - timestep * insertBatchSize - 9);
                            for (int k = 1; k < colNum; k++) {
                                importSql.append(", ").append(-j);
                            }
                            importSql.append(") ");
                            stmt.executeUpdate(importSql.toString());
                            if (sleepBetweenInserts > 0) {
                                Thread.currentThread().sleep(sleepBetweenInserts);
                            }
                            timer.stop();
                            count = 0;
                            if (j < totalRowsToInsertPerTable - 1) {
                                insertSql = new StringBuilder("insert into ").append(tbName).append(" values ");
                            } else {
                                insertSql = new StringBuilder("insert into ");
                            }
                        }
                    }
                    if (count > 0) {
                        timer.start();
                        rowsAffected += stmt.executeUpdate(insertSql.toString());
                        stmt.executeUpdate(importSql.toString());
                        importSql = new StringBuilder("import into ").append(tbName).append(" values ");
                        importSql.append("(").append(ts - timestep * insertBatchSize - 9);
                        for (int k = 1; k < colNum; k++) {
                            importSql.append(", ").append(-123);
                        }
                        importSql.append(") ");
                        stmt.executeUpdate(importSql.toString());
                        if (sleepBetweenInserts > 0) {
                            Thread.currentThread().sleep(sleepBetweenInserts);
                        }
                        timer.stop();
                        count = 0;
                    }
                }
                System.out.printf("\t%s: Historical data insert completed! Total rows affected: %d; total time: %fs\n", threadName, rowsAffected, timer.getTimeInSeconds());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("%s: failed to execute sql: %s\n", threadName, insertSql.toString());
            }
        }
    }

    private class IntervalQueryTask implements Runnable {
        private int repeats = 10000;

        @Override
        public void run() {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
            Connection connection = new ConnectionFactory().getConnection(host, properties);
            String sql = "select count(*) from "
                    + stb + " where ts >= " + dataStartTime
                    + " and ts <= " + dataEndTime + " interval(10d) order by ts desc limit 1";
            long lastCount = 0L;
            long lastTime = 0L;
            long count = 0L;
            long time = 0L;
            try {
                Statement stmt = connection.createStatement();
                ResultSet resultSet;
                for (int i = 0; i < repeats; i++) {
                    resultSet = stmt.executeQuery(sql);
                    if (resultSet.next()) {
                        time = resultSet.getLong(1);
                        count = resultSet.getLong(2);
                        if (time == lastTime) {
                            if (count < lastCount) {
                                throw new TestFailureException("Decreasing row count on ");
                            }
                        } else {
                            lastTime = time;
                            lastCount = count;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private class RealTimeInsertTask implements Runnable {

        private int threadId;

        public RealTimeInsertTask (int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {

            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
            Connection connection = new ConnectionFactory().getConnection(host, properties);
            String threadName = Thread.currentThread().getName().replaceAll("pool-1-thread-", "thread");
            long tbs = tbNum / insertThreadNum; // number of tables assigned to this thread
            tbs += (threadId <= tbNum % insertThreadNum) ? 1 : 0;

            System.out.printf("\t%s: simulate real-time data insert: %d tables; with a pace of 1 row per %d ms...\n", threadName, tbs, timestep);
            long loops = 0l;
            long rowsAffected = 0l; // counter for total affected rows
            long counter = 0l;
            long ts0 = System.currentTimeMillis();
            long ts = ts0;
            long singleThreadInsertSpeed = (tbNum * 1000 / timestep / insertThreadNum);
            while (loops < totalRowsToInsertPerTable) {
                String sql = "";
                try {
                    Thread.currentThread().sleep(timestep);
                    Statement stmt = connection.createStatement();
                    stmt.executeUpdate("use " + db);
                    String tbName = "";
                    StringBuilder insertSql = new StringBuilder("insert into ");
                    ts = ts + timestep;
                    for (int t = 1; t <= tbNum; t++) {
                        if ((t - 1) % insertThreadNum + 1 == threadId) {
                            tbName = tbPrefix + t; // create table name
                            insertSql.append(tbName).append(" values ");
//                            ts = ts + timestep;
                            insertSql.append("(").append(ts).append(", ");
                            String colValue = String.valueOf(loops);
                            for (int k = 1; k < colNum; k++) {
                                insertSql.append(colValue).append(", ");
                            }
                            insertSql.append(") ");
                            counter++;
                        }
                        if (counter == insertBatchSize) {
                            sql = insertSql.toString();
                            rowsAffected += stmt.executeUpdate(sql);
                            insertSql.delete(12, insertSql.length());
                            counter = 0l;
                        }
                    }

                    if (counter > 0) {
                        sql = insertSql.toString();
                        rowsAffected += stmt.executeUpdate(sql);
                    }
                    loops++;
                    if (rowsAffected >= singleThreadInsertSpeed) {
                        System.out.printf("\t%s: %s %d rows have been inserted!\n", new Timestamp(System.currentTimeMillis()), threadName, rowsAffected);
                        rowsAffected = 0;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.printf("%s: failed to execute sql: %s\n", threadName, sql);
                }
            }
        }
    }
}

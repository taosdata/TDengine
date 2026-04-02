package com.taosdata.iot.javaTestSuit.tools;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;
import com.taosdata.jdbc.TSDBDriver;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 18-11-14
 */
public class LicensePlateDataGenerator {

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
    private static String mt = "mt"; // metric name
    private static String tbPrefix = "tb"; // table name
    private static int colNum = cols.length;

    // data scale
    private static long timestep = 1; // sample time interval in milliseconds
    private static long tbNum = 10; // number of tables
    private static long dataEndTime; // historical data end time
    private static long dataTimeRange = 365; // time range for historical data (in days)
    private static long totalRowsPerTable = 0;

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
    private static InsertOption insertOption;
    private static int threadNum = 1;
    private static int batchSize = 10;
    private static int batchNum = 100;
    private static long sleepBetweenInserts = 0L;

    // constants
    private static final int camMeshSize = 71;
    private static Double[] camLongitudes = new Double[camMeshSize];
    private static Double[] camLatitudes = new Double[camMeshSize];
    // utils
//    private static ConnectionFactory connectionFactory = new ConnectionFactory();

    public LicensePlateDataGenerator() {
    }

    public static void main(String[] args) {
        Timer mainTimer = new Timer();
        mainTimer.start();
        LicensePlateDataGenerator generator = new LicensePlateDataGenerator();

        // read params from json and set to environment
        generator.setParams(args[0]);
        generator.createCamLocationData();

        // constraint check
//        if (batchNum < 1) {
//            System.out.println("Aborted: batch size is too large for given data scale!");
//            return;
//        }
        if (totalRowsPerTable < 1) {
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

    private void setParams(String jsonFilePath) {
        System.out.println("Reading parameters from the json file...");
        JsonParser jsonParser = new JsonParser();
        Gson gson = new Gson();
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
            mt = params.get("mt").getAsString();
            tbPrefix = params.get("tbPrefix").getAsString();
            cols = params.get("cols").getAsString().split(",");
            tags = params.get("tags").getAsString().split(",");
            timestep = params.get("timestep").getAsLong();
            tbNum = params.get("tbNum").getAsLong();
            dataEndTime = params.get("dataEndTime").getAsLong();
            dataTimeRange = params.get("dataTimeRange").getAsLong();

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

            insertOption = gson.fromJson(params.get("insertOption"), InsertOption.class);
            threadNum = params.get("threadNum").getAsInt();
            batchSize = params.get("batchSize").getAsInt();
            batchNum = params.get("batchNum").getAsInt();
            sleepBetweenInserts = params.get("sleepBetweenInserts").getAsLong();

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (dataEndTime <= 0) {
            dataEndTime = System.currentTimeMillis();
        }
        totalRowsPerTable = (long)(dataTimeRange * 24 * 3600 * 1000 / timestep);
        batchNum = (int)totalRowsPerTable / batchSize;
        colNum = cols.length;

        System.out.println("Parameters are set!");

        System.out.println("=======================Paramerters========================");
        System.out.printf("host IP: \t%s\n", host);
        System.out.printf("user: \t%s\n", user);
        System.out.printf("password: \t%s\n", password);
        System.out.printf("insert option: \t%s\n", insertOption);
        System.out.printf("database name: \t%s\n", db);
        System.out.printf("stable name: \t%s\n", mt);
        System.out.printf("table prefix: \t%s\n", tbPrefix);
        System.out.printf("columns: \t%s\n", Arrays.asList(cols).toString());
        System.out.printf("tags: \t%s\n", Arrays.asList(tags).toString());
        System.out.printf("time step: \t%d ms\n", timestep);
        System.out.printf("number of tables: \t%d\n", tbNum);
        System.out.printf("time range: \t%d days\n", dataTimeRange);
        if (InsertOption.HISTORICAL.equals(insertOption)) {
            System.out.printf("historical data end time: \t%s\n", new Timestamp(dataEndTime));
            System.out.printf("historical data start time: \t%s\n", new Timestamp(dataEndTime - totalRowsPerTable * timestep));
            System.out.printf("rows expected in each table: \t%d\n", batchNum * batchSize);
            System.out.printf("sleep time between insertions: \t%d\n", sleepBetweenInserts);
        } else {
            System.out.printf("rows expected in each table: \t%d\n", totalRowsPerTable);
        }
        System.out.printf("replica: %d\n", replica);
        System.out.printf("days: %d\n", days);
        System.out.printf("keep: %d\n", keep);
        System.out.printf("tables: %d\n", tables);
        System.out.printf("rows: %d\n", rows);
        System.out.printf("cache: %d\n", cache);
        System.out.printf("ablocks: %f\n", ablocks);
        System.out.printf("tblocks: %d\n", tblocks);
        System.out.printf("ctime: %d\n", ctime);
        System.out.printf("clog: %d\n", clog);
        System.out.printf("comp: %d\n", comp);
        System.out.printf("number of threads: \t%d\n", threadNum);
        System.out.printf("batch size for insertion: \t%d\n", batchSize);
        System.out.printf("number of batches needed: \t%d\n", batchNum);
        System.out.println("==========================================================");
    }

    /**
     * Generate a metrix of camera locations.
     */
    private void createCamLocationData() {
        System.out.println("Creating camera data...");
        // latitude & longitude of Tian'anmen Quare
        double clatitude = 36.915405;
        double clongitude = 116.403874;
        // mesh size
        double step = 0.005303;
        camLatitudes[0] = clatitude - Math.floor(camMeshSize / 2) * step;
        camLongitudes[0] = clongitude - Math.floor(camMeshSize / 2) * step;
        for (int i = 1; i < camMeshSize; i++) {
            camLatitudes[i] = camLatitudes[i-1] + step;
            camLongitudes[i] = camLongitudes[i-1] + step;
        }
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

            // create mt
            sql = SqlGenerator.getCreateMetricSql(mt, cols, tags);
            stmt.executeUpdate(sql);

            // create tb
            for (int i = 1; i <= tbNum; i++) {
                sql = SqlGenerator.getCreateTableUsingMetricSql(tbPrefix + i, mt, new String[]{String.valueOf(i)});
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
        System.out.printf("Number of tables: %d\nNumber of rows in each table: %d\n", tbNum, batchNum * batchSize);
    }

    private void insertAsHistoricalData() {
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        try {
            for (int i = 1; i <= threadNum; i++) {
                executorService.execute(new HistoricalInsertTask(i));
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
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        try {
            for (int i = 1; i <= threadNum; i++) {
                executorService.execute(new RealTimeInsertTask(i));
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
            String sql = "";
            Timer timer = new Timer();
            long tbs = tbNum / threadNum; // number of tables assigned to this thread
            tbs += (threadId <= tbNum % threadNum) ? 1 : 0;
            Random random = new Random();
            int count = 0;
            StringBuilder insertSql = new StringBuilder("");

            try {
                Statement stmt = connection.createStatement();
                System.out.printf("\t%s: start to batch insert %d records into %d tables...\n", threadName, batchNum * batchSize, tbs);
                timer.reset();
                stmt.executeUpdate("use " + db);
                String tbName = "";
                long ts0 = dataEndTime - totalRowsPerTable * timestep;
                long ts = 0l;
                long tbStartId = (threadId - 1) * tbs + 1;
                long tbEndId = (threadId == threadNum) ? (threadId * tbs + tbNum % tbs) : (threadId * tbs);

                insertSql = new StringBuilder("insert into ");
                for (long t = tbStartId; t <= tbEndId; t++){
                    ts = ts0;
                    tbName = tbPrefix + t; // create table name
                    int latId = random.nextInt(camMeshSize);
                    int lonId = random.nextInt(camMeshSize);
                    double lat = camLatitudes[latId];
                    double lon = camLongitudes[lonId];
                    insertSql.append(tbName).append(" values ");
                    for (int j = 0; j < totalRowsPerTable; j++) {
                        ts = ts + timestep;
                        insertSql.append("(").append(ts).append(", ");

                        // move a step
                        int latStep = random.nextInt(5) - 3;
                        int lonStep = random.nextInt(5) - 3;
                        if (latId + latStep < 0) {
                            latId++;
                        } else if (latId + latStep > camMeshSize - 1) {
                            latId--;
                        } else {
                            latId = latId + latStep;
                        }
                        if (lonId + lonStep < 0) {
                            lonId++;
                        } else if (lonId + lonStep > camMeshSize - 1) {
                            lonId--;
                        } else {
                            lonId = lonId + lonStep;
                        }
                        int camId = latId * camMeshSize + lonId;
                        insertSql.append(camLongitudes[lonId]).append(", ")
                                .append(camLatitudes[latId]).append(", ")
                                .append(camId).append(") ");
                        count++;

                        if (count >= batchSize) {
                            timer.start();
                            rowsAffected += stmt.executeUpdate(insertSql.toString());
                            if (sleepBetweenInserts > 0) {
                                Thread.currentThread().sleep(sleepBetweenInserts);
                            }
                            timer.stop();
                            count = 0;
                            if (j < totalRowsPerTable - 1) {
                                insertSql = new StringBuilder("insert into ").append(tbName).append(" values ");
                            } else {
                                insertSql = new StringBuilder("insert into ");
                            }
                        }
                    }
                }
                if (count > 0) {
                    timer.start();
                    rowsAffected += stmt.executeUpdate(insertSql.toString());
                    if (sleepBetweenInserts > 0) {
                        Thread.currentThread().sleep(sleepBetweenInserts);
                    }
                    timer.stop();
                    count = 0;
                }
//                }
                System.out.printf("\t%s: Historical data insert completed! Total rows affected: %d; total time: %fs\n", threadName, rowsAffected, timer.getTimeInSeconds());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("%s: failed to execute sql: %s\n", threadName, insertSql.toString());
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
            long tbs = tbNum / threadNum; // number of tables assigned to this thread
            tbs += (threadId <= tbNum % threadNum) ? 1 : 0;

            System.out.printf("\t%s: simulate real-time data insert: %d tables; with a pace of 1 row per %d ms...\n", threadName, tbs, timestep);
            long loops = 0l;
            long rowsAffected = 0l; // counter for total affected rows
            long counter = 0l;
            long ts0 = System.currentTimeMillis();
            long ts = ts0;
            long singleThreadInsertSpeed = (tbNum * 1000 / timestep / threadNum);
            while (loops < totalRowsPerTable) {
                String sql = "";
                try {
                    Thread.currentThread().sleep(timestep);
                    Statement stmt = connection.createStatement();
                    stmt.executeUpdate("use " + db);
                    String tbName = "";
                    StringBuilder insertSql = new StringBuilder("insert into ");
                    ts = ts + timestep;
                    for (int t = 1; t <= tbNum; t++) {
                        if ((t - 1) % threadNum + 1 == threadId) {
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
                        if (counter == batchSize) {
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


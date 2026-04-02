package com.taosdata.iot.javaTestSuit.suit_01;

import com.google.common.base.Strings;
import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

public class InsertPerfTestTask extends TestTask {

    // task tools
    private Properties properties;
    private Timer timer = new Timer();

    /**
     *  user-controlled params
     */
    // db params
    private int replica;
    private int cache;
    private double ablocks;
    private int tables;

    // tb params

    private int tableNum; // number of tables to create per thread
    private int rowSize;
    private int columns;

    // batch
    private int batchSize;
    private int batches;

    // insert methods
    private int insertMethod;

    /**
     * fixed params for test
     */
    private final long ts0 = 1539054000000l; // '2018-10-09 11:00:00.000', start time in each table
    private final long timeStep = 1000l; // 1 second
    private final int sameTableBatchInsert = 1;
    private final int oneByOneInsert = 2;
    private final int blendedInsert = 3;

    public InsertPerfTestTask() {
        // set default values
        this.properties = new Properties();
        this.replica = 1;
        this.cache = 16384;
        this.ablocks = 4.0;
        this.tables = 1000;
        this.rowSize = 9;
        this.columns  = 2;
    }

    public InsertPerfTestTask(int replica, int cache, int ablocks, int tables, int tableNum, int rowSize, int columns, int batchSize, int batches) {

        this.replica = replica;
        this.cache = cache;
        this.ablocks = ablocks;
        this.tables = tables;
        this.rowSize = rowSize;
        this.columns = columns;
        this.batchSize = batchSize;
        this.batches = batches;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public int getReplica() {
        return replica;
    }

    public void setReplica(int replica) {
        this.replica = replica;
    }

    public int getCache() {
        return cache;
    }

    public void setCache(int cache) {
        this.cache = cache;
    }

    public double getAblocks() {
        return ablocks;
    }

    public void setAblocks(double ablocks) {
        this.ablocks = ablocks;
    }

    public int getTables() {
        return tables;
    }

    public void setTables(int tables) {
        this.tables = tables;
    }

    public int getTableNum() {
        return tableNum;
    }

    public void setTableNum(int tableNum) {
        this.tableNum = tableNum;
    }

    public int getRowSize() {
        return rowSize;
    }

    public void setRowSize(int rowSize) {
        this.rowSize = rowSize;
    }

    public int getColumns() {
        return columns;
    }

    public void setColumns(int columns) {
        this.columns = columns;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatches() {
        return batches;
    }

    public void setBatches(int batches) {
        this.batches = batches;
    }

    public int getInsertMethod() {
        return insertMethod;
    }

    public void setInsertMethod(int insertMethod) {
        this.insertMethod = insertMethod;
    }

    @Override
    public void run() {
        Connection connection = getConnection(properties);
        String db = createDb(connection);
        createTables(connection);
//        insert(sameTableBatchInsert, connection);
        insert(insertMethod, connection);
        cleanUp(connection, db);
    }

    private String createDb(Connection connection) throws TestFailureException {
        String sql = "";
        String db = "db_" + Thread.currentThread().getName().replaceAll("-", "_");
        try {
            Statement stmt = connection.createStatement();
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(db, replica, null, null, null, cache, ablocks,
                    null, tables, null, null, null );
            stmt.executeUpdate(sql);
            stmt.executeUpdate("use " + db);
            System.out.printf("\t%s: database created\n", Thread.currentThread().getName());
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            db = null;
            throw new TestFailureException("Failed to create db");
        }
        return db;
    }

    private void createTables(Connection connection) throws TestFailureException {
        String tb = "tb";
        String sql = "";

        try {
            Statement stmt = connection.createStatement();
            for (int t = 0; t < tableNum; t++) {
                sql = SqlGenerator.getCreateTableSql(tb + t, rowSize, columns);
                stmt.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insert(int insertMethod, Connection connection) {
        switch (insertMethod) {
            case 1:
                sameTableBatchInsert(connection);
                break;
            case 2:
                oneByOneInsert(connection);
                break;
            case 3:
                blendedBatchInsert(connection);
                break;
        }
    }

    private void sameTableBatchInsert(Connection connection) {

        int inserted = 0; // counter for total inserted records
        int singleColSize = (rowSize - 8) / (columns - 1);
        int lastColSize = (rowSize - 8) % singleColSize + singleColSize;
        long ts = ts0;
        String colValue = "\"" + Strings.repeat("t", singleColSize) + "\"";
        String lastColValue = "\"" + Strings.repeat("t", lastColSize) + "\"";
        String threadName = Thread.currentThread().getName();
        String sql = "";

        try {
            Statement stmt = connection.createStatement();
            System.out.printf("\t%s: sameTableBatchInsert %d records into %d tables...\n", threadName, batches * batchSize, tableNum);
            timer.reset();
            for (int t = 0; t < tableNum; t++) {
                StringBuilder tb = new StringBuilder("tb").append(t); // create table name
                for (int i = 0; i < batches; i++) {
                    StringBuilder insertSql = new StringBuilder("insert into ").append(tb).append(" values ");
                    for (int j = 0; j < batchSize; j++) {
                        ts = ts + timeStep;
                        insertSql.append("(").append(ts).append(", ");
                        for (int k = 1; k < columns - 1; k++) {
                            insertSql.append(colValue).append(", ");
                        }
                        insertSql.append(lastColValue).append(") ");
                    }
                    sql = insertSql.toString();
                    timer.start();
                    inserted += stmt.executeUpdate(sql);
                    timer.stop();
                }
            }
            System.out.printf("\t%s: sameTableBatchInsert completed!\nTotal records inserted: %d\nTotal time used for insertion: %fs\n", threadName, inserted, timer.getTimeInSeconds());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("%s: failed to execute sql: %s\n", threadName, sql);
        }
    }

    private void oneByOneInsert(Connection connection) {

        int inserted = 0; // counter for total inserted records
        int singleColSize = (rowSize - 8) / (columns - 1);
        int lastColSize = (rowSize - 8) % singleColSize + singleColSize;
        int totalRecords = batches * batchSize;
        long ts = ts0;
        String colValue = Strings.repeat("t", singleColSize);
        String lastColValue = Strings.repeat("t", lastColSize);
        String threadName = Thread.currentThread().getName();
        String sql = "";

        try {
            Statement stmt = connection.createStatement();
            System.out.printf("\t%s: sameTableBatchInsert %d records into %d tables...\n", threadName, batches * batchSize, tableNum);
            timer.reset();
            for (int t = 0; t < tableNum; t++) {
                StringBuilder tb = new StringBuilder("tb").append(t);
                for (int i = 0; i <= totalRecords; i++) {
                    ts = ts + timeStep;
                    String[] values = new String[columns];
                    values[0] = String.valueOf(ts);
                    for (int k = 1; k < columns - 1; k++) {
                        values[k] = colValue;
                    }
                    values[columns - 1] = lastColValue;
                    sql = SqlGenerator.getSingleInsertSql(tb.toString(), values);
                    timer.start();
                    inserted += stmt.executeUpdate(sql);
                    timer.stop();
                }
            }
            System.out.printf("\t%s: oneByOneInsert completed!\nTotal records inserted: %d\nTotal time used for insertion: %fs\n", threadName, inserted, timer.getTimeInSeconds());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("%s: failed to execute sql: %s\n", threadName, sql);
        }
    }

    /**
     * Note this method currently requires batchSize to be a divisor of tableNum
     * @param connection
     */
    private void blendedBatchInsert(Connection connection) {

        int inserted = 0; // counter for total inserted records
        int singleColSize = (rowSize - 8) / (columns - 1);
        int lastColSize = (rowSize - 8) % singleColSize + singleColSize;
        int totalRecords = batches * batchSize;
        int tbsPerBatch = tableNum / batchSize;
        long ts = ts0;
        String colValue = Strings.repeat("t", singleColSize);
        String lastColValue = Strings.repeat("t", lastColSize);
        String threadName = Thread.currentThread().getName();
        StringBuilder sql = null;

        try {
            Statement stmt = connection.createStatement();
            System.out.printf("\t%s: sameTableBatchInsert %d records into %d tables...\n", threadName, batches * batchSize, tableNum);
            timer.reset();

            for (int i = 0; i < totalRecords; i++) {
                ts = ts + timeStep;
                for (int t = 0; t < tbsPerBatch; t++) {
                    sql = new StringBuilder("insert into ");
                    for (int j = 0; j < batchSize; j++) {
                        int tbIndex = t * batchSize + j;
                        StringBuilder tb = new StringBuilder("tb").append(tbIndex);
                        sql.append(tb).append(" values(").append(ts).append(", ");
                        for (int k = 1; k < columns - 1; k++) {
                            sql.append(colValue).append(", ");
                        }
                        sql.append(lastColValue).append(") ");
                    }
                    timer.start();
                    inserted += stmt.executeUpdate(sql.toString());
                    timer.stop();
                }
            }
            System.out.printf("\t%s: blendedBatchInsert completed!\nTotal records inserted: %d\nTotal time used for insertion: %fs\n", threadName, inserted, timer.getTimeInSeconds());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("%s: failed to execute sql: %s\n", threadName, sql.toString());
        }

    }

    private void cleanUp(Connection connection, String db) {

        try {
//            Statement stmt = connection.createStatement();
//            stmt.executeUpdate("drop database if exists " + db);
//            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("%s: failed to clean up\n", Thread.currentThread().getName());
        }
    }

}

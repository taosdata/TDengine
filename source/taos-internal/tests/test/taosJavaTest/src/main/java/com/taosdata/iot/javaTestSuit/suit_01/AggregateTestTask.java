package com.taosdata.iot.javaTestSuit.suit_01;

import com.google.common.base.Strings;
import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class AggregateTestTask extends TestTask {

    private Properties properties;

    public AggregateTestTask() {
        this.properties = new Properties();
    }

    public AggregateTestTask(Properties properties) {
        this.properties = properties;
    }

    public void run() {

        System.out.printf("\t%s started...\n", Thread.currentThread().getName());
        sumTest();

    }

    /**
     * Test sum
     * @return
     */
    public void sumTest() throws TestFailureException {

        System.out.println("\tSum test");
        System.out.println("\tStarting AggregateTestTask.sumTest...");

        // get connection
        Connection connection = getConnection(properties);
        if (connection == null) {
            // failed to connect to TSDB, terminate task and return false
            throw new TestFailureException("Failed to connect to TDengine!");
        }
        Statement stmt;
        String sql = "";
        String db = "db_" + Thread.currentThread().getName().replaceAll("-","_");
        String tb = "tb";
        try {
            // create database
            stmt = connection.createStatement();
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(db);
            stmt.executeUpdate(sql);
            sql = String.format("use %s", db);
            stmt.executeUpdate(sql);

            // create table
            String[] columns = {"ts timestamp", "c1 int"};
            sql = SqlGenerator.getCreateTableSql(tb, columns);
            stmt.executeUpdate(sql);

            // insert records
            int numOfRecords = 10000; // number of records
            long ts = 1539054000000l; // '2018-10-09 11:00:00.000'
            long increase = 1000; // 1 second
            for (int i = 0; i < numOfRecords; i++) {
                ts = ts + increase;
                String[] values = {String.valueOf(ts), String.valueOf(i + 1)};
                sql = SqlGenerator.getSingleInsertSql(tb, values);
                stmt.executeUpdate(sql);
            }

            // select records
            sql = new StringBuilder("select sum(c1) from ").append(tb)
                    .append(" where ts >= '2018-10-09 11:00:00.000' and ts < '2018-10-12 11:00:00.000'").toString();
            long start = System.nanoTime();
            ResultSet resSet = stmt.executeQuery(sql);
            long end = System.nanoTime();
            System.out.printf("\tTotal time used: %f ms\n", BigDecimal.valueOf(end - start).divide(BigDecimal.valueOf(1e9)));

            ResultSetMetaData metaData = resSet.getMetaData();
            for (int col = 1; col <= metaData.getColumnCount(); ++col) {
                System.out.printf("%s|", Strings.padEnd(metaData.getColumnName(col),
                        metaData.getColumnDisplaySize(col), ' '));
            }
            System.out.printf("\n");
            if (resSet.next()) {
                if (resSet.getLong(1) == numOfRecords * (numOfRecords + 1) / 2) {
                    for (int col = 1; col <= metaData.getColumnCount(); col++) {
                        System.out.printf("%s|", Strings.padEnd(String.valueOf(resSet.getObject(col)),
                                metaData.getColumnDisplaySize(col), ' '));
                    }
                    System.out.printf("\n");
                } else {
                    throw new TestFailureException("Sum not correct");
                }
            }
            resSet.close();

            // clean up
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }
        return;
    }

}

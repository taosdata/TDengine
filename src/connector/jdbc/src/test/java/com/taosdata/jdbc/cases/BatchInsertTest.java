package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.lib.TSDBCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BatchInsertTest {

    static String host = "localhost";
    static String dbName = "test";
    static String stbName = "meters";
    static int numOfTables = 30;
    final static int numOfRecordsPerTable = 1000;
    static long ts = 1496732686000l;
    final static String tablePrefix = "t";

    private Connection connection;

    @Before
    public void before() {
        try {
            connection = TSDBCommon.getConn(host);
            TSDBCommon.createDatabase(connection, dbName);
            TSDBCommon.createStable(connection, stbName);
            TSDBCommon.createTables(connection, numOfTables, stbName, tablePrefix);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBatchInsert(){
        ExecutorService executorService = Executors.newFixedThreadPool(numOfTables);
        for (int i = 0; i < numOfTables; i++) {
            final int index = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        long startTime = System.currentTimeMillis();
                        Statement statement = connection.createStatement(); // get statement
                        StringBuilder sb = new StringBuilder();
                        sb.append("INSERT INTO " + tablePrefix + index + " VALUES");
                        Random rand = new Random();
                        for (int j = 1; j <= numOfRecordsPerTable; j++) {
                            sb.append("(" + (ts + j) + ", ");
                            sb.append(rand.nextInt(100) + ", ");
                            sb.append(rand.nextInt(100) + ", ");
                            sb.append(rand.nextInt(100) + ")");
                        }
                        statement.addBatch(sb.toString());
                        statement.executeBatch();
                        long endTime = System.currentTimeMillis();
                        System.out.println("Thread " + index + " takes " + (endTime - startTime) +  " microseconds");
                        connection.commit();
                        statement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try{
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from meters");
            int num = 0;
            while (rs.next()) {
                num++;
            }
            assertEquals(num, numOfTables * numOfRecordsPerTable);
            rs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}

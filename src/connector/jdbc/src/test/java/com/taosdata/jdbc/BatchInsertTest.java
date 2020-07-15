package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

public class BatchInsertTest extends BaseTest {

    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String stbName = "meters";
    static String host = "localhost";
    static int numOfTables = 30;
    final static int numOfRecordsPerTable = 1000;
    static long ts = 1496732686000l;
    final static String tablePrefix = "t";

    @Before
    public void createDatabase() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);
                
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);

        String createTableSql = "create table " + stbName + "(ts timestamp, f1 int, f2 int, f3 int) tags(areaid int, loc binary(20))";  
        statement.executeUpdate(createTableSql); 

        for(int i = 0; i < numOfTables; i++) {
            String loc = i % 2 == 0 ? "beijing" : "shanghai";
            String createSubTalbesSql = "create table " + tablePrefix + i + " using " + stbName + " tags(" + i + ", '" + loc + "')";             
            statement.executeUpdate(createSubTalbesSql); 
        }

    }
    
    @Test
    public void testBatchInsert() throws SQLException{
        
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

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from meters"); 
        int num = 0;
        while (rs.next()) {
            num++;
        }           
        assertEquals(num, numOfTables * numOfRecordsPerTable);        
        rs.close();
    }


    @After
    public void close() throws Exception {        
        statement.close();
        connection.close();
        Thread.sleep(10);
    }
    
}
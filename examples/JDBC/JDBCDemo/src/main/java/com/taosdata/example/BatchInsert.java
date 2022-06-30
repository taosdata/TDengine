package com.taosdata.example;

import java.sql.*;
import java.util.*;

public class BatchInsert {

    private static final String host = "127.0.0.1";    
    private static final String user = "root";
    private static final String password = "taosdata";


    private static final String dbname = "test";
    private static final String stbname = "stb";
    private static final int tables= 100;
    private static final int rows = 500;
    private static final long ts = 1604877767000l;

    private Connection conn;
    
    private void init() {
        // final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;
        final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=" + user + "&password=" + password;
        
        // get connection
        try {
            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            conn = DriverManager.getConnection(url, properties);
            if (conn != null){
                System.out.println("[ OK ] Connection established.");
            }
            
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table " + dbname + "." + stbname + "(ts timestamp, col int) tags(id int)");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String generateSql() {
        StringBuilder sb = new StringBuilder();
        Random rand = new Random();
        sb.append("insert into ");
        for (int i = 0; i < tables; i++) {
            sb.append(dbname + ".tb" + i + " using " + dbname + "." + stbname + " tags(" + i +  ") values");
            for (int j = 0; j < rows; j++) {
                sb.append("(");
                sb.append(ts + j);
                sb.append(",");
                sb.append(rand.nextInt(1000));
                sb.append(") ");
            }
        }
        return sb.toString();
    }

    private void executeQuery(String sql) {        
        try (Statement stmt = conn.createStatement()) {
            long start = System.currentTimeMillis();
            stmt.execute(sql);
            long end = System.currentTimeMillis();
            
            System.out.println("insert " + tables * rows + " records, cost " + (end - start)+ "ms");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BatchInsert bi = new BatchInsert();

        String sql = bi.generateSql();
        bi.init();
        bi.executeQuery(sql);
    }


}

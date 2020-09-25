package com.taosdata.example.jdbcTaosdemo.utils;

import java.util.Random;

public class SqlSpeller {
    private static final Random random = new Random(System.currentTimeMillis());
    private static final String[] locations = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen",
            "HangZhou", "Tianjin", "Wuhan", "Changsha", "Nanjing", "Xian"
    };

    public static String createDatabaseSQL(String dbName, int keep, int days) {
        return "create database if not exists " + dbName + " keep " + keep + " days " + days;
    }

    public static String dropDatabaseSQL(String dbName) {
        return "drop database if exists " + dbName;
    }

    public static String useDatabaseSQL(String dbName) {
        return "use " + dbName;
    }

    public static String createSuperTableSQL(String superTableName) {
        return "create table if not exists " + superTableName + "(ts timestamp, current float, voltage int, phase float) tags(location binary(64), groupId int)";
    }

    public static String dropSuperTableSQL(String dbName, String superTableName) {
        return "drop table if exists " + dbName + "." + superTableName;
    }

    public static String createTableSQL(int tableIndex, String dbName, String superTableName) {
        String location = locations[random.nextInt(locations.length)];
        return "create table d" + tableIndex + " using " + dbName + "." + superTableName + " tags('" + location + "'," + tableIndex + ")";
    }

    public static String insertOneRowSQL(String dbName, String tbPrefix, int tableIndex, long ts) {
        float current = 10 + random.nextFloat();
        int voltage = 200 + random.nextInt(20);
        float phase = random.nextFloat();
        String sql = "insert into " + dbName + "." + tbPrefix + "" + tableIndex + " " + "values(" + ts + ", " + current + ", " + voltage + ", " + phase + ")";
        return sql;
    }

    public static String insertBatchSizeRowsSQL(String dbName, String tbPrefix, int tbIndex, long ts, int valuesCount) {
        float current = 10 + random.nextFloat();
        int voltage = 200 + random.nextInt(20);
        float phase = random.nextFloat();
        StringBuilder sb = new StringBuilder();
        sb.append("insert into " + dbName + "." + tbPrefix + "" + tbIndex + " " + "values");
        for (int i = 0; i < valuesCount; i++) {
            sb.append("(" + (ts + i) + ", " + current + ", " + voltage + ", " + phase + ") ");
        }
        return sb.toString();
    }


}
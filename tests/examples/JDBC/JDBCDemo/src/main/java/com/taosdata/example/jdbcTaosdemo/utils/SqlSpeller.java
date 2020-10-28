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

    public static String selectFromTableLimitSQL(String dbName, String tbPrefix, int tbIndex, int limit, int offset) {
        return "select * from " + dbName + "." + tbPrefix + "" + tbIndex + " limit " + limit + " offset " + offset;
    }

    public static String selectCountFromTableSQL(String dbName, String tbPrefix, int tbIndex) {
        return "select count(*) from " + dbName + "." + tbPrefix + "" + tbIndex;
    }

    public static String selectAvgMinMaxFromTableSQL(String field, String dbName, String tbPrefix, int tbIndex) {
        return "select avg(" + field + "),min(" + field + "),max(" + field + ") from " + dbName + "." + tbPrefix + "" + tbIndex;
    }

    public static String selectFromSuperTableLimitSQL(String dbName, String stbName, int limit, int offset) {
        return "select * from " + dbName + "." + stbName + " limit " + limit + " offset " + offset;
    }

    public static String selectCountFromSuperTableSQL(String dbName, String stableName) {
        return "select count(*) from " + dbName + "." + stableName;
    }

    public static String selectAvgMinMaxFromSuperTableSQL(String field, String dbName, String stbName) {
        return "select avg(" + field + "),min(" + field + "),max(" + field + ") from " + dbName + "." + stbName + "";
    }

    public static String selectLastFromTableSQL(String dbName, String tbPrefix, int tbIndex) {
        return "select last(*) from " + dbName + "." + tbPrefix + "" + tbIndex;
    }

    //select avg ,max from stb where tag
    public static String selectAvgMinMaxFromSuperTableWhere(String field, String dbName, String stbName) {
        return "select avg(" + field + "),min(" + field + "),max(" + field + ") from " + dbName + "." + stbName + " where location = '" + locations[random.nextInt(locations.length)] + "'";
    }

    //select last from stb where
    public static String selectLastFromSuperTableWhere(String field, String dbName, String stbName) {
        return "select last(" + field + ") from " + dbName + "." + stbName + " where location = '" + locations[random.nextInt(locations.length)] + "'";
    }

    public static String selectGroupBy(String field, String dbName, String stbName) {
        return "select avg(" + field + ") from " + dbName + "." + stbName + " group by location";
    }

    public static String selectLike(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where location like 'S%'";
    }

    public static String selectLastOneHour(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where ts >= now - 1h";
    }

    public static String selectLastOneDay(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where ts >= now - 1d";
    }

    public static String selectLastOneWeek(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where ts >= now - 1w";
    }

    public static String selectLastOneMonth(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where ts >= now - 1n";
    }

    public static String selectLastOneYear(String dbName, String stbName) {
        return "select * from " + dbName + "." + stbName + " where ts >= now - 1y";
    }

    // select group by
    // select like
    // select ts >= ts<=
}
package com.taosdata.jdbc.oom;

import com.taosdata.jdbc.TSDBPreparedStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;

public class BatchInsertTest {
    Connection connection;
    String host = "127.0.0.1";
    String dbname = "batch_test";

    @Test
    public void batchInsert() throws SQLException {
        Statement statement = connection.createStatement();
        String tname = "weather_test";
        String dropSql = "drop table if exists " + tname;
        String createSql = "create table " + tname + "(ts timestamp, f1 nchar(10), f5 int)";
        statement.execute(dropSql);
        statement.execute(createSql);
        TSDBPreparedStatement preparedStatement = (TSDBPreparedStatement) connection.prepareStatement("insert into ? values(?, ?, ?)");
        preparedStatement.setTableName(tname);

        int numOfRows = 1000;

        ArrayList<Long> ts = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            ts.add(System.currentTimeMillis() - 1000);
        }
        preparedStatement.setTimestamp(0, ts);

        ArrayList<String> stringList = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            stringList.add("test" + i);
        }
        preparedStatement.setNString(1, stringList, 10);

        ArrayList<Integer> intList = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            intList.add(i);
        }
        preparedStatement.setInt(2, intList);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
        preparedStatement.columnDataClearBatch();
        preparedStatement.close();
    }

    @Before
    public void before() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            connection = DriverManager.getConnection(url);
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + dbname);
            statement.execute("create database if not exists " + dbname);
            statement.execute("use " + dbname);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbname);
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}

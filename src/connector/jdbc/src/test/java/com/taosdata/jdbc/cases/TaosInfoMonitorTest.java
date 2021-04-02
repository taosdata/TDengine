package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaosInfoMonitorTest {

    @Test
    public void testCreateTooManyConnection() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        final String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

        List<Connection> connectionList = IntStream.range(0, 100).mapToObj(i -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
                return DriverManager.getConnection(url);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());

        connectionList.stream().forEach(conn -> {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("show databases");
                while (rs.next()) {

                }
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        connectionList.stream().forEach(conn -> {
            try {
                conn.close();
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}

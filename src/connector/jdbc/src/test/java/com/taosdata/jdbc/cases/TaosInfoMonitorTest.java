package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaosInfoMonitorTest {

    @Test
    public void testCreateTooManyConnection() throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        int conCnt = 0;
        final String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

        List<Connection> connectionList = IntStream.range(0, 100).mapToObj(i -> {
            try {
                return DriverManager.getConnection(url);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
        connectionList.stream().forEach(conn -> {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}

package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class FailOverTest {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Test
    public void testFailOver() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        final String url = "jdbc:TAOS://:/?user=root&password=taosdata";

        long end = System.currentTimeMillis() + 1000 * 60 * 5;
        while (System.currentTimeMillis() < end) {
            try (Connection conn = DriverManager.getConnection(url); Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("show dnodes");
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                    }
                    System.out.println();
                }
                System.out.println("=======================");
                rs.close();
                TimeUnit.SECONDS.sleep(5);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}

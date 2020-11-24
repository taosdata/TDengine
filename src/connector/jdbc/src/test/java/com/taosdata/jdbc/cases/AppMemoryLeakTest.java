package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AppMemoryLeakTest {

    @Test
    public void testAppMemoryLeak() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");

            while (true) {
                DriverManager.getConnection("jdbc:TAOS://localhost:6030/?user=root&password=taosdata");
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
}

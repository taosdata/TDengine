package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.lib.TSDBCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class FailOverTest {

    private Connection conn;

    @Before
    public void before() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            final String url = "jdbc:TAOS://:/?user=root&password=taosdata";
            conn = DriverManager.getConnection(url);
            TSDBCommon.createDatabase(conn, "failover_test");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFailOver() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select server_status()");
            while (true) {
                resultSet.next();
                int status = resultSet.getInt("server_status()");
                System.out.println(">>>>>>>>> status : " + status);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @After
    public void after() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

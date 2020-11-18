package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.lib.TSDBCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FailOverTest {

    private Connection conn;
    private static final String host = "localhost";

    @Before
    public void before() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            conn = TSDBCommon.getConn(host);
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

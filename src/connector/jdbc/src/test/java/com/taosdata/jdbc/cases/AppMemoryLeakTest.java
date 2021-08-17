package com.taosdata.jdbc.cases;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class AppMemoryLeakTest {

    @Test(expected = SQLException.class)
    public void testCreateTooManyConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        while (true) {
            Connection conn = DriverManager.getConnection("jdbc:TAOS://localhost:6030/?user=root&password=taosdata");
            Assert.assertNotNull(conn);
        }
    }

    @Test(expected = Exception.class)
    public void testCreateTooManyStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:TAOS://localhost:6030/?user=root&password=taosdata");
        while (true) {
            Statement stmt = conn.createStatement();
            Assert.assertNotNull(stmt);
        }
    }

}

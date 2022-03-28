package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectWrongDatabaseTest {

    @Test(expected = SQLException.class)
    public void connectByJni() throws SQLException {
        DriverManager.getConnection("jdbc:TAOS://localhost:6030/wrong_db?user=root&password=taosdata");
    }

    @Test(expected = SQLException.class)
    public void connectByRestful() throws SQLException {
        DriverManager.getConnection("jdbc:TAOS-RS://localhost:6041/wrong_db?user=root&password=taosdata");
    }

}

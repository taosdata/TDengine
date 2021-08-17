package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBErrorNumbers;
import org.junit.Assert;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectWrongDatabaseTest {

    @Test
    public void connect() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            DriverManager.getConnection("jdbc:TAOS://localhost:6030/wrong_db?user=root&password=taosdata");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, e.getErrorCode());
        }
    }

}

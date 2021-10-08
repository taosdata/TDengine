package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ResetQueryCacheTest {

    @Test
    public void jni() throws SQLException {
        // given
        Connection connection = DriverManager.getConnection("jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8");
        Statement statement = connection.createStatement();

        // when
        boolean execute = statement.execute("reset query cache");

        // then
        assertFalse(execute);
        assertEquals(0, statement.getUpdateCount());

        statement.close();
        connection.close();
    }

    @Test
    public void restful() throws SQLException {
        // given
        Connection connection = DriverManager.getConnection("jdbc:TAOS-RS://127.0.0.1:6041/?user=root&password=taosdata&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8");
        Statement statement = connection.createStatement();

        // when
        boolean execute = statement.execute("reset query cache");

        // then
        assertFalse(execute);
        assertEquals(0, statement.getUpdateCount());

        statement.close();
        connection.close();
    }

}
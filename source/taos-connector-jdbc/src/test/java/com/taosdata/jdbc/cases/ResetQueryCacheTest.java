package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
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
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":0/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8";
        } else {
            url += "&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8";
        }
        Connection connection = DriverManager.getConnection(url);
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
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8";
        } else {
            url += "&timezone=UTC-8&charset=UTF-8&locale=en_US.UTF-8";
        }
        Connection connection = DriverManager.getConnection(url);
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
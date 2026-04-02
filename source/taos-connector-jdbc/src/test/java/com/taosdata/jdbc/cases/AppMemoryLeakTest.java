package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Ignore
public class AppMemoryLeakTest {

    private static String url;

    @Test(expected = SQLException.class)
    public void testCreateTooManyConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        while (true) {
            Connection conn = DriverManager.getConnection(url);
            Assert.assertNotNull(conn);
        }
    }

    @Test(expected = Exception.class)
    public void testCreateTooManyStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Connection conn = DriverManager.getConnection(url);
        while (true) {
            Statement stmt = conn.createStatement();
            Assert.assertNotNull(stmt);
        }
    }

    @BeforeClass
    public static void before() {
        url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
    }

}


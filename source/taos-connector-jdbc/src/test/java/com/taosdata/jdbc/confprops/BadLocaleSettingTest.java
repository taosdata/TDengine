package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Ignore
public class BadLocaleSettingTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(BadLocaleSettingTest.class);
    private static Connection conn;

    @Test
    public void canSetLocale() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        Assert.assertNotNull(stmt);
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("drop table if exists weather");
        stmt.execute("create table weather(ts timestamp, temperature float, humidity int)");
        stmt.executeUpdate("insert into weather values(1624071506435, 12.3, 4)");
        stmt.close();
    }

    @Before
    public void beforeClass() {
        System.setProperty("sun.jnu.encoding", "ANSI_X3.4-1968");
        System.setProperty("file.encoding", "ANSI_X3.4-1968");
    }

    @After
    public void afterClass() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database " + DB_NAME);
            statement.close();
            conn.close();
        }
    }
}
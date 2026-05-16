package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestfulCompressTest {

    private static Connection connection;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(RestfulCompressTest.class);
    private static final String TABLE_NAME = "compressA";

    @Description("inertRows")
    @Test
    public void inertRows() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + DB_NAME + "." + TABLE_NAME + " (tbname, location, groupId, ts, current, voltage, phase) \n" +
                    "                values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) \n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:36.780', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:37.781', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:59.779', 10.16, 217, 0.33)");

            ResultSet resultSet = statement.executeQuery("select * from " + DB_NAME + "." + TABLE_NAME + " order by ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(2) > 10.15);
        }
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getRestUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");
            connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.execute("create database " + DB_NAME);
            statement.execute("use " + DB_NAME);
            statement.execute("CREATE STABLE " + TABLE_NAME + " (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.close();
            connection.close();
        }
    }

}


package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class WSGeometryTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WSGeometryTest.class);
    static final String TABLE_NATIVE = "geometry_noraml";
    static final String TABLE_STMT = "geometry_stmt";
    static Connection connection;
    static Statement statement;
    final byte[] expectedArray = StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");

    @Test
    public void testInsert() throws Exception {
        String sql = "insert into ? using stable3 tags(?) values(?,?)";

        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NATIVE + " tags( \"POINT(3 5)\") values(now, \"POINT(1 2)\")");
        ResultSet resultSet = statement.executeQuery("select c1 from subt_a");

        resultSet.next();
        byte[] result1 = resultSet.getBytes(1);
        Assert.assertArrayEquals(expectedArray, result1);
    }

    @Test
    public void testPrepare() throws SQLException {
        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into ? using  " + DB_NAME + "." + TABLE_STMT + "  tags(?) values (?, ?)");
        preparedStatement.setTableName("subt_b");
        preparedStatement.setTagGeometry(0, expectedArray);

        long current = System.currentTimeMillis();
        ArrayList<Long> tsList = new ArrayList<>();
        tsList.add(current);
        tsList.add(current + 1);

        preparedStatement.setTimestamp(0, tsList);

        ArrayList<byte[]> list = new ArrayList<>();
        byte[] byteArray = StringUtils.hexToBytes("0101000020E6100000000000000000F03F0000000000000040");
        list.add(byteArray);
        byte[] byteArray1 = StringUtils.hexToBytes("0102000020E610000002000000000000000000F03F000000000000004000000000000008400000000000001040");
        list.add(byteArray1);

        preparedStatement.setGeometry(1, list, 50);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
        ResultSet resultSet = statement.executeQuery("select c1 from subt_b order by ts asc");
        if (resultSet.next()) {
            Assert.assertArrayEquals(byteArray, resultSet.getBytes(1));
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + DB_NAME);
        statement.executeUpdate("create database if not exists " + DB_NAME);
        statement.executeUpdate("use " + DB_NAME);
        statement.executeUpdate("create table " + TABLE_NATIVE + " (ts timestamp, c1 GEOMETRY(50))  tags(t1 GEOMETRY(50))");
        statement.executeUpdate("create table " + TABLE_STMT + " (ts timestamp, c1 GEOMETRY(50))    tags(t1 GEOMETRY(50))");
    }

    @AfterClass
    public static void after() {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}


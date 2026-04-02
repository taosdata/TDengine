package com.taosdata.jdbc;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "Schemaless", author = "huolibo", version = "2.0.36")
public class SchemalessRawInsertNewTest {
    private static final String HOST = TestEnvUtil.getHost();
    private final String dbName = TestUtils.camelToSnake(SchemalessRawInsertNewTest.class);
    private Connection conn;

    /**
        * schemaless insert compatible with influxdb
        *
        * @throws SQLException execute error
        */
    @Test
    @Description("line insert")
    public void schemalessInsert() throws SQLException {
        // given
        String line = "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
                "measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
                "measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
                "measurement,host=host1 field1=2i,field2=2.0 1577837600000";
        // when
        int num = ((AbstractConnection)conn).writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        Assert.assertEquals(4, num);
        // then
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from measurement");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        rs.next();
        Assert.assertEquals(4, rs.getInt(1));
        rs.close();
        statement.close();
    }

    /**
        * telnet insert compatible with opentsdb
        *
        * @throws SQLException execute error
        */
    @Test
    @Description("telnet insert")
    public void telnetInsert() throws SQLException {
        // given
        String lines = "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
                "meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
                "meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
                "meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
                "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
                "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
                "meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
                "meters.voltage 1648432611250 217 location=California.LosAngeles group=3";

        // when

        ((AbstractConnection)conn).writeRaw(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("show stables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(2, rowCnt);
        rs.close();
        statement.close();
    }

    /**
        * json insert compatible with opentsdb json format
        *
        * @throws SQLException execute error
        */
    @Test
    @Description("json insert")
    public void jsonInsert() throws SQLException {
        // given
        String json = "[{\"metric\": \"meters.current\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": " +
                "{\"location\": \"California.SanFrancisco\", \"groupid\": 2 } }, {\"metric\": \"meters.voltage\", " +
                "\"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"California.LosAngeles\", " +
                "\"groupid\": 1 } }, {\"metric\": \"meters.current\", \"timestamp\": 1648432611250, \"value\": 12.6, " +
                "\"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2 } }, {\"metric\": \"meters.voltage\", " +
                "\"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", " +
                "\"groupid\": 1 } }]";

        // when
        ((AbstractConnection)conn).writeRaw(json, SchemalessProtocolType.JSON, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }

        Assert.assertEquals(2, rowCnt);
        rs.close();
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName + " precision 'ns'");
        stmt.execute("use " + dbName);
    }

    @After
    public void after() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop database if exists " + dbName);
        }
        conn.close();
    }
}


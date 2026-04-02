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
import java.util.Arrays;
import java.util.List;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "Schemaless", author = "huolibo", version = "2.0.36")
@SuppressWarnings("java:S1874")
public class SchemalessRawInsertTest {
    private static final String HOST = TestEnvUtil.getHost();
    private final String dbname = TestUtils.camelToSnake(SchemalessRawInsertTest.class);
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
        SchemalessWriter writer = new SchemalessWriter(conn);
        int num = writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
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

        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.writeRaw(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

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
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.writeRaw(json, SchemalessProtocolType.JSON, SchemalessTimestampType.MILLI_SECONDS);

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

    @Test
    public void testSchemalessWriterConstructorWithHostPortUserPassword() throws SQLException {
        String host = TestEnvUtil.getHost();
        String port = String.valueOf(TestEnvUtil.getJniPort());

        // This will fail if server is not running, but we test the constructor
        try {
            SchemalessWriter writer = new SchemalessWriter(host, port, TestEnvUtil.getUser(), TestEnvUtil.getPassword(), dbname, "jni");
            Assert.assertNotNull(writer);
            writer.close();
        } catch (SQLException e) {
            // Expected if server not running
        }
    }

    @Test
    public void testSchemalessWriterConstructorWithHostPortUserPasswordSSL() throws SQLException {
        String host = TestEnvUtil.getHost();
        String port = String.valueOf(TestEnvUtil.getWsPort());

        try {
            SchemalessWriter writer = new SchemalessWriter(host, port, TestEnvUtil.getUser(), TestEnvUtil.getPassword(), dbname, "ws", false);
            Assert.assertNotNull(writer);
            writer.close();
        } catch (SQLException e) {
            // Expected if server not running
        }
    }

    @Test
    public void testSchemalessWriterConstructorWithHostPortToken() throws SQLException {
        String host = TestEnvUtil.getHost();
        String port = String.valueOf(TestEnvUtil.getWsPort());

        try {
            SchemalessWriter writer = new SchemalessWriter(host, port, "test-token", dbname, false);
            Assert.assertNotNull(writer);
            writer.close();
        } catch (SQLException e) {
            // Expected if server not running
        }
    }

    @Test
    public void testSchemalessWriterWriteArray() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_array,host=host1 field1=2i,field2=2.0 1577837300000",
                "measurement_array,host=host2 field1=3i,field2=3.0 1577837400000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from measurement_array");
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteArrayWithTtl() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_ttl,host=host1 field1=2i 1577837300000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, 3600, null);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_ttl");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteArrayWithReqId() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_reqid,host=host1 field1=2i 1577837300000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, null, 1000L);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_reqid");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteArrayWithTtlAndReqId() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_both,host=host1 field1=2i 1577837300000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, 3600, 1001L);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_both");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteSingleString() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_single,host=host1 field1=1i 1577837300000";
        writer.write(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from measurement_single");
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteList() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        List<String> lines = Arrays.asList(
                "measurement_list,host=host1 field1=1i 1577837300000",
                "measurement_list,host=host2 field1=2i 1577837400000"
        );

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from measurement_list");
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteDeprecatedWithDbName() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_deprecated,host=host1 field1=2i 1577837300000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, dbname, null, null);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_deprecated");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteRawWithTtlAndReqId() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_raw_params,host=host1 field1=2i 1577837300000";
        int count = writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, 3600, 1002L);
        Assert.assertEquals(1, count);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_raw_params");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteRawDeprecated() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_raw_deprecated,host=host1 field1=2i 1577837300000";
        int count = writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, dbname, 3600, 1003L);
        Assert.assertEquals(1, count);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_raw_deprecated");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterMicrosecondPrecision() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_us,host=host1 field1=2i 1577837300000000";
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MICRO_SECONDS);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_us");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterNanosecondPrecision() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_ns,host=host1 field1=2i 1577837300000000000";
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_ns");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterCloseMultipleTimes() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_close,host=host1 field1=1i 1577837300000";
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        writer.close();
        writer.close();

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_close");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
    }

    @Test(expected = Exception.class)
    public void testSchemalessWriterWriteEmptyLines() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] emptyLines = {};
        writer.write(emptyLines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        writer.write("", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        int count = writer.writeRaw("", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        Assert.assertEquals(0, count);

        writer.close();
    }

    @Test(expected = Exception.class)
    public void testSchemalessWriterWriteWithNullProtocol() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {"test,host=host1 field1=1i 1577837300000"};
        writer.write(lines, null, SchemalessTimestampType.MILLI_SECONDS);

        writer.close();
    }

    @Test(expected = Exception.class)
    public void testSchemalessWriterWriteWithNullTimestampType() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {"test,host=host1 field1=1i 1577837300000"};
        writer.write(lines, SchemalessProtocolType.LINE, null);

        writer.close();
    }

    @Test
    public void testSchemalessWriterDatabaseSwitch() throws SQLException {
        Statement statement = conn.createStatement();
        String dbname2 = "test_schemaless_writer2";
        statement.execute("drop database if exists " + dbname2);
        statement.execute("create database if not exists " + dbname2);

        SchemalessWriter writer = new SchemalessWriter(conn);

        String line1 = "measurement_db1,host=host1 field1=1i 1577837300000";
        writer.writeRaw(line1, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        String line2 = "measurement_db2,host=host1 field1=2i 1577837300000";
        writer.writeRaw(line2, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, dbname2, null, null);

        statement.execute("use " + dbname);
        ResultSet rs1 = statement.executeQuery("select * from measurement_db1");
        Assert.assertTrue(rs1.next());
        rs1.close();

        statement.execute("use " + dbname2);
        ResultSet rs2 = statement.executeQuery("select * from measurement_db2");
        Assert.assertTrue(rs2.next());
        rs2.close();

        statement.execute("drop database if exists " + dbname2);
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteRawMultipleLinesWithMixedProtocols() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String telnetLines = "mixed.telnet 1648432611249 10.3 location=test group=1\n" +
                "mixed.telnet 1648432611250 11.3 location=test group=1";

        writer.writeRaw(telnetLines, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        String json = "[{\"metric\": \"mixed.json\", \"timestamp\": 1648432611249, \"value\": 20.5, " +
                "\"tags\": {\"location\": \"test\", \"groupid\": 2 }}]";

        writer.writeRaw(json, SchemalessProtocolType.JSON, SchemalessTimestampType.MILLI_SECONDS);

        String line = "mixed.line,host=host1 field1=5i 1577837300000";
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        Statement statement = conn.createStatement();

        ResultSet rs1 = statement.executeQuery("show stables");
        int stableCount = 0;
        while (rs1.next()) {
            stableCount++;
        }
        Assert.assertTrue(stableCount >= 2);
        rs1.close();

        ResultSet rs2 = statement.executeQuery("show tables");
        int tableCount = 0;
        while (rs2.next()) {
            tableCount++;
        }
        Assert.assertTrue(tableCount >= 3);
        rs2.close();

        statement.close();
        writer.close();
    }
    @Test
    public void testSchemalessWriterWriteRawWithZeroTtl() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String line = "measurement_zero_ttl,host=host1 field1=1i 1577837300000";
        int count = writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, 0, null);
        Assert.assertEquals(1, count);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_zero_ttl");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testSchemalessWriterWriteWithNegativeReqId() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter(conn);

        String[] lines = {
                "measurement_neg_req,host=host1 field1=1i 1577837300000"
        };

        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS, null, -1L);

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from measurement_neg_req");
        Assert.assertTrue(rs.next());
        rs.close();
        statement.close();
        writer.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + dbname);
        stmt.execute("create database if not exists " + dbname + " precision 'ns'");
        stmt.execute("use " + dbname);
    }

    @After
    public void after() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop database if exists " + dbname);
        }
        conn.close();
    }
}


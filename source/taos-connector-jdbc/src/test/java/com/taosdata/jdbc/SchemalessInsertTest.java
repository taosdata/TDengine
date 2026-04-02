package com.taosdata.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "Schemaless", author = "huolibo", version = "2.0.36")
@SuppressWarnings("java:S1874")
public class SchemalessInsertTest {

    static final String HOST = TestEnvUtil.getHost();
            private final String dbname = TestUtils.camelToSnake(SchemalessInsertTest.class);
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
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};
        // when
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

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
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }
    @Test
    public void testLine2() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        SchemalessWriter writer = new SchemalessWriter(conn, dbname);
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 10000, 100L);

        // then
        Statement statement = conn.createStatement();
        statement.executeUpdate("use " + dbname);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void testLine3() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, dbname, 10000, 100L);

        // then
        Statement statement = conn.createStatement();
        statement.executeUpdate("use " + dbname);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    @Description("line insert")
    public void testWriteRaw() throws SQLException {
        // given
        String line = "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";

        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

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
        Assert.assertEquals(1, rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }
    @Test
    public void testWriteRaw2() throws SQLException {
        // given
        String line = "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
        // when
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 10000, 100L);

        // then
        Statement statement = conn.createStatement();
        statement.executeUpdate("use " + dbname);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(1, rowCnt);
        rs.close();
        statement.close();
        writer.close();
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
        String[] lines = new String[]{
                "stb0_0 1626006833 4 host=host0 interface=eth0",
                "stb0_1 1626006833 4 host=host0 interface=eth0",
                "stb0_2 1626006833 4 host=host0 interface=eth0 id=\"special_name\"",
        };

        // when

        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.NOT_CONFIGURED);

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
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }

    /**
        * json insert compatible with opentsdb json format
        *
        * @throws SQLException execute error
        */
    @Test
    @Description("json insert")
    public void jsonInsert() throws SQLException, JsonProcessingException {
        // given
        String json = "[\n" +
                "  {\n" +
                "    \"metric\": \"cpu_load_1\",\n" +
                "    \"timestamp\": 1626006833,\n" +
                "    \"value\": 55.5,\n" +
                "    \"tags\": {\n" +
                "      \"host\": \"ubuntu\",\n" +
                "      \"interface\": \"eth1\",\n" +
                "      \"Id\": \"tb1\"\n" +
                "    }\n" +
                "  },\n" +
                "  {\n" +
                "    \"metric\": \"cpu_load_2\",\n" +
                "    \"timestamp\": 1626006834,\n" +
                "    \"value\": 55.5,\n" +
                "    \"tags\": {\n" +
                "      \"host\": \"ubuntu\",\n" +
                "      \"interface\": \"eth2\",\n" +
                "      \"Id\": \"tb2\"\n" +
                "    }\n" +
                "  }\n" +
                "]";

        // when
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write(json, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);

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

        JsonNode jsonArray = JsonUtil.getObjectReader().readTree(json);
        Assert.assertEquals(jsonArray.size(), rowCnt);
        rs.close();
        statement.close();
        writer.close();
    }

    @Test
    public void telnetListInsert() throws SQLException {
        // given
        List<String> list = new ArrayList<>();
        list.add("stb0_0 1626006833 4 host=host0 interface=eth0");
        list.add("stb0_1 1626006833 4 host=host0 interface=eth0");
        list.add("stb0_2 1626006833 4 host=host0 interface=eth0 id=\"special_name\"");
        // when

        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write(list, SchemalessProtocolType.TELNET, SchemalessTimestampType.NOT_CONFIGURED);

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
        Assert.assertEquals(list.size(), rowCnt);
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
        stmt.executeUpdate("drop database if exists " + dbname);
        stmt.executeUpdate("create database if not exists " + dbname + " precision 'ns'");
        stmt.executeUpdate("use " + dbname);
    }

    @After
    public void after() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop database if exists " + dbname);
        }
        conn.close();
    }
}


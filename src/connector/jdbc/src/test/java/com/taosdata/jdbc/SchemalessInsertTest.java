package com.taosdata.jdbc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "Schemaless",author = "huolibo", version = "2.0.36")
public class SchemalessInsertTest {
    private final String dbname = "test_schemaless_insert";
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

        Assert.assertEquals(((JSONArray) JSONObject.parse(json)).size(), rowCnt);
        rs.close();
        statement.close();
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
    }

    @Before
    public void before() {
        String host = "127.0.0.1";
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision 'ns'");
            stmt.execute("use " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

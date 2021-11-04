package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class SchemalessInsertTest {
    private String host = "127.0.0.1";
    private String dbname = "test_schemaless_insert";
    private Connection conn;

    @Test
    public void schemalessInsert() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};
        // when
        try (Statement statement = conn.createStatement();
             SchemalessStatement schemalessStatement = new SchemalessStatement(statement)) {
            schemalessStatement.executeSchemaless(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
        }

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

    @Test
    public void telnetInsert() throws SQLException {
        // given
        String[] lines = new String[]{
                "stb0_0 1626006833 4 host=host0 interface=eth0",
                "stb0_1 1626006833 4 host=host0 interface=eth0",
                "stb0_2 1626006833 4 host=host0 interface=eth0 id=\"special_name\"",
        };

        // when
        try (Statement statement = conn.createStatement();
             SchemalessStatement schemalessStatement = new SchemalessStatement(statement)) {
            schemalessStatement.executeSchemaless(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.NOT_CONFIGURED);
        }

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

    @Test
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
        try (Statement statement = conn.createStatement();
             SchemalessStatement schemalessStatement = new SchemalessStatement(statement)) {
            schemalessStatement.executeSchemaless(json, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
        }

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
//        Assert.assertEquals(json.length, rowCnt);
        rs.close();
        statement.close();
    }

    @Before
    public void before() {
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

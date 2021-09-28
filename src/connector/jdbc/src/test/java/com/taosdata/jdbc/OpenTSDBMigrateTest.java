package com.taosdata.jdbc;

import com.alibaba.fastjson.JSONObject;
import org.junit.*;

import java.sql.*;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class OpenTSDBMigrateTest {
    private static final String host = "127.0.0.1";
    private static final String dbname = "opentsdb_migrate_test";
    private static TSDBConnection conn;

    @Test
    public void telnetPut() throws SQLException {
        // given
        String[] lines = new String[]{
                "stb0_0 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\"",
                "stb0_1 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\"",
                "stb0.2 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\" id=\"name\""
        };

        // when
        conn.getConnector().insertTelnetLines(lines);

        // then
        Set<String> collect = Arrays.stream(lines)
                .map(String::trim).filter(s -> s.length() > 0)
                .map(s -> s.split("\\s+")[0].replaceAll("\\.", "_"))
                .collect(Collectors.toSet());

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("show stables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        while (rs.next()) {
            Assert.assertTrue(collect.contains(rs.getString(1)));
        }
        rs.close();
        statement.close();
    }

    @Test
    public void jsonPut() throws SQLException {

        // given
        String json = "{\n" +
                "    \"metric\":   \"stb0.3\",\n" +
                "    \"timestamp\":        1626006833610123,\n" +
                "    \"value\":    10,\n" +
                "    \"tags\":     {\n" +
                "        \"t1\":   true,\n" +
                "        \"t2\":   false,\n" +
                "        \"t3\":   10,\n" +
                "        \"t4\":   \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\"\n" +
                "    }\n" +
                "}";

        // when
        conn.getConnector().insertJsonPayload(json);

        // then
        JSONObject jObject = JSONObject.parseObject(json);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("show stables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        while (rs.next()) {
            Assert.assertEquals(jObject.getString("metric").replaceAll("\\.", "_"), rs.getString(1));
        }
        statement.execute("drop table " + jObject.getString("metric").replaceAll("\\.", "_"));
        rs.close();
        statement.close();
    }

    @BeforeClass
    public static void beforeClass() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = (TSDBConnection) DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision 'ns'");
            stmt.execute("use " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

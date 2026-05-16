package com.taosdata.jdbc.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WSSchemalessNewTest {

        static final String HOST = TestEnvUtil.getHost();
        private static final String DB_NAME = TestUtils.camelToSnake(WSSchemalessNewTest.class);
    public static Connection connection;

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");

        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB_NAME);
            statement.executeUpdate("create database " + DB_NAME);
            statement.executeUpdate("use " + DB_NAME);
        }
    }

    @Test
    public void testLine() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
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
    public void testLine2() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 10000, 100L);
        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
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
    @Description("line insert")
    public void testWriteRaw() throws SQLException {
        // given
        String line = "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";

        ((AbstractConnection)connection).writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

        // then
        Statement statement = connection.createStatement();
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
    }
    @Test
    public void testWriteRaw2() throws SQLException {
        // given
        String line = "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
        // when
        ((AbstractConnection)connection).writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 10000, 100L);
        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
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
    }

    @Test
    @Description("telnet insert with raw data")
    @Ignore
    public void testWriteRawTelnet() throws SQLException {

        String[] lines = new String[]{
                "stb1 1742374817157 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817158 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817159 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817160 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817161 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817162 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817163 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817164 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817165 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\"",
                "stb1 1742374817166 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\""
        };

        // given
        String line = String.join("\n", lines);

        ((AbstractConnection)connection).writeRaw(line, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, Utils.getSqlRows(connection, DB_NAME + ".stb1"));
        rs.close();
        statement.close();
    }

    @Test
    public void testLineTtl() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 1000, 1L);
    }

    @Test
    public void telnetInsert() throws SQLException {
        // given
        String[] lines = new String[]{
                "stb1 1742374817157 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817158 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817159 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817160 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817161 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817162 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817163 true t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817164 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817165 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" ",
                "stb1 1742374817166 false t0=-834409214i32 t1=491614887i64 t2=226345616.000000f32 t3=646745464.983636f64 t4=9183i16 t5=103i8 t6=true t7=L\"qMCx\" "
        };

        // when

        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, Utils.getSqlRows(connection, DB_NAME + ".stb1"));
        rs.close();
        statement.close();
    }

    @Test
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
        ((AbstractConnection)connection).write(json, SchemalessProtocolType.JSON, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }

        JsonNode jsonArray = JsonUtil.getObjectReader().readTree(json);
        // 断言 JSON 数组的大小
        Assert.assertEquals(jsonArray.size(), rowCnt);
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

        ((AbstractConnection)connection).write(list, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + DB_NAME);
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

    @AfterClass
    public static void after() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}


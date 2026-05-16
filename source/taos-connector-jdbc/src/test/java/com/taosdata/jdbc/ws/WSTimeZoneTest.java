package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

@RunWith(Parameterized.class)
@TestTarget(alias = "websocket timezon test", author = "sheyj", version = "3.5.0")
public class WSTimeZoneTest {

    // 定义参数

    static final String HOST = TestEnvUtil.getHost();
    private final String precision;

    // 构造函数
    public WSTimeZoneTest(String precision) {
        this.precision = precision;
    }

        static final int PORT = TestEnvUtil.getWsPort();
    private final String dbName = TestUtils.camelToSnake(WSTimeZoneTest.class) + "_" + UUID.randomUUID().toString().replace("-", "_");
    private static final String TABLE_NAME = "simple_t";
    private static final String FULL_TABLE_NAME = "full_t";
    private Connection connection;

    // 提供参数
    @Parameterized.Parameters
    public static Collection<String> data() {
        return Arrays.asList("'ms'", "'us'", "'ns'");
    }

    private static final HashMap<String, String> precisionMap = new HashMap<String, String>() {{
        put("'ms'", "123");
        put("'us'", "123456");
        put("'ns'", "123456789");
    }};

    @Test
    public void TimeZoneQueryTest() throws SQLException, InterruptedException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select * from "  + dbName + "." + TABLE_NAME + " limit 1")) {
            while (resultSet.next()) {

                Timestamp ts = resultSet.getTimestamp("ts");

                System.out.println("ts: " + ts);
                System.out.println("ts: " + ts.getTime());
                Assert.assertEquals("2024-01-01 01:00:00." + precisionMap.get(this.precision), ts.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void TimeZoneStmtTimestampTest() throws SQLException, InterruptedException {
        String sql = "insert into " + dbName + "." + TABLE_NAME + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);

        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 2, 1, 2, 3, 123456789);
        ZoneId zoneId = ZoneId.of("Asia/Tokyo");
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        Instant instant = zonedDateTime.toInstant();

        Timestamp timestamp = DateTimeUtils.getTimestamp(instant, zoneId);

        statement.setTimestamp(1, timestamp);
        statement.setInt(2, 2);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();

        try (Statement qStmt = connection.createStatement();
                ResultSet resultSet = qStmt.executeQuery("select * from "  + dbName + "." + TABLE_NAME + " where f = 2 limit 1")) {
            while (resultSet.next()) {

                Timestamp ts = resultSet.getTimestamp("ts");
                Assert.assertEquals("2024-01-02 01:02:03." + precisionMap.get(this.precision), ts.toString());

                Date date = resultSet.getDate("ts");
                Assert.assertEquals("2024-01-02", date.toString());

                Time time = resultSet.getTime("ts");
                Assert.assertEquals("01:02:03", time.toString());

                Instant ins = resultSet.getObject("ts", Instant.class);
                Assert.assertEquals(ins.toEpochMilli(), instant.toEpochMilli());

                ZonedDateTime zdt = resultSet.getObject("ts", ZonedDateTime.class);
                Assert.assertEquals(zdt.toInstant().toEpochMilli(), instant.toEpochMilli());

                OffsetDateTime odt = resultSet.getObject("ts", OffsetDateTime.class);
                Assert.assertEquals(odt.toInstant().toEpochMilli(), instant.toEpochMilli());

                LocalDateTime ldt = resultSet.getObject("ts", LocalDateTime.class);
                Assert.assertEquals(ldt.truncatedTo(ChronoUnit.MILLIS), instant.atZone(zoneId).toLocalDateTime().truncatedTo(ChronoUnit.MILLIS));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void TimeZoneStmtAllTimeTypeTest() throws SQLException, InterruptedException {
        String sql = "insert into " + dbName + "." + FULL_TABLE_NAME + " values(?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);

        // 创建一个 LocalDateTime 对象，表示特定的日期和时间
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 2, 1, 2, 3, 123456789);

        // 指定时区
        ZoneId zoneId = ZoneId.of("Asia/Tokyo");

        // 将 LocalDateTime 转换为 ZonedDateTime
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);

        // 将 ZonedDateTime 转换为 Instant
        Instant instant = zonedDateTime.toInstant();

        statement.setObject(1, instant);
        statement.setObject(2, ZonedDateTime.ofInstant(instant, zoneId));
        statement.setObject(3, OffsetDateTime.ofInstant(instant, zoneId));
        statement.setObject(4, localDateTime);

        LocalDate localDate = localDateTime.toLocalDate();
        LocalTime localTime = localDateTime.toLocalTime();

        statement.setObject(5, Date.valueOf(localDate));
        statement.setObject(6, Time.valueOf(localTime));

        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();

        try (Statement qStmt = connection.createStatement();
                ResultSet resultSet = qStmt.executeQuery("select * from "  + dbName + "." + FULL_TABLE_NAME + " limit 1")) {
            while (resultSet.next()) {

                Timestamp ts1 = resultSet.getTimestamp("ts1");
                Timestamp ts2 = resultSet.getTimestamp("ts2");
                Timestamp ts3 = resultSet.getTimestamp("ts3");
                Timestamp ts4 = resultSet.getTimestamp("ts4");
                Timestamp ts5 = resultSet.getTimestamp("ts5");
                Timestamp ts6 = resultSet.getTimestamp("ts6");

                Assert.assertEquals("2024-01-02 01:02:03." + precisionMap.get(this.precision), ts1.toString());
                Assert.assertEquals("2024-01-02 01:02:03." + precisionMap.get(this.precision), ts2.toString());
                Assert.assertEquals("2024-01-02 01:02:03." + precisionMap.get(this.precision), ts3.toString());
                Assert.assertEquals("2024-01-02 01:02:03." + precisionMap.get(this.precision), ts4.toString());

                Assert.assertEquals("2024-01-02", new Date(ts5.getTime()).toString());
                Assert.assertEquals("01:02:03", new Time(ts6.getTime()).toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void TimeZoneTest3() throws SQLException, InterruptedException {
        long value = 1704038400000L; // 示例时间戳
        ZoneId zoneId = ZoneId.of("Asia/Tokyo");

        // 获取当前 UTC 时间
        Instant instant1 = Instant.ofEpochMilli(value);
        System.out.println("Original Instant (UTC): " + instant1);

        // 将 Instant 转换为特定时区的 ZonedDateTime
        ZonedDateTime zonedDateTime = instant1.atZone(zoneId);
        System.out.println("ZonedDateTime in Asia/Shanghai: " + zonedDateTime);

        // 将 ZonedDateTime 转换回 Timestamp
        Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
        System.out.println("Timestamp in Asia/Shanghai: " + timestamp);
    }

    @Test(expected = Exception.class)
    public void InvalidTimeZoneTest() throws SQLException, InterruptedException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "invalid/Tokyo");
        DriverManager.getConnection(url, properties);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Tokyo");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database " + dbName + " precision " + precision);
        statement.execute("use " + dbName);
        statement.execute("create table if not exists " + dbName + "." + TABLE_NAME + " (ts timestamp, f int)");
        statement.execute("create table if not exists " + dbName + "." + FULL_TABLE_NAME + " (ts1 timestamp, ts2 timestamp, ts3 timestamp, ts4 timestamp, ts5 timestamp, ts6 timestamp)");

        // Asia/Shanghai +08:00, 2024-01-01 00:00:00
        statement.execute("insert into " + dbName + "." + TABLE_NAME + " values (\"2024-01-01T00:00:00.123456789+08:00\", 1)");

        statement.close();
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop database if exists " + dbName);
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }
}


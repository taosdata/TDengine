package com.taosdata.jdbc.cases;


import com.taosdata.jdbc.TSDBDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.text.Format;
import java.text.SimpleDateFormat;

public class TimestampPrecisionInNanoInJniTest {

    private static final String host = "127.0.0.1";
    private static final String ns_timestamp_db = "ns_precision_test";
    private static final long timestamp1 = System.currentTimeMillis();
    private static final long timestamp2 = timestamp1 * 1000_000 + 123455;
    private static final long timestamp3 = (timestamp1 + 10) * 1000_000 + 123456;
    private static final Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String date1 = format.format(new Date(timestamp1));
    private static final String date4 = format.format(new Date(timestamp1 + 10l));
    private static final String date2 = date1 + "123455";
    private static final String date3 = date4 + "123456";
    

    private static Connection conn;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        conn = DriverManager.getConnection(url, properties);

        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + ns_timestamp_db);
        stmt.execute("create database if not exists " + ns_timestamp_db + " precision 'ns'");
        stmt.execute("create table " + ns_timestamp_db + ".weather(ts timestamp, ts2 timestamp, f1 int)");
        stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(\"" + date3 + "\", \"" + date3 +  "\", 128)");
        stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(" + timestamp2 + "," + timestamp2 + ", 127)");
        stmt.close();
    }

    @After
    public void afterEach() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + ns_timestamp_db);
        stmt.execute("create database if not exists " + ns_timestamp_db + " precision 'ns'");
        stmt.execute("create table " + ns_timestamp_db + ".weather(ts timestamp, ts2 timestamp, f1 int)");
        stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(\"" + date3 + "\", \"" + date3 +  "\", 128)");
        stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(" + timestamp2 + "," + timestamp2 + ", 127)");
        stmt.close();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void checkCount(long count, ResultSet rs) throws SQLException {
        if (count == 0) {
            Assert.fail();
        }
        rs.next();
        long test_count = rs.getLong(1);
        Assert.assertEquals(count, test_count);
    }

    private void checkTime(long ts, ResultSet rs) throws SQLException {
        rs.next();
        int nanos = rs.getTimestamp(1).getNanos();
        Assert.assertEquals(ts % 1000_000_000l, nanos);
        long test_ts = rs.getLong(1);
        Assert.assertEquals(ts / 1000_000l, test_ts);
    }

    @Test
    public void canInsertTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts = '" + date3 + "'");
            checkTime(timestamp3, rs);
            rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 = '" + date3 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("import into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(\"" + date1 + "123123\", \"" + date1 +  "123123\", 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + date1 + "123123'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts = '" + date1 + "123123'");
            checkTime(timestamp1 * 1000_000l + 123123l, rs);
            rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + date1 + "123123'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 = '" + date1 + "123123'");
            checkTime(timestamp1 * 1000_000l + 123123l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canInsertTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts = '" + timestamp2 + "'");
            checkTime(timestamp2, rs);
            rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 = '" + timestamp2 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            long timestamp4 = timestamp1 * 1000_000 + 123123;
            stmt.executeUpdate("import into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(" + timestamp4 + ", " + timestamp4 +  ", 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + timestamp4 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts = '" + timestamp4 + "'");
            checkTime(timestamp4, rs);
            rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + timestamp4 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 = '" + timestamp4 + "'");
            checkTime(timestamp4, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts) from " + ns_timestamp_db + ".weather");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts2) from " + ns_timestamp_db + ".weather");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts) from " + ns_timestamp_db + ".weather");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts2) from " + ns_timestamp_db + ".weather");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts > '" + date2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts > '" + date2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 > '" + date2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 > '" + date2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }    

    @Test
    public void canQueryLargerThanInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts > '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts > '" + timestamp2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 > '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 > '" + timestamp2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts >= '" + date2 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 >= '" + date2 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts >= '" + timestamp2 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 >= '" + timestamp2 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }    

    @Test
    public void canQueryLessThanInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts < '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts < '" + date3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 < '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 < '" + date3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts < '" + timestamp3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts < '" + timestamp3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 < '" + timestamp3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 < '" + timestamp3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts <= '" + date3 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <= '" + date3 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }    

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts <= '" + timestamp3 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <= '" + timestamp3 + "'");
            checkCount(2l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts <= '" + date3 + "' AND ts > '" + date2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts <= '" + date3 + "' AND ts > '" + date2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <= '" + date3 + "' AND ts2 > '" + date2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 <= '" + date3 + "' AND ts2 > '" + date2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts <= '" + timestamp3 + "' AND ts > '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts from " + ns_timestamp_db + ".weather where ts <= '" + timestamp3 + "' AND ts > '" + timestamp2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <= '" + timestamp3 + "' AND ts2 > '" + timestamp2 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 <= '" + timestamp3 + "' AND ts2 > '" + timestamp2 + "'");
            checkTime(timestamp3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <> '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 <> '" + date3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 <> '" + timestamp3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 <> '" + timestamp3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 != '" + date3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 != '" + date3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 != '" + timestamp3 + "'");
            checkCount(1l, rs);
            rs = stmt.executeQuery("select ts2 from " + ns_timestamp_db + ".weather where ts2 != '" + timestamp3 + "'");
            checkTime(timestamp2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canInsertTimestampWithNowAndNsOffsetInBothFirstAndSecondCol(){
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(now + 1000b, now - 1000b, 128)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather");
            checkCount(3l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForFirstCol(){
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + ns_timestamp_db + ".weather where ts >= '" + date2 + "' and ts <= '" + date3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(2);
            Assert.assertEquals(127l, sum);
            rs.next();
            sum = rs.getLong(2);
            Assert.assertEquals(128l, sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForSecondCol(){
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + ns_timestamp_db + ".weather where ts2 >= '" + date2 + "' and ts <= '" + date3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(2);
            Assert.assertEquals(127l, sum);
            rs.next();
            sum = rs.getLong(2);
            Assert.assertEquals(128l, sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDataOutOfRangeExceptionForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(123456789012345678, 1234567890123456789, 127)");
        } catch (SQLException e) {
            Assert.assertEquals("TDengine ERROR (8000060b): Timestamp data out of range", e.getMessage());
        }
    }

    @Test
    public void testDataOutOfRangeExceptionForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values(1234567890123456789, 123456789012345678, 127)");
        } catch (SQLException e) {
            Assert.assertEquals("TDengine ERROR (8000060b): Timestamp data out of range", e.getMessage());
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values('" + date1 + "', '" + date1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + date1 + "000000'");
            checkCount(1l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values('" + date1 + "', '" + date1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + date1 + "000000'");
            checkCount(1l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values('" + date1 + "999999999', '" + date1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts = '" + date1 + "999999'");
            checkCount(1l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + ns_timestamp_db + ".weather(ts, ts2, f1) values('" + date1 + "999999999', '" + date1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + ns_timestamp_db + ".weather where ts2 = '" + date1 + "999999'");
            checkCount(1l, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

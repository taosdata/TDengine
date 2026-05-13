package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class TimestampPrecisionInNanoInJniTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String NS_TIMESTAMP_DB = TestUtils.camelToSnake(TimestampPrecisionInNanoInJniTest.class);
    private static final long TIMESTAMP_1 = System.currentTimeMillis();
    private static final long TIMESTAMP_2 = TIMESTAMP_1 * 1000_000 + 123455;
    private static final long TIMESTAMP_3 = (TIMESTAMP_1 + 10) * 1000_000 + 123456;
    private static final Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String DATE_1 = format.format(new Date(TIMESTAMP_1));
    private static final String DATE_4 = format.format(new Date(TIMESTAMP_1 + 10L));
    private static final String DATE_2 = DATE_1 + "123455";
    private static final String DATE_3 = DATE_4 + "123456";

    private static Connection conn;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);

        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + NS_TIMESTAMP_DB);
        stmt.execute("create database if not exists " + NS_TIMESTAMP_DB + " precision 'ns'");
        stmt.execute("create table " + NS_TIMESTAMP_DB + ".weather(ts timestamp, ts2 timestamp, f1 int)");
        stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(\"" + DATE_3 + "\", \"" + DATE_3 + "\", 128)");
        stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(" + TIMESTAMP_2 + "," + TIMESTAMP_2 + ", 127)");
        stmt.close();
    }

    @After
    public void afterEach() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + NS_TIMESTAMP_DB);
        stmt.execute("create database if not exists " + NS_TIMESTAMP_DB + " precision 'ns'");
        stmt.execute("create table " + NS_TIMESTAMP_DB + ".weather(ts timestamp, ts2 timestamp, f1 int)");
        stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(\"" + DATE_3 + "\", \"" + DATE_3 + "\", 128)");
        stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(" + TIMESTAMP_2 + "," + TIMESTAMP_2 + ", 127)");
        stmt.close();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null) {
                Statement statement = conn.createStatement();
                statement.execute("drop database if exists " + NS_TIMESTAMP_DB);
                statement.close();
                conn.close();
            }
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
        Assert.assertEquals(ts % 1000_000_000L, nanos);
        long test_ts = rs.getLong(1);
        Assert.assertEquals(ts, test_ts);
    }

    @Test
    public void canInsertTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_3 + "'");
            checkTime(TIMESTAMP_3, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_3 + "'");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("import into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(\"" + DATE_1 + "123123\", \"" + DATE_1 + "123123\", 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "123123'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "123123'");
            checkTime(TIMESTAMP_1 * 1000_000L + 123123L, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "123123'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "123123'");
            checkTime(TIMESTAMP_1 * 1000_000L + 123123L, rs);
        }
    }

    @Test
    public void canInsertTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = " + TIMESTAMP_2);
            checkTime(TIMESTAMP_2, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + TIMESTAMP_2);
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long timestamp4 = TIMESTAMP_1 * 1000_000 + 123123;
            stmt.executeUpdate("import into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(" + timestamp4 + ", " + timestamp4 + ", 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = " + timestamp4);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = " + timestamp4);
            checkTime(timestamp4, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + timestamp4 );
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + timestamp4 );
            checkTime(timestamp4, rs);
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts2) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts2) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryLargerThanInNumberTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryLargerThanInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts >= '" + DATE_2 + "'");
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= '" + DATE_2 + "'");
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts >= " + TIMESTAMP_2);
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= " + TIMESTAMP_2);
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLessThanInDateTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts < '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts < '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryLessThanInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 < '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 < '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts < " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts < " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 < " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 < " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "'");
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "'");
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3);
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3);
            checkCount(2L, rs);
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "' AND ts > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "' AND ts > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "' AND ts2 > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "' AND ts2 > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3 + " AND ts > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3 + " AND ts > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3 + " AND ts2 > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3 + " AND ts2 > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        }
    }

    @Test
    public void canQueryNotEqualToInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <> '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <> '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryNotEqualToInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <> " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <> " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryNotEqualInDateTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 != '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 != '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canQueryNotEqualInNumberTypeForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 != " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 != " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        }
    }

    @Test
    public void canInsertTimestampWithNowAndNsOffsetInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(now + 1000b, now - 1000b, 128)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather");
            checkCount(3L, rs);
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + NS_TIMESTAMP_DB + ".weather where ts >= '" + DATE_2 + "' and ts <= '" + DATE_3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(1);
            Assert.assertEquals(127L, sum);
            rs.next();
            sum = rs.getLong(1);
            Assert.assertEquals(128L, sum);
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= '" + DATE_2 + "' and ts <= '" + DATE_3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(1);
            Assert.assertEquals(127L, sum);
            rs.next();
            sum = rs.getLong(1);
            Assert.assertEquals(128L, sum);
        }
    }

    @Test
    public void testDataOutOfRangeExceptionForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(123456789012345678, 1234567890123456789, 127)");
        } catch (SQLException e) {
            Assert.assertEquals("TDengine ERROR (0x8000060b): Timestamp data out of range", e.getMessage());
        }
    }

    @Test
    public void testDataOutOfRangeExceptionForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(1234567890123456789, 123456789012345678, 127)");
        } catch (SQLException e) {
            Assert.assertEquals("TDengine ERROR (0x8000060b): Timestamp data out of range", e.getMessage());
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "', '" + DATE_1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "000000'");
            checkCount(1L, rs);
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "', '" + DATE_1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "000000'");
            checkCount(1L, rs);
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "999999999', '" + DATE_1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "999999'");
            checkCount(1L, rs);
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "999999999', '" + DATE_1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "999999'");
            checkCount(1L, rs);
        }
    }
}


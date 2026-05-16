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

public class TimestampPrecisionInNanoRestTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String NS_TIMESTAMP_DB = TestUtils.camelToSnake(TimestampPrecisionInNanoRestTest.class);
    private static final long TIMESTAMP_1 = System.currentTimeMillis();
    private static final long TIMESTAMP_2 = TIMESTAMP_1 * 1000_000 + 123455;
    private static final long TIMESTAMP_3 = (TIMESTAMP_1 + 10) * 1000_000 + 123456;
    private static final Format FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String DATE_1 = FORMAT.format(new Date(TIMESTAMP_1));
    private static final String DATE_4 = FORMAT.format(new Date(TIMESTAMP_1 + 10L));
    private static final String DATE_2 = DATE_1 + "123455";
    private static final String DATE_3 = DATE_4 + "123456";

    private static Connection conn;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
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
    public void canInsertTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_3 + "'");
            checkTime(TIMESTAMP_3, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_3 + "'");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInDateTypeInBothFirstAndSecondCol() {
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canInsertTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = " + TIMESTAMP_2);
            checkTime(TIMESTAMP_2, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + TIMESTAMP_2);
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canImportTimestampAndQueryByEqualToInNumberTypeInBothFirstAndSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long timestamp4 = TIMESTAMP_1 * 1000_000 + 123123;
            stmt.executeUpdate("import into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(" + timestamp4 + ", " + timestamp4 + ", 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = " + timestamp4);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts = " + timestamp4 );
            checkTime(timestamp4, rs);
            rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + timestamp4);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 = " + timestamp4 );
            checkTime(timestamp4, rs);
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectLastRowFromWeatherForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last(ts2) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canSelectFirstRowFromWeatherForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select first(ts2) from " + NS_TIMESTAMP_DB + ".weather");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts >= '" + DATE_2 + "'");
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= '" + DATE_2 + "'");
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts >= " + TIMESTAMP_2);
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLargerThanOrEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= " + TIMESTAMP_2);
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts < '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts < '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 < '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 < '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts < " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts < " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 < " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 < " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "'");
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "'");
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3);
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryLessThanOrEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3);
            checkCount(2L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "' AND ts > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts <= '" + DATE_3 + "' AND ts > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "' AND ts2 > '" + DATE_2 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <= '" + DATE_3 + "' AND ts2 > '" + DATE_2 + "'");
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3 + " AND ts > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts from " + NS_TIMESTAMP_DB + ".weather where ts <= " + TIMESTAMP_3 + " AND ts > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryBetweenAndInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3 + " AND ts2 > " + TIMESTAMP_2);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <= " + TIMESTAMP_3 + " AND ts2 > " + TIMESTAMP_2);
            checkTime(TIMESTAMP_3, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualToInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <> '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <> '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualToInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 <> " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 <> " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualInDateTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 != '" + DATE_3 + "'");
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 != '" + DATE_3 + "'");
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canQueryNotEqualInNumberTypeForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 != " + TIMESTAMP_3);
            checkCount(1L, rs);
            rs = stmt.executeQuery("select ts2 from " + NS_TIMESTAMP_DB + ".weather where ts2 != " + TIMESTAMP_3);
            checkTime(TIMESTAMP_2, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canInsertTimestampWithNowAndNsOffsetInBothFirstAndSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(now + 1000b, now - 1000b, 128)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather");
            checkCount(3L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + NS_TIMESTAMP_DB + ".weather where ts >= '" + DATE_2 + "' and ts <= '" + DATE_3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(1);
            Assert.assertEquals(127L, sum);
            rs.next();
            sum = rs.getLong(1);
            Assert.assertEquals(128L, sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void canIntervalAndSlidingAcceptNsUnitForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select sum(f1) from " + NS_TIMESTAMP_DB + ".weather where ts2 >= '" + DATE_2 + "' and ts <= '" + DATE_3 + "' interval(10000000b) sliding(10000000b)");
            rs.next();
            long sum = rs.getLong(1);
            Assert.assertEquals(127L, sum);
            rs.next();
            sum = rs.getLong(1);
            Assert.assertEquals(128L, sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = SQLException.class)
    public void testDataOutOfRangeExceptionForFirstCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(123456789012345678, 1234567890123456789, 127)");
        }
    }

    @Test(expected = SQLException.class)
    public void testDataOutOfRangeExceptionForSecondCol() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values(1234567890123456789, 123456789012345678, 127)");
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "', '" + DATE_1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "000000'");
            checkCount(1L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyFillToNsUnitWithZerosForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "', '" + DATE_1 + "', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "000000'");
            checkCount(1L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForFirstCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "999999999', '" + DATE_1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts = '" + DATE_1 + "999999'");
            checkCount(1L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void willAutomaticallyDropDigitExceedNsDigitNumberForSecondCol() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into " + NS_TIMESTAMP_DB + ".weather(ts, ts2, f1) values('" + DATE_1 + "999999999', '" + DATE_1 + "999999999', 127)");
            ResultSet rs = stmt.executeQuery("select count(*) from " + NS_TIMESTAMP_DB + ".weather where ts2 = '" + DATE_1 + "999999'");
            checkCount(1L, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


package com.taosdata.jdbc.rs;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SQLTest {

    private static final String host = "127.0.0.1";
    private static Connection connection;

    @Test
    public void testCase001() {
        // given
        String sql = "create database if not exists restful_test";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase002() {
        // given
        String sql = "use restful_test";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase003() {
        // given
        String sql = "show databases";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase004() {
        // given
        String sql = "show tables";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase005() {
        // given
        String sql = "show stables";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase006() {
        // given
        String sql = "show dnodes";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase007() {
        // given
        String sql = "show vgroups";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase008() {
        // given
        String sql = "drop table if exists restful_test.weather";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase009() {
        // given
        String sql = "create table if not exists restful_test.weather(ts timestamp, temperature float) tags(location nchar(64))";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase010() {
        // given
        String sql = "create table t1 using restful_test.weather tags('北京')";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase011() {
        // given
        String sql = "insert into restful_test.t1 values(now, 22.22)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase012() {
        // given
        String sql = "insert into restful_test.t1 values('2020-01-01 00:00:00.000', 22.22)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase013() {
        // given
        String sql = "insert into restful_test.t1 values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase014() {
        // given
        String sql = "insert into restful_test.t2 using weather tags('上海') values('2020-01-01 00:03:00.000', 22.22)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase015() {
        // given
        String sql = "insert into restful_test.t2 using weather tags('上海') values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase016() {
        // given
        String sql = "insert into t1 values('2020-01-01 01:0:00.000', 22.22),('2020-01-01 02:00:00.000', 22.22) t2 values('2020-01-01 01:0:00.000', 33.33),('2020-01-01 02:00:00.000', 33.33)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase017() {
        // given
        String sql = "Insert into t3 using weather tags('广东') values('2020-01-01 01:0:00.000', 22.22),('2020-01-01 02:00:00.000', 22.22) t4 using weather tags('天津') values('2020-01-01 01:0:00.000', 33.33),('2020-01-01 02:00:00.000', 33.33)";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase018() {
        // given
        String sql = "select * from restful_test.t1";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase019() {
        // given
        String sql = "select * from restful_test.weather";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase020() {
        // given
        String sql = "select ts, temperature from restful_test.t1";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase021() {
        // given
        String sql = "select ts, temperature from restful_test.weather";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase022() {
        // given
        String sql = "select temperature, ts from restful_test.t1";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase023() {
        // given
        String sql = "select temperature, ts from restful_test.weather";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertTrue(execute);
    }

    @Test
    public void testCase024() {
        // given
        String sql = "import into restful_test.t5 using weather tags('石家庄') values('2020-01-01 00:01:00.000', 22.22)";
        // when
        int affectedRows = executeUpdate(connection, sql);
        // then
        Assert.assertEquals(1, affectedRows);
    }

    @Test
    public void testCase025() {
        // given
        String sql = "import into restful_test.t6 using weather tags('沈阳') values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";
        // when
        int affectedRows = executeUpdate(connection, sql);
        // then
        Assert.assertEquals(2, affectedRows);
    }

    @Test
    public void testCase026() {
        // given
        String sql = "import into restful_test.t7 using weather tags('长沙') values('2020-01-01 00:01:00.000', 22.22) restful_test.t8 using weather tags('吉林') values('2020-01-01 00:01:00.000', 22.22)";

        // when
        int affectedRows = executeUpdate(connection, sql);
        // then
        Assert.assertEquals(2, affectedRows);
    }

    @Test
    public void testCase027() {
        // given
        String sql = "import into restful_test.t9 using weather tags('武汉') values('2020-01-01 00:01:00.000', 22.22) ,('2020-01-02 00:01:00.000', 22.22) restful_test.t10 using weather tags('哈尔滨') values('2020-01-01 00:01:00.000', 22.22),('2020-01-02 00:01:00.000', 22.22)";
        // when
        int affectedRows = executeUpdate(connection, sql);
        // then
        Assert.assertEquals(4, affectedRows);
    }

    @Test
    public void testCase028() {
        // given
        String sql = "select location, temperature, ts from restful_test.weather where temperature > 1";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase029() {
        String sql = "select location, temperature, ts from restful_test.weather where temperature < 1";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase030() {
        String sql = "select location, temperature, ts from restful_test.weather where ts  > now";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase031() {
        String sql = "select location, temperature, ts from restful_test.weather where ts  < now";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase032() {
        String sql = "select count(*) from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase033() {
        String sql = "select first(*) from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase034() {
        String sql = "select last(*) from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase035() {
        String sql = "select last_row(*) from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase036() {
        String sql = "select ts, ts as primary_key from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }


    @Test
    public void testCase037() {
        String sql = "select database()";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase038() {
        String sql = "select client_version()";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase039() {
        String sql = "select server_status()";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase040() {
        String sql = "select server_status() as status";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase041() {
        String sql = "select tbname, location from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase042() {
        String sql = "select count(tbname) from restful_test.weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase043() {
        String sql = "select * from restful_test.weather where ts < now - 1h";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase044() {
        String sql = "select * from restful_test.weather where ts < now - 1h and location like '%'";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase045() {
        String sql = "select * from restful_test.weather where ts < now - 1h order by ts";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase046() {
        String sql = "select last(*) from restful_test.weather where ts < now - 1h group by tbname order by tbname";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase047() {
        String sql = "select * from restful_test.weather limit 2";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase048() {
        String sql = "select * from restful_test.weather limit 2 offset 5";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase049() {
        String sql = "select * from restful_test.t1, restful_test.t3 where t1.ts = t3.ts ";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase050() {
        String sql = "select * from restful_test.t1, restful_test.t3 where t1.ts = t3.ts";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase051() {
        String sql = "select * from restful_test.t1 tt, restful_test.t3 yy where tt.ts = yy.ts";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    @Test
    public void testCase052() {
        String sql = "select server_status()";
        // when
        ResultSet rs = executeQuery(connection, sql);
        // then
        Assert.assertNotNull(rs);
    }

    private boolean execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private ResultSet executeQuery(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            return statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int executeUpdate(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @BeforeClass
    public static void before() throws SQLException {
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
    }

    @AfterClass
    public static void after() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists restful_test");
        stmt.close();
        connection.close();
    }

}

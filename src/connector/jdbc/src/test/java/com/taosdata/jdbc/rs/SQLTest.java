package com.taosdata.jdbc.rs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SQLTest {
    private static final String host = "master";

    private static Connection connection;

    @Test
    public void testCase001() {
        String sql = "create database if not exists restful_test";
        execute(sql);
    }

    @Test
    public void testCase002() {
        String sql = "use restful_test";
        execute(sql);
    }

    @Test
    public void testCase003() {
        String sql = "show databases";
        executeWithResult(sql);
    }

    @Test
    public void testCase004() {
        String sql = "show tables";
        executeWithResult(sql);
    }

    @Test
    public void testCase005() {
        String sql = "show stables";
        executeWithResult(sql);
    }

    @Test
    public void testCase006() {
        String sql = "show dnodes";
        executeWithResult(sql);
    }

    @Test
    public void testCase007() {
        String sql = "show vgroups";
        executeWithResult(sql);
    }

    @Test
    public void testCase008() {
        String sql = "drop table if exists restful_test.weather";
        execute(sql);
    }

    @Test
    public void testCase009() {
        String sql = "create table if not exists restful_test.weather(ts timestamp, temperature float) tags(location nchar(64))";
        execute(sql);
    }

    @Test
    public void testCase010() {
        String sql = "create table t1 using restful_test.weather tags('北京')";
        execute(sql);
    }

    @Test
    public void testCase011() {
        String sql = "insert into restful_test.t1 values(now, 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase012() {
        String sql = "insert into restful_test.t1 values('2020-01-01 00:00:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase013() {
        String sql = "insert into restful_test.t1 values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase014() {
        String sql = "insert into restful_test.t2 using weather tags('上海') values('2020-01-01 00:03:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase015() {
        String sql = "insert into restful_test.t2 using weather tags('上海') values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase016() {
        String sql = "insert into t1 values('2020-01-01 01:0:00.000', 22.22),('2020-01-01 02:00:00.000', 22.22) t2 values('2020-01-01 01:0:00.000', 33.33),('2020-01-01 02:00:00.000', 33.33)";
        executeUpdate(sql);
    }

    @Test
    public void testCase017() {
        String sql = "Insert into t3 using weather tags('广东') values('2020-01-01 01:0:00.000', 22.22),('2020-01-01 02:00:00.000', 22.22) t4 using weather tags('天津') values('2020-01-01 01:0:00.000', 33.33),('2020-01-01 02:00:00.000', 33.33)";
        executeUpdate(sql);
    }

    @Test
    public void testCase018() {
        String sql = "select * from restful_test.t1";
        executeQuery(sql);
    }

    @Test
    public void testCase019() {
        String sql = "select * from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase020() {
        String sql = "select ts, temperature from restful_test.t1";
        executeQuery(sql);
    }

    @Test
    public void testCase021() {
        String sql = "select ts, temperature from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase022() {
        String sql = "select temperature, ts from restful_test.t1";
        executeQuery(sql);
    }

    @Test
    public void testCase023() {
        String sql = "select temperature, ts from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase024() {
        String sql = "import into restful_test.t5 using weather tags('石家庄') values('2020-01-01 00:01:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase025() {
        String sql = "import into restful_test.t6 using weather tags('沈阳') values('2020-01-01 00:01:00.000', 22.22),('2020-01-01 00:02:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase026() {
        String sql = "import into restful_test.t7 using weather tags('长沙') values('2020-01-01 00:01:00.000', 22.22) restful_test.t8 using weather tags('吉林') values('2020-01-01 00:01:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase027() {
        String sql = "import into restful_test.t9 using weather tags('武汉') values('2020-01-01 00:01:00.000', 22.22) ,('2020-01-02 00:01:00.000', 22.22) restful_test.t10 using weather tags('哈尔滨') values('2020-01-01 00:01:00.000', 22.22),('2020-01-02 00:01:00.000', 22.22)";
        executeUpdate(sql);
    }

    @Test
    public void testCase028() {
        String sql = "select location, temperature, ts from restful_test.weather where temperature > 1";
        executeQuery(sql);
    }

    @Test
    public void testCase029() {
        String sql = "select location, temperature, ts from restful_test.weather where temperature < 1";
        executeQuery(sql);
    }

    @Test
    public void testCase030() {
        String sql = "select location, temperature, ts from restful_test.weather where ts  > now";
        executeQuery(sql);
    }

    @Test
    public void testCase031() {
        String sql = "select location, temperature, ts from restful_test.weather where ts  < now";
        executeQuery(sql);
    }

    @Test
    public void testCase032() {
        String sql = "select count(*) from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase033() {
        String sql = "select first(*) from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase034() {
        String sql = "select last(*) from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase035() {
        String sql = "select last_row(*) from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase036() {
        String sql = "select ts, ts as primary_key from restful_test.weather";
        executeQuery(sql);
    }


    @Test
    public void testCase037() {
        String sql = "select database()";
        execute("use restful_test");
        executeQuery(sql);
    }

    @Test
    public void testCase038() {
        String sql = "select client_version()";
        executeQuery(sql);
    }

    @Test
    public void testCase039() {
        String sql = "select server_status()";
        executeQuery(sql);
    }

    @Test
    public void testCase040() {
        String sql = "select server_status() as status";
        executeQuery(sql);
    }

    @Test
    public void testCase041() {
        String sql = "select tbname, location from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase042() {
        String sql = "select count(tbname) from restful_test.weather";
        executeQuery(sql);
    }

    @Test
    public void testCase043() {
        String sql = "select * from restful_test.weather where ts < now - 1h";
        executeQuery(sql);
    }

    @Test
    public void testCase044() {
        String sql = "select * from restful_test.weather where ts < now - 1h and location like '%'";
        executeQuery(sql);
    }

    @Test
    public void testCase045() {
        String sql = "select * from restful_test.weather where ts < now - 1h order by ts";
        executeQuery(sql);
    }

    @Test
    public void testCase046() {
        String sql = "select last(*) from restful_test.weather where ts < now - 1h group by tbname order by tbname";
        executeQuery(sql);
    }

    @Test
    public void testCase047() {
        String sql = "select * from restful_test.weather limit 2";
        executeQuery(sql);
    }

    @Test
    public void testCase048() {
        String sql = "select * from restful_test.weather limit 2 offset 5";
        executeQuery(sql);
    }

    @Test
    public void testCase049() {
        String sql = "select * from restful_test.t1, restful_test.t3 where t1.ts = t3.ts ";
        executeQuery(sql);
    }

    @Test
    public void testCase050() {
        String sql = "select * from restful_test.t1, restful_test.t3 where t1.ts = t3.ts and t1.location = t3.location";
        executeQuery(sql);
    }

    @Test
    public void testCase051() {
        String sql = "select * from restful_test.t1 tt, restful_test.t3 yy where tt.ts = yy.ts";
        executeQuery(sql);
    }

    private void executeUpdate(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            int affectedRows = statement.executeUpdate(sql);
            long end = System.currentTimeMillis();
            System.out.println("[ affected rows : " + affectedRows + " ] time cost: " + (end - start) + " ms, execute statement ====> " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void executeWithResult(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            printResult(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, execute, (end - start));
        } catch (SQLException e) {
            System.out.println("ERROR execute SQL ===> " + sql);
            e.printStackTrace();
        }
    }

    private static void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            System.out.println("ERROR execute SQL ===> " + sql);
            e.printStackTrace();
        }
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                sb.append(columnLabel + ": " + value + "\t");
            }
            System.out.println(sb.toString());
        }
    }

    @BeforeClass
    public static void before() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=root&password=taosdata");
    }

    @AfterClass
    public static void after() throws SQLException {
        connection.close();
    }

}

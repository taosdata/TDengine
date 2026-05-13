package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class MultiConnectionWithDifferentDbTest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String DB_1 = "db1";
    private static final String DB_2 = "db2";

    private long ts;

    @Test
    public void test() {
        List<Thread> threads = IntStream.range(1, 3).mapToObj(i -> new Thread(new Runnable() {
            @Override
            public void run() {
                for (int j = 0; j < 10; j++) {
                    try {
                        queryDb();
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ignored) {
                    } catch (SQLException throwables) {
                        fail();
                    }
                }
            }

            private void queryDb() throws SQLException {
                String url = SpecifyAddress.getInstance().getRestWithoutUrl();
                if (url == null) {
                    url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/db" + i + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
                } else {
                    url = url + "db" + i + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
                }
                try (Connection connection = DriverManager.getConnection(url)) {
                    Statement stmt = connection.createStatement();

                    ResultSet rs = stmt.executeQuery("select * from weather");
                    assertNotNull(rs);
                    rs.next();
                    long actual = rs.getTimestamp("ts").getTime();
                    assertEquals(ts, actual);

                    int f1 = rs.getInt("f1");
                    assertEquals(i, f1);

                    String loc = i == 1 ? "beijing" : "shanghai";
                    String locActual = rs.getString("loc");
                    assertEquals(loc, locActual);

                    stmt.close();
                }
            }
        }, "thread-" + i)).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Before
    public void before() throws SQLException {
        ts = System.currentTimeMillis();

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_1);
            stmt.execute("create database if not exists " + DB_1);
            stmt.execute("use " + DB_1);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('beijing') values(" + ts + ", 1)");

            stmt.execute("drop database if exists " + DB_2);
            stmt.execute("create database if not exists " + DB_2);
            stmt.execute("use " + DB_2);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('shanghai') values(" + ts + ", 2)");
        }
    }

    @After
    public void after() {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + DB_1);
            statement.execute("drop database if exists " + DB_2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}


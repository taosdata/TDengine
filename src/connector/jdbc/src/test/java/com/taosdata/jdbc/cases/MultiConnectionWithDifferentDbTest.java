package com.taosdata.jdbc.cases;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class MultiConnectionWithDifferentDbTest {

    private static String host = "127.0.0.1";
    private static String db1 = "db1";
    private static String db2 = "db2";

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
                String url = "jdbc:TAOS-RS://" + host + ":6041/db" + i + "?user=root&password=taosdata";
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
                    String loc_actual = rs.getString("loc");
                    assertEquals(loc, loc_actual);

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

        try (Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata")) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + db1);
            stmt.execute("create database if not exists " + db1);
            stmt.execute("use " + db1);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('beijing') values(" + ts + ", 1)");

            stmt.execute("drop database if exists " + db2);
            stmt.execute("create database if not exists " + db2);
            stmt.execute("use " + db2);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('shanghai') values(" + ts + ", 2)");
        }
    }

}

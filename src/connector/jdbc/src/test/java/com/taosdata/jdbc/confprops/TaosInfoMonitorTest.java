package com.taosdata.jdbc.confprops;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaosInfoMonitorTest {

    private static final String host = "127.0.0.1";
    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void testCreateTooManyConnection() throws InterruptedException {

        List<Thread> threads = IntStream.range(1, 11).mapToObj(i -> new Thread(() -> {
            final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";

            int connSize = random.nextInt(10);
            for (int j = 0; j < connSize; j++) {

                try {
                    Connection conn = DriverManager.getConnection(url);
                    TimeUnit.MILLISECONDS.sleep(random.nextInt(3000));

                    int stmtSize = random.nextInt(100);
                    for (int k = 0; k < stmtSize; k++) {
                        Statement stmt = conn.createStatement();
                        TimeUnit.MILLISECONDS.sleep(random.nextInt(3000));

                        ResultSet rs = stmt.executeQuery("show databases");
                        while (rs.next()) {
                        }
                        rs.close();
                        stmt.close();
                    }
                } catch (SQLException | InterruptedException throwables) {
                    Assert.fail();
                }
            }
        }, "thread-" + i)).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            thread.join();
        }
    }
}

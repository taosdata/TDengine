package com.taosdata.jdbc.rs;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HttpKeepAliveTest {

    private static final String host = "127.0.0.1";

    @Test
    public void test() throws SQLException {
        //given
        int multi = 4000;
        AtomicInteger exceptionCount = new AtomicInteger();

        //when
        Properties props = new Properties();
        props.setProperty("httpKeepAlive", "false");
        props.setProperty("httpPoolSize", "20");
        Connection connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata", props);

        List<Thread> threads = IntStream.range(0, multi).mapToObj(i -> new Thread(
                () -> {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("insert into log.tb_not_exists values(now, 1)");
                        stmt.execute("select last(*) from log.dn");
                    } catch (SQLException throwables) {
                        exceptionCount.getAndIncrement();
                    }
                }
        )).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //then
        Assert.assertEquals(multi, exceptionCount.get());
    }

}

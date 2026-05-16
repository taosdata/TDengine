package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Ignore
public class TaosInfoMonitorTest {

    static final String HOST = TestEnvUtil.getHost();
            private final Random random = new Random(System.currentTimeMillis());

    @Test
    public void testCreateTooManyConnection() throws InterruptedException {

        List<Thread> threads = IntStream.range(1, 11).mapToObj(i -> new Thread(() -> {
            String url = SpecifyAddress.getInstance().getJniUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
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


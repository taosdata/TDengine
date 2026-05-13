package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PreparedStatementBatchInsertJNITest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String DB_NAME = TestUtils.camelToSnake(PreparedStatementBatchInsertJNITest.class);

    private final Random random = new Random(System.currentTimeMillis());
    private Connection conn;

    @Test
    public void test() {
        // given
        long ts = System.currentTimeMillis();
        List<Object[]> rows = IntStream.range(0, 10).mapToObj(i -> {
            Object[] row = new Object[6];
            final String groupId = String.format("%02d", random.nextInt(100));
            // table name (d + groupId)组合
            row[0] = "d" + groupId;
            // tag
            row[1] = groupId;
            // ts
            row[2] = ts + i;
            // current 电流
            row[3] = random.nextFloat();
            // voltage 电压
            row[4] = Math.random() > 0.5 ? 220 : 380;
            // phase 相位
            row[5] = random.nextInt(10);
            return row;
        }).collect(Collectors.toList());
        final String sql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?)";

        // when
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // then
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from meters");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(10, count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("create table meters(ts timestamp, current float, voltage int, phase int) tags(groupId int)");

    }

    @After
    public void after() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
           // ignore
        }
    }

}


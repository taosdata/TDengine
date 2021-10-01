package com.taosdata.jdbc.oom;

import com.google.common.collect.Lists;
import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBPreparedStatement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BindTest {
    private static TSDBConnection conn;
    private static final String host = "127.0.0.1";
    private static final String dbname = "memory_test";

    @Test
    public void memoryTest() throws SQLException {
        String sql = "insert into ? using st tags (?) (ts, speed, longitude, latitude, altitude, guid, origin_byte) VALUES (?, ?, ?, ?, ?, ?, ?)";
        TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement(sql);
        while (true) {
            long l = System.currentTimeMillis() - 1000;
            List<RealTimeData> list = Lists.newArrayList();
            for (int i = 0; i < 1000; i++) {
                RealTimeData rd = new RealTimeData();
                rd.setTs(l + i);
                rd.setLongitude(11000L);
                rd.setLatitude(10000L);
                rd.setAltitude(100);
                rd.setSpeed(100);
                rd.setGuid("123456");
                rd.setOriginByte("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
                list.add(rd);
            };
            bindTest(list, "t_1",s);
        }
    }

    public static void bindTest(List<RealTimeData> list, String table , TSDBPreparedStatement s) throws SQLException {
        ArrayList<Long> ts = new ArrayList<>();
        ArrayList<Integer> speed = new ArrayList<>();
        ArrayList<Long> longitude = new ArrayList<>();
        ArrayList<Long> latitude = new ArrayList<>();
        ArrayList<Long> altitude = new ArrayList<>();
        ArrayList<String> guid = new ArrayList<>();
        ArrayList<String> originByte = new ArrayList<>();

        ts.addAll(list.stream().map(i -> i.getTs()).collect(Collectors.toList()));
        speed.addAll(list.stream().map(i -> i.getSpeed()).collect(Collectors.toList()));
        longitude.addAll(list.stream().map(i -> i.getLongitude()).collect(Collectors.toList()));
        latitude.addAll(list.stream().map(i -> i.getLatitude()).collect(Collectors.toList()));
        altitude.addAll(list.stream().map(i -> i.getAltitude()).collect(Collectors.toList()));
        guid.addAll(list.stream().map(i -> i.getGuid()).collect(Collectors.toList()));
        originByte.addAll(list.stream().map(i -> i.getOriginByte()).collect(Collectors.toList()));
        s.setTableName(table);
        s.setTagInt(0, 2);
        s.setTimestamp(0, ts);
        s.setInt(1, speed);
        s.setLong(2, longitude);
        s.setLong(3, latitude);
        s.setLong(4, altitude);
        s.setNString(5, guid, 20);
        s.setNString(6, originByte, 200);

        s.columnDataAddBatch();
        s.columnDataExecuteBatch();
        s.columnDataClearBatch();
    }

    @BeforeClass
    public static void beforeClass() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = (TSDBConnection) DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create stable if not exists st (ts timestamp, speed int, longitude bigint, latitude bigint, altitude bigint, guid nchar(100), origin_byte nchar(150)) tags (t int)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

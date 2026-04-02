package com.taosdata.tsync.service;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.ProducerConfig;
import com.taosdata.tsync.factory.NetToTQueueRunnableTaskFactory;
import com.taosdata.tsync.tqueue.TQueueProducer;
import org.junit.*;

import java.io.*;
import java.net.Socket;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class NetToTQueueRunnableTaskTest {

    private static final String host_tq = "192.168.17.156";
    private static final String topic = "tq_test";
    private static final int partition = 1;
    private static Connection conn;
    Properties props;

    @Test
    public void testReceiveMessageFromNetToTQueue() {
        // given
        int port = 8899;
        TQueueProducer producer = new TQueueProducer(props);

        // when
        NetToTQueueRunnableTask task = new NetToTQueueRunnableTaskFactory()
                .setListeningPort(port)
                .setProducer(producer)
                .build();
        Thread thread = new Thread(task);
        thread.start();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sendFewMessageToNet();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        task.shutdown();
        // then
        Assert.assertEquals(10, countMessage());
    }

    private void sendFewMessageToNet() {
        try {
            Socket socket = new Socket("127.0.0.1", 8899);
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.getOutputStream()))));
            for (int i = 0; i < 10; i++) {
                JSONObject obj = new JSONObject();
                obj.put("topic", topic);
                obj.put("partition", partition);
                obj.put("message", "Hello~~~");
                out.println(obj.toJSONString());
                out.flush();
                TimeUnit.SECONDS.sleep(1);
            }
            socket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int countMessage() {
        int count = 0;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + topic + ".ps");
            rs.next();
            count = rs.getInt("count(*)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }


    @Before
    public void before() {
        props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host_tq);
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            Statement stmt = conn.createStatement();
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 10");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
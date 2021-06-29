package com.taosdata.tsync.service;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.ConsumerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsumeToNetRunnableTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToNetRunnableTask.class);

    private TQueueConsumer consumer;
    private String topic;
    private List<Integer> partitionsToWrite;
    private int pollingInterval;

    private String host;
    private int port;

    private volatile boolean isClosed;

    public void close() {
        this.isClosed = true;
    }

    public void shutdown() {
        this.isClosed = true;
    }

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.toArray()) + " to net [" + host + ":" + port + "]");
        try {
            Socket socket = new Socket(host, port);
            while (!isClosed && !Thread.currentThread().isInterrupted()) {
                try {
                    pollAndSendToNet(socket);
                } catch (InterruptedException e) {
                    logger.warn(Thread.currentThread().getName() + " is interrupted.");
                    break;
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void pollAndSendToNet(Socket socket) throws InterruptedException, IOException, TQueueException {
        for (int partitionId : partitionsToWrite) {
            consumer.assign(topic, partitionId);
            List<ConsumerRecord> records = consumer.poll();
            for (ConsumerRecord record : records) {
                final String topic = record.topic();
                final int partition = record.partition();
                final long offset = record.offset();
                String message = new String(record.value(), StandardCharsets.UTF_8);

                JSONObject obj = new JSONObject();
                obj.put("topic", topic);
                obj.put("partition", partition);
                obj.put("message", message);

                logger.trace(String.format("topic: %s, partition: %d, offset: %d, value = %s", topic, partition, offset, message));
                trySendToNet(socket, obj.toJSONString());
            }
        }
        TimeUnit.MILLISECONDS.sleep(pollingInterval);
    }

    private void trySendToNet(Socket socket, String message) throws IOException {
        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.getOutputStream()))));
        for (int i = 0; i < 10; i++) {
            out.println(message);
            out.flush();
        }
    }

    //setters
    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(TQueueConsumer consumer) {
        this.consumer = consumer;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

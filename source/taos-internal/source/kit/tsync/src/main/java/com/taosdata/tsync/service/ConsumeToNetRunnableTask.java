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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class ConsumeToNetRunnableTask implements Runnable, Countable, Stoppable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToNetRunnableTask.class);
    private static final AtomicLong count = new AtomicLong(0);

    private TQueueConsumer consumer;
    private String topic;
    private int[] partitionsToWrite;
    private int pollingInterval;
    private long startOffset;
    private String host;
    private int port;

    private volatile boolean isClosed;

    @Override
    public void shutdown() {
        this.isClosed = true;
    }

    @Override
    public long getCount() {
        return count.get();
    }

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite) + " to net [" + host + ":" + port + "]");
        IntStream.of(partitionsToWrite).forEach(partition -> {
            try {
                consumer.assign(topic, partition, startOffset);
            } catch (TQueueException e) {
                e.printStackTrace();
            }
        });

        try {
            Socket socket = new Socket(host, port);
            while (!isClosed && !Thread.currentThread().isInterrupted()) {
                pollAndSendToNet(socket);
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void pollAndSendToNet(Socket socket) {
        try {
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

                    logger.debug(String.format("count: %d, topic: %s, partition: %d, offset: %d, value = %s", count.incrementAndGet(), topic, partition, offset, message));
                    trySendToNet(socket, obj.toJSONString());
                }
            }
            if (pollingInterval > 0)
                TimeUnit.MILLISECONDS.sleep(pollingInterval);
        } catch (TQueueException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.debug(Thread.currentThread().getName() + " interrupted");
            shutdown();
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": IOException happened");
            e.printStackTrace();
        }
    }

    private void trySendToNet(Socket socket, String message) throws IOException {
        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.getOutputStream()))));
        out.println(message);
        out.flush();
    }

    //setters
    public void setPartitionsToWrite(int[] partitionsToWrite) {
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

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }
}

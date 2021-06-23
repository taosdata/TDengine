package com.taosdata.tsync.service;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.ProducerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.tqueue.TQueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

public class NetToTQueueRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NetToTQueueRunnableTask.class);

    private volatile boolean isClosed;
    private int listeningPort;
    private TQueueProducer producer;
    private ServerSocket serverSocket;

    public void shutdown() {
        this.isClosed = true;
    }

    @Override
    public void run() {
        try {
            logger.info("server socket start and listening : " + listeningPort);
            serverSocket = new ServerSocket(listeningPort);
            fromNetToTQueue();
            serverSocket.close();
            logger.info("server socket closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fromNetToTQueue() throws IOException {
        while (!isClosed && !Thread.currentThread().isInterrupted()) {
            receiveFromNet(serverSocket);
        }
    }

    private void receiveFromNet(ServerSocket serverSocket) throws IOException {
        Socket socket = serverSocket.accept();

        try (InputStream in = socket.getInputStream()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(in)));
            String line;
            while ((line = reader.readLine()) != null) {
                logger.trace("receive message: " + line);
                writeToTQueue(line);
            }
        }

        socket.close();
    }

    private void writeToTQueue(String line) {
        JSONObject messageObj;
        try {
            messageObj = JSONObject.parseObject(line);
        } catch (JSONException e) {
            String errorMsg = "message must be a JSONObject";
            logger.error(errorMsg);
            return;
        }

        String topic = messageObj.getString("topic");
        Integer partition = messageObj.getInteger("partition");
        String message = messageObj.getString("message");

        try {
            ProducerRecord record = new ProducerRecord(topic, partition, message);
            producer.send(record);
        } catch (TQueueException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // setters
    public void setProducer(TQueueProducer producer) {
        this.producer = producer;
    }

    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }
}

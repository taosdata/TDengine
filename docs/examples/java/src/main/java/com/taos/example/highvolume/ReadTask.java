package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class ReadTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadTask.class);
    private final int taskId;
    private final List<BlockingQueue<String>> taskQueues;
    private final int queueCount;
    private final int tableCount;
    private boolean active = true;

    public ReadTask(int readTaskId, List<BlockingQueue<String>> queues, int tableCount) {
        this.taskId = readTaskId;
        this.taskQueues = queues;
        this.queueCount = queues.size();
        this.tableCount = tableCount;
    }

    /**
     * Assign data received to different queues.
     * Here we use the suffix number in table name.
     * You are expected to define your own rule in practice.
     *
     * @param line record received
     * @return which queue to use
     */
    public int getQueueId(String line) {
        String tbName = line.substring(0, line.indexOf(',')); // For example: tb1_101
        String suffixNumber = tbName.split("_")[1];
        return Integer.parseInt(suffixNumber) % this.queueCount;
    }

    @Override
    public void run() {
        logger.info("started");
        Iterator<String> it = new MockDataSource("tb" + this.taskId, tableCount);
        try {
            while (it.hasNext() && active) {
                String line = it.next();
                int queueId = getQueueId(line);
                taskQueues.get(queueId).put(line);
            }
        } catch (Exception e) {
            logger.error("Read Task Error", e);
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
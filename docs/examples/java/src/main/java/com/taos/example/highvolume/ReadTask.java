package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Generate test data
 */
class MockDataSource implements Iterator {
    private String tbNamePrefix;
    private int tableCount = 1000;
    private long maxRowsPerTable = 1000000000L;

    // 100 milliseconds between two neighbouring rows.
    long startMs = System.currentTimeMillis() - maxRowsPerTable * 100;
    private int currentRow = 0;
    private int currentTbId = -1;

    // mock values
    String[] location = {"LosAngeles", "SanDiego", "Hollywood", "Compton", "San Francisco"};
    float[] current = {8.8f, 10.7f, 9.9f, 8.9f, 9.4f};
    int[] voltage = {119, 116, 111, 113, 118};
    float[] phase = {0.32f, 0.34f, 0.33f, 0.329f, 0.141f};

    public MockDataSource(String tbNamePrefix) {
        this.tbNamePrefix = tbNamePrefix;
    }

    @Override
    public boolean hasNext() {
        currentTbId += 1;
        if (currentTbId == tableCount) {
            currentTbId = 0;
            currentRow += 1;
        }
        return currentRow < maxRowsPerTable;
    }

    @Override
    public String next() {
        long ts = startMs + 100 * currentRow;
        int groupId = currentTbId % 5 == 0 ? currentTbId / 5 : currentTbId / 5 + 1;
        StringBuilder sb = new StringBuilder(tbNamePrefix + "_" + currentTbId + ","); // tbName
        sb.append(ts).append(','); // ts
        sb.append(current[currentRow % 5]).append(','); // current
        sb.append(voltage[currentRow % 5]).append(','); // voltage
        sb.append(phase[currentRow % 5]).append(','); // phase
        sb.append(location[currentRow % 5]).append(','); // location
        sb.append(groupId); // groupID

        return sb.toString();
    }
}

// ANCHOR: ReadTask
class ReadTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadTask.class);
    private final int taskId;
    private final List<BlockingQueue<String>> taskQueues;
    private final int queueCount;
    private boolean active = true;

    public ReadTask(int readTaskId, List<BlockingQueue<String>> queues) {
        this.taskId = readTaskId;
        this.taskQueues = queues;
        this.queueCount = queues.size();
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
        Iterator<String> it = new MockDataSource("tb" + this.taskId);
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

// ANCHOR_END: ReadTask
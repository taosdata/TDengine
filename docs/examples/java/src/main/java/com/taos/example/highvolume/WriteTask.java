package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

class WriteTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteTask.class);
    private final int maxBatchSize;

    // the queue from which this writing task get raw data.
    private final BlockingQueue<String> queue;

    // A flag indicate whether to continue.
    private boolean active = true;

    public WriteTask(BlockingQueue<String> taskQueue, int maxBatchSize) {
        this.queue = taskQueue;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void run() {
        logger.info("started");
        String line = null; // data getting from the queue just now.
        SQLWriter writer = new SQLWriter(maxBatchSize);
        try {
            writer.init();
            while (active) {
                line = queue.poll();
                if (line != null) {
                    // parse raw data and buffer the data.
                    writer.processLine(line);
                } else if (writer.hasBufferedValues()) {
                    // write data immediately if no more data in the queue
                    writer.flush();
                } else {
                    // sleep a while to avoid high CPU usage if no more data in the queue and no buffered records, .
                    Thread.sleep(100);
                }
            }
            if (writer.hasBufferedValues()) {
                writer.flush();
            }
        } catch (Exception e) {
            String msg = String.format("line=%s, bufferedCount=%s", line, writer.getBufferedCount());
            logger.error(msg, e);
        } finally {
            writer.close();
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
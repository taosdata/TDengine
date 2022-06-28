package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

// ANCHOR: WriteTask
class WriteTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteTask.class);
    private final static int maxBatchSize = 500;
    private final BlockingQueue<String> queue;
    private boolean active = true;

    public WriteTask(BlockingQueue<String> taskQueue) {
        this.queue = taskQueue;
    }

    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        return DriverManager.getConnection(jdbcURL);
    }

    @Override
    public void run() {
        logger.info("started");
        int bufferedCount = 0;
        String line = null;
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                Map<String, String> tbValues = new HashMap<>();
                while (active) {
                    line = queue.poll();
                    if (line != null) {
                        processLine(tbValues, line);
                        bufferedCount += 1;
                        if (bufferedCount == maxBatchSize) {
                            // trigger writing when count of buffered records reached maxBachSize
                            flushValues(stmt, tbValues);
                            bufferedCount = 0;
                        }
                    } else if (bufferedCount == 0) {
                        // if queue is empty and no buffered records, sleep a while to avoid high CPU usage.
                        Thread.sleep(500);
                    } else {
                        // if queue is empty and there are buffered records then flush immediately
                        flushValues(stmt, tbValues);
                        bufferedCount = 0;
                    }
                }
                if (bufferedCount > 0) {
                    flushValues(stmt, tbValues);
                }
            }
        } catch (Exception e) {
            String msg = String.format("line=%s, bufferedCount=%s", line, bufferedCount);
            logger.error(msg, e);
        }
    }

    private void processLine(Map<String, String> tbValues, String line) {
    }

    private void flushValues(Statement stmt, Map<String, String> tbValues) {
        StringBuilder sb = new StringBuilder("INSERT INTO");
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}
// ANCHOR_END: WriteTask
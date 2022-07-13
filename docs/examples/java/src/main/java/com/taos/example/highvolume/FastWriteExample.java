package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;

    public DataBaseMonitor init() throws SQLException {
        if (conn == null) {
            String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
        }
        return this;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    public void prepareDatabase() throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS test");
        stmt.execute("CREATE DATABASE test");
        stmt.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    }

    public Long count() throws SQLException {
        if (!stmt.isClosed()) {
            ResultSet result = stmt.executeQuery("SELECT count(*) from test.meters");
            result.next();
            return result.getLong(1);
        }
        return null;
    }
}

// ANCHOR: main
public class FastWriteExample {
    final static Logger logger = LoggerFactory.getLogger(FastWriteExample.class);

    final static int taskQueueCapacity = 10000000;
    final static List<BlockingQueue<String>> taskQueues = new ArrayList<>();
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();
    final static DataBaseMonitor databaseMonitor = new DataBaseMonitor();

    public static void stopAll() {
        logger.info("shutting down");
        readTasks.forEach(task -> task.stop());
        writeTasks.forEach(task -> task.stop());
        databaseMonitor.close();
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        int readTaskCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int writeTaskCount = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int tableCount = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        int maxBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 3000;

        logger.info("readTaskCount={}, writeTaskCount={} tableCount={} maxBatchSize={}",
                readTaskCount, writeTaskCount, tableCount, maxBatchSize);

        databaseMonitor.init().prepareDatabase();

        // Create task queues, whiting tasks and start writing threads.
        for (int i = 0; i < writeTaskCount; ++i) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(taskQueueCapacity);
            taskQueues.add(queue);
            WriteTask task = new WriteTask(queue, maxBatchSize);
            Thread t = new Thread(task);
            t.setName("WriteThread-" + i);
            t.start();
        }

        // create reading tasks and start reading threads
        int tableCountPerTask = tableCount / readTaskCount;
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, taskQueues, tableCountPerTask);
            Thread t = new Thread(task);
            t.setName("ReadThread-" + i);
            t.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::stopAll));

        long lastCount = 0;
        while (true) {
            Thread.sleep(10000);
            long count = databaseMonitor.count();
            logger.info("count={} speed={}", count, (count - lastCount) / 10);
            lastCount = count;
        }
    }
}
// ANCHOR_END: main
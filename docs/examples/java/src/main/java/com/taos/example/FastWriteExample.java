package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


class ReadTask {
    private final int taskId;
    private Thread readThread;
    private List<WriteTask> writeTasks;

    public ReadTask(int readTaskId, List<WriteTask> writeTasks) {
        this.taskId = readTaskId;
        this.writeTasks = writeTasks;
        this.readThread = new Thread(this::doReadTask);
    }

    /**
     * Simulate getting data from datasource.
     */
    private Iterator<String> getSourceDataIterator() {
        long now = System.currentTimeMillis();
        int tableCount = 1000;
        int numberRows = 10 ^ 7;
        int tsStep = 100; // 100 milliseconds
        String tbNamePrefix = "tb" + taskId + "-";

        return new Iterator<String>() {
            private long ts = now - numberRows * tsStep;
            private int tbId = 0;

            @Override
            public boolean hasNext() {
                return ts < now;
            }

            @Override
            public String next() {
                if (tbId < tableCount) {
                    tbId += 1;
                } else {
                    ts += tsStep;
                    tbId = 0;
                }
                StringBuilder sb = new StringBuilder(tbNamePrefix + tbId + ","); // tbName
                sb.append(ts).append(','); // ts
                sb.append(1.0).append(','); // current
                sb.append(110).append(','); // voltage
                sb.append(0.32).append(','); // phase
                sb.append(3).append(','); // groupID
                sb.append("Los Angeles"); // location

                return sb.toString();
            }
        };
    }

    /**
     * Read lines from datasource. And assign each line to a writing task according the table name.
     */
    private void doReadTask() {
        int numberWriteTask = writeTasks.size();
        Iterator<String> it = getSourceDataIterator();

        try {
            while (it.hasNext()) {
                String line = it.next();
                String tbName = line.substring(0, line.indexOf(','));
                int writeTaskId = tbName.hashCode() % numberWriteTask;
                writeTasks.get(writeTaskId).put(line);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * start reading thread
     */
    public void start() {
        this.readThread.start();
    }
}

class WriteTask {
    final static int maxBachSize = 500;
    final static int taskQueueCapacity = 1000;

    private Thread writeThread = new Thread(this::doWriteTask);

    private BlockingQueue<String> queue = new LinkedBlockingDeque<>(taskQueueCapacity);

    public void put(String line) throws InterruptedException {
        queue.put(line);
    }

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    public void doWriteTask() {
        try {
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();
            while (true) {
                String line = queue.poll();
                if (line != null) {

                } else {
                    Thread.sleep(1);
                }
            }
        } catch (Exception e) {
            // handle exception
        }

    }

    /**
     * start writing thread
     */
    public void start() {
        writeThread.start();
    }
}

public class FastWriteExample {

    final static int readTaskCount = 1;
    final static int writeTaskCount = 4;
    final static List<ReadTask> readTasks = new ArrayList<>();
    final static List<WriteTask> writeTasks = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        // Create write tasks
        for (int i = 0; i < writeTaskCount; ++i) {
            WriteTask task = new WriteTask();
            task.start();
            writeTasks.add(new WriteTask());
        }

        // Create read tasks
        for (int i = 0; i < readTaskCount; ++i) {
            ReadTask task = new ReadTask(i, writeTasks);
            task.start();
            readTasks.add(task);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}

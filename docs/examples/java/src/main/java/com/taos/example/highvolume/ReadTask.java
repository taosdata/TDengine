package com.taos.example.highvolume;

import java.util.Iterator;
import java.util.List;


class FakeDataSource implements Iterator {
    private long now = System.currentTimeMillis();
    private int tableCount = 1000;
    private int numberRows = 10 ^ 7;
    private int tsStep = 100; // 100 milliseconds
    private String tbNamePrefix;
    private long ts = now - numberRows * tsStep;
    private int tbId = 0;

    public FakeDataSource(String tbNamePrefix) {
        this.tbNamePrefix = tbNamePrefix;

    }

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
}

// ANCHOR: ReadTask
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
     * Read lines from datasource.
     * And assign each line to a writing task according to the table name.
     */
    private void doReadTask() {
        int numberWriteTask = writeTasks.size();
        Iterator<String> it = new FakeDataSource("t" + this.taskId + "tb");
        try {
            while (it.hasNext()) {
                String line = it.next();
                String tbName = line.substring(0, line.indexOf(','));
                int writeTaskId = Math.abs(tbName.hashCode()) % numberWriteTask;
                writeTasks.get(writeTaskId).put(line);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Start reading thread.
     */
    public void start() {
        this.readThread.start();
    }
}

// ANCHOR_END: ReadTask
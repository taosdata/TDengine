package com.zddt.internel;

import java.util.ArrayList;

public class TDTask {
    protected int taskIndex;
    protected String taskName;
    protected int fetchedRows = 0;
    protected int insertedRows = 0;
    protected float fetchedTimeTs = 0;

    public boolean init() {
        return true;
    }

    public TDLine getNextLine() throws Exception {
        return null;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getFetchedRows() {
        return this.fetchedRows;
    }

    public int getInsertedRows() {
        return this.insertedRows;
    }

    public float getFetchedTimeSec() {
        return this.fetchedTimeTs;
    }

    public synchronized void addInsertRows(int insertedRows) {
        this.insertedRows += insertedRows;
    }

    public void close() { }
}

package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Generate test data
 */
class MockDataSource implements Iterator<Meters> {
    private final static Logger logger = LoggerFactory.getLogger(WorkTask.class);

    private final int tableStartIndex;
    private final int tableEndIndex;
    private final long maxRowsPerTable;

    long currentMs = System.currentTimeMillis();
    private int index = 0;

    // mock values

    public MockDataSource(int tableStartIndex, int tableEndIndex, int maxRowsPerTable) {
        this.tableStartIndex = tableStartIndex;
        this.tableEndIndex = tableEndIndex;
        this.maxRowsPerTable = maxRowsPerTable;
    }

    @Override
    public boolean hasNext() {
        return index < (tableEndIndex - tableStartIndex + 1) * maxRowsPerTable;
    }

    @Override
    public Meters next() {
        // use interlace rows one to simulate the data distribution in real world
        if (index % (tableEndIndex - tableStartIndex + 1) == 0) {
            currentMs += 1000;
        }

        long currentTbId = index % (tableEndIndex - tableStartIndex + 1) + tableStartIndex;

        Meters meters = new Meters();

        meters.setTableName(Util.getTableNamePrefix() + currentTbId);
        meters.setTs(new java.sql.Timestamp(currentMs));
        meters.setCurrent((float) (Math.random() * 100));
        meters.setVoltage((int) (Math.random() * 100));
        meters.setPhase((float) (Math.random() * 100));

        index ++;
        return meters;
    }
}

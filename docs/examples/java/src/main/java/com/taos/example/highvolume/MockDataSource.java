package com.taos.example.highvolume;

import java.util.Iterator;

/**
 * Generate test data
 */
class MockDataSource implements Iterator {
    private String tbNamePrefix;
    private int tableCount;
    private long maxRowsPerTable = 1000000000L;

    // 100 milliseconds between two neighbouring rows.
    long startMs = System.currentTimeMillis() - maxRowsPerTable * 100;
    private int currentRow = 0;
    private int currentTbId = -1;

    // mock values
    String[] location = {"California.LosAngeles", "California.SanDiego", "California.SanJose", "California.Campbell", "California.SanFrancisco"};
    float[] current = {8.8f, 10.7f, 9.9f, 8.9f, 9.4f};
    int[] voltage = {119, 116, 111, 113, 118};
    float[] phase = {0.32f, 0.34f, 0.33f, 0.329f, 0.141f};

    public MockDataSource(String tbNamePrefix, int tableCount) {
        this.tbNamePrefix = tbNamePrefix;
        this.tableCount = tableCount;
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

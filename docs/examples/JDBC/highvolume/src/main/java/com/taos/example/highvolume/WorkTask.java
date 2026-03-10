package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;

class WorkTask implements Runnable, Stoppable {
    private static final Logger logger = LoggerFactory.getLogger(WorkTask.class);
    private final int taskId;
    private final int writeThreadCount;
    private final int batchSizeByRow;
    private final int cacheSizeByRow;
    private final int rowsPerTable;
    private final int subTableStartIndex;
    private final int subTableEndIndex;
    private final String dbName;
    private volatile boolean  active = true;
    public WorkTask(int taskId,
                    int writeThradCount,
                    int batchSizeByRow,
                    int cacheSizeByRow,
                    int rowsPerTable,
                    int subTableStartIndex,
                    int subTableEndIndex,
                    String dbName) {
        this.taskId = taskId;
        this.writeThreadCount = writeThradCount;
        this.batchSizeByRow = batchSizeByRow;
        this.cacheSizeByRow = cacheSizeByRow;
        this.rowsPerTable = rowsPerTable;
        this.subTableStartIndex = subTableStartIndex;  // for this task, the start index of sub table
        this.subTableEndIndex = subTableEndIndex; // for this task, the end index of sub table
        this.dbName = dbName;
    }

    @Override
    public void run() {
        logger.info("task {} started", taskId);
        Iterator<Meters> it = new MockDataSource(subTableStartIndex, subTableEndIndex, rowsPerTable);
        try (Connection connection = Util.getConnection(batchSizeByRow, cacheSizeByRow, writeThreadCount);
             PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + dbName +".meters (tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)")) {
            long i = 0L;
            while (it.hasNext() && active) {
                i++;
                Meters meters = it.next();
                pstmt.setString(1, meters.getTableName());
                pstmt.setTimestamp(2, meters.getTs());
                pstmt.setFloat(3, meters.getCurrent());
                pstmt.setInt(4, meters.getVoltage());
                pstmt.setFloat(5, meters.getPhase());
                pstmt.addBatch();

                if (i % batchSizeByRow == 0) {
                    pstmt.executeBatch();
                }

                if (i % (10L * batchSizeByRow) == 0){
                    pstmt.executeUpdate();
                }
            }
        } catch (Exception e) {
            logger.error("Work Task {} Error", taskId, e);
        }
        logger.info("task {} stopped", taskId);
    }

    public void stop() {
        logger.info("task {} stopping", taskId);
        this.active = false;
    }
}
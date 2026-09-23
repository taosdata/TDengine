/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
                    int writeThreadCount,
                    int batchSizeByRow,
                    int cacheSizeByRow,
                    int rowsPerTable,
                    int subTableStartIndex,
                    int subTableEndIndex,
                    String dbName) {
        this.taskId = taskId;
        this.writeThreadCount = writeThreadCount;
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
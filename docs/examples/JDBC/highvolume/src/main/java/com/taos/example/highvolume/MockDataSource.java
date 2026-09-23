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

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate test data
 */
class MockDataSource implements Iterator<Meters> {
    private final static Logger logger = LoggerFactory.getLogger(MockDataSource.class);

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
        // use interlace rows to simulate the data distribution in real world
        if (index % (tableEndIndex - tableStartIndex + 1) == 0) {
            currentMs += 1000;
        }

        long currentTbId = index % (tableEndIndex - tableStartIndex + 1) + tableStartIndex;

        Meters meters = new Meters();

        meters.setTableName(Util.getTableNamePrefix() + currentTbId);
        meters.setTs(new java.sql.Timestamp(currentMs));
        meters.setCurrent((float) (Math.random() * 100));
        meters.setVoltage(ThreadLocalRandom.current().nextInt());
        meters.setPhase((float) (Math.random() * 100));

        index ++;
        return meters;
    }
}

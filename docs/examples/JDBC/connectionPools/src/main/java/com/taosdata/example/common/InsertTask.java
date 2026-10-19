/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taosdata.example.common;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class InsertTask implements Runnable {
    private final Random random = new Random(System.currentTimeMillis());
    private static final Logger logger = LogManager.getLogger(InsertTask.class);

    private final DataSource ds;
    private final String dbName;
    private final long tableSize;
    private final long batchSize;

    public InsertTask(DataSource ds, String dbName, long tableSize, long batchSize) {
        this.ds = ds;
        this.dbName = dbName;
        this.tableSize = tableSize;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        int affectedRows = 0;
        long start = System.currentTimeMillis();
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            for (int tb_index = 1; tb_index <= tableSize; tb_index++) {
                StringBuilder sb = new StringBuilder();
                sb.append("insert into ").append(dbName).append(".t_").append(tb_index).append("(ts, temperature, humidity) values ");
                for (int i = 0; i < batchSize; i++) {
                    sb.append("(").append(start + i).append(", ").append(random.nextFloat() * 30).append(", ").append(random.nextInt(70)).append(") ");
                }
                logger.info("SQL >>> " + sb.toString());
                affectedRows += stmt.executeUpdate(sb.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info(">>> affectedRows:" + affectedRows + "  TimeCost:" + (System.currentTimeMillis() - start) + " ms");
    }
}

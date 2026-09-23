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
import java.sql.SQLException;
import java.sql.Statement;

class CreateSubTableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CreateSubTableTask.class);
    private final int taskId;
    private final int subTableStartIndex;
    private final int subTableEndIndex;
    private final String dbName;

    public CreateSubTableTask(int taskId,
                              int subTableStartIndex,
                              int subTableEndIndex,
                              String dbName) {
        this.taskId = taskId;
        this.subTableStartIndex = subTableStartIndex;
        this.subTableEndIndex = subTableEndIndex;
        this.dbName = dbName;
    }

    @Override
    public void run() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()){
            statement.execute("use " + dbName);
            StringBuilder sql = new StringBuilder();
            sql.append("create table");
            int i = 0;
            for (int tableNum = subTableStartIndex; tableNum <= subTableEndIndex; tableNum++) {
                sql.append(" if not exists " + Util.getTableNamePrefix() + tableNum + " using meters" + " tags(" + tableNum + ", " + "\"location_" + tableNum + "\"" + ")");

                if (i < 1000) {
                    i++;
                } else {
                    statement.execute(sql.toString());
                    sql = new StringBuilder();
                    sql.append("create table");
                    i = 0;
                }
            }
            if (sql.length() > "create table".length()) {
                statement.execute(sql.toString());
            }
        } catch (SQLException e) {
            logger.error("task id {}, failed to create sub table", taskId, e);
        }
    }


}
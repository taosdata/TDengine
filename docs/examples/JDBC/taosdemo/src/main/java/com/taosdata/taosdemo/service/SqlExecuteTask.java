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

package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.utils.Printer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlExecuteTask implements Runnable {
    private final DataSource dataSource;
    private final String sql;

    public SqlExecuteTask(DataSource dataSource, String sql) {
        this.dataSource = dataSource;
        this.sql = sql;
    }

    @Override
    public void run() {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = stmt.execute(sql);
            long end = System.currentTimeMillis();
            if (execute) {
                ResultSet rs = stmt.getResultSet();
                Printer.printResult(rs);
            } else {
                Printer.printSql(sql, true, (end - start));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

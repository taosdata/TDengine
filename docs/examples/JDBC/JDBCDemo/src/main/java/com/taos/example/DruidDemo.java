/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taos.example;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.Statement;

public class DruidDemo {
    // ANCHOR: connection_pool
    public static void main(String[] args) throws Exception {
        String url = "jdbc:TAOS-WS://127.0.0.1:6041/log?varcharAsString=true";

        DruidDataSource dataSource = new DruidDataSource();
        // jdbc properties
        dataSource.setDriverClassName("com.taosdata.jdbc.ws.WebSocketDriver");
        dataSource.setUrl(url);
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");
        // pool configurations
        dataSource.setInitialSize(10);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(10);
        dataSource.setMaxWait(30000);

        Connection connection = dataSource.getConnection(); // get connection
        Statement statement = connection.createStatement(); // get statement
        // query or insert
        // ...

        statement.close();
        connection.close(); // put back to connection pool
        dataSource.close();
    }
    // ANCHOR_END: connection_pool
}

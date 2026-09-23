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

package com.taosdata.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.Statement;

public class HikariDemo {
    // ANCHOR: connection_pool
    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        // jdbc properties
        config.setJdbcUrl("jdbc:TAOS-WS://127.0.0.1:6041/log?varcharAsString=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        // connection pool configurations
        config.setMinimumIdle(10); // minimum number of idle connection
        config.setMaximumPoolSize(10); // maximum number of connection in the pool
        config.setConnectionTimeout(30000); // maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0); // maximum life time for each connection
        config.setIdleTimeout(0); // max idle time for recycle idle connection
        config.setConnectionTestQuery("SELECT 1"); // validation query

        HikariDataSource dataSource = new HikariDataSource(config); // create datasource

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

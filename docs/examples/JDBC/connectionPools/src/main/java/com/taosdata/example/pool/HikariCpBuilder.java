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

package com.taosdata.example.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class HikariCpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        HikariConfig config = new HikariConfig();
        // jdbc properties
        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        config.setJdbcUrl("jdbc:TAOS://" + host + ":6030");
        config.setUsername("root");
        config.setPassword("taosdata");
        // pool configurations
        config.setMinimumIdle(poolSize);           //minimum number of idle connection
        config.setMaximumPoolSize(poolSize);      //maximum number of connection in the pool
        config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select SERVER_VERSION()"); //validation query

        HikariDataSource ds = new HikariDataSource(config);
        return ds;
    }
}

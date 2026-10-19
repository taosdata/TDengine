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

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DruidPoolBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        final String url = "jdbc:TAOS://" + host + ":6030";

        DruidDataSource dataSource = new DruidDataSource();
        // jdbc properties
        dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        dataSource.setUrl(url);
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");
        // pool configurations
        dataSource.setInitialSize(poolSize);
        dataSource.setMinIdle(poolSize);
        dataSource.setMaxActive(poolSize);
        dataSource.setMaxWait(30000);
        dataSource.setValidationQuery("select SERVER_VERSION()");
        return dataSource;
    }

}

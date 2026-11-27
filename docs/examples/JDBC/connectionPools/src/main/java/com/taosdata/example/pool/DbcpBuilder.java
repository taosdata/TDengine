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

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public class DbcpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        ds.setUrl("jdbc:TAOS://" + host + ":6030");
        ds.setUsername("root");
        ds.setPassword("taosdata");

        ds.setMaxTotal(poolSize);
        ds.setMinIdle(poolSize);
        ds.setInitialSize(poolSize);
        return ds;
    }
}

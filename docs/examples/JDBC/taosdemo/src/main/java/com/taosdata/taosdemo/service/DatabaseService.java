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

import com.taosdata.taosdemo.dao.DatabaseMapper;
import com.taosdata.taosdemo.dao.DatabaseMapperImpl;

import javax.sql.DataSource;
import java.util.Map;

public class DatabaseService {

    private final DatabaseMapper databaseMapper;

    public DatabaseService(DataSource dataSource) {
        this.databaseMapper = new DatabaseMapperImpl(dataSource);
    }

    // Create database with specified name
    public void createDatabase(String database) {
        databaseMapper.createDatabase(database);
    }

    // Create database with specified parameters such as keep, days, replica, etc.
    public void createDatabase(Map<String, String> map) {
        if (map.isEmpty())
            return;
        if (map.containsKey("database") && map.size() == 1) {
            createDatabase(map.get("database"));
            return;
        }
        databaseMapper.createDatabaseWithParameters(map);
    }

    // drop database
    public void dropDatabase(String dbname) {
        databaseMapper.dropDatabase(dbname);
    }

    // use database
    public void useDatabase(String dbname) {
        databaseMapper.useDatabase(dbname);
    }
}

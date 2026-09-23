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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DatabaseServiceTest {

    private static DatabaseService service;

    @Test
    public void testCreateDatabase1() {
        service.createDatabase("testXXXX");
    }

    @Test
    public void dropDatabase() {
        service.dropDatabase("testXXXX");
    }

    @Test
    public void useDatabase() {
        service.useDatabase("test");
    }

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
        config.setUsername("root");
        config.setPassword("taosdata");
        HikariDataSource dataSource = new HikariDataSource(config);
        service = new DatabaseService(dataSource);
    }

}
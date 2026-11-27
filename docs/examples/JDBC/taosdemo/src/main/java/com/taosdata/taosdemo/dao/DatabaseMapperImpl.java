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

package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Map;

public class DatabaseMapperImpl implements DatabaseMapper {
    private static final Logger logger = LogManager.getLogger(DatabaseMapperImpl.class);

    private final JdbcTemplate jdbcTemplate;

    public DatabaseMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    @Override
    public void createDatabase(String dbname) {
        String sql = "create database if not exists " + dbname;
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void dropDatabase(String dbname) {
        String sql = "drop database if exists " + dbname;
        jdbcTemplate.update(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void createDatabaseWithParameters(Map<String, String> map) {
        String sql = SqlSpeller.createDatabase(map);
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void useDatabase(String dbname) {
        String sql = "use " + dbname;
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }
}

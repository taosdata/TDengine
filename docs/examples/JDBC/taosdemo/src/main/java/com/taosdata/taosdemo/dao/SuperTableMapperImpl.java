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

import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class SuperTableMapperImpl implements SuperTableMapper {
    private static final Logger logger = LogManager.getLogger(SuperTableMapperImpl.class);
    private JdbcTemplate jdbcTemplate;

    public SuperTableMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void createSuperTable(SuperTableMeta tableMetadata) {
        String sql = SqlSpeller.createSuperTable(tableMetadata);
        logger.debug("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }

    @Override
    public void dropSuperTable(String database, String name) {
        String sql = "drop table if exists " + database + "." + name;
        logger.debug("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }
}

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

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class SubTableMapperImpl implements SubTableMapper {

    private static final Logger logger = LogManager.getLogger(SubTableMapperImpl.class);
    private final JdbcTemplate jdbcTemplate;

    public SubTableMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void createUsingSuperTable(SubTableMeta subTableMeta) {
        String sql = SqlSpeller.createTableUsingSuperTable(subTableMeta);
        logger.debug("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValues(subTableValue);
        logger.debug("SQL >>> " + sql);

        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValuesUsingSuperTable(subTableValue);
        logger.debug("SQL >>> " + sql);

        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertMultiTableMultiValues(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiSubTableMultiValues(tables);
        logger.debug("SQL >>> " + sql);
        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesUsingSuperTable(tables);
        logger.debug("SQL >>> " + sql);
        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }
}

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

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class TableMapperImpl implements TableMapper {
    private static final Logger logger = LogManager.getLogger(TableMapperImpl.class);
    private JdbcTemplate template;

    @Override
    public void create(TableMeta tableMeta) {
        String sql = SqlSpeller.createTable(tableMeta);
        logger.debug("SQL >>> " + sql);
        template.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValues(values);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertOneTableMultiValuesWithColumns(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValuesWithColumns(values);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValues(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValues(tables);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValuesWithColumns(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesWithColumns(tables);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }
}

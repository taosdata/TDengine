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
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TableMapper {

    // Create: Normal table
    void create(TableMeta tableMeta);

    // Insert: Multiple records into one table
    int insertOneTableMultiValues(TableValue values);

    // Insert: Multiple records into one table, specified columns
    int insertOneTableMultiValuesWithColumns(TableValue values);

    // Insert: Multiple records into multiple tables
    int insertMultiTableMultiValues(List<TableValue> tables);

    // Insert: Multiple records into multiple tables, specified columns
    int insertMultiTableMultiValuesWithColumns(List<TableValue> tables);
}

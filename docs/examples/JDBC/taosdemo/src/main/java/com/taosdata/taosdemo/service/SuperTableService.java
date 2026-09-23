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

import com.taosdata.taosdemo.dao.SuperTableMapper;
import com.taosdata.taosdemo.dao.SuperTableMapperImpl;
import com.taosdata.taosdemo.domain.SuperTableMeta;

import javax.sql.DataSource;

public class SuperTableService {

    private SuperTableMapper superTableMapper;

    public SuperTableService(DataSource dataSource) {
        this.superTableMapper = new SuperTableMapperImpl(dataSource);
    }

    // Create super table, specifying the name and type of each field and each tag
    public void create(SuperTableMeta superTableMeta) {
        superTableMapper.createSuperTable(superTableMeta);
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}

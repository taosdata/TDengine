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
import org.springframework.stereotype.Repository;

@Repository
public interface SuperTableMapper {

    // Create super table: create table if not exists xxx.xxx (f1 type1, f2 type2,
    // ... ) tags( t1 type1, t2 type2 ...)
    void createSuperTable(SuperTableMeta tableMetadata);

    // Drop super table: drop table if exists xxx;
    void dropSuperTable(String database, String name);

    // <!-- TODO: Query all super table information show stables -->

    // <!-- TODO: Query table structure describe stable -->

    // <!-- TODO: Add column alter table ${tablename} add column fieldName dataType
    // -->

    // <!-- TODO: Drop column alter table ${tablename} drop column fieldName -->

    // <!-- TODO: Add tag alter table ${tablename} add tag new_tagName tag_type -->

    // <!-- TODO: Drop tag alter table ${tablename} drop tag_name -->

    // <!-- TODO: Change tag name alter table ${tablename} change tag old_tagName
    // new_tagName -->
}

/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface MetersMapper extends BaseMapper<Meters> {

    @Update("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
    int createTable();

    @Insert("insert into meters (tbname, ts, groupid, location, current, voltage, phase) values(#{tbname}, #{ts}, #{groupid}, #{location}, #{current}, #{voltage}, #{phase})")
    int insertOne(Meters one);
    @Update("drop stable if exists meters")
    void dropTable();

    @Select("select count(*) from meters where tbname = '${tbname}'")
    int countBySql(String tbname);
}

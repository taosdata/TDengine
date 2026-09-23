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

package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Update;

public interface WeatherMapper extends BaseMapper<Weather> {

    @Update("CREATE TABLE if not exists weather(ts timestamp, temperature float, humidity int, location nchar(100))")
    int createTable();

    @Insert("insert into weather (ts, temperature, humidity, location) values(#{ts}, #{temperature}, #{humidity}, #{location, jdbcType=NCHAR})")
    int insertOne(Weather one);

    @Update("drop table if exists weather")
    void dropTable();
}

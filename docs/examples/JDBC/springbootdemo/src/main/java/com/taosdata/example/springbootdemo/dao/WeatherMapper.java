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

package com.taosdata.example.springbootdemo.dao;

import com.taosdata.example.springbootdemo.domain.Weather;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface WeatherMapper {

    Map<String, Object> lastOne();

    void dropDB();

    void createDB();

    void createSuperTable();

    void createTable(Weather weather);

    List<Weather> select(@Param("limit") Long limit, @Param("offset") Long offset);

    int insert(Weather weather);

    int count();

    List<String> getSubTables();

    List<Weather> avg();

}

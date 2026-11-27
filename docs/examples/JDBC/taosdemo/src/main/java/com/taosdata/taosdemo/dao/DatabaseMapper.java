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

import java.util.Map;

public interface DatabaseMapper {

    // create database if not exists XXX
    void createDatabase(String dbname);

    // drop database if exists XXX
    void dropDatabase(String dbname);

    // create database if not exists XXX keep XX days XX replica XX
    void createDatabaseWithParameters(Map<String, String> map);

    // use XXX
    void useDatabase(String dbname);

    //TODO: alter database

    //TODO: show database

}

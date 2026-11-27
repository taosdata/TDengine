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

const taos = require("@tdengine/client");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();

function createDatabase() {
  cursor.execute("CREATE DATABASE test");
  cursor.execute("USE test");
}

function insertData() {
  const lines = [
    "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
    "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
    "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
    "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
  ];
  cursor.schemalessInsert(
    lines,
    taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL,
    taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS
  );
}

try {
  createDatabase();
  insertData();
} finally {
  cursor.close(); 
  conn.close();
}

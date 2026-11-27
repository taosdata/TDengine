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
    {
      metric: "meters.current",
      timestamp: 1648432611249,
      value: 10.3,
      tags: { location: "California.SanFrancisco", groupid: 2 },
    },
    {
      metric: "meters.voltage",
      timestamp: 1648432611249,
      value: 219,
      tags: { location: "California.LosAngeles", groupid: 1 },
    },
    {
      metric: "meters.current",
      timestamp: 1648432611250,
      value: 12.6,
      tags: { location: "California.SanFrancisco", groupid: 2 },
    },
    {
      metric: "meters.voltage",
      timestamp: 1648432611250,
      value: 221,
      tags: { location: "California.LosAngeles", groupid: 1 },
    },
  ];

  cursor.schemalessInsert(
    [JSON.stringify(lines)],
    taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL,
    taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NOT_CONFIGURED
  );
}

try {
  createDatabase();
  insertData();
} finally {
  cursor.close(); 
  conn.close();
}

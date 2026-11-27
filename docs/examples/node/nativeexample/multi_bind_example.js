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

function prepareSTable() {
  cursor.execute("CREATE DATABASE power");
  cursor.execute("USE power");
  cursor.execute(
    "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
  );
}

//ANCHOR: insertData
function insertData() {
  // init
  cursor.stmtInit();
  // prepare
  cursor.stmtPrepare(
    "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)"
  );

  // bind table name and tags
  let tagBind = new taos.TaosMultiBindArr(2);
  tagBind.multiBindBinary(["California.SanFrancisco"]);
  tagBind.multiBindInt([2]);
  cursor.stmtSetTbnameTags("d1001", tagBind.getMultiBindArr());

  // bind values
  let valueBind = new taos.TaosMultiBindArr(4);
  valueBind.multiBindTimestamp([1648432611249, 1648432611749]);
  valueBind.multiBindFloat([10.3, 12.6]);
  valueBind.multiBindInt([219, 218]);
  valueBind.multiBindFloat([0.31, 0.33]);
  cursor.stmtBindParamBatch(valueBind.getMultiBindArr());
  cursor.stmtAddBatch();

  // execute
  cursor.stmtExecute();
  cursor.stmtClose();
}
//ANCHOR_END: insertData

try {
  prepareSTable();
  insertData();
} finally {
  cursor.close();
  conn.close();
}

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

//A cursor also needs to be initialized in order to interact with TDengine from Node.js.
const taos = require("@tdengine/client");
var conn = taos.connect({
  host: "127.0.0.1",
  user: "root",
  password: "taosdata",
  config: "/etc/taos",
  port: 0,
});
var cursor = conn.cursor(); // Initializing a new cursor

//Close a connection
conn.close();
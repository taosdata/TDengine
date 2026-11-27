/*
 * Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
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

const { options, connect } = require("@tdengine/rest");

async function sqlInsert() {
    options.path = "/rest/sql";
    options.host = "localhost";
    options.port = 6041;
    let conn = connect(options);
    let cursor = conn.cursor();
    try {
        let res = await cursor.query('CREATE DATABASE power');
        res = await cursor.query('CREATE STABLE power.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)');
        res = await cursor.query('INSERT INTO power.d1001 USING power.meters TAGS ("California.SanFrancisco", 2) VALUES (NOW, 10.2, 219, 0.32)');
        console.log("res.getResult()", res.getResult());
    } catch (err) {
        console.log(err);
    }
}
sqlInsert();


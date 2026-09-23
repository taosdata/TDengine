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

const taos = require("@tdengine/websocket");

var host = null;
for(var i = 2; i < global.process.argv.length; i++){
  var key = global.process.argv[i].split("=")[0];
  var value = global.process.argv[i].split("=")[1];
  if("host" == key){
    host = value;
  }
}

if(host == null){
    host = 'localhost';
}

async function createConnect() {
    let dsn = 'ws://' + host + ':6041'
    console.log(dsn)
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    return await taos.sqlConnect(conf);
}

async function test() {
    let wsSql = null;
    let wsRows = null;
    let reqId = 0;
    try {
        wsSql = await createConnect()
        let version = await wsSql.version();
        console.log(version);
        let taosResult = await wsSql.exec('SHOW DATABASES', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('CREATE DATABASE IF NOT EXISTS power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('USE power', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('CREATE STABLE IF NOT EXISTS meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('DESCRIBE meters', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('INSERT INTO d1001 USING meters (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW, 10.2, 219, 0.32)', reqId++);
        console.log(taosResult);
        
        wsRows = await wsSql.query('SELECT * FROM meters', reqId++);
        let meta = wsRows.getMeta();
        console.log("wsRow:meta:=>", meta);

        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log('queryRes.Scan().then=>', result);
        }

    }
    catch (err) {
        console.error(err.code, err.message);
        throw err;
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
        taos.destroy();
    }
}

test()
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

const conn = taos.connect({ host: "localhost", database: "power" });
var cursor = conn.cursor();

function runConsumer() {

    // create topic 
    cursor.execute("create topic topic_name_example as select * from meters");

    let consumer = taos.consumer({
        'group.id': 'tg2',
        'td.connect.user': 'root',
        'td.connect.pass': 'taosdata',
        'msg.with.table.name': 'true',
        'enable.auto.commit': 'true'
    });
    
    // subscribe the topic just created.
    consumer.subscribe("topic_name_example");

    // get subscribe topic list
    let topicList = consumer.subscription();
    console.log(topicList);

    for (let i = 0; i < 5; i++) {
        let msg = consumer.consume(100);
        console.log(msg.topicPartition);
        console.log(msg.block);
        console.log(msg.fields)
        consumer.commit(msg);
        console.log(`=======consumer ${i} done`)
    }

    consumer.unsubscribe();
    consumer.close();

    // drop topic
    cursor.execute("drop topic topic_name_example");
}


try {
    runConsumer();
} finally {

    setTimeout(() => {
        cursor.close();
        conn.close();
    }, 2000);
}

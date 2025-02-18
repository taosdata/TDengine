const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')
const { CreateSql, createTopic, dropTopic, colData1, colData2, colData3, tagData1, tagData2, jsonTag1, jsonTag2 } = require('../utils/tmqSql')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'tmq_db';
const topics = ['topic_n1', 'topic_n2', 'topic_j1']
const tables = ['tmq_t1', 'tmq_t2', 'tmq_j1']

// This is a taos connection
let conn;
// This is a Cursor
let c1;
// TMQ consumer
let consumer;

function executeUpdate(sql, printFlag = false) {
    if (printFlag === true) {
        console.log(sql);
    }
    c1.execute(sql, { 'quiet': false });
}

function executeQuery(sql, printFlag = false) {
    if (printFlag === true) {
        console.log(sql);
    }

    c1.execute(sql, { quiet: true })
    var data = c1.fetchall({ 'quiet': false });
    let fields = c1.fields;
    let resArr = [];

    data.forEach(row => {
        row.forEach(data => {
            if (data instanceof Date) {
                resArr.push(data.taosTimestamp());
            } else {
                resArr.push(data);
            }
        })
    })
    return { resData: resArr, resFields: fields };
}

beforeAll(() => {
    conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
    c1 = conn.cursor();
    consumer = taos.consumer({
        'group.id': 'sub_table',
        'td.connect.user': 'root',
        'td.connect.pass': 'taosdata',
        'msg.with.table.name': 'true',
        'enable.auto.commit': 'true'
    })
    executeUpdate(`create database if not exists ${db} keep 36500;`);
    executeUpdate(`use ${db};`);

    // create table 
    tables.forEach((table, index) => {
        executeUpdate(CreateSql(table, index == tables.length - 1 ? true : false))
    });

    // create topic 
    topics.forEach((topic, index) => {
        executeUpdate(createTopic(topic, tables[index],''))
    })

});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
    // drop topic
    // topics.forEach(topic => {
    //     executeUpdate(dropTopic(topic));
    // })
    //close consumer
    consumer.close();

    //executeUpdate(`drop database if exists ${db};`);
    c1.close();
    conn.close();
});

describe.skip("tmq_subscribe_tables", () => {
    test('name:tmq subscribe single topic;' +
        `author:${author};` +
        `desc:TMQ subscribe single topic using one consumer;` +
        `filename:${fileName};` +
        `result:${result}`, () => {

            let insertSql = buildInsertSql(tables[0] + "_s01", tables[0], colData1, tagData1, 14)
            let expectResFields = getFieldArr(getFeildsFromDll(CreateSql(tables[0])));
            let expectResData = getResData(colData1, tagData1, 14);
            executeUpdate(insertSql);

            consumer.subscribe(topics[0]);
            let subscribeTopics = consumer.subscription();
            expect(subscribeTopics).toEqual([topics[0]]);
            let msg = consumer.consume(200);

            // assert topicPartitions
            expect(msg.topicPartition[0].topic).toEqual(topics[0]);
            expect(msg.topicPartition[0].table).toEqual(tables[0] + "_s01");
            expect(msg.topicPartition[0].db).toEqual(db);

            // assert data
            expectResData.forEach((item, index) => {
                if (msg.block[0][index] instanceof Date) {
                    expect(item).toEqual(msg.block[0][index].taosTimestamp())
                } else {
                    expect(item).toEqual(msg.block[0][index])
                }

            });

            // assert fields
            expectResFields.forEach((item, index) => {
                expect(item).toEqual(msg.fields[0][index])
            })
            consumer.unsubscribe();

            expect(2).toEqual(2);
        })

    test('name:tmq subscribe multiple topics;' +
        `author:${author};` +
        `desc:TMQ subscribe multiple topics using one consumer;` +
        `filename:${fileName};` +
        `result:${result}`, () => {

            let insertSql = buildInsertSql(tables[0] + "_s01", tables[0], colData1, tagData1, 14)
            let insertSql2 = buildInsertSql(tables[1] + "_s02", tables[1], colData2, tagData2, 14)
            let insertJson = buildInsertSql(tables[2] + "_j01", tables[2], colData3, jsonTag1, 14)

            let expectResFields = getFieldArr(getFeildsFromDll(CreateSql(tables[0])));
            let expectResData = getResData(colData1, tagData1, 14);

            let expectResFields2 = getFieldArr(getFeildsFromDll(CreateSql(tables[1])));
            let expectResData2 = getResData(colData2, tagData2, 14);

            let expectResFieldsJson = getFieldArr(getFeildsFromDll(CreateSql(tables[2], true)));
            let expectResDataJson = getResData(colData3, jsonTag1, 14);

            // =============== assert topics ==========
            consumer.subscribe(topics.slice(0, 3));
            let subTopics = consumer.subscription();

            // assert topics
            subTopics.forEach(item => {
                expect(topics).toContain(item);
            })

            executeUpdate(insertSql);
            executeUpdate(insertSql2);
            executeUpdate(insertJson);

            let expectFieldMap = new Map();
            let expectDataMap = new Map();

            for (let i = 0; i < 5; i++) {
                let msg = consumer.consume(200);
                if (msg.topicPartition.length != 0) {

                    let tmpArr = msg.block[0];
                    tmpArr.forEach((item, index) => {
                        if (item instanceof Date) {
                            tmpArr[index] = item.taosTimestamp()
                        }
                    })
                    expectDataMap.set(msg.topicPartition[0].table, tmpArr);
                    expectFieldMap.set(msg.topicPartition[0].table, msg.fields[0])
                }

            }

            // =============== assert table 1 =============
            let fields = expectFieldMap.get(tables[0] + "_s01");
            // assert fields
            expectResFields.forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            let vals = expectDataMap.get(tables[0] + "_s01");
            // assert data
            expectResData.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })

            // =============== assert table 2 =============
            fields = expectFieldMap.get(tables[1] + "_s02");
            // assert fields
            expectResFields2.forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            vals = expectDataMap.get(tables[1] + "_s02");
            // assert data
            expectResData2.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })

            // =============== assert table 3 json table =============
            fields = expectFieldMap.get(tables[2] + "_j01");
            // assert fields
            expectResFieldsJson.forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            vals = expectDataMap.get(tables[2] + "_j01");
            // assert data
            expectResDataJson.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })
            consumer.unsubscribe();
        })
})

const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')
const { CreateSql, createTopic, dropTopic, colData1, colData2, colData3, tagData1, tagData2, jsonTag1, jsonTag2 } = require('../utils/tmqSql')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'tmq_db_sub';
const topics = ['topic_db']
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
        'group.id': 'sub_db',
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
        executeUpdate(createTopic(topic,  '' ,  db ))
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


describe.skip("tmq_subscribe_db", () => {
    test('name:tmq subscribe topic of database;' +
        `author:${author};` +
        `desc:TMQ subscribe single topic for a DB using one consumer;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            let insertSql = buildInsertSql(tables[0] + "_s01", tables[0], colData1, tagData1, 14)
            let insertSql2 = buildInsertSql(tables[1] + "_s02", tables[1], colData2, tagData2, 14)
            let insertJson = buildInsertSql(tables[2] + "_j01", tables[2], colData3, jsonTag1, 14)

            let expectResFields = getFieldArr(getFeildsFromDll(CreateSql(tables[0])));
            let expectResFields2 = getFieldArr(getFeildsFromDll(CreateSql(tables[1])));
            let expectResFieldsJson = getFieldArr(getFeildsFromDll(CreateSql(tables[2], true)));

            // =============== assert topics ==========
            consumer.subscribe(topics);
            let subTopics = consumer.subscription();

            // assert topics
            expect(topics).toContain(subTopics[0]);

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
                    // console.log(msg.topicPartition[0])
                    // console.log(tmpArr)
                    // console.log(msg.fields[0])
                    // console.log("===========");

                    expectDataMap.set(msg.topicPartition[0].table, tmpArr);
                    expectFieldMap.set(msg.topicPartition[0].table, msg.fields[0])
                }
                consumer.commit(msg);

            }

            // =============== assert table 1 =============
            let fields = expectFieldMap.get(tables[0] + "_s01");

            // assert fields
            // notice!! consume from db, child table will not retrieve tags.
            expectResFields.slice(0,13).forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            let vals = expectDataMap.get(tables[0] + "_s01");
            // assert data
            colData1.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })

            // =============== assert table 2 =============
            fields = expectFieldMap.get(tables[1] + "_s02");
            
            // assert fields
            // notice!! consume from db, child table will not retrieve tags.
            expectResFields2.slice(0,13).forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            vals = expectDataMap.get(tables[1] + "_s02");
            // assert data
            colData2.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })

            // =============== assert table 3 json table =============
            fields = expectFieldMap.get(tables[2] + "_j01");
            
            // assert fields
            // notice!! consume from db, child table will not retrieve tags.
            expectResFieldsJson.slice(0,13).forEach((item, index) => {
                expect(fields[index]).toEqual(item);
            })

            vals = expectDataMap.get(tables[2] + "_j01");
            // assert data
            colData3.forEach((item, index) => {
                expect(vals[index]).toEqual(item);
            })

            consumer.unsubscribe();

        })
})
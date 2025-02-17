// const tmq = require();

const taos = require('../tdengine');
const PrepareData = require('./prepare')

const db = 'tmq_db';
const table = 'tmq_stb';
const ifStable = true;
const subscribeDB = false;
const topic = 'topic_test'

var conn = taos.connect({ host: "localhost" });
var cursor = conn.cursor();
var prepareData = new PrepareData(db, table, 5, ifStable, 1657683600000, 3);

function executeUpdate(updateSql) {
    console.log(updateSql);
    cursor.execute(updateSql);
}
function executeQuery(querySql) {
    let query = cursor.query(querySql);
    query.execute().then((result => {
        console.log(querySql);
        result.pretty();
    }));
}

function initData() {

    executeUpdate(prepareData.createDB());
    executeUpdate(prepareData.useDB());
    executeUpdate(prepareData.createTable());
    let insert = prepareData.insertStable();
    insert.forEach(sql => {
        executeUpdate(sql);
    })

}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

function consumerExample() {
    let consumer = taos.consumer({
        'group.id': 'tg2',
        'td.connect.user': 'root',
        'td.connect.pass': 'taosdata',
        'msg.with.table.name': 'true',
        'enable.auto.commit': 'true'
    });

    // const topic = new Array()['topic_table']
    consumer.subscribe([topic]);
    // consumer.subscribe([topic,topic2]);
    for (let t = 0; t < 10; t++) {
        msg = consumer.consume(200);
        console.log(msg.topicPartition);
        console.log(msg.block);
        console.log(msg.fields);
        consumer.commit(msg);
    }

    let topicList = consumer.subscription();
    console.log(topicList);

    consumer.commit(msg);
    consumer.unsubscribe();
    consumer.close();

}

function createTopic() {
    if (subscribeDB == false) {
        executeUpdate(`create topic ${topic} as select * from ${table}`)
    }
    else {
        executeUpdate(`create topic ${topic} as database ${db}`)
    }
}

function dispose() {
    executeUpdate(`drop topic ${topic} `)
    executeUpdate(prepareData.dropTable());
    executeUpdate(prepareData.dropDB());
    cursor.close();
    conn.close();
}

try {
    initData();
    createTopic();
    consumerExample();

} finally {
    sleep(1000).then(dispose)
}


function tmqCommitCallback(tmq, code, param) {
    console.log(`tmq:${tmq}`);
    console.log(`code:${code}`);
    console.log(`param:${param}`)
}

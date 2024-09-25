const taos = require("@tdengine/websocket");

const db = 'power';
const stable = 'meters';
const topic = 'topic_meters'
const topics = [topic];
const groupId = "group1";
const clientId = "client1";


// ANCHOR: create_consumer
async function createConsumer() {
    let configMap = new Map([
        [taos.TMQConstants.GROUP_ID, "group1"],
        [taos.TMQConstants.CLIENT_ID, 'client1'],
        [taos.TMQConstants.CONNECT_USER, "root"],
        [taos.TMQConstants.CONNECT_PASS, "taosdata"],
        [taos.TMQConstants.AUTO_OFFSET_RESET, "latest"],
        [taos.TMQConstants.WS_URL, 'ws://localhost:6041'],
        [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
        [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000']
    ]);
    try {
        return await taos.tmqConnect(configMap);
    } catch (err) {
        console.error(err);
        throw err;
    }

}
// ANCHOR_END: create_consumer 

async function prepare() {
    let conf = new taos.WSConfig('ws://localhost:6041');
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    const createDB = `CREATE DATABASE IF NOT EXISTS ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`;
    const createStable = `CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`;

    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(createDB);
    await wsSql.exec(createStable);

    let createTopic = `CREATE TOPIC IF NOT EXISTS ${topics[0]} AS SELECT * FROM ${db}.${stable}`;
    await wsSql.exec(createTopic);


    for (let i = 0; i < 10; i++) {
        await wsSql.exec(`INSERT INTO d1001 USING ${stable} (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW, ${10 + i}, ${200 + i}, ${0.32 + i})`);
    }
    await wsSql.close();
}

// ANCHOR: subscribe 
async function subscribe(consumer) {
    try {
        await consumer.subscribe(['topic_meters']);
        for (let i = 0; i < 50; i++) {
            let res = await consumer.poll(100);
            for (let [key, value] of res) {
                // Add your data processing logic here
                console.log(`data: ${key} ${value}`);
            }
        }
    } catch (err) {
        console.error(`Failed to poll data, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }

}
// ANCHOR_END: subscribe

// ANCHOR: offset 
async function consumer() {
    let consumer = null;
    try {
        await prepare();
        let consumer = await createConsumer()
        await consumer.subscribe(['topic_meters']);
        let res = new Map();
        while (res.size == 0) {
            res = await consumer.poll(100);
        }

        let assignment = await consumer.assignment();
        await consumer.seekToBeginning(assignment);
        console.log("Assignment seek to beginning successfully");
    }
    catch (err) {
        console.error(`Failed to seek offset, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (consumer) {
            await consumer.close();
        }
        taos.destroy();
    }
}
// ANCHOR_END: offset 

async function test() {
    console.log("begin tmq_seek_example")
    await consumer();
    console.log("end tmq_seek_example")
 }
 
test()

const { sleep } = require("@tdengine/websocket");
const taos = require("@tdengine/websocket");

// ANCHOR: create_consumer
const db = 'power';
const stable = 'meters';
const url = 'ws://localhost:6041';
const topic = 'topic_meters'
const topics = [topic];
const groupId = "group1";
const clientId = "client1";

async function createConsumer() {

    let groupId = "group1";
    let clientId = "client1";
    let configMap = new Map([
        [taos.TMQConstants.GROUP_ID, groupId],
        [taos.TMQConstants.CLIENT_ID, clientId],
        [taos.TMQConstants.CONNECT_USER, "root"],
        [taos.TMQConstants.CONNECT_PASS, "taosdata"],
        [taos.TMQConstants.AUTO_OFFSET_RESET, "latest"],
        [taos.TMQConstants.WS_URL, url],
        [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
        [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000']
    ]);
    try {
        conn = await taos.tmqConnect(configMap);
        console.log(`Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`)
        return conn;
    } catch (err) {
        console.error(`Failed to create websocket consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }

}
// ANCHOR_END: create_consumer 

async function prepare() {
    let conf = new taos.WSConfig('ws://localhost:6041');
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    const createDB = `CREATE DATABASE IF NOT EXISTS ${db}`;
    const createStable = `CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`;

    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(createDB);
    await wsSql.exec(createStable);

    let createTopic = `CREATE TOPIC IF NOT EXISTS ${topics[0]} AS SELECT * FROM ${db}.${stable}`;
    await wsSql.exec(createTopic);
    await wsSql.close();
}

async function insert() {
    let conf = new taos.WSConfig('ws://localhost:6041');
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    let wsSql = await taos.sqlConnect(conf);
    for (let i = 0; i < 50; i++) {
        await wsSql.exec(`INSERT INTO d1001 USING ${stable} (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW, ${10 + i}, ${200 + i}, ${0.32 + i})`);
        await sleep(100);
    }
    await wsSql.close();
}

async function subscribe(consumer) {
    // ANCHOR: commit 
    try {
        await consumer.subscribe(['topic_meters']);
        for (let i = 0; i < 50; i++) {
            let res = await consumer.poll(100);
            for (let [key, value] of res) {
                // Add your data processing logic here
                console.log(`data: ${key} ${value}`);
            }
            await consumer.commit();
            console.log("Commit offset manually successfully.");
        }
    } catch (err) {
        console.error(`Failed to poll data, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    // ANCHOR_END: commit
}

async function consumer() {
    // ANCHOR: unsubscribe
    let consumer = null;
    try {
        await prepare();
        consumer = await createConsumer();
        const allPromises = [];
        allPromises.push(subscribe(consumer));
        allPromises.push(insert());
        await Promise.all(allPromises);
        await consumer.unsubscribe();
        console.log("Consumer unsubscribed successfully.");
    }
    catch (err) {
        console.error(`Failed to unsubscribe consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (consumer) {
            await consumer.close();
            console.log("Consumer closed successfully.");
        }
        taos.destroy();
    }
    // ANCHOR_END: unsubscribe
}

async function test() {
    console.log("begin tmq_example")
    await consumer();
    console.log("end tmq_example")
}

test()

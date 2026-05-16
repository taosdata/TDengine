import { WSConfig } from "../src/common/config";
import { TMQConstants } from "../src/tmq/constant";
import { destroy, setLogLevel, sqlConnect, tmqConnect } from "../src";
import { WsConsumer } from "../src/tmq/wsTmq";

const db = "power";
const stable = "meters";
const url = "ws://localhost:6041";
const topic = "topic_meters";
const topics = [topic];
const groupId = "group-50";
const clientId = "client-50";

async function createConsumer() {
    let configMap = new Map([
        [TMQConstants.GROUP_ID, groupId],
        [TMQConstants.CLIENT_ID, clientId],
        [TMQConstants.CONNECT_USER, "root"],
        [TMQConstants.CONNECT_PASS, "taosdata"],
        [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
        [TMQConstants.WS_URL, url],
        [TMQConstants.ENABLE_AUTO_COMMIT, "false"],
        [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
    ]);
    try {
        let conn = await tmqConnect(configMap);
        console.log(
            `Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`
        );
        return conn;
    } catch (err: any) {
        console.error(
            `Failed to create websocket consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
        );
        throw err;
    }
}
// ANCHOR_END: create_consumer

async function prepare() {
    let conf = new WSConfig("ws://localhost:6041");
    conf.setUser("root");
    conf.setPwd("taosdata");
    conf.setDb(db);
    const createDB = `CREATE DATABASE IF NOT EXISTS ${db}`;
    const createStable = `CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`;

    let wsSql = await sqlConnect(conf);
    await wsSql.exec(createDB);
    await wsSql.exec(createStable);

    let createTopic = `CREATE TOPIC IF NOT EXISTS ${topics[0]} AS SELECT * FROM ${db}.${stable}`;
    await wsSql.exec(createTopic);
    wsSql.close();
}

async function insert() {
    let conf = new WSConfig("ws://localhost:6041");
    conf.setUser("root");
    conf.setPwd("taosdata");
    conf.setDb(db);
    let wsSql = await sqlConnect(conf);
    for (let i = 0; i < 10000; i++) {
        await wsSql.exec(
            `INSERT INTO d1001 USING ${stable} (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW + ${i}a, ${10 + i
            }, ${200 + i}, ${0.32 + i})`
        );
    }
    await wsSql.close();
    console.log("insert fininsh!!!!!");
}

async function subscribe(consumer: WsConsumer) {
    // ANCHOR: commit
    try {
        let count = 0;
        await consumer.subscribe(topics);
        let bFinish = false;
        let bBegin = false;
        const startTime = new Date().getTime();
        while (!bFinish) {
            let res = await consumer.poll(100);
            for (let [key, value] of res) {
                // Add your data processing logic here
                let data = value.getData();
                if (data) {
                    if (data.length == 0 && bBegin) {
                        bFinish = true;
                        break;
                    } else if (data.length > 0) {
                        bBegin = true;
                    }
                    count += data.length;
                    console.log("poll end ------>", count);
                }
            }
            // await consumer.commit();
        }
        const endTime = new Date().getTime();
        console.log(count, endTime - startTime);
    } catch (err: any) {
        console.error(
            `Failed to poll data, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
        );
        throw err;
    }
    // ANCHOR_END: commit
}

async function consumer() {
    // ANCHOR: unsubscribe
    setLogLevel("debug");
    let consumer = null;
    try {
        // await prepare();
        consumer = await createConsumer();
        // const allPromises = [];
        // allPromises.push(subscribe(consumer));
        // allPromises.push(insert());
        // await Promise.all(allPromises);
        // await insert();
        await subscribe(consumer);
        await consumer.unsubscribe();
        console.log("Consumer unsubscribed successfully.");
    } catch (err: any) {
        console.error(
            `Failed to unsubscribe consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`
        );
        throw err;
    } finally {
        if (consumer) {
            await consumer.close();
            console.log("Consumer closed successfully.");
        }
        destroy();
    }
    // ANCHOR_END: unsubscribe
}

async function test() {
    await consumer();
}

test();

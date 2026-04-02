import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { ReqId } from "@src/common/reqid";
import { WsSql } from "@src/sql/wsSql";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { Sleep, testPassword, testUsername } from "@test-helpers/utils";
import { setLevel } from "@src/common/log";
import { WsStmt1 } from "@src/stmt/wsStmt1";

let dsn = `ws://${testUsername()}:${testPassword()}@localhost:6041`;
let tags = ["California.SanFrancisco", 3];
let multi = [
    [1709183268567, 1709183268568, 1709183268569],
    [10.2, 10.3, 10.4],
    [292, 293, 294],
    [0.32, 0.33, 0.34],
];

let configMap = new Map([
    [TMQConstants.GROUP_ID, "gId"],
    [TMQConstants.CONNECT_USER, testUsername()],
    [TMQConstants.CONNECT_PASS, testPassword()],
    [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
    [TMQConstants.CLIENT_ID, "test_tmq_client"],
    [TMQConstants.WS_URL, "ws://localhost:6041"],
    [TMQConstants.ENABLE_AUTO_COMMIT, "true"],
    [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
]);
const stable = "meters";
const db = "power_connect";
const topics: string[] = ["pwer_meters_topic"];
let createTopic = `create topic if not exists ${topics[0]} as select * from ${db}.${stable}`;
let stmtIds: bigint[] = [];
setLevel("debug");

async function connect() {
    let dsn = `ws://${testUsername()}:${testPassword()}@localhost:6041`;
    let wsSql = null;
    let conf: WSConfig = new WSConfig(dsn);
    conf.setDb(db);
    wsSql = await WsSql.open(conf);
    expect(wsSql.state()).toBeGreaterThan(0);
    console.log(await wsSql.version());
    await wsSql.close();
}

async function stmtConnect() {
    let dsn = `ws://${testUsername()}:${testPassword()}@localhost:6041`;
    let wsConf = new WSConfig(dsn, "100.100.100.100");
    wsConf.setDb(db);
    let connector = await WsSql.open(wsConf);
    let stmt = (await connector.stmtInit()) as WsStmt1;
    let id = stmt.getStmtId();
    if (id) {
        stmtIds.push(id);
    }
    expect(stmt).toBeTruthy();
    await stmt.prepare(
        `INSERT INTO ? USING ${stable} (location, groupId) TAGS (?, ?) VALUES (?, ?, ?, ?)`
    );
    await stmt.setTableName("d1001");
    await stmt.setJsonTags(tags);
    let lastTs = 0;
    const allp: any[] = [];
    for (let i = 0; i < 10; i++) {
        for (let j = 0; j < multi[0].length; j++) {
            multi[0][j] = multi[0][0] + j;
            lastTs = multi[0][j];
        }
        allp.push(stmt.jsonBind(multi));
        multi[0][0] = lastTs + 1;
    }
    await Promise.all(allp);
    await stmt.batch();
    await stmt.exec();
    expect(stmt.getLastAffected()).toEqual(30);
    await stmt.close();
    await connector.close();
}

async function tmqConnect() {
    let consumer = null;
    try {
        consumer = await WsConsumer.newConsumer(configMap);
        await consumer.subscribe(topics);

        let res = await consumer.poll(100);
        for (let [key, value] of res) {
            console.log(key, value.getMeta());
            let data = value.getData();
            if (data == null || data.length == 0) {
                break;
            }

            for (let record of data) {
                console.log(record);
            }
        }

        await consumer.commit();

        let assignment = await consumer.assignment();
        console.log(assignment);
        if (arguments && arguments.length > 0)
            await consumer.seekToBeginning(assignment);

        await consumer.unsubscribe();
    } catch (e) {
        console.error(e);
    } finally {
        if (consumer) {
            await consumer.close();
        }
    }
}

beforeAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    let ws = await WsSql.open(conf);
    await ws.exec(
        `create database if not exists ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`
    );
    await Sleep(100);
    await ws.exec(
        `CREATE STABLE if not exists ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`
    );
    await Sleep(100);
    await ws.exec(createTopic, ReqId.getReqID());
    await ws.close();
});

describe("TDWebSocket.WsSql()", () => {
    jest.setTimeout(60 * 1000);
    test("ReqId", async () => {
        const allp: any[] = [];
        for (let i = 0; i < 10; i++) {
            allp.push(console.log(ReqId.getReqID()));
        }
        await Promise.all(allp);
    });

    test("normal connect", async () => {
        const allp: any[] = [];
        for (let i = 0; i < 20; i++) {
            allp.push(connect());
            allp.push(stmtConnect());
            allp.push(tmqConnect());
        }
        await Promise.all(allp);
        console.log(stmtIds);
    });
});

afterAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());

    let wsSql = await WsSql.open(conf);
    await wsSql.exec(`drop topic if exists ${topics[0]};`);
    await wsSql.exec(`drop database if exists ${db};`);
    await wsSql.close();

    WebSocketConnectionPool.instance().destroyed();
});

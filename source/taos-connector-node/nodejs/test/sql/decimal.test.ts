import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { Sleep, testPassword, testUsername } from "@test-helpers/utils";
import { setLevel } from "@src/common/log";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";

let dns = "ws://localhost:6041";
let createTopic = `create topic if not exists topic_decimal_test as select * from power.decimal_test`;
let dropTopic = `DROP TOPIC IF EXISTS topic_decimal_test;`;
setLevel("debug");
beforeAll(async () => {
    let conf: WSConfig = new WSConfig(dns);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());
    let wsSql = await WsSql.open(conf);
    await wsSql.exec(dropTopic);
    await wsSql.exec("drop database if exists power");
    await wsSql.exec(
        "create database if not exists power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
    );
    await Sleep(100);
    await wsSql.exec("use power");
    await wsSql.exec(
        "CREATE STABLE if not exists decimal_test (ts timestamp, dec64 decimal(10,6), dec128 decimal(24,10), int1 int) TAGS (location binary(64), groupId int);"
    );
    await wsSql.exec(createTopic);
    await wsSql.close();
});

const expectedResultsMap = new Map([
    [
        "-1234.654321",
        {
            dec128: "-123456789012.0987654321",
            int1: 3,
            location: "California",
            groupId: 3,
        },
    ],
    [
        "-0.000654",
        {
            dec128: "-0.0009876543",
            int1: 2,
            location: "California",
            groupId: 3,
        },
    ],
    [
        "9876.123456",
        {
            dec128: "1234567890.0987654321",
            int1: 1,
            location: "California",
            groupId: 3,
        },
    ],
]);

describe("TDWebSocket.WsSql()", () => {
    jest.setTimeout(20 * 1000);

    test("insert recoder", async () => {
        let conf: WSConfig = new WSConfig(dns);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("use power");
        console.log(taosResult);
        expect(taosResult).toBeTruthy();

        taosResult = await wsSql.exec("describe decimal_test");
        console.log(taosResult);

        taosResult = await wsSql.exec(
            'INSERT INTO d1001 USING decimal_test (location, groupid) TAGS ("California", 3) VALUES (NOW, "9876.123456", "1234567890.0987654321", 1) (NOW + 1a, "-0.000654", "-0.0009876543", 2) (NOW + 2a, "-1234.654321", "-123456789012.0987654321", 3)'
        );

        console.log(taosResult);
        expect(taosResult.getAffectRows()).toBeGreaterThanOrEqual(3);
        let wsRows = await wsSql.query("select * from decimal_test");
        expect(wsRows).toBeTruthy();
        let meta = wsRows.getMeta();
        expect(meta).toBeTruthy();
        console.log("wsRow:meta:=>", meta);
        let count = 0;
        while (await wsRows.next()) {
            let result = wsRows.getData();
            if (result != null && result.length > 0) {
                if (expectedResultsMap.has(result[1])) {
                    console.log("result:=>", result);
                    const expected = expectedResultsMap.get(result[1]);
                    expect(result[2]).toBe(expected?.dec128);
                    expect(result[3]).toBe(expected?.int1);
                    expect(result[4]).toBe(expected?.location);
                    expect(result[5]).toBe(expected?.groupId);
                    count++;
                }
            }
        }
        await wsSql.close();
        expect(count).toBe(3);
    });
});

test("normal Subscribe", async () => {
    let configMap = new Map([
        [TMQConstants.GROUP_ID, "gId"],
        [TMQConstants.CONNECT_USER, testUsername()],
        [TMQConstants.CONNECT_PASS, testPassword()],
        [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
        [TMQConstants.CLIENT_ID, "test_tmq_client"],
        [TMQConstants.WS_URL, dns],
        [TMQConstants.ENABLE_AUTO_COMMIT, "true"],
        [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
        ["session.timeout.ms", "10000"],
        ["max.poll.interval.ms", "30000"],
        ["msg.with.table.name", "true"],
    ]);
    let consumer = await WsConsumer.newConsumer(configMap);
    await consumer.subscribe(["topic_decimal_test"]);

    let assignment = await consumer.assignment();
    console.log(assignment);
    let useTime: number[] = [];
    let count = 0;
    for (let i = 0; i < 5; i++) {
        let startTime = new Date().getTime();
        let res = await consumer.poll(500);
        let currTime = new Date().getTime();
        useTime.push(Math.abs(currTime - startTime));

        for (let [key, value] of res) {
            console.log(key, value.getMeta());
            let data = value.getData();
            if (data == null || data.length == 0) {
                break;
            }

            for (let record of data) {
                console.log("record:=----------->", record);
                if (expectedResultsMap.has(record[1])) {
                    const expected = expectedResultsMap.get(record[1]);
                    expect(record[2]).toBe(expected?.dec128);
                    expect(record[3]).toBe(expected?.int1);
                    expect(record[4]).toBe(expected?.location);
                    expect(record[5]).toBe(expected?.groupId);
                    count++;
                }
            }
        }
    }
    await consumer.unsubscribe();
    await consumer.close();
    expect(count).toBe(3);
});

afterAll(async () => {
    let conf: WSConfig = new WSConfig(dns);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());
    let wsSql = await WsSql.open(conf);
    await wsSql.exec(dropTopic);
    await wsSql.exec("drop database power");
    await wsSql.close();
    WebSocketConnectionPool.instance().destroyed();
});

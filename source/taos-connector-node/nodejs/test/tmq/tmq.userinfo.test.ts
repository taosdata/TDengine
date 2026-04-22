import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { Sleep, testNon3360, testPassword, testUsername } from "@test-helpers/utils";

describe("TDWebSocket.Tmq user connection options", () => {
    jest.setTimeout(20 * 1000);

    testNon3360("applies dsn user_app and user_ip to tmq connection info", async () => {
        const expectedUserIp = "192.168.1.1";
        const expectedUserApp = "node_tmq_test";
        const tmqConf = new Map<string, any>([
            [TMQConstants.WS_URL, "ws://localhost:6041"],
            [TMQConstants.USER_APP, expectedUserApp],
            [TMQConstants.USER_IP, expectedUserIp],
            [TMQConstants.CONNECT_USER, testUsername()],
            [TMQConstants.CONNECT_PASS, testPassword()],
            [TMQConstants.GROUP_ID, "1038"],
            [TMQConstants.CLIENT_ID, "3837"],
            [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
            [TMQConstants.ENABLE_AUTO_COMMIT, "true"],
            [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
        ]);

        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const wsSql = await WsSql.open(conf);

        let consumer: WsConsumer | null = null;
        let wsRows;

        const db = "test_1776241160";
        const topic = "topic_1776241160";

        try {
            await wsSql.exec(`drop topic if exists ${topic}`);
            await wsSql.exec(`drop database if exists ${db}`);
            await wsSql.exec(`create database ${db}`);
            await wsSql.exec(`create topic ${topic} as database ${db}`);

            consumer = await WsConsumer.newConsumer(tmqConf);
            await consumer.subscribe([topic]);
            await Sleep(2000);

            wsRows = await wsSql.query("show connections");
            const meta = wsRows.getMeta() || [];
            const userIpIndex = meta.findIndex((field) => field.name === "user_ip");
            const userAppIndex = meta.findIndex((field) => field.name === "user_app");

            expect(userIpIndex).toBeGreaterThanOrEqual(0);
            expect(userAppIndex).toBeGreaterThanOrEqual(0);

            let found = false;
            while (await wsRows.next()) {
                const row = wsRows.getData();
                if (!Array.isArray(row)) {
                    continue;
                }
                if (
                    row[userIpIndex] === expectedUserIp &&
                    row[userAppIndex] === expectedUserApp
                ) {
                    found = true;
                    break;
                }
            }
            expect(found).toBe(true);
        } finally {
            if (wsRows) {
                await wsRows.close();
            }
            if (consumer) {
                await consumer.unsubscribe();
                await consumer.close();
            }
            await wsSql.exec(`drop topic if exists ${topic}`);
            await wsSql.exec(`drop database if exists ${db}`);
            await wsSql.close();
        }
    });
});

afterAll(async () => {
    WebSocketConnectionPool.instance().destroyed();
});

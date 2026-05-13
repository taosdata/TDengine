import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { compareUint8Arrays, testNon3360, testPassword, testUsername } from "@test-helpers/utils";

describe("TDWebSocket.TMQ.BLOB", () => {
    jest.setTimeout(20 * 1000);

    afterAll(() => {
        WebSocketConnectionPool.instance().destroyed();
    });

    testNon3360("consume blob data as ArrayBuffer", async () => {
        const db = "test_1776049124";
        const topic = "topic_1776049124";
        const payload = "tmq_blob_payload";
        const expected = new TextEncoder().encode(payload);

        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        let consumer: WsConsumer | null = null;

        try {
            await ws.exec(`drop topic if exists ${topic}`);
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(`create table ctb (ts timestamp, data blob)`);
            await ws.exec(`create topic ${topic} as select data from ${db}.ctb`);
            await ws.exec(`insert into ctb values(now, '${payload}')`);

            const configMap = new Map<string, string>([
                [TMQConstants.WS_URL, "ws://localhost:6041"],
                [TMQConstants.CONNECT_USER, testUsername()],
                [TMQConstants.CONNECT_PASS, testPassword()],
                [TMQConstants.GROUP_ID, "8763"],
                [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
                [TMQConstants.CLIENT_ID, "tmq_blob_client"],
                [TMQConstants.ENABLE_AUTO_COMMIT, "true"],
                [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
            ]);

            consumer = await WsConsumer.newConsumer(configMap);
            await consumer.subscribe([topic]);

            let actual: ArrayBuffer | null = null;
            for (let i = 0; i < 10 && !actual; i++) {
                const res = await consumer.poll(500);
                for (const [, value] of res) {
                    const data = value.getData();
                    if (!data || data.length === 0) {
                        continue;
                    }
                    if (data[0][0] instanceof ArrayBuffer) {
                        actual = data[0][0];
                        break;
                    }
                }
            }

            expect(actual).toBeTruthy();
            if (!actual) {
                throw new Error("consume empty blob message");
            }
            expect(
                compareUint8Arrays(new Uint8Array(actual), expected)
            ).toBe(true);
        } finally {
            if (consumer) {
                await consumer.unsubscribe();
                await consumer.close();
            }
            await ws.exec(`drop topic if exists ${topic}`);
            await ws.exec(`drop database if exists ${db}`);
            await ws.close();
        }
    });
});

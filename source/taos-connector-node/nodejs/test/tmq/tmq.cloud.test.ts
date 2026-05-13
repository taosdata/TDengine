import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { setLevel } from "@src/common/log";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import {
    testCloud,
    testCloudWsUrl,
    testPassword,
    testUsername,
} from "@test-helpers/utils";

const url = testCloudWsUrl();

beforeAll(async () => {
    if (!url) {
        return;
    }
    let wsSql = null;
    try {
        const conf = new WSConfig(url);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        wsSql = await WsSql.open(conf);
        let sql = `INSERT INTO dmeters.d1001 USING dmeters.meters (groupid, location) TAGS(2, 'SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
            dmeters.d1002 USING dmeters.meters (groupid, location) TAGS(3, 'SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)`;
        let res = await wsSql.exec(sql);
        console.log(res);
        expect(res.getAffectRows()).toBeGreaterThanOrEqual(3);
    } catch (err) {
        throw err;
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }
});

describe("TDWebSocket.Tmq()", () => {
    jest.setTimeout(20 * 1000);

    testCloud("normal connect", async () => {
        const topic = "topic_meters";
        const topics = [topic];
        const groupId = "group1";
        const clientId = "client1";
        let configMap = new Map([
            [TMQConstants.GROUP_ID, groupId],
            [TMQConstants.CLIENT_ID, clientId],
            [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
            [TMQConstants.WS_URL, url],
            [TMQConstants.ENABLE_AUTO_COMMIT, "true"],
            [TMQConstants.AUTO_COMMIT_INTERVAL_MS, "1000"],
        ]);

        setLevel("debug");
        // create consumer
        let consumer = await WsConsumer.newConsumer(configMap);
        console.log(
            `Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`
        );
        // subscribe
        await consumer.subscribe(topics);
        console.log(`Subscribe topics successfully, topics: ${topics}`);
        let res = new Map();
        while (res.size == 0) {
            // poll
            res = await consumer.poll(1000);
            for (let [key, value] of res) {
                // Add your data processing logic here
                console.log(`data: ${key} ${value}`);
            }
            // commit
            await consumer.commit();
        }

        // seek
        let assignment = await consumer.assignment();
        await consumer.seekToBeginning(assignment);
        console.log("Assignment seek to beginning successfully");

        // clean
        await consumer.unsubscribe();
        await consumer.close();
    });
});

afterAll(async () => {
    WebSocketConnectionPool.instance().destroyed();
});

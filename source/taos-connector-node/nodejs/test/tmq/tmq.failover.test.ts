import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { TMQConstants } from "@src/tmq/constant";
import { WsConsumer } from "@src/tmq/wsTmq";
import { testPassword, testUsername } from "@test-helpers/utils";
import { WsProxy } from "@test-helpers/wsProxy";

function parseJsonAction(rawData: Buffer | string): string | null {
    if (typeof rawData !== "string") {
        return null;
    }
    try {
        const parsed = JSON.parse(rawData);
        return typeof parsed.action === "string" ? parsed.action : null;
    } catch (_err) {
        return null;
    }
}

describe("tmq failover", () => {
    jest.setTimeout(120 * 1000);

    afterEach(async () => {
        WebSocketConnectionPool.instance().destroyed();
        jest.restoreAllMocks();
    });

    test("tmq failover recovers subscribe context and replays inflight poll", async () => {
        const dbName = "test_1774096925";
        const tableName = "t0";
        const topicName = "topic_1774096925";
        const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
        let setupSql: WsSql | null = null;
        let cleanupSql: WsSql | null = null;
        let consumer: WsConsumer | null = null;
        let restartTriggered = false;
        let proxyAHadActivity = false;

        setupSql = await WsSql.open(new WSConfig(localDsn));
        try {
            await setupSql.exec(`drop topic if exists ${topicName}`);
            await setupSql.exec(`drop database if exists ${dbName}`);
            await setupSql.exec(`create database ${dbName}`);
            await setupSql.exec(`create table ${dbName}.${tableName}(ts timestamp, c1 int)`);
            await setupSql.exec(`insert into ${dbName}.${tableName} values(now - 1s, 1) (now, 2)`);
            await setupSql.exec(`create topic ${topicName} as select * from ${dbName}.${tableName}`);
        } finally {
            await setupSql.close();
            setupSql = null;
        }

        const proxyA = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event) => {
                if (event.type === "client_connected") {
                    proxyAHadActivity = true;
                    return;
                }
                if (
                    event.type === "message" &&
                    event.direction === "client_to_upstream"
                ) {
                    proxyAHadActivity = true;
                }
            },
        });

        const proxyB = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event, control) => {
                if (event.type !== "message") {
                    return;
                }
                if (event.direction !== "client_to_upstream") {
                    return;
                }
                const action = parseJsonAction(event.rawData);
                if (action === "poll" && !restartTriggered) {
                    restartTriggered = true;
                    void control.restart({
                        downtimeMs: 800,
                        reason: "trigger tmq poll failover",
                    });
                }
            },
        });

        const tmqConf = new Map<string, any>([
            [TMQConstants.GROUP_ID, `g_${Date.now()}`],
            [TMQConstants.CLIENT_ID, `c_${Date.now()}`],
            [TMQConstants.CONNECT_USER, testUsername()],
            [TMQConstants.CONNECT_PASS, testPassword()],
            [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
            [TMQConstants.ENABLE_AUTO_COMMIT, false],
            [TMQConstants.AUTO_COMMIT_INTERVAL_MS, 1000],
            [TMQConstants.WS_URL,
            `ws://${testUsername()}:${testPassword()}` +
            `@127.0.0.1:${proxyA.getPort()},127.0.0.1:${proxyB.getPort()}` +
            `?retries=5&retry_backoff_ms=20&retry_backoff_max_ms=20`
            ],
        ]);
        const randomSpy = jest.spyOn(Math, "random").mockReturnValue(0);

        try {
            consumer = await WsConsumer.newConsumer(tmqConf);
            await consumer.subscribe([topicName]);

            let rows = 0;
            for (let i = 0; i < 8 && rows === 0; i++) {
                const res = await consumer.poll(800);
                for (const [, value] of res) {
                    const data = value.getData();
                    rows += data?.length || 0;
                }
            }

            expect(restartTriggered).toBe(true);
            expect(proxyAHadActivity).toBe(true);
            expect(rows).toBeGreaterThan(0);
        } finally {
            if (consumer) {
                try {
                    await consumer.unsubscribe();
                } catch (_err) {
                    // ignore cleanup error
                }
                await consumer.close();
            }
            await Promise.all([
                proxyA.stop("test cleanup"),
                proxyB.stop("test cleanup"),
            ]);
            randomSpy.mockRestore();

            cleanupSql = await WsSql.open(new WSConfig(localDsn));
            try {
                await cleanupSql.exec(`drop topic if exists ${topicName}`);
                await cleanupSql.exec(`drop database if exists ${dbName}`);
            } finally {
                await cleanupSql.close();
                cleanupSql = null;
            }
        }
    }, 180 * 1000);

    test("tmq single-address reconnect resumes poll and consumes all 5000 rows", async () => {
        const targetRows = 5000;
        const batchSize = 1000;
        const baseTs = 1700020000000;
        const dbName = "test_1774186545";
        const tableName = "t0";
        const topicName = "topic_1774186545";
        const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
        let setupSql: WsSql | null = null;
        let cleanupSql: WsSql | null = null;
        let consumer: WsConsumer | null = null;
        let pollRequestCount = 0;
        let restartInFlight = false;
        let restartCount = 0;

        setupSql = await WsSql.open(new WSConfig(localDsn));
        try {
            await setupSql.exec(`drop topic if exists ${topicName}`);
            await setupSql.exec(`drop database if exists ${dbName}`);
            await setupSql.exec(`create database ${dbName}`);
            await setupSql.exec(`create table ${dbName}.${tableName}(ts timestamp, c1 int)`);

            for (let start = 0; start < targetRows; start += batchSize) {
                const end = Math.min(start + batchSize, targetRows);
                const values: string[] = [];
                for (let i = start; i < end; i++) {
                    values.push(`(${baseTs + i}, ${i})`);
                }
                await setupSql.exec(
                    `insert into ${dbName}.${tableName} values ${values.join(" ")}`
                );
            }
            await setupSql.exec(
                `create topic ${topicName} as select * from ${dbName}.${tableName}`
            );
        } finally {
            await setupSql.close();
            setupSql = null;
        }

        const proxy = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event, control) => {
                if (event.type !== "message") {
                    return;
                }
                if (event.direction !== "client_to_upstream") {
                    return;
                }
                const action = parseJsonAction(event.rawData);
                if (action !== "poll") {
                    return;
                }

                pollRequestCount += 1;
                if (restartInFlight) {
                    return;
                }

                const shouldRestart = Math.random() < 0.18 ||
                    (restartCount === 0 && pollRequestCount >= 1);
                if (!shouldRestart) {
                    return;
                }

                restartInFlight = true;
                restartCount += 1;
                const downtimeMs = 20 + Math.floor(Math.random() * 90);
                void control.restart({
                    downtimeMs,
                    reason: `random poll restart #${restartCount}`,
                }).finally(() => {
                    restartInFlight = false;
                });
            },
        });

        const tmqConf = new Map<string, any>([
            [TMQConstants.GROUP_ID, `g_${Date.now()}`],
            [TMQConstants.CLIENT_ID, `c_${Date.now()}`],
            [TMQConstants.CONNECT_USER, testUsername()],
            [TMQConstants.CONNECT_PASS, testPassword()],
            [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
            [TMQConstants.ENABLE_AUTO_COMMIT, false],
            [TMQConstants.AUTO_COMMIT_INTERVAL_MS, 1000],
            [TMQConstants.WS_URL,
            `ws://${testUsername()}:${testPassword()}` +
            `@127.0.0.1:${proxy.getPort()}` +
            `?retries=60&retry_backoff_ms=10&retry_backoff_max_ms=40`
            ],
        ]);

        try {
            consumer = await WsConsumer.newConsumer(tmqConf);
            await consumer.subscribe([topicName]);

            let consumedRows = 0;
            const deadline = Date.now() + 120 * 1000;
            while (Date.now() < deadline && consumedRows < targetRows) {
                const res = await consumer.poll(1200);
                for (const [, value] of res) {
                    const data = value.getData();
                    consumedRows += data?.length || 0;
                }
            }

            expect(pollRequestCount).toBeGreaterThan(0);
            expect(restartCount).toBeGreaterThan(0);
            expect(consumedRows).toBe(targetRows);
        } finally {
            if (consumer) {
                try {
                    await consumer.unsubscribe();
                } catch (_err) {
                    // ignore cleanup error
                }
                await consumer.close();
            }
            await proxy.stop("test cleanup");

            cleanupSql = await WsSql.open(new WSConfig(localDsn));
            try {
                await cleanupSql.exec(`drop topic if exists ${topicName}`);
                await cleanupSql.exec(`drop database if exists ${dbName}`);
            } finally {
                await cleanupSql.close();
                cleanupSql = null;
            }
        }
    }, 300 * 1000);

    test("tmq three-address failover resumes poll and consumes all 5000 rows", async () => {
        const targetRows = 5000;
        const batchSize = 1000;
        const baseTs = 1700021000000;
        const dbName = "test_1774187557";
        const tableName = "t0";
        const topicName = "topic_1774187557";
        const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
        let setupSql: WsSql | null = null;
        let cleanupSql: WsSql | null = null;
        let consumer: WsConsumer | null = null;
        let totalPollRequestCount = 0;
        let totalRestartCount = 0;
        const proxyStates = new Map<string, { pollRequests: number; restarts: number; restartInFlight: boolean }>();

        setupSql = await WsSql.open(new WSConfig(localDsn));
        try {
            await setupSql.exec(`drop topic if exists ${topicName}`);
            await setupSql.exec(`drop database if exists ${dbName}`);
            await setupSql.exec(`create database ${dbName}`);
            await setupSql.exec(`create table ${dbName}.${tableName}(ts timestamp, c1 int)`);

            for (let start = 0; start < targetRows; start += batchSize) {
                const end = Math.min(start + batchSize, targetRows);
                const values: string[] = [];
                for (let i = start; i < end; i++) {
                    values.push(`(${baseTs + i}, ${i})`);
                }
                await setupSql.exec(
                    `insert into ${dbName}.${tableName} values ${values.join(" ")}`
                );
            }
            await setupSql.exec(
                `create topic ${topicName} as select * from ${dbName}.${tableName}`
            );
        } finally {
            await setupSql.close();
            setupSql = null;
        }

        const createPollRestartProxy = async (name: string) => {
            proxyStates.set(name, {
                pollRequests: 0,
                restarts: 0,
                restartInFlight: false,
            });
            return WsProxy.create({
                host: "127.0.0.1",
                port: 0,
                onEvent: (event, control) => {
                    if (event.type !== "message") {
                        return;
                    }
                    if (event.direction !== "client_to_upstream") {
                        return;
                    }
                    const action = parseJsonAction(event.rawData);
                    if (action !== "poll") {
                        return;
                    }

                    const state = proxyStates.get(name);
                    if (!state) {
                        return;
                    }

                    state.pollRequests += 1;
                    totalPollRequestCount += 1;
                    if (state.restartInFlight) {
                        return;
                    }

                    const shouldRestart = Math.random() < 0.18 ||
                        (totalRestartCount === 0 && totalPollRequestCount >= 1);
                    if (!shouldRestart) {
                        return;
                    }

                    state.restartInFlight = true;
                    state.restarts += 1;
                    totalRestartCount += 1;
                    const downtimeMs = 20 + Math.floor(Math.random() * 90);
                    void control.restart({
                        downtimeMs,
                        reason: `${name} random poll restart #${state.restarts}`,
                    }).finally(() => {
                        const latest = proxyStates.get(name);
                        if (latest) {
                            latest.restartInFlight = false;
                        }
                    });
                },
            });
        };

        const proxyA = await createPollRestartProxy("proxy_a");
        const proxyB = await createPollRestartProxy("proxy_b");
        const proxyC = await createPollRestartProxy("proxy_c");

        const tmqConf = new Map<string, any>([
            [TMQConstants.GROUP_ID, `g_${Date.now()}`],
            [TMQConstants.CLIENT_ID, `c_${Date.now()}`],
            [TMQConstants.CONNECT_USER, testUsername()],
            [TMQConstants.CONNECT_PASS, testPassword()],
            [TMQConstants.AUTO_OFFSET_RESET, "earliest"],
            [TMQConstants.ENABLE_AUTO_COMMIT, false],
            [TMQConstants.AUTO_COMMIT_INTERVAL_MS, 1000],
            [TMQConstants.WS_URL,
            `ws://${testUsername()}:${testPassword()}` +
            `@127.0.0.1:${proxyA.getPort()},127.0.0.1:${proxyB.getPort()},127.0.0.1:${proxyC.getPort()}` +
            `?retries=60&retry_backoff_ms=10&retry_backoff_max_ms=40`
            ],
        ]);

        try {
            consumer = await WsConsumer.newConsumer(tmqConf);
            await consumer.subscribe([topicName]);

            let consumedRows = 0;
            const deadline = Date.now() + 120 * 1000;
            while (Date.now() < deadline && consumedRows < targetRows) {
                const res = await consumer.poll(1200);
                for (const [, value] of res) {
                    const data = value.getData();
                    consumedRows += data?.length || 0;
                }
            }

            expect(totalPollRequestCount).toBeGreaterThan(0);
            expect(totalRestartCount).toBeGreaterThan(0);
            expect(consumedRows).toBe(targetRows);
        } finally {
            if (consumer) {
                try {
                    await consumer.unsubscribe();
                } catch (_err) {
                    // ignore cleanup error
                }
                await consumer.close();
            }
            await Promise.all([
                proxyA.stop("test cleanup"),
                proxyB.stop("test cleanup"),
                proxyC.stop("test cleanup"),
            ]);

            cleanupSql = await WsSql.open(new WSConfig(localDsn));
            try {
                await cleanupSql.exec(`drop topic if exists ${topicName}`);
                await cleanupSql.exec(`drop database if exists ${dbName}`);
            } finally {
                await cleanupSql.close();
                cleanupSql = null;
            }
        }
    }, 300 * 1000);
});

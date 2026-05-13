import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { testPassword, testUsername } from "@test-helpers/utils";
import { WsProxy, WsProxyEvent } from "@test-helpers/wsProxy";

function parseBinaryAction(rawData: Buffer | string): bigint | null {
    if (typeof rawData === "string" || rawData.byteLength < 24) {
        return null;
    }
    return rawData.readBigInt64LE(16);
}

describe("sql failover", () => {
    jest.setTimeout(120 * 1000);

    afterEach(async () => {
        WebSocketConnectionPool.instance().destroyed();
        jest.restoreAllMocks();
    });

    test("switches to next address when active proxy restarts with downtime", async () => {
        let restartTriggered = false;
        let proxyBHadActivity = false;
        let wsSql: WsSql | null = null;

        const proxyA = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event, control) => {
                if (event.type !== "message") {
                    return;
                }
                if (event.direction !== "client_to_upstream" || !event.isBinary) {
                    return;
                }

                const action = parseBinaryAction(event.rawData);
                if (action === 6n && !restartTriggered) {
                    restartTriggered = true;
                    void control.restart({
                        downtimeMs: 800,
                        reason: "trigger dual-address failover",
                    });
                }
            },
        });

        const proxyB = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event: WsProxyEvent) => {
                if (event.type === "client_connected") {
                    proxyBHadActivity = true;
                    return;
                }
                if (
                    event.type === "message" &&
                    event.direction === "client_to_upstream"
                ) {
                    proxyBHadActivity = true;
                }
            },
        });

        const randomSpy = jest.spyOn(Math, "random").mockReturnValue(0);
        try {
            const dsn =
                `ws://${testUsername()}:${testPassword()}` +
                `@127.0.0.1:${proxyA.getPort()},127.0.0.1:${proxyB.getPort()}` +
                `?retries=5&retry_backoff_ms=20&retry_backoff_max_ms=20`;
            const conf = new WSConfig(dsn);
            conf.setTimeOut(6000);
            wsSql = await WsSql.open(conf);

            const result = await wsSql.exec("select server_version()");
            expect(result).toBeTruthy();
            expect(restartTriggered).toBe(true);
            expect(proxyBHadActivity).toBe(true);
        } finally {
            randomSpy.mockRestore();
            if (wsSql) {
                await wsSql.close();
            }
            await proxyA.stop("test cleanup");
            await proxyB.stop("test cleanup");
        }
    });

    test("reconnects to same address after single proxy hard restart", async () => {
        let restartCount = 0;
        let wsSql: WsSql | null = null;

        const proxy = await WsProxy.create({
            host: "127.0.0.1",
            port: 0,
            onEvent: (event, control) => {
                if (event.type !== "message") {
                    return;
                }
                if (event.direction !== "client_to_upstream" || !event.isBinary) {
                    return;
                }

                const action = parseBinaryAction(event.rawData);
                if (action === 6n && restartCount === 0) {
                    restartCount += 1;
                    void control.restart({
                        downtimeMs: 120,
                        reason: "trigger single-address reconnect",
                    });
                }
            },
        });

        try {
            const dsn =
                `ws://${testUsername()}:${testPassword()}@127.0.0.1:${proxy.getPort()}` +
                `?retries=6&retry_backoff_ms=30&retry_backoff_max_ms=60`;
            const conf = new WSConfig(dsn);
            conf.setTimeOut(6000);
            wsSql = await WsSql.open(conf);

            const result = await wsSql.exec("select server_version()");
            expect(result).toBeTruthy();
            expect(restartCount).toBe(1);
        } finally {
            if (wsSql) {
                await wsSql.close();
            }
            await proxy.stop("test cleanup");
        }
    });

    test(
        "keeps all 5000 rows with single-address random proxy restarts during inserts",
        async () => {
            const targetRows = 5000;
            const baseTs = 1700000000000;
            const dbName = "test_1773988174";
            const tableName = "t0";
            let wsSql: WsSql | null = null;
            let setupSql: WsSql | null = null;
            let cleanupSql: WsSql | null = null;
            let writePhase = false;
            let restartInFlight = false;
            let restartCount = 0;
            let forwardedInsertFrames = 0;

            const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
            const setupConf = new WSConfig(localDsn);
            setupConf.setTimeOut(6000);
            setupSql = await WsSql.open(setupConf);
            try {
                await setupSql.exec(`drop database if exists ${dbName}`);
                await setupSql.exec(`create database if not exists ${dbName}`);
            } finally {
                await setupSql.close();
                setupSql = null;
            }

            const proxy = await WsProxy.create({
                host: "127.0.0.1",
                port: 0,
                onEvent: (event, control) => {
                    if (!writePhase || restartInFlight) {
                        return;
                    }
                    if (event.type !== "message") {
                        return;
                    }
                    if (event.direction !== "client_to_upstream" || !event.isBinary) {
                        return;
                    }

                    const action = parseBinaryAction(event.rawData);
                    if (action !== 6n) {
                        return;
                    }

                    forwardedInsertFrames += 1;
                    if (Math.random() < 0.003 || restartCount === 0) {
                        restartInFlight = true;
                        restartCount += 1;
                        const downtimeMs = 10 + Math.floor(Math.random() * 60);
                        void control.restart({
                            downtimeMs,
                            reason: `random restart #${restartCount}`,
                        }).finally(() => {
                            restartInFlight = false;
                        });
                    }
                },
            });

            try {
                const dsn =
                    `ws://${testUsername()}:${testPassword()}@127.0.0.1:${proxy.getPort()}` +
                    `?retries=30&retry_backoff_ms=5&retry_backoff_max_ms=20`;
                const conf = new WSConfig(dsn);
                conf.setDb(dbName);
                conf.setTimeOut(10000);
                wsSql = await WsSql.open(conf);
                await wsSql.exec(`create table ${tableName}(ts timestamp, c1 int)`);

                writePhase = true;
                for (let i = 0; i < targetRows; i++) {
                    await wsSql.exec(
                        `insert into ${tableName} values(${baseTs + i}, ${i})`
                    );
                }
                writePhase = false;

                const countResult = await wsSql.exec(
                    `select count(*) from ${tableName}`
                );
                const countValue = countResult.getData()?.[0]?.[0];
                const rowCount = typeof countValue === "bigint"
                    ? Number(countValue)
                    : Number(countValue || 0);

                expect(rowCount).toBe(targetRows);
                expect(forwardedInsertFrames).toBeGreaterThanOrEqual(targetRows);
                expect(restartCount).toBeGreaterThan(0);
            } finally {
                writePhase = false;
                if (wsSql) {
                    await wsSql.close();
                }
                await proxy.stop("test cleanup");

                const cleanupConf = new WSConfig(localDsn);
                cleanupConf.setTimeOut(6000);
                cleanupSql = await WsSql.open(cleanupConf);
                try {
                    await cleanupSql.exec(`drop database if exists ${dbName}`);
                } finally {
                    await cleanupSql.close();
                    cleanupSql = null;
                }
            }
        },
        300 * 1000
    );

    test(
        "keeps all 5000 rows with three-address random proxy restarts during inserts",
        async () => {
            const targetRows = 5000;
            const baseTs = 1700010000000;
            const dbName = "test_1773989170";
            const tableName = "t0";
            const proxyStates = new Map<string, { forwarded: number; restarting: boolean; restarts: number }>();
            let wsSql: WsSql | null = null;
            let setupSql: WsSql | null = null;
            let cleanupSql: WsSql | null = null;
            let writePhase = false;
            let forcedRestartTriggered = false;

            const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
            const setupConf = new WSConfig(localDsn);
            setupConf.setTimeOut(6000);
            setupSql = await WsSql.open(setupConf);
            try {
                await setupSql.exec(`drop database if exists ${dbName}`);
                await setupSql.exec(`create database if not exists ${dbName}`);
            } finally {
                await setupSql.close();
                setupSql = null;
            }

            const createRandomRestartProxy = async (name: string) => {
                proxyStates.set(name, {
                    forwarded: 0,
                    restarting: false,
                    restarts: 0,
                });
                return WsProxy.create({
                    host: "127.0.0.1",
                    port: 0,
                    onEvent: (event, control) => {
                        if (!writePhase) {
                            return;
                        }
                        if (event.type !== "message") {
                            return;
                        }
                        if (event.direction !== "client_to_upstream" || !event.isBinary) {
                            return;
                        }

                        const action = parseBinaryAction(event.rawData);
                        if (action !== 6n) {
                            return;
                        }

                        const state = proxyStates.get(name);
                        if (!state) {
                            return;
                        }
                        state.forwarded += 1;
                        if (state.restarting) {
                            return;
                        }
                        const shouldRestart = !forcedRestartTriggered || Math.random() < 0.003;
                        if (!shouldRestart) {
                            return;
                        }

                        forcedRestartTriggered = true;
                        state.restarting = true;
                        state.restarts += 1;
                        const downtimeMs = 80 + Math.floor(Math.random() * 120);
                        void control
                            .restart({
                                downtimeMs,
                                reason: `${name} random restart #${state.restarts}`,
                            })
                            .finally(() => {
                                const latestState = proxyStates.get(name);
                                if (latestState) {
                                    latestState.restarting = false;
                                }
                            });
                    },
                });
            };

            const proxyA = await createRandomRestartProxy("proxy_a");
            const proxyB = await createRandomRestartProxy("proxy_b");
            const proxyC = await createRandomRestartProxy("proxy_c");

            try {
                const dsn =
                    `ws://${testUsername()}:${testPassword()}` +
                    `@127.0.0.1:${proxyA.getPort()},127.0.0.1:${proxyB.getPort()},127.0.0.1:${proxyC.getPort()}` +
                    `?retries=24&retry_backoff_ms=8&retry_backoff_max_ms=25`;
                const conf = new WSConfig(dsn);
                conf.setDb(dbName);
                conf.setTimeOut(10000);
                wsSql = await WsSql.open(conf);
                await wsSql.exec(`create table ${tableName}(ts timestamp, c1 int)`);

                writePhase = true;
                for (let i = 0; i < targetRows; i++) {
                    await wsSql.exec(
                        `insert into ${tableName} values(${baseTs + i}, ${i})`
                    );
                }
                writePhase = false;

                const countResult = await wsSql.exec(
                    `select count(*) from ${tableName}`
                );
                const countValue = countResult.getData()?.[0]?.[0];
                const rowCount = typeof countValue === "bigint"
                    ? Number(countValue)
                    : Number(countValue || 0);

                const totalForwarded = Array.from(proxyStates.values()).reduce(
                    (sum, state) => sum + state.forwarded,
                    0
                );
                const totalRestarts = Array.from(proxyStates.values()).reduce(
                    (sum, state) => sum + state.restarts,
                    0
                );

                expect(rowCount).toBe(targetRows);
                expect(totalForwarded).toBeGreaterThanOrEqual(targetRows);
                expect(totalRestarts).toBeGreaterThan(0);
            } finally {
                writePhase = false;
                if (wsSql) {
                    await wsSql.close();
                }

                await Promise.all([
                    proxyA.stop("test cleanup"),
                    proxyB.stop("test cleanup"),
                    proxyC.stop("test cleanup"),
                ]);

                const cleanupConf = new WSConfig(localDsn);
                cleanupConf.setTimeOut(6000);
                cleanupSql = await WsSql.open(cleanupConf);
                try {
                    await cleanupSql.exec(`drop database if exists ${dbName}`);
                } finally {
                    await cleanupSql.close();
                    cleanupSql = null;
                }
            }
        },
        300 * 1000
    );
});

import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { testPassword, testUsername } from "@test-helpers/utils";
import { WsProxy, WsProxyEvent, WsProxyMessageEvent } from "@test-helpers/wsProxy";

interface StageCase {
    name: string;
    shouldRestart: (event: WsProxyEvent) => boolean;
}

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

function parseBinaryAction(rawData: Buffer | string): bigint | null {
    if (typeof rawData === "string" || rawData.byteLength < 24) {
        return null;
    }
    return rawData.readBigInt64LE(16);
}

function asClientToUpstreamMessage(event: WsProxyEvent): WsProxyMessageEvent | null {
    if (event.type !== "message") {
        return null;
    }
    if (event.direction !== "client_to_upstream") {
        return null;
    }
    return event;
}

const stageCases: StageCase[] = [
    {
        name: "init",
        shouldRestart: (event) => {
            const messageEvent = asClientToUpstreamMessage(event);
            return !!messageEvent &&
                parseJsonAction(messageEvent.rawData) === "stmt2_init";
        },
    },
    {
        name: "prepare",
        shouldRestart: (event) => {
            const messageEvent = asClientToUpstreamMessage(event);
            return !!messageEvent &&
                parseJsonAction(messageEvent.rawData) === "stmt2_prepare";
        },
    },
    {
        name: "bind",
        shouldRestart: (event) => {
            const messageEvent = asClientToUpstreamMessage(event);
            return !!messageEvent &&
                messageEvent.isBinary &&
                parseBinaryAction(messageEvent.rawData) === 9n;
        },
    },
    {
        name: "exec",
        shouldRestart: (event) => {
            const messageEvent = asClientToUpstreamMessage(event);
            return !!messageEvent &&
                parseJsonAction(messageEvent.rawData) === "stmt2_exec";
        },
    },
    {
        name: "result",
        shouldRestart: (event) => {
            const messageEvent = asClientToUpstreamMessage(event);
            return !!messageEvent &&
                parseJsonAction(messageEvent.rawData) === "stmt2_result";
        },
    },
];

describe("stmt2 failover with ws proxy", () => {
    jest.setTimeout(180 * 1000);

    afterEach(async () => {
        WebSocketConnectionPool.instance().destroyed();
        jest.restoreAllMocks();
    });

    test.each(stageCases)(
        "single-address reconnect recovers stmt2 when network error happens at $name stage",
        async ({ name, shouldRestart }) => {
            const dbName = `test_1774332664_${name}`;
            const tableName = "t0";
            const baseTs = 1700100000000;
            const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
            let setupSql: WsSql | null = null;
            let cleanupSql: WsSql | null = null;
            let wsSql: WsSql | null = null;
            let stmt: any = null;
            let wsRows: any = null;
            let restartTriggered = false;
            let clientConnectedCount = 0;
            let matchedStageMessageCount = 0;
            let rowCount = 0;

            setupSql = await WsSql.open(new WSConfig(localDsn));
            try {
                await setupSql.exec(`drop database if exists ${dbName}`);
                await setupSql.exec(`create database ${dbName}`);
                await setupSql.exec(`create table ${dbName}.${tableName}(ts timestamp, c1 int)`);
                await setupSql.exec(
                    `insert into ${dbName}.${tableName} values` +
                    ` (${baseTs}, 1) (${baseTs + 1}, 2) (${baseTs + 2}, 3)`
                );
            } finally {
                await setupSql.close();
                setupSql = null;
            }

            const proxy = await WsProxy.create({
                host: "127.0.0.1",
                port: 0,
                onEvent: (event, control) => {
                    if (event.type === "client_connected") {
                        clientConnectedCount += 1;
                    }
                    if (restartTriggered) {
                        return;
                    }
                    if (!shouldRestart(event)) {
                        return;
                    }
                    matchedStageMessageCount += 1;
                    restartTriggered = true;
                    void control.restart({
                        downtimeMs: 200,
                        reason: `trigger stmt2 ${name} failover`,
                    });
                },
            });

            try {
                const dsn =
                    `ws://${testUsername()}:${testPassword()}@127.0.0.1:${proxy.getPort()}` +
                    `?retries=20&retry_backoff_ms=15&retry_backoff_max_ms=80`;
                const conf = new WSConfig(dsn);
                conf.setDb(dbName);
                wsSql = await WsSql.open(conf);

                stmt = await wsSql.stmtInit();
                await stmt.prepare(
                    `select ts, c1 from ${tableName} where ts >= ? and ts <= ? order by ts`
                );
                const params = stmt.newStmtParam();
                params.setTimestamp([BigInt(baseTs)]);
                params.setTimestamp([BigInt(baseTs + 2)]);
                await stmt.bind(params);
                await stmt.exec();
                wsRows = await stmt.resultSet();

                while (await wsRows.next()) {
                    const data = wsRows.getData();
                    expect(data).toBeTruthy();
                    rowCount += 1;
                }

                expect(matchedStageMessageCount).toBe(1);
                expect(restartTriggered).toBe(true);
                expect(clientConnectedCount).toBeGreaterThanOrEqual(2);
                expect(rowCount).toBe(3);
            } finally {
                if (wsRows) {
                    try {
                        await wsRows.close();
                    } catch (_err) {
                        // ignore cleanup error
                    }
                    wsRows = null;
                }
                if (stmt) {
                    try {
                        await stmt.close();
                    } catch (_err) {
                        // ignore cleanup error
                    }
                    stmt = null;
                }
                if (wsSql) {
                    await wsSql.close();
                    wsSql = null;
                }
                await proxy.stop("test cleanup");

                cleanupSql = await WsSql.open(new WSConfig(localDsn));
                try {
                    await cleanupSql.exec(`drop database if exists ${dbName}`);
                } finally {
                    await cleanupSql.close();
                    cleanupSql = null;
                }
            }
        }
    );

    test(
        "single-address random proxy restarts on stmt2 steps still keeps all 5000 inserted rows",
        async () => {
            const targetRows = 5000;
            const batchSize = 50;
            const baseTs = 1700110000000;
            const dbName = "test_1774338668";
            const stableName = "meters";
            const subTableName = "d0";
            const fullSubTableName = `${dbName}.${subTableName}`;
            const localDsn = `ws://${testUsername()}:${testPassword()}@127.0.0.1:6041`;
            let setupSql: WsSql | null = null;
            let cleanupSql: WsSql | null = null;
            let wsSql: WsSql | null = null;
            let insertStmt: any = null;
            let countStmt: any = null;
            let countRows: any = null;
            let writePhase = false;
            let verifyPhase = false;
            let restartInFlight = false;
            let restartCount = 0;
            let rowCount = 0;
            const stepSeenCount: Record<"init" | "prepare" | "bind" | "exec" | "result", number> = {
                init: 0,
                prepare: 0,
                bind: 0,
                exec: 0,
                result: 0,
            };

            setupSql = await WsSql.open(new WSConfig(localDsn));
            try {
                await setupSql.exec(`drop database if exists ${dbName}`);
                await setupSql.exec(`create database ${dbName}`);
                await setupSql.exec(
                    `create stable ${dbName}.${stableName}(ts timestamp, c1 int)` +
                    ` tags(location binary(64), gid int)`
                );
            } finally {
                await setupSql.close();
                setupSql = null;
            }

            const proxy = await WsProxy.create({
                host: "127.0.0.1",
                port: 0,
                onEvent: (event, control) => {
                    const messageEvent = asClientToUpstreamMessage(event);
                    if (!messageEvent) {
                        return;
                    }

                    let step: keyof typeof stepSeenCount | null = null;
                    if (messageEvent.isBinary) {
                        const action = parseBinaryAction(messageEvent.rawData);
                        if (action === 9n) {
                            step = "bind";
                        }
                    } else {
                        const action = parseJsonAction(messageEvent.rawData);
                        if (action === "stmt2_init") {
                            step = "init";
                        } else if (action === "stmt2_prepare") {
                            step = "prepare";
                        } else if (action === "stmt2_exec") {
                            step = "exec";
                        } else if (action === "stmt2_result") {
                            step = "result";
                        }
                    }

                    if (!step) {
                        return;
                    }

                    stepSeenCount[step] += 1;

                    if (!writePhase && !verifyPhase) {
                        return;
                    }
                    if (restartInFlight) {
                        return;
                    }

                    const shouldRestart = Math.random() < 0.05 ||
                        (restartCount === 0 && step === "bind");
                    if (!shouldRestart) {
                        return;
                    }

                    restartInFlight = true;
                    restartCount += 1;
                    const downtimeMs = 20 + Math.floor(Math.random() * 100);
                    void control.restart({
                        downtimeMs,
                        reason: `stmt2 random step restart #${restartCount} (${step})`,
                    }).finally(() => {
                        restartInFlight = false;
                    });
                },
            });

            try {
                const dsn =
                    `ws://${testUsername()}:${testPassword()}@127.0.0.1:${proxy.getPort()}` +
                    `?retries=80&retry_backoff_ms=10&retry_backoff_max_ms=50`;
                const conf = new WSConfig(dsn);
                conf.setDb(dbName);
                wsSql = await WsSql.open(conf);

                insertStmt = await wsSql.stmtInit();
                const insertSql =
                    `insert into ? using ${stableName} (location, gid) tags (?, ?) values(?, ?)`;
                await insertStmt.prepare(insertSql);

                writePhase = true;
                for (let start = 0; start < targetRows; start += batchSize) {
                    const end = Math.min(start + batchSize, targetRows);
                    const tsBatch: bigint[] = [];
                    const c1Batch: number[] = [];
                    for (let i = start; i < end; i++) {
                        tsBatch.push(BigInt(baseTs + i));
                        c1Batch.push(i);
                    }

                    await insertStmt.setTableName(fullSubTableName);
                    const tagParams = insertStmt.newStmtParam();
                    tagParams.setVarchar(["beijing"]);
                    tagParams.setInt([1]);
                    await insertStmt.setTags(tagParams);

                    const params = insertStmt.newStmtParam();
                    params.setTimestamp(tsBatch);
                    params.setInt(c1Batch);
                    await insertStmt.bind(params);
                    await insertStmt.exec();
                }
                writePhase = false;

                verifyPhase = true;
                countStmt = await wsSql.stmtInit();
                await countStmt.prepare(
                    `select ts, c1 from ${subTableName} where ts >= ? and ts <= ? order by ts limit 1`
                );
                const countParams = countStmt.newStmtParam();
                countParams.setTimestamp([BigInt(baseTs)]);
                countParams.setTimestamp([BigInt(baseTs + targetRows - 1)]);
                await countStmt.bind(countParams);
                await countStmt.exec();
                countRows = await countStmt.resultSet();
                if (await countRows.next()) {
                    expect(countRows.getData()).toBeTruthy();
                }
                verifyPhase = false;

                const countResult = await wsSql.exec(
                    `select count(*) from ${subTableName}`
                );
                const countValue = countResult.getData()?.[0]?.[0];
                rowCount = typeof countValue === "bigint"
                    ? Number(countValue)
                    : Number(countValue || 0);

                expect(rowCount).toBe(targetRows);
                expect(restartCount).toBeGreaterThan(0);
                expect(stepSeenCount.init).toBeGreaterThan(0);
                expect(stepSeenCount.prepare).toBeGreaterThan(0);
                expect(stepSeenCount.bind).toBeGreaterThan(0);
                expect(stepSeenCount.exec).toBeGreaterThan(0);
                expect(stepSeenCount.result).toBeGreaterThan(0);
            } finally {
                writePhase = false;
                verifyPhase = false;
                if (countRows) {
                    try {
                        await countRows.close();
                    } catch (_err) {
                        // ignore cleanup error
                    }
                    countRows = null;
                }
                if (countStmt) {
                    try {
                        await countStmt.close();
                    } catch (_err) {
                        // ignore cleanup error
                    }
                    countStmt = null;
                }
                if (insertStmt) {
                    try {
                        await insertStmt.close();
                    } catch (_err) {
                        // ignore cleanup error
                    }
                    insertStmt = null;
                }
                if (wsSql) {
                    await wsSql.close();
                    wsSql = null;
                }
                await proxy.stop("test cleanup");

                cleanupSql = await WsSql.open(new WSConfig(localDsn));
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

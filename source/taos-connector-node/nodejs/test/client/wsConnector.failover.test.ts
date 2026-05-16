import { RetryConfig, WebSocketConnector } from "@src/client/wsConnector";
import { WsEventCallback } from "@src/client/wsEventCallback";
import { AddressConnectionTracker } from "@src/common/addressConnectionTracker";
import { parse } from "@src/common/dsn";

function createInflightStore(): any {
    let nextMsgId = 1n;
    const reqIdToMsgId = new Map<bigint, bigint>();
    const msgIdToRequest = new Map<bigint, any>();

    return {
        insert(req: any) {
            const msgId = nextMsgId;
            nextMsgId += 1n;
            reqIdToMsgId.set(req.reqId, msgId);
            msgIdToRequest.set(msgId, req);
        },
        remove(reqId: bigint) {
            const msgId = reqIdToMsgId.get(reqId);
            if (msgId === undefined) {
                return;
            }
            reqIdToMsgId.delete(reqId);
            msgIdToRequest.delete(msgId);
        },
        getRequests() {
            return Array.from(msgIdToRequest.entries())
                .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                .map(([, req]) => req);
        },
        clear() {
            nextMsgId = 1n;
            reqIdToMsgId.clear();
            msgIdToRequest.clear();
        },
    };
}

function createBareConnector(
    dsn: string = "ws://root:taosdata@host1:6041,host2:6042"
): any {
    const connector = Object.create(WebSocketConnector.prototype) as any;
    connector._timeout = 5000;
    connector._dsn = parse(dsn);
    connector._currentAddress = connector._dsn.addresses[0];
    connector._retryConfig = new RetryConfig(1, 1, 8);
    connector._reconnectLock = null;
    connector._isReconnecting = false;
    connector._allowReconnect = true;
    connector._connectionReady = Promise.resolve();
    connector._suppressedSockets = new WeakSet();
    connector._sessionRecoveryHook = null;
    connector._inflightStore = createInflightStore();
    connector._conn = {
        readyState: 1,
        send: jest.fn(),
        close: jest.fn(),
    };
    return connector;
}

function buildBinaryMessage(action: bigint): ArrayBuffer {
    const buffer = new ArrayBuffer(26);
    const view = new DataView(buffer);
    view.setBigInt64(16, action, true);
    return buffer;
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function hasInflightRequest(connector: any, reqId: bigint): boolean {
    return connector
        ._inflightStore
        .getRequests()
        .some((req: any) => req.reqId === reqId);
}

function listInflightReqIds(connector: any): bigint[] {
    return connector
        ._inflightStore
        .getRequests()
        .map((req: any) => req.reqId as bigint);
}

function resetAddressTracker(): void {
    const tracker = AddressConnectionTracker.instance();
    (tracker as any)._counts.clear();
}

function resetCallbackRegistry(): void {
    const CallbackClass = WsEventCallback as any;
    CallbackClass._msgActionRegister = new Map();
}

describe("WebSocketConnector failover and retry", () => {
    beforeEach(() => {
        resetAddressTracker();
        resetCallbackRegistry();
    });

    afterEach(() => {
        jest.restoreAllMocks();
        resetAddressTracker();
        resetCallbackRegistry();
    });

    test("deduplicates concurrent reconnect triggers with reconnect lock", async () => {
        const connector = createBareConnector();
        const reconnectImpl = jest.fn(async () => {
            await delay(20);
        });
        connector._doReconnect = reconnectImpl;

        await Promise.all([
            connector.triggerReconnect(),
            connector.triggerReconnect(),
            connector.triggerReconnect(),
        ]);

        expect(reconnectImpl).toHaveBeenCalledTimes(1);
    });

    test("switches to least-connected address after retries are exhausted", async () => {
        const connector = createBareConnector();
        const leastSelector = jest
            .spyOn(AddressConnectionTracker.instance(), "selectLeastConnected")
            .mockImplementation(() => 0);
        const attempts: string[] = [];
        connector.sleep = jest.fn(async () => { });
        connector.reconnect = jest.fn(async () => {
            const current = connector._currentAddress;
            const addr = `${current.host}:${current.port}`;
            attempts.push(addr);
            if (addr === "host1:6041") {
                throw new Error("host1 down");
            }
        });

        await connector.attemptReconnect();

        expect(attempts).toEqual([
            "host1:6041",
            "host2:6042",
        ]);
        expect(leastSelector).toHaveBeenCalledWith([
            connector._dsn.addresses[1],
        ]);
        expect(`${connector._currentAddress.host}:${connector._currentAddress.port}`)
            .toBe("host2:6042");
    });

    test("attemptReconnect does not reselect failed addresses in one reconnect round", async () => {
        const connector = createBareConnector(
            "ws://root:taosdata@host1:6041,host2:6042,host3:6043"
        );
        const leastSelector = jest
            .spyOn(AddressConnectionTracker.instance(), "selectLeastConnected")
            .mockImplementation(() => 0);
        const attempts: string[] = [];
        connector.sleep = jest.fn(async () => { });
        connector.reconnect = jest.fn(async () => {
            const current = connector._currentAddress;
            attempts.push(`${current.host}:${current.port}`);
            throw new Error("all down");
        });

        await expect(connector.attemptReconnect()).rejects.toThrow(
            "Failed to reconnect to any available address"
        );

        expect(attempts).toEqual([
            "host1:6041",
            "host2:6042",
            "host3:6043",
        ]);
        expect(leastSelector).toHaveBeenNthCalledWith(1, [
            connector._dsn.addresses[1],
            connector._dsn.addresses[2],
        ]);
        expect(leastSelector).toHaveBeenNthCalledWith(2, [
            connector._dsn.addresses[2],
        ]);
    });

    test("attemptReconnect keeps retrying same address for single-address dsn", async () => {
        const connector = createBareConnector("ws://root:taosdata@host1:6041");
        connector._retryConfig = new RetryConfig(3, 1, 8);
        connector.sleep = jest.fn(async () => { });
        const attempts: string[] = [];
        connector.reconnect = jest.fn(async () => {
            const current = connector._currentAddress;
            attempts.push(`${current.host}:${current.port}`);
            throw new Error("host1 down");
        });

        await expect(connector.attemptReconnect()).rejects.toThrow(
            "Failed to reconnect to any available address"
        );

        expect(attempts).toEqual([
            "host1:6041",
            "host1:6041",
            "host1:6041",
        ]);
    });

    test("attemptReconnect throws after all addresses and retries are exhausted", async () => {
        const connector = createBareConnector();
        jest
            .spyOn(AddressConnectionTracker.instance(), "selectLeastConnected")
            .mockImplementation(() => 0);
        connector.sleep = jest.fn(async () => { });
        connector.reconnect = jest.fn(async () => {
            throw new Error("all down");
        });

        await expect(connector.attemptReconnect()).rejects.toThrow(
            "Failed to reconnect to any available address"
        );
        expect(connector.reconnect).toHaveBeenCalledTimes(2);
        expect(`${connector._currentAddress.host}:${connector._currentAddress.port}`)
            .toBe("host2:6042");
    });

    test("handleDisconnect skips reconnect when reconnect is disabled", async () => {
        const connector = createBareConnector();
        connector._allowReconnect = false;
        connector.triggerReconnect = jest.fn();

        await connector.handleDisconnect(connector._conn);
        expect(connector.triggerReconnect).not.toHaveBeenCalled();
    });

    test("handleDisconnect skips reconnect on normal close", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn();

        await connector.handleDisconnect(
            connector._conn,
            { code: 1000, reason: "normal close" }
        );
        expect(connector.triggerReconnect).not.toHaveBeenCalled();
    });

    test("handleDisconnect swallows reconnect failure", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(async () => {
            throw new Error("reconnect failed");
        });

        await expect(
            connector.handleDisconnect(
                connector._conn,
                { code: 1006, reason: "abnormal close" }
            )
        ).resolves.toBeUndefined();
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
    });

    test("handleDisconnect rejects non-retriable pending callbacks immediately", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => { });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "query",
                args: {
                    req_id: 1201,
                },
            })
        );
        void connector.handleDisconnect(
            connector._conn,
            { code: 1006, reason: "abnormal close" }
        );

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(40).then(() => "pending"),
        ]);

        expect(state).toBe("rejected");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 1201n)).toBe(false);
    });

    test("handleDisconnect keeps retriable pending callbacks for replay", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => { });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "insert",
                args: {
                    req_id: 1202,
                },
            })
        );
        void pending.catch(() => { });
        void connector.handleDisconnect(
            connector._conn,
            { code: 1006, reason: "abnormal close" }
        );

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(40).then(() => "pending"),
        ]);

        expect(state).toBe("pending");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 1202n)).toBe(true);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("_doReconnect fails all inflight requests when reconnect fails", async () => {
        const connector = createBareConnector();
        const rejectSpy = jest.fn();
        connector.attemptReconnect = jest.fn(async () => {
            throw new Error("reconnect failed");
        });
        connector._inflightStore.insert({
            reqId: 901n,
            action: "insert",
            message: JSON.stringify({
                action: "insert",
                args: { req_id: 901 },
            }),
            resolve: jest.fn(),
            reject: rejectSpy,
        });

        await expect(connector._doReconnect()).rejects.toThrow("reconnect failed");
        expect(rejectSpy).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 901n)).toBe(false);
    });

    test("close disables reconnect and closes current socket", () => {
        const connector = createBareConnector();
        const decrementSpy = jest
            .spyOn(AddressConnectionTracker.instance(), "decrement")
            .mockImplementation(() => { });
        connector.close();
        expect(decrementSpy).toHaveBeenCalledWith("host1:6041");
        expect(connector._allowReconnect).toBe(false);
        expect(connector._conn.close).toHaveBeenCalledTimes(1);
        expect(connector._suppressedSockets.has(connector._conn)).toBe(true);
    });

    test("attemptReconnect aborts silently when close is called mid-retry", async () => {
        const connector = createBareConnector("ws://root:taosdata@host1:6041");
        connector._retryConfig = new RetryConfig(3, 1, 8);
        connector.reconnect = jest.fn(async () => {
            throw new Error("host down");
        });
        connector.sleep = jest.fn(async () => {
            connector.close();
        });

        await expect(connector.attemptReconnect()).resolves.toBeUndefined();
        expect(connector.reconnect).toHaveBeenCalledTimes(1);
    });

    test("close rejects retriable inflight request immediately", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "insert",
                args: {
                    req_id: 401,
                },
            })
        );
        void pending.catch(() => { });

        await delay(20);
        connector.close();

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(40).then(() => "pending"),
        ]);

        expect(state).toBe("rejected");
        expect(hasInflightRequest(connector, 401n)).toBe(false);
    });

    test("close during callback registration does not leave callback entry until timeout", async () => {
        const connector = createBareConnector();
        connector._conn.send = jest.fn();

        const callback = WsEventCallback.instance();
        const originalRegister = callback.registerCallback.bind(callback);
        jest
            .spyOn(callback, "registerCallback")
            .mockImplementation(async (id: any, res: any, rej: any) => {
                await delay(20);
                await originalRegister(id, res, rej);
            });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "insert",
                args: {
                    req_id: 402,
                },
            })
        );
        void pending.catch(() => { });

        await delay(5);
        connector.close();
        await delay(30);

        expect((WsEventCallback as any)._msgActionRegister.size).toBe(0);
        expect(connector._conn.send).not.toHaveBeenCalled();
    });

    test("reconnect decrements current address before closing old socket", async () => {
        const connector = createBareConnector();
        const decrementSpy = jest
            .spyOn(AddressConnectionTracker.instance(), "decrement")
            .mockImplementation(() => { });
        connector.createConnection = jest.fn(() => {
            connector._connectionReady = Promise.resolve();
        });

        await connector.reconnect();

        expect(decrementSpy).toHaveBeenCalledWith("host1:6041");
        expect(connector._conn.close).toHaveBeenCalledTimes(1);
        expect(connector.createConnection).toHaveBeenCalledTimes(1);
    });

    test("triggers reconnect and keeps retriable string request inflight when send throws", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "insert",
                args: {
                    req_id: 101,
                },
            })
        );
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("pending");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 101n)).toBe(true);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("triggers reconnect and keeps retriable poll request inflight when send throws", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "poll",
                args: {
                    req_id: 109,
                    blocking_time: 500,
                    message_id: 0,
                },
            })
        );
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("pending");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 109n)).toBe(true);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("triggers reconnect and keeps retriable subscribe request inflight when send throws", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "subscribe",
                args: {
                    req_id: 110,
                    topics: ["topic_a"],
                },
            })
        );
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("pending");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 110n)).toBe(true);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("rejects non-retriable string request immediately when send throws", async () => {
        const connector = createBareConnector();
        connector._conn.send = jest.fn(() => {
            throw new Error("send failed");
        });

        await expect(
            connector.sendMsg(
                JSON.stringify({
                    action: "query",
                    args: {
                        req_id: 102,
                    },
                })
            )
        ).rejects.toThrow("send failed");
        expect(hasInflightRequest(connector, 102n)).toBe(false);
    });

    test("rejects retriable string request immediately when send throws with non-network error", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("serialize failed");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "insert",
                args: {
                    req_id: 111,
                },
            })
        );
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("rejected");
        expect(connector.triggerReconnect).not.toHaveBeenCalled();
        expect(hasInflightRequest(connector, 111n)).toBe(false);
    });

    test("rejects retriable subscribe request immediately when send throws with non-network error", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("serialize failed");
        });

        const pending = connector.sendMsg(
            JSON.stringify({
                action: "subscribe",
                args: {
                    req_id: 113,
                    topics: ["topic_a"],
                },
            })
        );
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("rejected");
        expect(connector.triggerReconnect).not.toHaveBeenCalled();
        expect(hasInflightRequest(connector, 113n)).toBe(false);
    });

    test("triggers reconnect and rejects non-retriable string request on network send error", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        await expect(
            connector.sendMsg(
                JSON.stringify({
                    action: "query",
                    args: {
                        req_id: 112,
                    },
                })
            )
        ).rejects.toThrow("cannot call send() while not connected");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 112n)).toBe(false);
    });

    test("triggers reconnect and keeps retriable binary request inflight when send throws", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const message = buildBinaryMessage(6n);
        const pending = connector.sendBinaryMsg(201n, "binary_query", message);
        void pending.catch(() => { });

        const state = await Promise.race([
            pending.then(() => "resolved").catch(() => "rejected"),
            delay(20).then(() => "pending"),
        ]);

        expect(state).toBe("pending");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
        expect(hasInflightRequest(connector, 201n)).toBe(true);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("rejects non-retriable binary request immediately when send throws", async () => {
        const connector = createBareConnector();
        connector._conn.send = jest.fn(() => {
            throw new Error("send failed");
        });

        const message = buildBinaryMessage(7n);
        await expect(
            connector.sendBinaryMsg(202n, "fetch", message)
        ).rejects.toThrow("send failed");
        expect(hasInflightRequest(connector, 202n)).toBe(false);
    });

    test("sendMsgDirect unregisters callback on network send error without reconnect", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const registerSpy = jest
            .spyOn(WsEventCallback.instance(), "registerCallback")
            .mockResolvedValue();
        const unregisterSpy = jest
            .spyOn(WsEventCallback.instance(), "unregisterCallback")
            .mockResolvedValue();

        await expect(
            connector.sendMsgDirect(
                JSON.stringify({
                    action: "conn",
                    args: {
                        req_id: 3001,
                    },
                })
            )
        ).rejects.toThrow("cannot call send() while not connected");

        expect(registerSpy).toHaveBeenCalledTimes(1);
        expect(unregisterSpy).toHaveBeenCalledWith(3001n);
        expect(connector.triggerReconnect).not.toHaveBeenCalled();
    });

    test("sendMsgNoResp rejects on network send error and triggers reconnect", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        await expect(
            connector.sendMsgNoResp(
                JSON.stringify({
                    action: "query",
                    args: {
                        req_id: 3002,
                    },
                })
            )
        ).rejects.toThrow("cannot call send() while not connected");
        expect(connector.triggerReconnect).toHaveBeenCalledTimes(1);
    });

    test("keeps inflight store consistent when retriable requests are enqueued concurrently", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const reqIds = [301, 302, 303, 304];
        await Promise.all(
            reqIds.map(async (reqId) => {
                const pending = connector.sendMsg(
                    JSON.stringify({
                        action: "insert",
                        args: {
                            req_id: reqId,
                            data: `insert into t values(now, ${reqId})`,
                        },
                    })
                );
                void pending.catch(() => { });
            })
        );

        await delay(20);

        expect(listInflightReqIds(connector)).toEqual([301n, 302n, 303n, 304n]);
        for (const reqId of reqIds) {
            expect(hasInflightRequest(connector, BigInt(reqId))).toBe(true);
        }
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("replays inflight requests in the same put order", async () => {
        const connector = createBareConnector();
        connector.triggerReconnect = jest.fn(() => new Promise<void>(() => { }));
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        const reqIds = [101, 102, 103, 104];
        for (const reqId of reqIds) {
            const pending = connector.sendMsg(
                JSON.stringify({
                    action: "insert",
                    args: {
                        req_id: reqId,
                        data: `insert into t values(now, ${reqId})`,
                    },
                })
            );
            void pending.catch(() => { });
        }

        await delay(20);

        const replaySend = jest.fn();
        connector._conn.send = replaySend;

        await connector.replayRequests();

        const replayedReqIds = replaySend.mock.calls.map(([payload]: [string]) => {
            const parsed = JSON.parse(payload);
            return parsed.args.req_id;
        });

        expect(replayedReqIds).toEqual([101, 102, 103, 104]);
        connector.failAllInflightRequests(new Error("cleanup"));
    });

    test("replayRequests stops current round and keeps inflight when network send error happens", async () => {
        const connector = createBareConnector();
        const reject1 = jest.fn();
        const reject2 = jest.fn();

        connector._inflightStore.insert({
            reqId: 501n,
            action: "insert",
            registerCallback: false,
            message: JSON.stringify({
                action: "insert",
                args: {
                    req_id: 501,
                },
            }),
            resolve: jest.fn(),
            reject: reject1,
        });

        connector._inflightStore.insert({
            reqId: 502n,
            action: "insert",
            registerCallback: false,
            message: JSON.stringify({
                action: "insert",
                args: {
                    req_id: 502,
                },
            }),
            resolve: jest.fn(),
            reject: reject2,
        });

        connector._conn.readyState = 3;
        connector._conn.send = jest.fn(() => {
            throw new Error("cannot call send() while not connected");
        });

        await expect(connector.replayRequests()).rejects.toThrow(
            "cannot call send() while not connected"
        );

        expect(hasInflightRequest(connector, 501n)).toBe(true);
        expect(hasInflightRequest(connector, 502n)).toBe(true);
        expect(reject1).not.toHaveBeenCalled();
        expect(reject2).not.toHaveBeenCalled();
    });
});

describe("RetryConfig", () => {
    test("uses defaults when retry params are not provided", () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const config = RetryConfig.fromDsn(dsn);
        expect(config.retries).toBe(5);
        expect(config.retryBackoffMs).toBe(200);
        expect(config.retryBackoffMaxMs).toBe(2000);
    });

    test("reads retry params from dsn", () => {
        const dsn = parse(
            "ws://root:taosdata@localhost:6041?retries=7&retry_backoff_ms=150&retry_backoff_max_ms=3200"
        );
        const config = RetryConfig.fromDsn(dsn);
        expect(config.retries).toBe(7);
        expect(config.retryBackoffMs).toBe(150);
        expect(config.retryBackoffMaxMs).toBe(3200);
    });

    test("normalizes invalid params to safe defaults", () => {
        const dsn = parse(
            "ws://root:taosdata@localhost:6041?retries=-1&retry_backoff_ms=abc&retry_backoff_max_ms=0"
        );
        const config = RetryConfig.fromDsn(dsn);
        expect(config.retries).toBe(5);
        expect(config.retryBackoffMs).toBe(200);
        expect(config.retryBackoffMaxMs).toBe(2000);
    });

    test("computes exponential backoff and caps at max", () => {
        const config = new RetryConfig(3, 100, 350);
        expect(config.getBackoffDelay(0)).toBe(100);
        expect(config.getBackoffDelay(1)).toBe(200);
        expect(config.getBackoffDelay(2)).toBe(350);
        expect(config.getBackoffDelay(10)).toBe(350);
    });
});

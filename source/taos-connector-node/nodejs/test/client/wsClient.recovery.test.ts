import { w3cwebsocket } from "websocket";
import { WsClient } from "@src/client/wsClient";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { parse, WS_TMQ_ENDPOINT } from "@src/common/dsn";
import { WebSocketQueryError } from "@src/common/wsError";

function resetPoolSingleton() {
    const PoolClass = WebSocketConnectionPool as any;
    if (PoolClass._instance) {
        PoolClass._instance.destroyed();
        PoolClass._instance = undefined;
    }
}

function createMockConnector(sessionReady: boolean = true) {
    let currentSessionReady = sessionReady;
    return {
        readyState: jest.fn(() => w3cwebsocket.OPEN),
        ready: jest.fn(async () => { }),
        setSessionRecoveryHook: jest.fn(),
        isSessionReady: jest.fn(() => currentSessionReady),
        markSessionReady: jest.fn(() => {
            currentSessionReady = true;
        }),
        sendMsgDirect: jest.fn(async (_message: string) => ({
            msg: { code: 0, message: "" }
        })),
        sendMsg: jest.fn(async () => ({ msg: { code: 0, message: "" } })),
        sendMsgNoResp: jest.fn(async () => { }),
    };
}

function createPooledLifecycleConnector(dsn: ReturnType<typeof parse>, sessionReady: boolean = true) {
    const pool = WebSocketConnectionPool.instance();
    const poolKey = (pool as any).getPoolKey(dsn);
    let currentSessionReady = sessionReady;
    let recoveryHook: (() => Promise<void>) | null = null;

    return {
        readyState: jest.fn(() => w3cwebsocket.OPEN),
        ready: jest.fn(async () => { }),
        refreshRetryConfig: jest.fn(),
        getPoolKey: jest.fn(() => poolKey),
        close: jest.fn(),
        setSessionRecoveryHook: jest.fn((hook?: (() => Promise<void>) | null) => {
            recoveryHook = hook || null;
        }),
        isSessionReady: jest.fn(() => currentSessionReady),
        markSessionReady: jest.fn(() => {
            currentSessionReady = true;
        }),
        sendMsgDirect: jest.fn(async (_message: string) => ({
            msg: { code: 0, message: "" }
        })),
        sendMsg: jest.fn(async () => ({ msg: { code: 0, message: "" } })),
        sendMsgNoResp: jest.fn(async () => { }),
        simulateIdleReconnect: async () => {
            currentSessionReady = false;
            if (recoveryHook) {
                await recoveryHook();
            }
        },
    };
}

describe("WsClient recovery hook", () => {
    beforeEach(() => {
        resetPoolSingleton();
    });

    afterEach(() => {
        jest.restoreAllMocks();
        resetPoolSingleton();
    });

    test("uses dsn endpoint when creating connection", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        dsn.endpoint = WS_TMQ_ENDPOINT;
        const connector = createMockConnector();
        const getConnectionSpy = jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 4321);
        await client.ready();

        expect(getConnectionSpy).toHaveBeenCalledWith(dsn, 4321);
    });

    test("runs custom recovery hook without sql conn recovery on tmq path", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        dsn.endpoint = WS_TMQ_ENDPOINT;
        const connector = createMockConnector();
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const customRecoveryHook = jest.fn(async () => { });
        const client = new WsClient(dsn, 5000);
        client.setSessionRecoveryHook(customRecoveryHook);
        await client.ready();

        const hookCalls = connector.setSessionRecoveryHook.mock.calls;
        const hook = hookCalls[hookCalls.length - 1]?.[0];
        expect(hook).toBeTruthy();
        await hook();

        expect(connector.sendMsgDirect).not.toHaveBeenCalled();
        expect(customRecoveryHook).toHaveBeenCalledTimes(1);
    });

    test("keeps sql conn recovery and then runs custom recovery on sql endpoint", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createMockConnector();
        const callOrder: string[] = [];
        connector.sendMsgDirect.mockImplementation(async (message: string) => {
            const action = JSON.parse(message).action;
            callOrder.push(action);
            return { msg: { code: 0, message: "" } };
        });
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 5000);
        (client as any)._connectedDatabase = "db_recovery";
        client.setSessionRecoveryHook(async () => {
            callOrder.push("custom");
        });
        await client.ready();

        const hookCalls = connector.setSessionRecoveryHook.mock.calls;
        const hook = hookCalls[hookCalls.length - 1]?.[0];
        expect(hook).toBeTruthy();
        await hook();

        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(1);
        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.db).toBe("db_recovery");
        expect(callOrder).toEqual(["conn", "custom"]);
    });

    test("includes user app and ip in sql recovery conn message", async () => {
        const dsn = parse(
            "ws://root:taosdata@localhost:6041?user_app=myApp&user_ip=192.168.1.100"
        );
        const connector = createMockConnector();
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 5000);
        await client.connect("db_recovery");

        const hookCalls = connector.setSessionRecoveryHook.mock.calls;
        const hook = hookCalls[hookCalls.length - 1]?.[0];
        expect(hook).toBeTruthy();
        await hook();

        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.app).toBe("myApp");
        expect(connMsg.args.ip).toBe("192.168.1.100");
    });

    test("throws WebSocketQueryError when sql recovery direct call fails", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createMockConnector();
        connector.sendMsgDirect.mockResolvedValue({
            msg: {
                code: 9001,
                message: "conn failed",
            },
        });
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const customRecoveryHook = jest.fn(async () => { });
        const client = new WsClient(dsn, 5000);
        client.setSessionRecoveryHook(customRecoveryHook);
        await client.ready();

        const hookCalls = connector.setSessionRecoveryHook.mock.calls;
        const hook = hookCalls[hookCalls.length - 1]?.[0];
        expect(hook).toBeTruthy();
        await expect(hook()).rejects.toBeInstanceOf(WebSocketQueryError);
        expect(customRecoveryHook).not.toHaveBeenCalled();
    });

    test("restores default information_schema during sql recovery when no db provided", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createMockConnector();
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 5000);
        await client.connect();

        const hookCalls = connector.setSessionRecoveryHook.mock.calls;
        const hook = hookCalls[hookCalls.length - 1]?.[0];
        expect(hook).toBeTruthy();
        await hook();

        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(1);
        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.db).toBe("information_schema");
    });

    test("restores sql conn session before reusing an OPEN pooled connector", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createMockConnector(false);
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 5000);
        await client.connect("db_from_borrower");

        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(1);
        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.db).toBe("db_from_borrower");
    });

    test("restores sql conn session in ready path before reusing an OPEN pooled connector", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createMockConnector(false);
        jest
            .spyOn(WebSocketConnectionPool.instance(), "getConnection")
            .mockResolvedValue(connector as any);

        const client = new WsClient(dsn, 5000);
        await client.ready();

        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(1);
        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.db).toBe("information_schema");
    });

    test("restores sql conn session after idle pooled reconnect and next borrow", async () => {
        const dsn = parse("ws://root:taosdata@localhost:6041");
        const connector = createPooledLifecycleConnector(dsn, true);
        const pool = WebSocketConnectionPool.instance();

        await pool.releaseConnection(connector as any);

        const firstBorrower = new WsClient(dsn, 5000);
        await firstBorrower.connect("db_first_borrow");
        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(0);

        await firstBorrower.close();
        await connector.simulateIdleReconnect();

        const secondBorrower = new WsClient(dsn, 5000);
        await secondBorrower.connect("db_second_borrow");

        expect(connector.sendMsgDirect).toHaveBeenCalledTimes(1);
        const firstCall = connector.sendMsgDirect.mock.calls[0] as [string];
        const connMsg = JSON.parse(firstCall[0]);
        expect(connMsg.action).toBe("conn");
        expect(connMsg.args.db).toBe("db_second_borrow");

        await secondBorrower.close();
    });
});

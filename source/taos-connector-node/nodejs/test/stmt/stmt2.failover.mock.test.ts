import { WsStmt2 } from "@src/stmt/wsStmt2";
import * as wsProto from "@src/stmt/wsProto";

const Step = {
    INIT: 0,
    PREPARE: 1,
    BIND: 2,
    EXEC: 3,
    RESULT: 4,
} as const;

function createMockWsClient() {
    return {
        getState: jest.fn(() => 1),
        connect: jest.fn(async () => { }),
        checkVersion: jest.fn(async () => { }),
        exec: jest.fn(async () => ({
            totalTime: 0,
            msg: {
                code: 0,
                message: "",
                timing: 0,
                stmt_id: 1,
                is_insert: false,
                fields_count: 1,
            },
        })),
        execNoResp: jest.fn(async () => { }),
        sendBinaryMsg: jest.fn(async () => ({
            totalTime: 0,
            msg: {
                code: 0,
                message: "",
                timing: 0,
                stmt_id: 1,
                affected: 0,
            },
        })),
        waitForReady: jest.fn(async () => { }),
        isNetworkError: jest.fn((_err: unknown) => false),
        getReconnectRetries: jest.fn(() => 5),
    };
}

function createBareStmt() {
    const wsClient = createMockWsClient();
    const stmt = new (WsStmt2 as any)(wsClient);
    stmt._stmt_id = 1n;
    return {
        stmt,
        wsClient,
    };
}

function makeExecReady(stmt: any): void {
    const tableInfo = {
        getParams: () => ({ ok: true }),
    };
    stmt._currentTableInfo = tableInfo;
    stmt._stmtTableInfoList = [tableInfo];
    stmt._toBeBindTagCount = 0;
    stmt._toBeBindColCount = 0;
    stmt._toBeBindTableNameIndex = undefined;
}

function makeSavedBindBytes(reqId: bigint = 0n, stmtId: bigint = 1n): ArrayBuffer {
    const bytes = new ArrayBuffer(16);
    const view = new DataView(bytes);
    view.setBigUint64(0, reqId, true);
    view.setBigUint64(8, stmtId, true);
    return bytes;
}

describe("WsStmt2 failover (mock)", () => {
    afterEach(() => {
        jest.restoreAllMocks();
    });

    test("init triggers context recovery on network error", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("socket closed");

        wsClient.isNetworkError.mockReturnValue(true);
        jest.spyOn(stmt, "doInit").mockRejectedValueOnce(networkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);

        const result = await stmt.init(undefined);

        expect(result).toBe(stmt);
        expect(recoverSpy).toHaveBeenCalledWith(Step.INIT);
    });

    test("prepare caches sql and recovers on network error", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("socket closed");
        wsClient.isNetworkError.mockReturnValue(true);
        jest.spyOn(stmt, "doPrepare").mockRejectedValueOnce(networkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);

        const sql = "select * from meters where ts > ?";
        await stmt.prepare(sql);

        expect(stmt._savedSql).toBe(sql);
        expect(recoverSpy).toHaveBeenCalledWith(Step.PREPARE);
    });

    test("exec caches bind bytes and recovers on network error", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("cannot call send() while not connected");
        const bindBytes = new Uint8Array([1, 2, 3]).buffer;
        makeExecReady(stmt);
        stmt._isInsert = false;
        wsClient.isNetworkError.mockReturnValue(true);
        jest.spyOn(wsProto, "stmt2BinaryBlockEncode").mockReturnValue(bindBytes);
        jest.spyOn(stmt, "doSendBindBytes").mockRejectedValueOnce(networkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);
        const cleanupSpy = jest.spyOn(stmt, "cleanup");

        await stmt.exec();

        expect(stmt._savedBindBytes).toBe(bindBytes);
        expect(recoverSpy).toHaveBeenCalledWith(Step.EXEC);
        expect(cleanupSpy).not.toHaveBeenCalled();
    });

    test("resultSet recovers on network error and cleans up", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("connection reset");
        const rebuiltRows = { id: "rebuilt" };
        stmt._savedSql = "select * from t where ts > ?";
        wsClient.isNetworkError.mockReturnValue(true);
        jest.spyOn(stmt, "doResult").mockRejectedValueOnce(networkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(rebuiltRows);
        const cleanupSpy = jest.spyOn(stmt, "cleanup");

        const result = await stmt.resultSet();

        expect(result).toBe(rebuiltRows);
        expect(recoverSpy).toHaveBeenCalledWith(Step.RESULT);
        expect(cleanupSpy).toHaveBeenCalledTimes(1);
        expect(stmt._savedSql).toBe("select * from t where ts > ?");
    });

    test("exec only cleans up immediately for insert statements", async () => {
        const bindBytes = new Uint8Array([7, 8, 9]).buffer;

        const insertCtx = createBareStmt();
        const insertStmt = insertCtx.stmt;
        makeExecReady(insertStmt);
        insertStmt._isInsert = true;
        insertStmt._savedSql = "insert into t values(?, ?)";
        jest.spyOn(wsProto, "stmt2BinaryBlockEncode").mockReturnValue(bindBytes);
        jest.spyOn(insertStmt, "doSendBindBytes").mockResolvedValue(undefined);
        jest.spyOn(insertStmt, "doExec").mockResolvedValue(undefined);
        const insertCleanupSpy = jest.spyOn(insertStmt, "cleanup");
        await insertStmt.exec();
        expect(insertCleanupSpy).toHaveBeenCalledTimes(1);
        expect(insertStmt._savedSql).toBe("insert into t values(?, ?)");

        const queryCtx = createBareStmt();
        const queryStmt = queryCtx.stmt;
        makeExecReady(queryStmt);
        queryStmt._isInsert = false;
        jest.spyOn(wsProto, "stmt2BinaryBlockEncode").mockReturnValue(bindBytes);
        jest.spyOn(queryStmt, "doSendBindBytes").mockResolvedValue(undefined);
        jest.spyOn(queryStmt, "doExec").mockResolvedValue(undefined);
        const queryCleanupSpy = jest.spyOn(queryStmt, "cleanup");
        await queryStmt.exec();
        expect(queryCleanupSpy).not.toHaveBeenCalled();
    });

    test("exec cleans up insert cache even when recover fails", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("cannot call send() while not connected");
        const recoverError = new Error("recover failed");
        const bindBytes = new Uint8Array([1, 2, 3]).buffer;
        makeExecReady(stmt);
        stmt._isInsert = true;
        wsClient.isNetworkError.mockReturnValue(true);
        jest.spyOn(wsProto, "stmt2BinaryBlockEncode").mockReturnValue(bindBytes);
        jest.spyOn(stmt, "doSendBindBytes").mockRejectedValueOnce(networkError);
        jest.spyOn(stmt, "recover").mockRejectedValueOnce(recoverError);
        const cleanupSpy = jest.spyOn(stmt, "cleanup");

        await expect(stmt.exec()).rejects.toThrow("recover failed");
        expect(cleanupSpy).toHaveBeenCalledTimes(1);
    });

    test("non-network errors are rethrown without recover in prepare", async () => {
        const { stmt, wsClient } = createBareStmt();
        const nonNetworkError = new Error("invalid sql");
        wsClient.isNetworkError.mockReturnValue(false);
        jest.spyOn(stmt, "doPrepare").mockRejectedValueOnce(nonNetworkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);

        await expect(stmt.prepare("bad sql")).rejects.toThrow("invalid sql");
        expect(recoverSpy).not.toHaveBeenCalled();
    });

    test("non-network errors are rethrown without recover in exec", async () => {
        const { stmt, wsClient } = createBareStmt();
        const bindBytes = new Uint8Array([4, 5, 6]).buffer;
        const nonNetworkError = new Error("invalid bind");
        makeExecReady(stmt);
        stmt._isInsert = false;
        wsClient.isNetworkError.mockReturnValue(false);
        jest.spyOn(wsProto, "stmt2BinaryBlockEncode").mockReturnValue(bindBytes);
        jest.spyOn(stmt, "doSendBindBytes").mockRejectedValueOnce(nonNetworkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);

        await expect(stmt.exec()).rejects.toThrow("invalid bind");
        expect(recoverSpy).not.toHaveBeenCalled();
    });

    test("non-network errors are rethrown without recover in resultSet", async () => {
        const { stmt, wsClient } = createBareStmt();
        const nonNetworkError = new Error("result failed");
        wsClient.isNetworkError.mockReturnValue(false);
        jest.spyOn(stmt, "doResult").mockRejectedValueOnce(nonNetworkError);
        const recoverSpy = jest.spyOn(stmt, "recover").mockResolvedValue(undefined);

        await expect(stmt.resultSet()).rejects.toThrow("result failed");
        expect(recoverSpy).not.toHaveBeenCalled();
    });

    test("recover replays steps in order to EXEC", async () => {
        const { stmt } = createBareStmt();
        const callOrder: string[] = [];
        stmt._savedSql = "insert into t values(?, ?)";
        stmt._savedBindBytes = makeSavedBindBytes();
        jest.spyOn(stmt, "doInit").mockImplementation(async () => {
            callOrder.push("init");
        });
        jest.spyOn(stmt, "doPrepare").mockImplementation(async () => {
            callOrder.push("prepare");
        });
        jest.spyOn(stmt, "doSendBindBytes").mockImplementation(async () => {
            callOrder.push("bind");
        });
        jest.spyOn(stmt, "doExec").mockImplementation(async () => {
            callOrder.push("exec");
        });
        jest.spyOn(stmt, "doResult").mockImplementation(async () => {
            callOrder.push("result");
            return { rows: 1 };
        });

        await stmt.recover(Step.EXEC);

        expect(callOrder).toEqual(["init", "prepare", "bind", "exec"]);
    });

    test("recover can rebuild to RESULT and return rows", async () => {
        const { stmt } = createBareStmt();
        const rows = { data: [1, 2, 3] };
        stmt._savedSql = "select * from t where ts > ?";
        stmt._savedBindBytes = makeSavedBindBytes();
        jest.spyOn(stmt, "doInit").mockResolvedValue(undefined);
        jest.spyOn(stmt, "doPrepare").mockResolvedValue(undefined);
        jest.spyOn(stmt, "doSendBindBytes").mockResolvedValue(undefined);
        jest.spyOn(stmt, "doExec").mockResolvedValue(undefined);
        jest.spyOn(stmt, "doResult").mockResolvedValue(rows);

        const result = await stmt.recover(Step.RESULT);

        expect(result).toBe(rows);
    });

    test("buildBindBytes rewrites req_id and stmt_id", () => {
        const { stmt } = createBareStmt();
        const originalBytes = makeSavedBindBytes(0n, 5n);
        stmt._savedBindBytes = originalBytes;
        stmt._stmt_id = 42n;

        const replayBytes = stmt.buildBindBytes();
        const originalView = new DataView(originalBytes);
        const replayView = new DataView(replayBytes);

        expect(replayBytes).not.toBe(originalBytes);
        expect(originalView.getBigUint64(8, true)).toBe(5n);
        expect(replayView.getBigUint64(8, true)).toBe(42n);
        expect(replayView.getBigUint64(0, true)).not.toBe(0n);
    });

    test("buildBindBytes throws when stmt_id is missing", () => {
        const { stmt } = createBareStmt();
        stmt._savedBindBytes = makeSavedBindBytes();
        stmt._stmt_id = null;

        expect(() => stmt.buildBindBytes()).toThrow(
            "stmt_id is missing for stmt2 rebuild"
        );
    });

    test("recover retries when another network error occurs", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("connection reset");
        stmt._savedSql = "select * from t";
        stmt._savedBindBytes = makeSavedBindBytes();
        wsClient.isNetworkError.mockImplementation(
            (err: unknown) => err === networkError
        );
        const initSpy = jest
            .spyOn(stmt, "doInit")
            .mockRejectedValueOnce(networkError)
            .mockResolvedValue(undefined);
        const prepareSpy = jest.spyOn(stmt, "doPrepare").mockResolvedValue(undefined);

        await stmt.recover(Step.PREPARE);

        expect(initSpy).toHaveBeenCalledTimes(2);
        expect(prepareSpy).toHaveBeenCalledTimes(1);
        expect(wsClient.waitForReady).toHaveBeenCalledTimes(2);
    });

    test("recover keeps retrying network errors and throws on first non-network error", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("connection reset");
        const fatalError = new Error("permission denied");
        stmt._savedSql = "select * from t";
        stmt._savedBindBytes = makeSavedBindBytes();
        wsClient.isNetworkError.mockImplementation((err: unknown) => err === networkError);
        const initSpy = jest
            .spyOn(stmt, "doInit")
            .mockRejectedValueOnce(networkError)
            .mockRejectedValueOnce(networkError)
            .mockRejectedValueOnce(fatalError);

        await expect(stmt.recover(Step.INIT)).rejects.toThrow("permission denied");
        expect(initSpy).toHaveBeenCalledTimes(3);
        expect(wsClient.waitForReady).toHaveBeenCalledTimes(3);
    });

    test("recover throws on non-network error", async () => {
        const { stmt, wsClient } = createBareStmt();
        const nonNetworkError = new Error("permission denied");
        stmt._savedSql = "select * from t";
        stmt._savedBindBytes = new Uint8Array([9, 9, 9]).buffer;
        wsClient.isNetworkError.mockReturnValue(false);
        jest.spyOn(stmt, "doInit").mockRejectedValueOnce(nonNetworkError);

        await expect(stmt.recover(Step.INIT)).rejects.toThrow("permission denied");
    });

    test("recover throws when network errors exceed max attempts", async () => {
        const { stmt, wsClient } = createBareStmt();
        const networkError = new Error("connection reset");
        stmt._savedSql = "select * from t";
        stmt._savedBindBytes = makeSavedBindBytes();
        wsClient.getReconnectRetries.mockReturnValue(2);
        wsClient.isNetworkError.mockImplementation((err: unknown) => err === networkError);
        const initSpy = jest
            .spyOn(stmt, "doInit")
            .mockRejectedValue(networkError);

        await expect(stmt.recover(Step.INIT)).rejects.toThrow(
            "stmt2 recover exceeded max attempts (2)"
        );
        expect(initSpy).toHaveBeenCalledTimes(2);
        expect(wsClient.waitForReady).toHaveBeenCalledTimes(2);
    });
});

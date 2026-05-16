import { WSConfig } from "@src/common/config";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WsSql } from "@src/sql/wsSql";
import {
    compareUint8Arrays,
    hexToBytes,
    testPassword,
    testNon3360,
    testUsername,
} from "@test-helpers/utils";

describe("TDWebSocket.SQL.BLOB", () => {
    jest.setTimeout(20 * 1000);

    afterAll(() => {
        WebSocketConnectionPool.instance().destroyed();
    });

    testNon3360("insert and query blob with hex string literal", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1775805396";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(`create table t0 (ts timestamp, data blob)`);
            await ws.exec(`insert into t0 values(now, '\\x393866343633')`);

            const result = await ws.exec(`select data from t0`);
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }

            const actual = data[0][0] as ArrayBuffer;
            const expected = hexToBytes("393866343633");
            expect(actual).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(
                    new Uint8Array(actual),
                    new Uint8Array(expected)
                )
            ).toBe(true);
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("insert and query blob with raw string literal", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1775805911";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(`create table t0 (ts timestamp, data blob)`);
            await ws.exec(`insert into t0 values(now, '98f46e')`);

            const result = await ws.exec(`select data from t0`);
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }

            const actual = data[0][0] as ArrayBuffer;
            const expected = new TextEncoder().encode("98f46e");
            expect(actual).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(new Uint8Array(actual), expected)
            ).toBe(true);
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("insert and query null blob", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1775814624";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(`create table t0 (ts timestamp, data blob)`);
            await ws.exec(`insert into t0 values(now, null)`);

            const result = await ws.exec(`select data from t0`);
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }

            expect(data[0][0]).toBe("NULL");
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });
});

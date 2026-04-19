import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { WsStmt2 } from "@src/stmt/wsStmt2";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { compareUint8Arrays, testNon3360, testPassword, testUsername } from "@test-helpers/utils";

describe("TDWebSocket.Stmt2.BLOB", () => {
    jest.setTimeout(20 * 1000);

    afterAll(() => {
        WebSocketConnectionPool.instance().destroyed();
    });

    testNon3360("stmt2 bind blob with ArrayBuffer", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        let stmt = null;
        const db = "test_1776043854";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec("create stable stb (ts timestamp, data blob) tags (location binary(64), groupId int)");

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare("insert into ? using stb tags (?, ?) values (?, ?)");
            await stmt.setTableName("d0");

            const tagParams = stmt.newStmtParam();
            tagParams.setBinary(["blob_loc"]);
            tagParams.setInt([1]);
            await stmt.setTags(tagParams);

            const blob = new TextEncoder().encode("stmt2_blob_buffer").buffer;
            const bindParams = stmt.newStmtParam();
            bindParams.setTimestamp([BigInt(Date.now())]);
            bindParams.setBlob([blob]);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();

            const result = await ws.exec("select data from d0");
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }
            const actual = data[0][0] as ArrayBuffer;
            expect(actual).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(new Uint8Array(actual), new Uint8Array(blob))
            ).toBe(true);
        } finally {
            if (stmt) {
                await stmt.close();
            }
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 bind blob with string", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        let stmt = null;
        const db = "test_1776044275";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec("create stable stb (ts timestamp, data blob) tags (location binary(64), groupId int)");

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare("insert into ? using stb tags (?, ?) values (?, ?)");
            await stmt.setTableName("d0");

            const tagParams = stmt.newStmtParam();
            tagParams.setBinary(["blob_loc"]);
            tagParams.setInt([1]);
            await stmt.setTags(tagParams);

            const bindParams = stmt.newStmtParam();
            bindParams.setTimestamp([BigInt(Date.now())]);
            bindParams.setBlob(["stmt2_blob_string"]);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();

            const result = await ws.exec("select data from d0");
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }
            const actual = data[0][0] as ArrayBuffer;
            const expected = new TextEncoder().encode("stmt2_blob_string");
            expect(actual).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(new Uint8Array(actual), expected)
            ).toBe(true);
        } finally {
            if (stmt) {
                await stmt.close();
            }
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 bind null blob", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        let stmt = null;
        const db = "test_1776044373";
        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec("create stable stb (ts timestamp, data blob) tags (location binary(64), groupId int)");

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare("insert into ? using stb tags (?, ?) values (?, ?)");
            await stmt.setTableName("d0");

            const tagParams = stmt.newStmtParam();
            tagParams.setBinary(["blob_loc"]);
            tagParams.setInt([1]);
            await stmt.setTags(tagParams);

            const bindParams = stmt.newStmtParam();
            bindParams.setTimestamp([BigInt(Date.now())]);
            bindParams.setBlob([null]);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();

            const result = await ws.exec("select data from d0");
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data || data.length === 0) {
                throw new Error("retrieve empty result");
            }
            expect(data[0][0]).toBe("NULL");
        } finally {
            if (stmt) {
                await stmt.close();
            }
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 bind blob with multi-subtables and multi-rows including null", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        let stmt = null;
        const db = "test_1776048731";
        const encoder = new TextEncoder();
        const baseTs = Date.now();
        const cases: Array<{
            table: string;
            location: string;
            groupId: number;
            valueTs: bigint[];
            valueBlobs: Array<ArrayBuffer>;
            nullTs: bigint;
            expected: Array<Uint8Array | null>;
        }> = [
                {
                    table: "d0",
                    location: "blob_loc_0",
                    groupId: 10,
                    valueTs: [BigInt(baseTs), BigInt(baseTs + 1)],
                    valueBlobs: [
                        encoder.encode("d0_row0").buffer,
                        encoder.encode("d0_row2").buffer,
                    ],
                    nullTs: BigInt(baseTs + 2),
                    expected: [
                        encoder.encode("d0_row0"),
                        encoder.encode("d0_row2"),
                        null,
                    ],
                },
                {
                    table: "d1",
                    location: "blob_loc_1",
                    groupId: 11,
                    valueTs: [BigInt(baseTs + 10), BigInt(baseTs + 11)],
                    valueBlobs: [
                        encoder.encode("d1_row0").buffer,
                        encoder.encode("d1_row1").buffer,
                    ],
                    nullTs: BigInt(baseTs + 12),
                    expected: [
                        encoder.encode("d1_row0"),
                        encoder.encode("d1_row1"),
                        null,
                    ],
                },
            ];

        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec("create stable stb (ts timestamp, data blob) tags (location binary(64), groupId int)");

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare("insert into ? using stb tags (?, ?) values (?, ?)");

            for (const item of cases) {
                await stmt.setTableName(item.table);

                const tagParams = stmt.newStmtParam();
                tagParams.setBinary([item.location]);
                tagParams.setInt([item.groupId]);
                await stmt.setTags(tagParams);

                const valueBindParams = stmt.newStmtParam();
                valueBindParams.setTimestamp(item.valueTs);
                valueBindParams.setBlob(item.valueBlobs);
                await stmt.bind(valueBindParams);
                await stmt.batch();

                await stmt.exec();

                await stmt.setTableName(item.table);

                const nullTagParams = stmt.newStmtParam();
                nullTagParams.setBinary([item.location]);
                nullTagParams.setInt([item.groupId]);
                await stmt.setTags(nullTagParams);

                const nullBindParams = stmt.newStmtParam();
                nullBindParams.setTimestamp([item.nullTs]);
                nullBindParams.setBlob([null]);
                await stmt.bind(nullBindParams);
                await stmt.batch();

                await stmt.exec();
            }

            for (const item of cases) {
                const result = await ws.exec(`select data from ${item.table}`);
                const data = result.getData();
                expect(data).toBeTruthy();
                if (!data || data.length === 0) {
                    throw new Error("retrieve empty result");
                }
                expect(data.length).toBe(item.expected.length);

                for (let i = 0; i < item.expected.length; i++) {
                    const expected = item.expected[i];
                    const actual = data[i][0];
                    if (expected === null) {
                        expect(actual).toBe("NULL");
                    } else {
                        expect(actual).toBeInstanceOf(ArrayBuffer);
                        expect(
                            compareUint8Arrays(
                                new Uint8Array(actual as ArrayBuffer),
                                expected
                            )
                        ).toBe(true);
                    }
                }
            }
        } finally {
            if (stmt) {
                await stmt.close();
            }
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });
});

import { FieldBindType, TDengineTypeCode } from "@src/common/constant";
import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { StmtFieldInfo } from "@src/stmt/wsProto";
import { Stmt1BindParams } from "@src/stmt/wsParams1";
import { Stmt2BindParams } from "@src/stmt/wsParams2";
import { WsStmt2 } from "@src/stmt/wsStmt2";
import { compareUint8Arrays, testNon3360, testPassword, testUsername } from "@test-helpers/utils";

describe("TDWebSocket.Stmt2.DECIMAL", () => {
    jest.setTimeout(20 * 1000);

    testNon3360("stmt2 bind decimal64/decimal with null, negative and scientific notation", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776150084";
        const tsBase = Date.now();
        let stmt: any = null;

        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(
                "create stable stb (" +
                "ts timestamp, dec64 decimal(18, 6), dec128 decimal(30, 10), c1 int) " +
                "tags (location binary(64), gid int)"
            );

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare("insert into ? using stb tags (?, ?) values (?, ?, ?, ?)");
            await stmt.setTableName("d0");

            const tagParams = stmt.newStmtParam();
            tagParams.setBinary(["beijing"]);
            tagParams.setInt([1]);
            await stmt.setTags(tagParams);

            const bindParams = new Stmt2BindParams();
            bindParams.setTimestamp([
                BigInt(tsBase),
                BigInt(tsBase + 1),
                BigInt(tsBase + 2),
                BigInt(tsBase + 3),
            ]);
            bindParams.setDecimal([
                "9876.123456",
                "-0.000654",
                "1.23e+2",
                null,
            ]);
            bindParams.setDecimal([
                "123456.7890123456",
                "-0.0009876543",
                "5.55e-2",
                null,
            ]);
            bindParams.setInt([1, 2, 3, 4]);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();
            expect(stmt.getLastAffected()).toBe(4);

            const result = await ws.exec("select dec64, dec128, c1 from d0");
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data) {
                throw new Error("retrieve empty result");
            }
            expect(data.length).toBe(4);

            expect(data[0][0]).toBe("9876.123456");
            expect(data[0][1]).toBe("123456.7890123456");
            expect(data[0][2]).toBe(1);

            expect(data[1][0]).toBe("-0.000654");
            expect(data[1][1]).toBe("-0.0009876543");
            expect(data[1][2]).toBe(2);

            expect(data[2][0]).toBe("123.000000");
            expect(data[2][1]).toBe("0.0555000000");
            expect(data[2][2]).toBe(3);

            expect(data[3][0]).toBe("NULL");
            expect(data[3][1]).toBe("NULL");
            expect(data[3][2]).toBe(4);
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

    testNon3360("stmt2 query bind supports decimal parameters", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776151034";
        const tsBase = 1700000000000;
        let queryStmt: any = null;
        let rows: any = null;

        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(
                "create table ctb (" +
                "ts timestamp, dec64 decimal(18, 6), dec128 decimal(30, 10), c1 int)"
            );
            await ws.exec(
                "insert into ctb values" +
                ` (${tsBase}, "1.100000", "11.1111111111", 10)` +
                ` (${tsBase + 1}, "2.200000", "22.2222222222", 20)` +
                ` (${tsBase + 2}, "3.300000", "33.3333333333", 30)`
            );

            queryStmt = await ws.stmtInit();
            expect(queryStmt).toBeInstanceOf(WsStmt2);
            await queryStmt.prepare("select dec64, dec128, c1 from ctb where dec64 >= ?");
            const queryParams = new Stmt2BindParams();
            queryParams.setDecimal(["2.0"]);
            await queryStmt.bind(queryParams);
            await queryStmt.exec();

            rows = await queryStmt.resultSet();
            const queryData: Array<Array<any>> = [];
            while (await rows.next()) {
                const row = rows.getData();
                if (row) {
                    queryData.push([...row]);
                }
            }

            expect(queryData.length).toBe(2);
            expect(queryData[0][0]).toBe("2.200000");
            expect(queryData[0][1]).toBe("22.2222222222");
            expect(queryData[0][2]).toBe(20);
            expect(queryData[1][0]).toBe("3.300000");
            expect(queryData[1][1]).toBe("33.3333333333");
            expect(queryData[1][2]).toBe(30);
        } finally {
            if (rows) {
                await rows.close();
            }
            if (queryStmt) {
                await queryStmt.close();
            }
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 with tbname and dynamic columns should support non-null, null and empty rows", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776243346";
        const tsBase = Date.now();
        let stmt: any = null;
        let allRowsStmt: any = null;
        let allRows: any = null;

        const encoder = new TextEncoder();
        const geoPoint = new Uint8Array([
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
        ]).buffer;
        const vbNonNull = encoder.encode("vb_non_null").buffer;
        const blobNonNull = encoder.encode("blob_non_null").buffer;

        try {
            await ws.exec(`drop database if exists ${db}`);
            await ws.exec(`create database ${db}`);
            await ws.exec(`use ${db}`);
            await ws.exec(
                "create stable stb (" +
                "ts timestamp, vc varchar(64), nc nchar(64), " +
                "dec64 decimal(18, 6), decv decimal(30, 10), " +
                "vb varbinary(64), geo geometry(512), bl blob, c1 int) " +
                "tags (location binary(64), gid int)"
            );

            stmt = await ws.stmtInit();
            expect(stmt).toBeInstanceOf(WsStmt2);
            await stmt.prepare(
                "insert into stb (tbname, location, gid, ts, vc, nc, dec64, decv, vb, geo, bl, c1) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );

            await stmt.setTableName("d0");

            const tagParams = stmt.newStmtParam();
            tagParams.setBinary(["beijing"]);
            tagParams.setInt([1]);
            await stmt.setTags(tagParams);

            const bindParams = stmt.newStmtParam();
            bindParams.setTimestamp([
                BigInt(tsBase),
                BigInt(tsBase + 1),
                BigInt(tsBase + 2),
            ]);
            bindParams.setVarchar(["vc_non_null", null, ""]);
            bindParams.setNchar(["nc_non_null", null, ""]);
            bindParams.setDecimal(["12.340000", null, "0.000000"]);
            bindParams.setDecimal(["123456.0000000001", null, "0.0000000000"]);
            bindParams.setVarBinary([vbNonNull, null, new ArrayBuffer(0)]);
            bindParams.setGeometry([geoPoint, null, null]);
            bindParams.setBlob([blobNonNull, null, new ArrayBuffer(0)]);
            bindParams.setInt([11, 22, 33]);

            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();
            expect(stmt.getLastAffected()).toBe(3);

            allRowsStmt = await ws.stmtInit();
            expect(allRowsStmt).toBeInstanceOf(WsStmt2);
            await allRowsStmt.prepare(
                "select location, gid, vc, nc, dec64, decv, vb, geo, bl, c1 " +
                "from stb where c1 >= ? order by c1"
            );
            const allRowsParams = new Stmt2BindParams();
            allRowsParams.setInt([1]);
            await allRowsStmt.bind(allRowsParams);
            await allRowsStmt.exec();

            allRows = await allRowsStmt.resultSet();
            const allRowsData: Array<Array<any>> = [];
            while (await allRows.next()) {
                const row = allRows.getData();
                if (row) {
                    allRowsData.push([...row]);
                }
            }

            expect(allRowsData.length).toBe(3);

            expect(allRowsData[0][0]).toBe("beijing");
            expect(allRowsData[0][1]).toBe(1);
            expect(allRowsData[0][2]).toBe("vc_non_null");
            expect(allRowsData[0][3]).toBe("nc_non_null");
            expect(allRowsData[0][4]).toBe("12.340000");
            expect(allRowsData[0][5]).toBe("123456.0000000001");
            expect(allRowsData[0][6]).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(
                    new Uint8Array(allRowsData[0][6] as ArrayBuffer),
                    new Uint8Array(vbNonNull)
                )
            ).toBe(true);
            expect(allRowsData[0][7]).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(
                    new Uint8Array(allRowsData[0][7] as ArrayBuffer),
                    new Uint8Array(geoPoint)
                )
            ).toBe(true);
            expect(allRowsData[0][8]).toBeInstanceOf(ArrayBuffer);
            expect(
                compareUint8Arrays(
                    new Uint8Array(allRowsData[0][8] as ArrayBuffer),
                    new Uint8Array(blobNonNull)
                )
            ).toBe(true);
            expect(allRowsData[0][9]).toBe(11);

            expect(allRowsData[1][0]).toBe("beijing");
            expect(allRowsData[1][1]).toBe(1);
            expect(allRowsData[1][2]).toBe("NULL");
            expect(allRowsData[1][3]).toBe("NULL");
            expect(allRowsData[1][4]).toBe("NULL");
            expect(allRowsData[1][5]).toBe("NULL");
            expect(allRowsData[1][6]).toBe("NULL");
            expect(allRowsData[1][7]).toBe("NULL");
            expect(allRowsData[1][8]).toBe("NULL");
            expect(allRowsData[1][9]).toBe(22);

            expect(allRowsData[2][0]).toBe("beijing");
            expect(allRowsData[2][1]).toBe(1);
            expect(allRowsData[2][2]).toBe("");
            expect(allRowsData[2][3]).toBe("");
            expect(allRowsData[2][4]).toBe("0.000000");
            expect(allRowsData[2][5]).toBe("0.0000000000");
            expect(allRowsData[2][6]).toBeInstanceOf(ArrayBuffer);
            expect((allRowsData[2][6] as ArrayBuffer).byteLength).toBe(0);
            expect(allRowsData[2][7]).toBe("NULL");
            expect(allRowsData[2][8]).toBeInstanceOf(ArrayBuffer);
            expect((allRowsData[2][8] as ArrayBuffer).byteLength).toBe(0);
            expect(allRowsData[2][9]).toBe(33);
        } finally {
            if (allRows) {
                await allRows.close();
            }
            if (allRowsStmt) {
                await allRowsStmt.close();
            }
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

function createMockWsClient() {
    return {
        getState: jest.fn(() => 1),
        connect: jest.fn(async () => { }),
        checkVersion: jest.fn(async () => { }),
        exec: jest.fn(async () => ({ totalTime: 0, msg: { code: 0, message: "", timing: 0, stmt_id: 1 } })),
        execNoResp: jest.fn(async () => { }),
        sendBinaryMsg: jest.fn(async () => ({ totalTime: 0, msg: { code: 0, message: "", timing: 0, stmt_id: 1 } })),
        waitForReady: jest.fn(async () => { }),
        isNetworkError: jest.fn((_err: unknown) => false),
        getReconnectRetries: jest.fn(() => 5),
    };
}

function createInsertFields(): StmtFieldInfo[] {
    return [
        {
            name: "tbname",
            field_type: TDengineTypeCode.VARCHAR,
            precision: 0,
            scale: 0,
            bytes: 0,
            bind_type: FieldBindType.TAOS_FIELD_TBNAME,
        },
        {
            name: "location",
            field_type: TDengineTypeCode.VARCHAR,
            precision: 0,
            scale: 0,
            bytes: 0,
            bind_type: FieldBindType.TAOS_FIELD_TAG,
        },
        {
            name: "gid",
            field_type: TDengineTypeCode.INT,
            precision: 0,
            scale: 0,
            bytes: 0,
            bind_type: FieldBindType.TAOS_FIELD_TAG,
        },
        {
            name: "d64",
            field_type: TDengineTypeCode.DECIMAL64,
            precision: 0,
            scale: 0,
            bytes: 0,
            bind_type: FieldBindType.TAOS_FIELD_COL,
        },
        {
            name: "v",
            field_type: TDengineTypeCode.INT,
            precision: 0,
            scale: 0,
            bytes: 0,
            bind_type: FieldBindType.TAOS_FIELD_COL,
        },
    ];
}

function createBareStmt(fields: StmtFieldInfo[], isInsert: boolean = true) {
    const stmt = new (WsStmt2 as any)(createMockWsClient());
    stmt._stmt_id = 1n;
    stmt.fields = fields;
    stmt._isInsert = isInsert;
    stmt._toBeBindTableNameIndex = 0;
    stmt._toBeBindTagCount = fields.filter(
        (f) => f.bind_type === FieldBindType.TAOS_FIELD_TAG
    ).length;
    stmt._toBeBindColCount = fields.filter(
        (f) => f.bind_type === FieldBindType.TAOS_FIELD_COL
    ).length;
    return stmt;
}

describe("Stmt2 decimal bind behavior (mock)", () => {
    test("stmt1 setDecimal should throw unsupported error", () => {
        const params = new Stmt1BindParams();
        expect(() => {
            params.setDecimal(["1.2300"]);
        }).toThrow("setDecimal is not supported in stmt1, please use stmt2 instead!");
    });

    test("stmt2 setDecimal should reject non-string values", () => {
        const params = new Stmt2BindParams();
        expect(() => {
            params.setDecimal([]);
        }).toThrow("SetDecimalColumn params is invalid!");

        expect(() => {
            params.setDecimal([123]);
        }).toThrow("SetDecimalColumn params is invalid!");
    });

    test("stmt2 setDecimal should be encoded as variable-length data", () => {
        const params = new Stmt2BindParams();
        params.setDecimal(["1.2300", null, "-0.0001"]);
        params.encode();
        const cols = params.getParams();
        expect(cols.length).toBe(1);
        expect(cols[0].type).toBe(TDengineTypeCode.DECIMAL);
        expect(cols[0]._haveLength).toBe(1);
        expect(cols[0]._rows).toBe(3);
    });

    test("bind should override DECIMAL to real column decimal type in full-binding insert", async () => {
        const fields = createInsertFields();
        const stmt = createBareStmt(fields, true);
        const params = new Stmt2BindParams(fields.length, 13, fields);

        params.setVarchar(["d0"]);
        params.setVarchar(["beijing"]);
        params.setInt([1]);
        params.setDecimal(["123.456700"]);
        params.setInt([10]);

        await stmt.bind(params);

        const tableInfo = stmt._stmtTableInfo.get("d0");
        const colParams = tableInfo?.getParams();
        expect(colParams?._fieldParams?.length).toBe(2);
        expect(colParams?._fieldParams?.[0].columnType).toBe(
            TDengineTypeCode.DECIMAL64
        );
    });

    test("bind should normalize full-binding decimal params in place", async () => {
        const fields = createInsertFields();
        const stmt = createBareStmt(fields, true);
        const params = new Stmt2BindParams(fields.length, 13, fields);

        params.setVarchar(["d0"]);
        params.setVarchar(["beijing"]);
        params.setInt([1]);
        params.setDecimal(["123.456700"]);
        params.setInt([10]);

        await stmt.bind(params);

        expect(params._fieldParams?.[3].columnType).toBe(TDengineTypeCode.DECIMAL64);

        const tableInfo = stmt._stmtTableInfo.get("d0");
        const colParams = tableInfo?.getParams();
        expect(colParams?._fieldParams?.[0].columnType).toBe(TDengineTypeCode.DECIMAL64);
    });

    test("bind should override DECIMAL to real column decimal type in non-full-binding insert", async () => {
        const fields = createInsertFields();
        const stmt = createBareStmt(fields, true);
        const params = new Stmt2BindParams();

        params.setDecimal(["-0.000001"]);
        params.setInt([11]);

        await stmt.bind(params);

        const bindParams = stmt._currentTableInfo.getParams();
        expect(bindParams).toBe(params);
        expect(bindParams?._fieldParams?.[0].columnType).toBe(
            TDengineTypeCode.DECIMAL64
        );
        expect(bindParams?._fieldParams?.[1].columnType).toBe(
            TDengineTypeCode.INT
        );
    });

    test("bind should still allow setDecimal after in-place normalization", async () => {
        const fields = createInsertFields();
        const stmt = createBareStmt(fields, true);
        const params = new Stmt2BindParams(fields.length, 13, fields);

        params.setVarchar(["d0"]);
        params.setVarchar(["beijing"]);
        params.setInt([1]);
        params.setDecimal(["123.456700"]);
        params.setInt([10]);

        await stmt.bind(params);

        expect(params._fieldParams?.[3].columnType).toBe(TDengineTypeCode.DECIMAL64);

        params.setVarchar(["d0"]);
        params.setVarchar(["beijing"]);
        params.setInt([1]);
        expect(() => {
            params.setDecimal(["223.456700"]);
        }).not.toThrow();
        params.setInt([11]);

        expect(params._fieldParams?.[3].params.length).toBe(2);
    });

    test("query mode should keep DECIMAL default type", async () => {
        const fields = createInsertFields();
        const stmt = createBareStmt(fields, false);
        const params = new Stmt2BindParams();
        params.setDecimal(["99.0001"]);

        await stmt.bind(params);

        const bindParams = stmt._currentTableInfo.getParams();
        expect(bindParams?._fieldParams?.[0].columnType).toBe(
            TDengineTypeCode.DECIMAL
        );
    });
});

const decimalBoundaryRows = [
    {
        tsOffset: 0,
        dec64: "-999999999999.999999",
        dec: "-99999999999999999999.9999999999",
        c1: 1,
    },
    {
        tsOffset: 1,
        dec64: "-0.000001",
        dec: "-0.0000000001",
        c1: 2,
    },
    {
        tsOffset: 2,
        dec64: "0.000000",
        dec: "0.0000000000",
        c1: 3,
    },
    {
        tsOffset: 3,
        dec64: "0.000001",
        dec: "0.0000000001",
        c1: 4,
    },
    {
        tsOffset: 4,
        dec64: "999999999999.999999",
        dec: "99999999999999999999.9999999999",
        c1: 5,
    },
];

async function insertBoundaryRowsWithStmt2(ws: WsSql, db: string): Promise<void> {
    let stmt: any = null;
    const tsBase = 1700000001000;

    await ws.exec(`drop database if exists ${db}`);
    await ws.exec(`create database ${db}`);
    await ws.exec(`use ${db}`);
    await ws.exec(
        "create stable stb (" +
        "ts timestamp, dec64 decimal(18, 6), decv decimal(30, 10), c1 int) " +
        "tags (location binary(64))"
    );

    try {
        stmt = await ws.stmtInit();
        expect(stmt).toBeInstanceOf(WsStmt2);
        await stmt.prepare("insert into ? using stb tags (?) values (?, ?, ?, ?)");
        await stmt.setTableName("d0");

        const tagParams = stmt.newStmtParam();
        tagParams.setBinary(["beijing"]);
        await stmt.setTags(tagParams);

        const bindParams = new Stmt2BindParams();
        bindParams.setTimestamp(decimalBoundaryRows.map((row) => BigInt(tsBase + row.tsOffset)));
        bindParams.setDecimal(decimalBoundaryRows.map((row) => row.dec64));
        bindParams.setDecimal(decimalBoundaryRows.map((row) => row.dec));
        bindParams.setInt(decimalBoundaryRows.map((row) => row.c1));

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        expect(stmt.getLastAffected()).toBe(decimalBoundaryRows.length);
    } finally {
        if (stmt) {
            await stmt.close();
        }
    }
}

async function queryWithStmt2DecimalParams(
    ws: WsSql,
    sql: string,
    decimalParams: string[]
): Promise<Array<Array<any>>> {
    let stmt: any = null;
    let rows: any = null;

    try {
        stmt = await ws.stmtInit();
        expect(stmt).toBeInstanceOf(WsStmt2);
        await stmt.prepare(sql);

        const params = new Stmt2BindParams();
        for (const param of decimalParams) {
            params.setDecimal([param]);
        }

        await stmt.bind(params);
        await stmt.exec();

        rows = await stmt.resultSet();
        const data: Array<Array<any>> = [];
        while (await rows.next()) {
            const row = rows.getData();
            if (row) {
                data.push([...row]);
            }
        }
        return data;
    } finally {
        if (rows) {
            await rows.close();
        }
        if (stmt) {
            await stmt.close();
        }
    }
}

describe("TDWebSocket.Stmt2.DECIMAL boundary query compare", () => {
    jest.setTimeout(20 * 1000);

    testNon3360("stmt2 insert decimal and decimal64 should preserve boundary values", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776153787";

        try {
            await insertBoundaryRowsWithStmt2(ws, db);

            const result = await ws.exec("select dec64, decv, c1 from d0 order by c1");
            const data = result.getData();
            expect(data).toBeTruthy();
            if (!data) {
                throw new Error("retrieve empty result");
            }

            expect(data.length).toBe(decimalBoundaryRows.length);
            for (let i = 0; i < decimalBoundaryRows.length; i++) {
                expect(data[i][0]).toBe(decimalBoundaryRows[i].dec64);
                expect(data[i][1]).toBe(decimalBoundaryRows[i].dec);
                expect(data[i][2]).toBe(decimalBoundaryRows[i].c1);
            }
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 query decimal and decimal64 with decimal params", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776153836";

        try {
            await insertBoundaryRowsWithStmt2(ws, db);

            const queryData = await queryWithStmt2DecimalParams(
                ws,
                "select dec64, decv, c1 from d0 where dec64 >= ? and decv >= ? order by c1",
                ["0.000000", "0.0000000000"]
            );

            expect(queryData.length).toBe(3);
            expect(queryData[0][0]).toBe("0.000000");
            expect(queryData[0][1]).toBe("0.0000000000");
            expect(queryData[0][2]).toBe(3);
            expect(queryData[1][0]).toBe("0.000001");
            expect(queryData[1][1]).toBe("0.0000000001");
            expect(queryData[1][2]).toBe(4);
            expect(queryData[2][0]).toBe("999999999999.999999");
            expect(queryData[2][1]).toBe("99999999999999999999.9999999999");
            expect(queryData[2][2]).toBe(5);
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });

    testNon3360("stmt2 query decimal boundary compare should be strict for >= and >", async () => {
        const conf = new WSConfig("ws://localhost:6041");
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const ws = await WsSql.open(conf);
        const db = "test_1776153838";

        try {
            await insertBoundaryRowsWithStmt2(ws, db);

            const geDec64Rows = await queryWithStmt2DecimalParams(
                ws,
                "select c1 from d0 where dec64 >= ? order by c1",
                ["0.000000"]
            );
            const gtDec64Rows = await queryWithStmt2DecimalParams(
                ws,
                "select c1 from d0 where dec64 > ? order by c1",
                ["0.0000000000"]
            );
            const geDecRows = await queryWithStmt2DecimalParams(
                ws,
                "select c1 from d0 where decv >= ? order by c1",
                ["0.0000000000"]
            );
            const gtDecRows = await queryWithStmt2DecimalParams(
                ws,
                "select c1 from d0 where decv > ? order by c1",
                ["0.0000000000"]
            );

            expect(geDec64Rows.map((row) => row[0])).toEqual([3, 4, 5]);
            expect(gtDec64Rows.map((row) => row[0])).toEqual([4, 5]);
            expect(geDecRows.map((row) => row[0])).toEqual([3, 4, 5]);
            expect(gtDecRows.map((row) => row[0])).toEqual([4, 5]);
        } finally {
            try {
                await ws.exec(`drop database if exists ${db}`);
            } finally {
                await ws.close();
            }
        }
    });
});

afterAll(() => {
    WebSocketConnectionPool.instance().destroyed();
});

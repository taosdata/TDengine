import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { setLevel } from "@src/common/log";
import { WsSql } from "@src/sql/wsSql";
import { WsStmt2 } from "@src/stmt/wsStmt2";
import {
    createBaseSTable,
    createSTableJSON,
    getInsertBind,
    testPassword,
    testUsername,
} from "@test-helpers/utils";

const stable = "ws_stmt_stb";
const table = "stmt_001";
const db = "ws_stmt2";
const createDB = `create database if not exists ${db} keep 3650`;
const useDB = `use ${db}`;
const dropDB = `drop database if exists ${db}`;

const tableCN = "stmt_cn";
const stableCN = "ws_stmt_stb_cn";
const jsonTable = "stmt_json";
const jsonTableCN = "stmt_json_cn";

const stableTags = [
    true,
    -1,
    -2,
    -3,
    -4,
    1,
    2,
    3,
    4,
    parseFloat((3.1415).toFixed(5)),
    parseFloat((3.14159265).toFixed(15)),
    "varchar_tag_1",
    "nchar_tag_1",
];
const stableCNTags = [
    false,
    -1 * 2,
    -2 * 2,
    -3 * 2,
    -4 * 2,
    1 * 2,
    2 * 2,
    3 * 2,
    4 * 2,
    parseFloat((3.1415 * 2).toFixed(5)),
    parseFloat((3.14159265 * 2).toFixed(15)),
    "varchar_标签_壹",
    "nchar_标签_贰",
];

const tableValues = [
    [1656677710000, 1656677720000, 1656677730000, 1656677740000, 1656677750000],
    [0, -1, -2, -3, -4],
    [-1, -2, -3, -4, -5],
    [-3, -4, -5, -6, -7],

    // [0, 1, 2, 3, 4],
    [BigInt(-2), BigInt(-3), BigInt(-4), BigInt(-5), BigInt(-6)],

    [0, 1, 2, 3, 4],
    [1, 2, 3, 4, 5],
    [2, 3, 4, 5, 6],

    // [0, 1, 2, 3, 4],
    [BigInt(3), BigInt(4), BigInt(5), BigInt(6), BigInt(7)],

    [
        parseFloat((3.1415).toFixed(5)),
        parseFloat((3.1415 * 2).toFixed(5)),
        parseFloat((3.1415 * 3).toFixed(5)),
        parseFloat((3.1415 * 4).toFixed(5)),
        parseFloat((3.1415 * 5).toFixed(5)),
    ],
    [
        parseFloat((3.14159265).toFixed(15)),
        parseFloat((3.14159265 * 2).toFixed(15)),
        parseFloat((3.14159265 * 3).toFixed(15)),
        parseFloat((3.14159265 * 4).toFixed(15)),
        parseFloat((3.14159265 * 5).toFixed(15)),
    ],
    [
        "varchar_col_1",
        "varchar_col_2",
        "varchar_col_3",
        "varchar_col_4",
        "varchar_col_5",
    ],
    ["nchar_col_1", "nchar_col_2", "", "nchar_col_4", "nchar_col_5"],
    [true, false, true, false, true],
    [null, null, null, null, null],
];

const tableCNValues = [
    [
        BigInt(1656677760000),
        BigInt(1656677770000),
        BigInt(1656677780000),
        BigInt(1656677790000),
        BigInt(1656677100000),
    ],
    [0, -1, -2, -3, -4],
    [-1, -2, -3, -4, -5],
    [-2, -3, -4, -5, -6],
    [BigInt(-3), BigInt(-4), BigInt(-5), BigInt(-6), BigInt(-7)],
    [0, 1, 2, 3, 4],
    [1, 2, 3, 4, 5],
    [2, 3, 4, 5, 6],
    [BigInt(3), BigInt(4), BigInt(5), BigInt(6), BigInt(7)],
    [
        parseFloat((3.1415).toFixed(5)),
        parseFloat((3.1415 * 2).toFixed(5)),
        parseFloat((3.1415 * 3).toFixed(5)),
        parseFloat((3.1415 * 4).toFixed(5)),
        parseFloat((3.1415 * 5).toFixed(5)),
    ],
    [
        parseFloat((3.14159265).toFixed(15)),
        parseFloat((3.14159265 * 2).toFixed(15)),
        parseFloat((3.14159265 * 3).toFixed(15)),
        parseFloat((3.14159265 * 4).toFixed(15)),
        parseFloat((3.14159265 * 5).toFixed(15)),
    ],
    [
        "varchar_列_壹",
        "varchar_列_贰",
        "varchar_列_叁",
        "varchar_列_肆",
        "varchar_列_伍",
    ],
    ["nchar_列_甲", "nchar_列_乙", "nchar_列_丙", "nchar_列_丁", "nchar_列_戊"],
    [true, false, true, false, true],
    [null, null, null, null, null],
];

let geoDataArray: any[] = [];
let varbinary: any[] = [];
const encoder = new TextEncoder();
for (let i = 0; i < 5; i++) {
    let data = new Uint8Array([
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59,
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
    ]);
    geoDataArray.push(data.buffer);
    let vdata = encoder.encode(`varchar_col_${i + 1}`);
    varbinary.push(vdata.buffer);
}

const jsonTags = [
    '{"key1":"taos","key2":null,"key3":"TDengine","key4":0,"key5":false}',
];
const jsonTagsCN = [
    '{"key1":"taosdata","key2":null,"key3":"TDengine涛思数据","key4":1,"key5":true}',
];

const selectStable = `select * from ${stable}`;
const selectStableCN = `select * from ${stableCN}`;
const selectTable = `select * from ${table}`;
const selectTableCN = `select * from ${tableCN}`;
const selectJsonTable = `select * from ${jsonTable}`;
const selectJsonTableCN = `select * from ${jsonTableCN}`;

let dsn = `ws://${testUsername()}:${testPassword()}@localhost:6041`;
setLevel("debug");
beforeAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    let ws = await WsSql.open(conf);
    await ws.exec(dropDB);
    await ws.exec(createDB);
    await ws.exec(useDB);
    await ws.exec(createBaseSTable(stable));
    await ws.exec(createSTableJSON(jsonTable));
    await ws.close();
});

describe("TDWebSocket.Stmt()", () => {
    jest.setTimeout(20 * 1000);
    test("normal BindParam", async () => {
        let wsConf = new WSConfig(dsn);
        wsConf.setDb(db);
        let connector = await WsSql.open(wsConf);
        let stmt = await connector.stmtInit();
        expect(stmt).toBeTruthy();
        expect(stmt).toBeInstanceOf(WsStmt2);
        expect(connector.state()).toBeGreaterThan(0);
        await stmt.prepare(
            getInsertBind(tableValues.length + 2, stableTags.length, db, stable)
        );
        await stmt.setTableName(table);
        let tagParams = stmt.newStmtParam();
        tagParams.setBoolean([stableTags[0]]);
        tagParams.setTinyInt([stableTags[1]]);
        tagParams.setSmallInt([stableTags[2]]);
        tagParams.setInt([stableTags[3]]);
        tagParams.setBigint([BigInt(stableTags[4])]);
        tagParams.setUTinyInt([stableTags[5]]);

        tagParams.setUSmallInt([stableTags[6]]);
        tagParams.setUInt([stableTags[7]]);
        tagParams.setUBigint([BigInt(stableTags[8])]);
        tagParams.setFloat([stableTags[9]]);
        tagParams.setDouble([stableTags[10]]);
        tagParams.setBinary([stableTags[11]]);
        tagParams.setNchar([stableTags[12]]);
        await stmt.setTags(tagParams);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(tableValues[0]);
        bindParams.setTinyInt(tableValues[1]);
        bindParams.setSmallInt(tableValues[2]);
        bindParams.setInt(tableValues[3]);
        bindParams.setBigint(tableValues[4]);
        bindParams.setUTinyInt(tableValues[5]);
        bindParams.setUSmallInt(tableValues[6]);
        bindParams.setUInt(tableValues[7]);
        bindParams.setUBigint(tableValues[8]);
        bindParams.setFloat(tableValues[9]);
        bindParams.setDouble(tableValues[10]);

        bindParams.setBinary(tableValues[11]);
        bindParams.setNchar(tableValues[12]);
        bindParams.setBoolean(tableValues[13]);
        bindParams.setInt(tableValues[14]);
        bindParams.setGeometry(geoDataArray);
        bindParams.setVarBinary(varbinary);

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        expect(stmt.getLastAffected()).toEqual(5);
        await stmt.close();
        let result = await connector.exec(`select * from ${db}.${stable}`);
        console.log(result);
        await connector.close();
    });

    test("normal CN BindParam", async () => {
        let wsConf = new WSConfig(dsn);
        wsConf.setDb(db);
        let connector = await WsSql.open(wsConf);
        let stmt = await (await connector).stmtInit();
        expect(stmt).toBeTruthy();
        expect(stmt).toBeInstanceOf(WsStmt2);
        expect(connector.state()).toBeGreaterThan(0);
        await stmt.prepare(
            getInsertBind(tableValues.length + 2, stableTags.length, db, stable)
        );
        await stmt.setTableName(table);
        let tagParams = stmt.newStmtParam();
        tagParams.setBoolean([stableCNTags[0]]);
        tagParams.setTinyInt([stableCNTags[1]]);
        tagParams.setSmallInt([stableCNTags[2]]);
        tagParams.setInt([stableCNTags[3]]);
        tagParams.setBigint([BigInt(stableCNTags[4])]);
        tagParams.setUTinyInt([stableCNTags[5]]);

        tagParams.setUSmallInt([stableCNTags[6]]);
        tagParams.setUInt([stableCNTags[7]]);
        tagParams.setUBigint([BigInt(stableCNTags[8])]);
        tagParams.setFloat([stableCNTags[9]]);
        tagParams.setDouble([stableCNTags[10]]);
        tagParams.setBinary([stableCNTags[11]]);
        tagParams.setNchar([stableCNTags[12]]);
        await stmt.setTags(tagParams);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(tableCNValues[0]);
        bindParams.setTinyInt(tableCNValues[1]);
        bindParams.setSmallInt(tableCNValues[2]);
        bindParams.setInt(tableCNValues[3]);
        bindParams.setBigint(tableCNValues[4]);
        bindParams.setUTinyInt(tableCNValues[5]);
        bindParams.setUSmallInt(tableCNValues[6]);
        bindParams.setUInt(tableCNValues[7]);
        bindParams.setUBigint(tableCNValues[8]);
        bindParams.setFloat(tableCNValues[9]);
        bindParams.setDouble(tableCNValues[10]);

        bindParams.setBinary(tableCNValues[11]);
        bindParams.setNchar(tableCNValues[12]);
        bindParams.setBoolean(tableCNValues[13]);
        bindParams.setInt(tableCNValues[14]);
        bindParams.setGeometry(geoDataArray);
        bindParams.setVarBinary(varbinary);

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        expect(stmt.getLastAffected()).toEqual(5);
        await stmt.close();
        let result = await connector.exec(
            `select count(*) from ${db}.${stable}`
        );
        console.log(result);
        await connector.close();
    });

    test("normal json tag BindParam", async () => {
        let wsConf = new WSConfig(dsn);
        wsConf.setDb(db);
        let connector = await WsSql.open(wsConf);
        let stmt = await (await connector).stmtInit();
        expect(stmt).toBeTruthy();
        expect(stmt).toBeInstanceOf(WsStmt2);
        expect(connector.state()).toBeGreaterThan(0);
        await stmt.prepare(
            getInsertBind(
                tableValues.length + 2,
                jsonTags.length,
                db,
                jsonTable
            )
        );
        await stmt.setTableName(`${jsonTable}_001`);
        let tagParams = stmt.newStmtParam();
        tagParams.setJson(jsonTags);
        await stmt.setTags(tagParams);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(tableCNValues[0]);
        bindParams.setTinyInt(tableCNValues[1]);
        bindParams.setSmallInt(tableCNValues[2]);
        bindParams.setInt(tableCNValues[3]);
        bindParams.setBigint(tableCNValues[4]);
        bindParams.setUTinyInt(tableCNValues[5]);
        bindParams.setUSmallInt(tableCNValues[6]);
        bindParams.setUInt(tableCNValues[7]);
        bindParams.setUBigint(tableCNValues[8]);
        bindParams.setFloat(tableCNValues[9]);
        bindParams.setDouble(tableCNValues[10]);

        bindParams.setBinary(tableCNValues[11]);
        bindParams.setNchar(tableCNValues[12]);
        bindParams.setBoolean(tableCNValues[13]);
        bindParams.setInt(tableCNValues[14]);
        bindParams.setGeometry(geoDataArray);
        bindParams.setVarBinary(varbinary);

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        expect(stmt.getLastAffected()).toEqual(5);
        await stmt.close();
        let result = await connector.exec(`select * from ${db}.${jsonTable}`);
        console.log(result);
        await connector.close();
    });

    test("normal json cn tag BindParam", async () => {
        let wsConf = new WSConfig(dsn);
        wsConf.setDb(db);
        let connector = await WsSql.open(wsConf);
        let stmt = await connector.stmtInit();
        expect(stmt).toBeTruthy();
        expect(stmt).toBeInstanceOf(WsStmt2);
        expect(connector.state()).toBeGreaterThan(0);
        await stmt.prepare(
            getInsertBind(
                tableValues.length + 2,
                jsonTags.length,
                db,
                jsonTable
            )
        );
        await stmt.setTableName(`${jsonTable}_001`);
        let tagParams = stmt.newStmtParam();
        tagParams.setJson(jsonTagsCN);
        await stmt.setTags(tagParams);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(tableCNValues[0]);
        bindParams.setTinyInt(tableCNValues[1]);
        bindParams.setSmallInt(tableCNValues[2]);
        bindParams.setInt(tableCNValues[3]);
        bindParams.setBigint(tableCNValues[4]);
        bindParams.setUTinyInt(tableCNValues[5]);
        bindParams.setUSmallInt(tableCNValues[6]);
        bindParams.setUInt(tableCNValues[7]);
        bindParams.setUBigint(tableCNValues[8]);
        bindParams.setFloat(tableCNValues[9]);
        bindParams.setDouble(tableCNValues[10]);

        bindParams.setBinary(tableCNValues[11]);
        bindParams.setNchar(tableCNValues[12]);
        bindParams.setBoolean(tableCNValues[13]);
        bindParams.setInt(tableCNValues[14]);
        bindParams.setGeometry(geoDataArray);
        bindParams.setVarBinary(varbinary);

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        expect(stmt.getLastAffected()).toEqual(5);
        await stmt.close();
        let result = await connector.exec(`select * from ${db}.${jsonTable}`);
        console.log(result);
        await connector.close();
    });
});

test("test bind exception cases", async () => {
    let wsConf = new WSConfig(dsn);
    let connector = await WsSql.open(wsConf);
    let stmt = await connector.stmtInit();
    expect(stmt).toBeInstanceOf(WsStmt2);
    const params = stmt.newStmtParam();

    const emptyArrayMethods = [
        { method: "setBoolean", name: "SetBooleanColumn" },
        { method: "setTinyInt", name: "SetTinyIntColumn" },
        { method: "setUTinyInt", name: "SetUTinyIntColumn" },
        { method: "setSmallInt", name: "SetSmallIntColumn" },
        { method: "setUSmallInt", name: "SetSmallIntColumn" },
        { method: "setInt", name: "SetIntColumn" },
        { method: "setUInt", name: "SetUIntColumn" },
        { method: "setBigint", name: "SetBigIntColumn" },
        { method: "setUBigint", name: "SetUBigIntColumn" },
        { method: "setFloat", name: "SetFloatColumn" },
        { method: "setDouble", name: "SetDoubleColumn" },
        { method: "setTimestamp", name: "SeTimestampColumn" },
    ];

    emptyArrayMethods.forEach(({ method, name }) => {
        expect(() => {
            (params as any)[method]([]);
        }).toThrow(`${name} params is invalid!`);

        expect(() => {
            (params as any)[method](null);
        }).toThrow(`${name} params is invalid!`);

        expect(() => {
            (params as any)[method](undefined);
        }).toThrow(`${name} params is invalid!`);
    });

    expect(() => {
        params.setBoolean(["not boolean"]);
        params.encode();
    }).toThrow("SetTinyIntColumn params is invalid!");

    expect(() => {
        params.setTinyInt(["not number"]);
        params.encode();
    }).toThrow("SetTinyIntColumn params is invalid!");

    expect(() => {
        params.setBigint(["not bigint"]);
        params.encode();
    }).toThrow("SetTinyIntColumn params is invalid!");
    await connector.close();
});

afterAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    let ws = await WsSql.open(conf);
    await ws.exec(dropDB);
    await ws.close();
    WebSocketConnectionPool.instance().destroyed();
});

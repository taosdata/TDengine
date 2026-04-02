import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import {
    compareUint8Arrays,
    createSTable,
    createSTableJSON,
    createTable,
    expectStableData,
    hexToBytes,
    insertNTable,
    insertStable,
    jsonMeta,
    tableMeta,
    tagMeta,
    testPassword,
    testUsername,
} from "@test-helpers/utils";

let dsn = `ws://${testUsername()}:${testPassword()}@localhost:6041`;
let conf: WSConfig = new WSConfig(dsn);
const resultMap: Map<string, any> = new Map();
resultMap.set(
    "POINT (4.0 8.0)",
    hexToBytes("010100000000000000000010400000000000002040")
);
resultMap.set(
    "POINT (3.0 5.0)",
    hexToBytes("010100000000000000000008400000000000001440")
);
resultMap.set(
    "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)",
    hexToBytes(
        "010200000003000000000000000000f03f000000000000f03f0000000000000040000000000000004000000000000014400000000000001440"
    )
);
resultMap.set(
    "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))",
    hexToBytes(
        "010300000001000000050000000000000000000840000000000000184000000000000014400000000000001840000000000000144000000000000020400000000000000840000000000000204000000000000008400000000000001840"
    )
);
resultMap.set(
    "POINT (7.0 9.0)",
    hexToBytes("01010000000000000000001c400000000000002240")
);
resultMap.set("0x7661726332", hexToBytes("307837363631373236333332"));
resultMap.set("0x7661726333", hexToBytes("307837363631373236333333"));
resultMap.set("0x7661726334", hexToBytes("307837363631373236333334"));
resultMap.set("0x7661726335", hexToBytes("307837363631373236333335"));

const table = "ws_q_n";
const stable = "ws_q_s";
const tableCN = "ws_q_n_cn";
const stableCN = "ws_q_s_cn";
const db = "ws_q_db";
const jsonTable = "ws_q_j";
const jsonTableCN = "ws_q_j_cn";

const createDB = `create database if not exists ${db} keep 3650`;
const dropDB = `drop database if exists ${db}`;
const useDB = `use ${db}`;

const stableTags = [
    true,
    -1,
    -2,
    -3,
    BigInt(-4),
    1,
    2,
    3,
    BigInt(4),
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
    BigInt(-4 * 2),
    1 * 2,
    2 * 2,
    3 * 2,
    BigInt(4 * 2),
    parseFloat((3.1415 * 2).toFixed(5)),
    parseFloat((3.14159265 * 2).toFixed(15)),
    "varchar_标签_壹",
    "nchar_标签_贰",
];

const tableValues = [
    [
        BigInt(1656677710000),
        0,
        -1,
        -2,
        BigInt(-3),
        0,
        1,
        2,
        BigInt(3),
        parseFloat((3.1415).toFixed(5)),
        parseFloat((3.14159265).toFixed(15)),
        "varchar_col_1",
        "nchar_col_1",
        true,
        "NULL",
        "POINT (4.0 8.0)",
        "0x7661726332",
    ],
    [
        BigInt(1656677720000),
        -1,
        -2,
        -3,
        BigInt(-4),
        1,
        2,
        3,
        BigInt(4),
        parseFloat((3.1415 * 2).toFixed(5)),
        parseFloat((3.14159265 * 2).toFixed(15)),
        "varchar_col_2",
        "nchar_col_2",
        false,
        "NULL",
        "POINT (3.0 5.0)",
        "0x7661726333",
    ],
    [
        BigInt(1656677730000),
        -2,
        -3,
        -4,
        BigInt(-5),
        2,
        3,
        4,
        BigInt(5),
        parseFloat((3.1415 * 3).toFixed(5)),
        parseFloat((3.14159265 * 3).toFixed(15)),
        "varchar_col_3",
        "nchar_col_3",
        true,
        "NULL",
        "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)",
        "0x7661726334",
    ],
    [
        BigInt(1656677740000),
        -3,
        -4,
        -5,
        BigInt(-6),
        3,
        4,
        5,
        BigInt(6),
        parseFloat((3.1415 * 4).toFixed(5)),
        parseFloat((3.14159265 * 4).toFixed(15)),
        "varchar_col_4",
        "nchar_col_4",
        false,
        "NULL",
        "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))",
        "0x7661726335",
    ],
    [
        BigInt(1656677750000),
        -4,
        -5,
        -6,
        BigInt(-7),
        4,
        5,
        6,
        BigInt(7),
        parseFloat((3.1415 * 5).toFixed(5)),
        parseFloat((3.14159265 * 5).toFixed(15)),
        "varchar_col_5",
        "nchar_col_5",
        true,
        "NULL",
        "POINT (7.0 9.0)",
        "0x7661726335",
    ],
];

const tableCNValues = [
    [
        BigInt(1656677760000),
        0,
        -1,
        -2,
        BigInt(-3),
        0,
        1,
        2,
        BigInt(3),
        parseFloat((3.1415).toFixed(5)),
        parseFloat((3.14159265).toFixed(15)),
        "varchar_列_壹",
        "nchar_列_甲",
        true,
        "NULL",
        "POINT (4.0 8.0)",
        "0x7661726332",
    ],
    [
        BigInt(1656677770000),
        -1,
        -2,
        -3,
        BigInt(-4),
        1,
        2,
        3,
        BigInt(4),
        parseFloat((3.1415 * 2).toFixed(5)),
        parseFloat((3.14159265 * 2).toFixed(15)),
        "varchar_列_贰",
        "nchar_列_乙",
        false,
        "NULL",
        "POINT (3.0 5.0)",
        "0x7661726333",
    ],
    [
        BigInt(1656677780000),
        -2,
        -3,
        -4,
        BigInt(-5),
        2,
        3,
        4,
        BigInt(5),
        parseFloat((3.1415 * 3).toFixed(5)),
        parseFloat((3.14159265 * 3).toFixed(15)),
        "varchar_列_叁",
        "nchar_列_丙",
        true,
        "NULL",
        "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)",
        "0x7661726334",
    ],
    [
        BigInt(1656677790000),
        -3,
        -4,
        -5,
        BigInt(-6),
        3,
        4,
        5,
        BigInt(6),
        parseFloat((3.1415 * 4).toFixed(5)),
        parseFloat((3.14159265 * 4).toFixed(15)),
        "varchar_列_肆",
        "nchar_列_丁",
        false,
        "NULL",
        "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))",
        "0x7661726335",
    ],
    [
        BigInt(1656677800000),
        -4,
        -5,
        -6,
        BigInt(-7),
        4,
        5,
        6,
        BigInt(7),
        parseFloat((3.1415 * 5).toFixed(5)),
        parseFloat((3.14159265 * 5).toFixed(15)),
        "varchar_列_伍",
        "nchar_列_戊",
        true,
        "NULL",
        "POINT (7.0 9.0)",
        "0x7661726335",
    ],
];
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

beforeAll(async () => {
    let ws = await WsSql.open(conf);
    await ws.exec(dropDB);
    await ws.exec(createDB);
    await ws.exec(useDB);
    await ws.exec(createSTable(stable));
    await ws.exec(createSTable(stableCN));
    await ws.exec(createTable(table));
    await ws.exec(createTable(tableCN));
    await ws.exec(createSTableJSON(jsonTable));
    await ws.exec(createSTableJSON(jsonTableCN));
    await ws.close();
});

describe("ws.query(stable)", () => {
    jest.setTimeout(20 * 1000);
    test("Insert query stable without CN character", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insert = insertStable(tableValues, stableTags, stable);
        let insertRes = await ws.exec(insert);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectStable);
        let expectMeta = tableMeta.concat(tagMeta);
        let expectData = expectStableData(tableValues, stableTags);
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        let buffer: ArrayBuffer = resultMap.get(
                            expectData[i][index]
                        );
                        if (buffer) {
                            let dbData: ArrayBuffer = d;
                            console.log(
                                i,
                                index,
                                dbData,
                                expectData[i][index],
                                buffer,
                                compareUint8Arrays(
                                    new Uint8Array(dbData),
                                    new Uint8Array(buffer)
                                )
                            );
                            expect(
                                compareUint8Arrays(
                                    new Uint8Array(dbData),
                                    new Uint8Array(buffer)
                                )
                            ).toBe(true);
                        }
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });

    test("query stable with CN character", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insertCN = insertStable(tableCNValues, stableCNTags, stableCN);
        let insertRes = await ws.exec(insertCN);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectStableCN);

        let expectMeta = tableMeta.concat(tagMeta);
        let expectData = expectStableData(tableCNValues, stableCNTags);
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        let buffer: ArrayBuffer = resultMap.get(
                            expectData[i][index]
                        );
                        if (buffer) {
                            let dbData: ArrayBuffer = d;
                            console.log(
                                i,
                                index,
                                dbData,
                                expectData[i][index],
                                buffer,
                                compareUint8Arrays(
                                    new Uint8Array(dbData),
                                    new Uint8Array(buffer)
                                )
                            );
                            expect(
                                compareUint8Arrays(
                                    new Uint8Array(dbData),
                                    new Uint8Array(buffer)
                                )
                            ).toBe(true);
                        }
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });
});

describe("ws.query(table)", () => {
    test("Insert query normal table without CN character", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insert = insertNTable(tableValues, table);
        let insertRes = await ws.exec(insert);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectTable);

        let expectMeta = tableMeta;
        let expectData = tableValues;
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        expect(d).toBeTruthy();
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });

    test("Insert query normal table with CN character", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insertCN = insertNTable(tableCNValues, tableCN);
        let insertRes = await ws.exec(insertCN);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectTableCN);

        let expectMeta = tableMeta;
        let expectData = tableCNValues;
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        expect(d).toBeTruthy();
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });
});

describe("ws.query(jsonTable)", () => {
    test("Insert and query json data from table without CN", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insert = insertStable(tableValues, jsonTags, jsonTable);

        let insertRes = await ws.exec(insert);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectJsonTable);
        let expectMeta = tableMeta.concat(jsonMeta);
        let expectData = expectStableData(tableValues, jsonTags);
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                //   console.log(meta);
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        expect(d).toBeTruthy();
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });

    test("Insert and query json data from table with CN", async () => {
        let ws = await WsSql.open(conf);
        await ws.exec(useDB);
        let insert = insertStable(tableCNValues, jsonTagsCN, jsonTableCN);

        let insertRes = await ws.exec(insert);
        expect(insertRes.getAffectRows()).toBe(5);

        let queryRes = await ws.exec(selectJsonTableCN);
        let expectMeta = tableMeta.concat(jsonMeta);
        let expectData = expectStableData(tableCNValues, jsonTagsCN);
        let actualMeta = queryRes.getMeta();
        let actualData = queryRes.getData();
        await ws.close();
        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                //   console.log(meta);
                expect(meta.name).toBe(expectMeta[index].name);
                expect(meta.type).toBe(expectMeta[index].type);
                expect(meta.length).toBe(expectMeta[index].length);
            });

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    if (
                        expectMeta[index].name == "geo" ||
                        expectMeta[index].name == "vbinary"
                    ) {
                        expect(d).toBeTruthy();
                    } else {
                        expect(d).toBe(expectData[i][index]);
                    }
                });
            }
        } else {
            throw new Error("retrieve empty result");
        }
    });
});

afterAll(async () => {
    let ws = await WsSql.open(conf);
    await ws.exec(dropDB);
    await ws.close();
    WebSocketConnectionPool.instance().destroyed();
});

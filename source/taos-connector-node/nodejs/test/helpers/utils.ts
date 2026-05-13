import logger from "@src/common/log";
import { TDengineMeta } from "@src/common/taosResult";

export function getInsertBind(
    valuesLen: number,
    tagsLen: number,
    db: string,
    stable: string
): string {
    let sql = `insert into ? using ${db}.${stable} tags ( ?`;

    for (let i = 1; i < tagsLen; i++) {
        sql += ", ?";
    }

    sql += ") values( ?";
    for (let i = 1; i < valuesLen; i++) {
        sql += ", ?";
    }
    sql += ")";
    logger.debug(`Insert bind sql: ${sql}`);
    return sql;
}

export function insertStable(
    values: Array<Array<any>>,
    tags: Array<any>,
    stable: string,
    table: string = "empty"
): string {
    let childTable = table == "empty" ? stable + "_s_01" : table;
    let sql = `insert into ${childTable} using ${stable} tags (`;
    tags.forEach((tag) => {
        if (typeof tag == "string") {
            if (tag == "NULL") {
                sql += tag + ",";
            } else {
                sql += `\'${tag}\',`;
            }
        } else {
            sql += tag;
            sql += ",";
        }
    });

    sql = sql.slice(0, sql.length - 1);
    sql += ")";

    sql += "values";
    values.forEach((value) => {
        sql += "(";
        value.forEach((v) => {
            if (typeof v == "string") {
                sql += `\'${v}\',`;
            } else {
                sql += v;
                sql += ",";
            }
        });
        sql = sql.slice(0, sql.length - 1);
        sql += ")";
    });
    return sql;
}

export function insertNTable(values: Array<Array<any>>, table: string): string {
    let sql = `insert into ${table} values `;
    values.forEach((value) => {
        sql += "(";
        value.forEach((v) => {
            if (typeof v == "string") {
                if (v == "NULL") {
                    sql += v + ",";
                } else {
                    sql += `\'${v}\',`;
                }
            } else {
                sql += v;
                sql += ",";
            }
        });
        sql = sql.slice(0, sql.length - 1);
        sql += ")";
    });

    return sql;
}

export const tableMeta: Array<TDengineMeta> = [
    {
        name: "ts",
        type: "TIMESTAMP",
        length: 8,
    },
    {
        name: "i1",
        type: "TINYINT",
        length: 1,
    },
    {
        name: "i2",
        type: "SMALLINT",
        length: 2,
    },
    {
        name: "i4",
        type: "INT",
        length: 4,
    },
    {
        name: "i8",
        type: "BIGINT",
        length: 8,
    },
    {
        name: "u1",
        type: "TINYINT UNSIGNED",
        length: 1,
    },
    {
        name: "u2",
        type: "SMALLINT UNSIGNED",
        length: 2,
    },
    {
        name: "u4",
        type: "INT UNSIGNED",
        length: 4,
    },
    {
        name: "u8",
        type: "BIGINT UNSIGNED",
        length: 8,
    },
    {
        name: "f4",
        type: "FLOAT",
        length: 4,
    },
    {
        name: "d8",
        type: "DOUBLE",
        length: 8,
    },
    {
        name: "bnr",
        type: "VARCHAR",
        length: 200,
    },
    {
        name: "nchr",
        type: "NCHAR",
        length: 200,
    },
    {
        name: "b",
        type: "BOOL",
        length: 1,
    },
    {
        name: "nilcol",
        type: "INT",
        length: 4,
    },
    {
        name: "geo",
        type: "GEOMETRY",
        length: 512,
    },
    {
        name: "vbinary",
        type: "VARBINARY",
        length: 32,
    },
];

export const jsonMeta: Array<TDengineMeta> = [
    {
        name: "json_tag",
        type: "JSON",
        length: 4095,
    },
];

export const tagMeta: Array<TDengineMeta> = [
    {
        name: "tb",
        type: "BOOL",
        length: 1,
    },
    {
        name: "ti1",
        type: "TINYINT",
        length: 1,
    },
    {
        name: "ti2",
        type: "SMALLINT",
        length: 2,
    },
    {
        name: "ti4",
        type: "INT",
        length: 4,
    },
    {
        name: "ti8",
        type: "BIGINT",
        length: 8,
    },
    {
        name: "tu1",
        type: "TINYINT UNSIGNED",
        length: 1,
    },
    {
        name: "tu2",
        type: "SMALLINT UNSIGNED",
        length: 2,
    },
    {
        name: "tu4",
        type: "INT UNSIGNED",
        length: 4,
    },
    {
        name: "tu8",
        type: "BIGINT UNSIGNED",
        length: 8,
    },
    {
        name: "tf4",
        type: "FLOAT",
        length: 4,
    },
    {
        name: "td8",
        type: "DOUBLE",
        length: 8,
    },
    {
        name: "tbnr",
        type: "VARCHAR",
        length: 200,
    },
    {
        name: "tnchr",
        type: "NCHAR",
        length: 200,
    },
];

export function createBaseSTable(stable: string): string {
    return (
        `create table if not exists ${stable}( ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int, g1 geometry(512), c4 varbinary(100))` +
        "tags( tb bool,ti1 tinyint,ti2 smallint,ti4 int,ti8 bigint,tu1 tinyint unsigned,tu2 smallint unsigned,tu4 int unsigned,tu8 bigint unsigned,tf4 float,td8 double,tbnr binary(200),tnchr nchar(200));"
    );
}
export function createBaseSTableJSON(stable: string): string {
    return (
        `create table if not exists ${stable}(ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int)` +
        "tags(json_tag json);"
    );
}
export function createBaseTable(table: string): string {
    return `create table if not exists ${table}(ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int)`;
}

export function createSTable(stable: string): string {
    return (
        `create table if not exists ${stable}( ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int, geo geometry(512), vbinary varbinary(32))` +
        "tags( tb bool,ti1 tinyint,ti2 smallint,ti4 int,ti8 bigint,tu1 tinyint unsigned,tu2 smallint unsigned,tu4 int unsigned,tu8 bigint unsigned,tf4 float,td8 double,tbnr binary(200),tnchr nchar(200));"
    );
}
export function createSTableJSON(stable: string): string {
    return (
        `create table if not exists ${stable}(ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int, geo geometry(512), vbinary varbinary(32))` +
        "tags(json_tag json);"
    );
}
export function createTable(table: string): string {
    return `create table if not exists ${table}(ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,f4 float,d8 double,bnr binary(200),nchr nchar(200),b bool,nilcol int, geo geometry(512), vbinary varbinary(32))`;
}

export function expectStableData(
    rows: Array<Array<any>>,
    tags: Array<any>
): Array<Array<any>> {
    let resArr: Array<Array<any>> = [];
    rows.forEach((row, index, rows) => {
        resArr.push(row.concat(tags));
    });
    return resArr;
}

export function hexToBytes(hex: string): ArrayBuffer {
    let byteLen = hex.length / 2;
    let a = new Uint8Array(byteLen);
    for (let i = 0, count = 0; i < hex.length; i += 2, count++) {
        let item = parseInt(hex.slice(i, i + 2), 16);
        a[count] = item;
    }
    return a.buffer;
}

export function compareUint8Arrays(
    arr1: Uint8Array,
    arr2: Uint8Array
): boolean {
    if (arr1.length !== arr2.length) {
        logger.debug(`${arr1.length} !== ${arr2.length}`);
        return false;
    }
    for (let i = 0; i < arr1.length; i++) {
        if (arr1[i] !== arr2[i]) {
            logger.debug(`${arr1[i]} !== ${arr2[i]}`);
            return false;
        }
    }
    return true;
}

export function Sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function testUsername(): string {
    return process.env.TDENGINE_TEST_USERNAME || "root";
}

export function testPassword(): string {
    return process.env.TDENGINE_TEST_PASSWORD || "taosdata";
}

export function testCloudUrl(): string | undefined {
    const cloudUrl = process.env.TDENGINE_CLOUD_URL?.trim();
    return cloudUrl && cloudUrl.length > 0 ? cloudUrl : undefined;
}

export function testCloudToken(): string | undefined {
    const cloudToken = process.env.TDENGINE_CLOUD_TOKEN?.trim();
    return cloudToken && cloudToken.length > 0 ? cloudToken : undefined;
}

export function testCloudWsUrl(): string | undefined {
    const cloudUrl = testCloudUrl();
    const cloudToken = testCloudToken();
    if (!cloudUrl || !cloudToken) {
        return undefined;
    }
    return `wss://${cloudUrl}?token=${cloudToken}`;
}

export function hasTestCloudEnv(): boolean {
    return !!testCloudWsUrl();
}

export const testCloud = hasTestCloudEnv() ? test : test.skip;
export const testEnterprise = process.env.TEST_ENTERPRISE?.toLowerCase() === "true" ? test : test.skip;
export const testNon3360 = process.env.TEST_3360?.toLowerCase() === "true" ? test.skip : test;

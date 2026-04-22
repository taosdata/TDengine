import pkg from "../../package.json";

export interface IndexableString {
    [index: number]: string;
}

export interface StringIndexable {
    [index: string]: number;
}

export interface NumberIndexable {
    [index: number]: number;
}

export const ConnectorInfo = `nodejs-ws-v${pkg.version}-ncid000`;
export const BinaryQueryMessage: bigint = BigInt(6);
export const FetchRawBlockMessage: bigint = BigInt(7);
export const MinStmt2Version: string = "3.3.6.0";
export const TDengineTypeName: IndexableString = {
    0: "NULL",
    1: "BOOL",
    2: "TINYINT",
    3: "SMALLINT",
    4: "INT",
    5: "BIGINT",
    6: "FLOAT",
    7: "DOUBLE",
    8: "VARCHAR",
    9: "TIMESTAMP",
    10: "NCHAR",
    11: "TINYINT UNSIGNED",
    12: "SMALLINT UNSIGNED",
    13: "INT UNSIGNED",
    14: "BIGINT UNSIGNED",
    15: "JSON",
    16: "VARBINARY",
    17: "DECIMAL",
    18: "BLOB",
    20: "GEOMETRY",
    21: "DECIMAL64",
};

export const ColumnsBlockType: StringIndexable = {
    SOLID: 0,
    VARCHAR: 1,
    NCHAR: 2,
    GEOMETRY: 3,
    VARBINARY: 4,
    BLOB: 5,
};

export enum TDengineTypeCode {
    NULL = 0,
    BOOL = 1,
    TINYINT = 2,
    SMALLINT = 3,
    INT = 4,
    BIGINT = 5,
    FLOAT = 6,
    DOUBLE = 7,
    BINARY = 8,
    VARCHAR = 8,
    TIMESTAMP = 9,
    NCHAR = 10,
    TINYINT_UNSIGNED = 11,
    SMALLINT_UNSIGNED = 12,
    INT_UNSIGNED = 13,
    BIGINT_UNSIGNED = 14,
    JSON = 15,
    VARBINARY = 16,
    DECIMAL = 17,
    BLOB = 18,
    GEOMETRY = 20,
    DECIMAL64 = 21,
}

export enum TSDB_OPTION_CONNECTION {
    TSDB_OPTION_CONNECTION_CHARSET,  // charset, Same as the scope supported by the system
    TSDB_OPTION_CONNECTION_TIMEZONE, // timezone, Same as the scope supported by the system
    TSDB_OPTION_CONNECTION_USER_IP,  // user ip
    TSDB_OPTION_CONNECTION_USER_APP, // user app
}

export enum FieldBindType {
    TAOS_FIELD_COL = 1,
    TAOS_FIELD_TAG = 2,
    TAOS_FIELD_QUERY = 3,
    TAOS_FIELD_TBNAME = 4,
}

export const TDenginePrecision: IndexableString = {
    0: "MILLISECOND",
    1: "MICROSECOND",
    2: "NANOSECOND",
};

export const TDengineTypeLength: NumberIndexable = {
    [TDengineTypeCode.BOOL]: 1,
    [TDengineTypeCode.TINYINT]: 1,
    [TDengineTypeCode.SMALLINT]: 2,
    [TDengineTypeCode.INT]: 4,
    [TDengineTypeCode.BIGINT]: 8,
    [TDengineTypeCode.FLOAT]: 4,
    [TDengineTypeCode.DOUBLE]: 8,
    [TDengineTypeCode.TIMESTAMP]: 8,
    [TDengineTypeCode.TINYINT_UNSIGNED]: 1,
    [TDengineTypeCode.SMALLINT_UNSIGNED]: 2,
    [TDengineTypeCode.INT_UNSIGNED]: 4,
    [TDengineTypeCode.BIGINT_UNSIGNED]: 8,
    [TDengineTypeCode.DECIMAL]: 16,
    [TDengineTypeCode.DECIMAL64]: 8,
};

export const PrecisionLength: StringIndexable = {
    ms: 13,
    us: 16,
    ns: 19,
};

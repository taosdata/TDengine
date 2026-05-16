interface IndexableString {
    [index: number]: string
}

interface StringIndexable {
    [index: string]: number
}

export const TDengineTypeName: IndexableString = {
    0: 'NULL',
    1: 'BOOL',
    2: 'TINYINT',
    3: 'SMALLINT',
    4: 'INT',
    5: 'BIGINT',
    6: 'FLOAT',
    7: 'DOUBLE',
    8: 'VARCHAR',
    9: 'TIMESTAMP',
    10: 'NCHAR',
    11: 'TINYINT UNSIGNED',
    12: 'SMALLINT UNSIGNED',
    13: 'INT UNSIGNED',
    14: 'BIGINT UNSIGNED',
    15: 'JSON',
}

export const ColumnsBlockType: StringIndexable = {
    'SOLID': 0,
    'VARCHAR': 1,
    'NCHAR': 2
}


export const TDengineTypeCode: StringIndexable = {
    'NULL': 0,
    'BOOL': 1,
    'TINYINT': 2,
    'SMALLINT': 3,
    'INT': 4,
    'BIGINT': 5,
    'FLOAT': 6,
    'DOUBLE': 7,
    'BINARY': 8,
    'VARCHAR': 8,
    'TIMESTAMP': 9,
    'NCHAR': 10,
    'TINYINT UNSIGNED': 11,
    'SMALLINT UNSIGNED': 12,
    'INT UNSIGNED': 13,
    'BIGINT UNSIGNED': 14,
    'JSON': 15,
}

export const TDenginePrecision: IndexableString = {
    0: 'MILLISECOND',
    1: "MICROSECOND",
    2: "NANOSECOND",
}

export const TDengineTypeLength: StringIndexable = {
    'BOOL': 1,
    'TINYINT': 1,
    'SMALLINT': 2,
    'INT': 4,
    'BIGINT': 8,
    'FLOAT': 4,
    'DOUBLE': 8,
    'TIMESTAMP': 8,
    'TINYINT UNSIGNED': 1,
    'SMALLINT UNSIGNED': 2,
    'INT UNSIGNED': 4,
    'BIGINT UNSIGNED': 8,
}
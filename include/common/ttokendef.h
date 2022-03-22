/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_COMMON_TOKEN_H_
#define _TD_COMMON_TOKEN_H_

#define TK_OR                               1
#define TK_AND                              2
#define TK_UNION                            3
#define TK_ALL                              4
#define TK_MINUS                            5
#define TK_EXCEPT                           6
#define TK_INTERSECT                        7
#define TK_NK_BITAND                        8
#define TK_NK_BITOR                         9
#define TK_NK_LSHIFT                       10
#define TK_NK_RSHIFT                       11
#define TK_NK_PLUS                         12
#define TK_NK_MINUS                        13
#define TK_NK_STAR                         14
#define TK_NK_SLASH                        15
#define TK_NK_REM                          16
#define TK_NK_CONCAT                       17
#define TK_CREATE                          18
#define TK_ACCOUNT                         19
#define TK_NK_ID                           20
#define TK_PASS                            21
#define TK_NK_STRING                       22
#define TK_ALTER                           23
#define TK_PPS                             24
#define TK_TSERIES                         25
#define TK_STORAGE                         26
#define TK_STREAMS                         27
#define TK_QTIME                           28
#define TK_DBS                             29
#define TK_USERS                           30
#define TK_CONNS                           31
#define TK_STATE                           32
#define TK_USER                            33
#define TK_PRIVILEGE                       34
#define TK_DROP                            35
#define TK_DNODE                           36
#define TK_PORT                            37
#define TK_NK_INTEGER                      38
#define TK_DNODES                          39
#define TK_NK_IPTOKEN                      40
#define TK_LOCAL                           41
#define TK_QNODE                           42
#define TK_ON                              43
#define TK_DATABASE                        44
#define TK_USE                             45
#define TK_IF                              46
#define TK_NOT                             47
#define TK_EXISTS                          48
#define TK_BLOCKS                          49
#define TK_CACHE                           50
#define TK_CACHELAST                       51
#define TK_COMP                            52
#define TK_DAYS                            53
#define TK_FSYNC                           54
#define TK_MAXROWS                         55
#define TK_MINROWS                         56
#define TK_KEEP                            57
#define TK_PRECISION                       58
#define TK_QUORUM                          59
#define TK_REPLICA                         60
#define TK_TTL                             61
#define TK_WAL                             62
#define TK_VGROUPS                         63
#define TK_SINGLE_STABLE                   64
#define TK_STREAM_MODE                     65
#define TK_RETENTIONS                      66
#define TK_FILE_FACTOR                     67
#define TK_NK_FLOAT                        68
#define TK_TABLE                           69
#define TK_NK_LP                           70
#define TK_NK_RP                           71
#define TK_STABLE                          72
#define TK_ADD                             73
#define TK_COLUMN                          74
#define TK_MODIFY                          75
#define TK_RENAME                          76
#define TK_TAG                             77
#define TK_SET                             78
#define TK_NK_EQ                           79
#define TK_USING                           80
#define TK_TAGS                            81
#define TK_NK_DOT                          82
#define TK_NK_COMMA                        83
#define TK_COMMENT                         84
#define TK_BOOL                            85
#define TK_TINYINT                         86
#define TK_SMALLINT                        87
#define TK_INT                             88
#define TK_INTEGER                         89
#define TK_BIGINT                          90
#define TK_FLOAT                           91
#define TK_DOUBLE                          92
#define TK_BINARY                          93
#define TK_TIMESTAMP                       94
#define TK_NCHAR                           95
#define TK_UNSIGNED                        96
#define TK_JSON                            97
#define TK_VARCHAR                         98
#define TK_MEDIUMBLOB                      99
#define TK_BLOB                           100
#define TK_VARBINARY                      101
#define TK_DECIMAL                        102
#define TK_SMA                            103
#define TK_ROLLUP                         104
#define TK_SHOW                           105
#define TK_DATABASES                      106
#define TK_TABLES                         107
#define TK_STABLES                        108
#define TK_MNODES                         109
#define TK_MODULES                        110
#define TK_QNODES                         111
#define TK_FUNCTIONS                      112
#define TK_INDEXES                        113
#define TK_FROM                           114
#define TK_LIKE                           115
#define TK_INDEX                          116
#define TK_FULLTEXT                       117
#define TK_FUNCTION                       118
#define TK_INTERVAL                       119
#define TK_TOPIC                          120
#define TK_AS                             121
#define TK_NK_BOOL                        122
#define TK_NK_VARIABLE                    123
#define TK_BETWEEN                        124
#define TK_IS                             125
#define TK_NULL                           126
#define TK_NK_LT                          127
#define TK_NK_GT                          128
#define TK_NK_LE                          129
#define TK_NK_GE                          130
#define TK_NK_NE                          131
#define TK_MATCH                          132
#define TK_NMATCH                         133
#define TK_IN                             134
#define TK_JOIN                           135
#define TK_INNER                          136
#define TK_SELECT                         137
#define TK_DISTINCT                       138
#define TK_WHERE                          139
#define TK_PARTITION                      140
#define TK_BY                             141
#define TK_SESSION                        142
#define TK_STATE_WINDOW                   143
#define TK_SLIDING                        144
#define TK_FILL                           145
#define TK_VALUE                          146
#define TK_NONE                           147
#define TK_PREV                           148
#define TK_LINEAR                         149
#define TK_NEXT                           150
#define TK_GROUP                          151
#define TK_HAVING                         152
#define TK_ORDER                          153
#define TK_SLIMIT                         154
#define TK_SOFFSET                        155
#define TK_LIMIT                          156
#define TK_OFFSET                         157
#define TK_ASC                            158
#define TK_DESC                           159
#define TK_NULLS                          160
#define TK_FIRST                          161
#define TK_LAST                           162

#define TK_NK_SPACE                       300
#define TK_NK_COMMENT                     301
#define TK_NK_ILLEGAL                     302
#define TK_NK_HEX                         303   // hex number  0x123
#define TK_NK_OCT                         304   // oct number
#define TK_NK_BIN                         305   // bin format data 0b111
#define TK_NK_FILE                        306
#define TK_NK_QUESTION                    307   // denoting the placeholder of "?",when invoking statement bind query

#define TK_NK_COLON                       500
#define TK_NK_BITNOT                      501
#define TK_INSERT                         502
#define TK_INTO                           503
#define TK_NOW                            504
#define TK_VALUES                         507
#define TK_IMPORT                         507
#define TK_NK_SEMI                        508

#define TK_NK_NIL                         65535

#endif /*_TD_COMMON_TOKEN_H_*/

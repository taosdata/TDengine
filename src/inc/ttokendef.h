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

#ifndef TDENGINE_TTOKENDEF_H
#define TDENGINE_TTOKENDEF_H


#define TK_ID                               1
#define TK_BOOL                             2
#define TK_TINYINT                          3
#define TK_SMALLINT                         4
#define TK_INTEGER                          5
#define TK_BIGINT                           6
#define TK_FLOAT                            7
#define TK_DOUBLE                           8
#define TK_STRING                           9
#define TK_TIMESTAMP                       10
#define TK_BINARY                          11
#define TK_NCHAR                           12
#define TK_OR                              13
#define TK_AND                             14
#define TK_NOT                             15
#define TK_EQ                              16
#define TK_NE                              17
#define TK_ISNULL                          18
#define TK_NOTNULL                         19
#define TK_IS                              20
#define TK_LIKE                            21
#define TK_GLOB                            22
#define TK_BETWEEN                         23
#define TK_IN                              24
#define TK_GT                              25
#define TK_GE                              26
#define TK_LT                              27
#define TK_LE                              28
#define TK_BITAND                          29
#define TK_BITOR                           30
#define TK_LSHIFT                          31
#define TK_RSHIFT                          32
#define TK_PLUS                            33
#define TK_MINUS                           34
#define TK_DIVIDE                          35
#define TK_TIMES                           36
#define TK_STAR                            37
#define TK_SLASH                           38
#define TK_REM                             39
#define TK_CONCAT                          40
#define TK_UMINUS                          41
#define TK_UPLUS                           42
#define TK_BITNOT                          43
#define TK_SHOW                            44
#define TK_DATABASES                       45
#define TK_TOPICS                          46
#define TK_MNODES                          47
#define TK_DNODES                          48
#define TK_ACCOUNTS                        49
#define TK_USERS                           50
#define TK_MODULES                         51
#define TK_QUERIES                         52
#define TK_CONNECTIONS                     53
#define TK_STREAMS                         54
#define TK_VARIABLES                       55
#define TK_SCORES                          56
#define TK_GRANTS                          57
#define TK_VNODES                          58
#define TK_IPTOKEN                         59
#define TK_DOT                             60
#define TK_CREATE                          61
#define TK_TABLE                           62
#define TK_DATABASE                        63
#define TK_TABLES                          64
#define TK_STABLES                         65
#define TK_VGROUPS                         66
#define TK_DROP                            67
#define TK_STABLE                          68
#define TK_TOPIC                           69
#define TK_DNODE                           70
#define TK_USER                            71
#define TK_ACCOUNT                         72
#define TK_USE                             73
#define TK_DESCRIBE                        74
#define TK_ALTER                           75
#define TK_PASS                            76
#define TK_PRIVILEGE                       77
#define TK_LOCAL                           78
#define TK_IF                              79
#define TK_EXISTS                          80
#define TK_PPS                             81
#define TK_TSERIES                         82
#define TK_DBS                             83
#define TK_STORAGE                         84
#define TK_QTIME                           85
#define TK_CONNS                           86
#define TK_STATE                           87
#define TK_KEEP                            88
#define TK_CACHE                           89
#define TK_REPLICA                         90
#define TK_QUORUM                          91
#define TK_DAYS                            92
#define TK_MINROWS                         93
#define TK_MAXROWS                         94
#define TK_BLOCKS                          95
#define TK_CTIME                           96
#define TK_WAL                             97
#define TK_FSYNC                           98
#define TK_COMP                            99
#define TK_PRECISION                      100
#define TK_UPDATE                         101
#define TK_CACHELAST                      102
#define TK_PARTITIONS                     103
#define TK_LP                             104
#define TK_RP                             105
#define TK_UNSIGNED                       106
#define TK_TAGS                           107
#define TK_USING                          108
#define TK_COMMA                          109
#define TK_AS                             110
#define TK_NULL                           111
#define TK_SELECT                         112
#define TK_UNION                          113
#define TK_ALL                            114
#define TK_DISTINCT                       115
#define TK_FROM                           116
#define TK_VARIABLE                       117
#define TK_INTERVAL                       118
#define TK_SESSION                        119
#define TK_FILL                           120
#define TK_SLIDING                        121
#define TK_ORDER                          122
#define TK_BY                             123
#define TK_ASC                            124
#define TK_DESC                           125
#define TK_GROUP                          126
#define TK_HAVING                         127
#define TK_LIMIT                          128
#define TK_OFFSET                         129
#define TK_SLIMIT                         130
#define TK_SOFFSET                        131
#define TK_WHERE                          132
#define TK_NOW                            133
#define TK_RESET                          134
#define TK_QUERY                          135
#define TK_SYNCDB                         136
#define TK_ADD                            137
#define TK_COLUMN                         138
#define TK_TAG                            139
#define TK_CHANGE                         140
#define TK_SET                            141
#define TK_KILL                           142
#define TK_CONNECTION                     143
#define TK_STREAM                         144
#define TK_COLON                          145
#define TK_ABORT                          146
#define TK_AFTER                          147
#define TK_ATTACH                         148
#define TK_BEFORE                         149
#define TK_BEGIN                          150
#define TK_CASCADE                        151
#define TK_CLUSTER                        152
#define TK_CONFLICT                       153
#define TK_COPY                           154
#define TK_DEFERRED                       155
#define TK_DELIMITERS                     156
#define TK_DETACH                         157
#define TK_EACH                           158
#define TK_END                            159
#define TK_EXPLAIN                        160
#define TK_FAIL                           161
#define TK_FOR                            162
#define TK_IGNORE                         163
#define TK_IMMEDIATE                      164
#define TK_INITIALLY                      165
#define TK_INSTEAD                        166
#define TK_MATCH                          167
#define TK_KEY                            168
#define TK_OF                             169
#define TK_RAISE                          170
#define TK_REPLACE                        171
#define TK_RESTRICT                       172
#define TK_ROW                            173
#define TK_STATEMENT                      174
#define TK_TRIGGER                        175
#define TK_VIEW                           176
#define TK_SEMI                           177
#define TK_NONE                           178
#define TK_PREV                           179
#define TK_LINEAR                         180
#define TK_IMPORT                         181
#define TK_TBNAME                         182
#define TK_JOIN                           183
#define TK_INSERT                         184
#define TK_INTO                           185
#define TK_VALUES                         186






#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_FILE                           306
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



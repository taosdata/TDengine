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
#define TK_FUNCTIONS                       47
#define TK_MNODES                          48
#define TK_DNODES                          49
#define TK_ACCOUNTS                        50
#define TK_USERS                           51
#define TK_MODULES                         52
#define TK_QUERIES                         53
#define TK_CONNECTIONS                     54
#define TK_STREAMS                         55
#define TK_VARIABLES                       56
#define TK_SCORES                          57
#define TK_GRANTS                          58
#define TK_VNODES                          59
#define TK_IPTOKEN                         60
#define TK_DOT                             61
#define TK_CREATE                          62
#define TK_TABLE                           63
#define TK_DATABASE                        64
#define TK_TABLES                          65
#define TK_STABLES                         66
#define TK_VGROUPS                         67
#define TK_DROP                            68
#define TK_STABLE                          69
#define TK_TOPIC                           70
#define TK_FUNCTION                        71
#define TK_DNODE                           72
#define TK_USER                            73
#define TK_ACCOUNT                         74
#define TK_USE                             75
#define TK_DESCRIBE                        76
#define TK_ALTER                           77
#define TK_PASS                            78
#define TK_PRIVILEGE                       79
#define TK_LOCAL                           80
#define TK_IF                              81
#define TK_EXISTS                          82
#define TK_AS                              83
#define TK_OUTPUTTYPE                      84
#define TK_PPS                             85
#define TK_TSERIES                         86
#define TK_DBS                             87
#define TK_STORAGE                         88
#define TK_QTIME                           89
#define TK_CONNS                           90
#define TK_STATE                           91
#define TK_KEEP                            92
#define TK_CACHE                           93
#define TK_REPLICA                         94
#define TK_QUORUM                          95
#define TK_DAYS                            96
#define TK_MINROWS                         97
#define TK_MAXROWS                         98
#define TK_BLOCKS                          99
#define TK_CTIME                          100
#define TK_WAL                            101
#define TK_FSYNC                          102
#define TK_COMP                           103
#define TK_PRECISION                      104
#define TK_UPDATE                         105
#define TK_CACHELAST                      106
#define TK_PARTITIONS                     107
#define TK_LP                             108
#define TK_RP                             109
#define TK_UNSIGNED                       110
#define TK_TAGS                           111
#define TK_USING                          112
#define TK_COMMA                          113
#define TK_NULL                           114
#define TK_SELECT                         115
#define TK_UNION                          116
#define TK_ALL                            117
#define TK_DISTINCT                       118
#define TK_FROM                           119
#define TK_VARIABLE                       120
#define TK_INTERVAL                       121
#define TK_FILL                           122
#define TK_SLIDING                        123
#define TK_ORDER                          124
#define TK_BY                             125
#define TK_ASC                            126
#define TK_DESC                           127
#define TK_GROUP                          128
#define TK_HAVING                         129
#define TK_LIMIT                          130
#define TK_OFFSET                         131
#define TK_SLIMIT                         132
#define TK_SOFFSET                        133
#define TK_WHERE                          134
#define TK_NOW                            135
#define TK_RESET                          136
#define TK_QUERY                          137
#define TK_ADD                            138
#define TK_COLUMN                         139
#define TK_TAG                            140
#define TK_CHANGE                         141
#define TK_SET                            142
#define TK_KILL                           143
#define TK_CONNECTION                     144
#define TK_STREAM                         145
#define TK_COLON                          146
#define TK_ABORT                          147
#define TK_AFTER                          148
#define TK_ATTACH                         149
#define TK_BEFORE                         150
#define TK_BEGIN                          151
#define TK_CASCADE                        152
#define TK_CLUSTER                        153
#define TK_CONFLICT                       154
#define TK_COPY                           155
#define TK_DEFERRED                       156
#define TK_DELIMITERS                     157
#define TK_DETACH                         158
#define TK_EACH                           159
#define TK_END                            160
#define TK_EXPLAIN                        161
#define TK_FAIL                           162
#define TK_FOR                            163
#define TK_IGNORE                         164
#define TK_IMMEDIATE                      165
#define TK_INITIALLY                      166
#define TK_INSTEAD                        167
#define TK_MATCH                          168
#define TK_KEY                            169
#define TK_OF                             170
#define TK_RAISE                          171
#define TK_REPLACE                        172
#define TK_RESTRICT                       173
#define TK_ROW                            174
#define TK_STATEMENT                      175
#define TK_TRIGGER                        176
#define TK_VIEW                           177
#define TK_COUNT                          178
#define TK_SUM                            179
#define TK_AVG                            180
#define TK_MIN                            181
#define TK_MAX                            182
#define TK_FIRST                          183
#define TK_LAST                           184
#define TK_TOP                            185
#define TK_BOTTOM                         186
#define TK_STDDEV                         187
#define TK_PERCENTILE                     188
#define TK_APERCENTILE                    189
#define TK_LEASTSQUARES                   190
#define TK_HISTOGRAM                      191
#define TK_DIFF                           192
#define TK_SPREAD                         193
#define TK_TWA                            194
#define TK_INTERP                         195
#define TK_LAST_ROW                       196
#define TK_RATE                           197
#define TK_IRATE                          198
#define TK_SUM_RATE                       199
#define TK_SUM_IRATE                      200
#define TK_AVG_RATE                       201
#define TK_AVG_IRATE                      202
#define TK_TBID                           203
#define TK_SEMI                           204
#define TK_NONE                           205
#define TK_PREV                           206
#define TK_LINEAR                         207
#define TK_IMPORT                         208
#define TK_METRIC                         209
#define TK_TBNAME                         210
#define TK_JOIN                           211
#define TK_METRICS                        212
#define TK_INSERT                         213
#define TK_INTO                           214
#define TK_VALUES                         215












#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_FILE                           306
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



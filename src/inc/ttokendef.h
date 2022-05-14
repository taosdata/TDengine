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
#define TK_JSON                            13
#define TK_OR                              14
#define TK_AND                             15
#define TK_NOT                             16
#define TK_EQ                              17
#define TK_NE                              18
#define TK_ISNULL                          19
#define TK_NOTNULL                         20
#define TK_IS                              21
#define TK_LIKE                            22
#define TK_MATCH                           23
#define TK_NMATCH                          24
#define TK_CONTAINS                        25
#define TK_GLOB                            26
#define TK_BETWEEN                         27
#define TK_IN                              28
#define TK_GT                              29
#define TK_GE                              30
#define TK_LT                              31
#define TK_LE                              32
#define TK_BITAND                          33
#define TK_BITOR                           34
#define TK_LSHIFT                          35
#define TK_RSHIFT                          36
#define TK_PLUS                            37
#define TK_MINUS                           38
#define TK_DIVIDE                          39
#define TK_TIMES                           40
#define TK_STAR                            41
#define TK_SLASH                           42
#define TK_REM                             43
#define TK_UMINUS                          44
#define TK_UPLUS                           45
#define TK_BITNOT                          46
#define TK_ARROW                           47
#define TK_SHOW                            48
#define TK_DATABASES                       49
#define TK_TOPICS                          50
#define TK_FUNCTIONS                       51
#define TK_MNODES                          52
#define TK_DNODES                          53
#define TK_ACCOUNTS                        54
#define TK_USERS                           55
#define TK_MODULES                         56
#define TK_QUERIES                         57
#define TK_CONNECTIONS                     58
#define TK_STREAMS                         59
#define TK_VARIABLES                       60
#define TK_SCORES                          61
#define TK_GRANTS                          62
#define TK_VNODES                          63
#define TK_DOT                             64
#define TK_CREATE                          65
#define TK_TABLE                           66
#define TK_STABLE                          67
#define TK_DATABASE                        68
#define TK_TABLES                          69
#define TK_STABLES                         70
#define TK_VGROUPS                         71
#define TK_DROP                            72
#define TK_TOPIC                           73
#define TK_FUNCTION                        74
#define TK_DNODE                           75
#define TK_USER                            76
#define TK_ACCOUNT                         77
#define TK_USE                             78
#define TK_DESCRIBE                        79
#define TK_DESC                            80
#define TK_ALTER                           81
#define TK_PASS                            82
#define TK_PRIVILEGE                       83
#define TK_LOCAL                           84
#define TK_COMPACT                         85
#define TK_LP                              86
#define TK_RP                              87
#define TK_IF                              88
#define TK_EXISTS                          89
#define TK_AS                              90
#define TK_OUTPUTTYPE                      91
#define TK_AGGREGATE                       92
#define TK_BUFSIZE                         93
#define TK_PPS                             94
#define TK_TSERIES                         95
#define TK_DBS                             96
#define TK_STORAGE                         97
#define TK_QTIME                           98
#define TK_CONNS                           99
#define TK_STATE                          100
#define TK_COMMA                          101
#define TK_KEEP                           102
#define TK_CACHE                          103
#define TK_REPLICA                        104
#define TK_QUORUM                         105
#define TK_DAYS                           106
#define TK_MINROWS                        107
#define TK_MAXROWS                        108
#define TK_BLOCKS                         109
#define TK_CTIME                          110
#define TK_WAL                            111
#define TK_FSYNC                          112
#define TK_COMP                           113
#define TK_PRECISION                      114
#define TK_UPDATE                         115
#define TK_CACHELAST                      116
#define TK_PARTITIONS                     117
#define TK_UNSIGNED                       118
#define TK_TAGS                           119
#define TK_USING                          120
#define TK_TO                             121
#define TK_SPLIT                          122
#define TK_NULL                           123
#define TK_NOW                            124
#define TK_VARIABLE                       125
#define TK_SELECT                         126
#define TK_UNION                          127
#define TK_ALL                            128
#define TK_DISTINCT                       129
#define TK_FROM                           130
#define TK_RANGE                          131
#define TK_INTERVAL                       132
#define TK_EVERY                          133
#define TK_SESSION                        134
#define TK_STATE_WINDOW                   135
#define TK_FILL                           136
#define TK_SLIDING                        137
#define TK_ORDER                          138
#define TK_BY                             139
#define TK_ASC                            140
#define TK_GROUP                          141
#define TK_HAVING                         142
#define TK_LIMIT                          143
#define TK_OFFSET                         144
#define TK_SLIMIT                         145
#define TK_SOFFSET                        146
#define TK_WHERE                          147
#define TK_TODAY                          148
#define TK_RESET                          149
#define TK_QUERY                          150
#define TK_SYNCDB                         151
#define TK_ADD                            152
#define TK_COLUMN                         153
#define TK_MODIFY                         154
#define TK_TAG                            155
#define TK_CHANGE                         156
#define TK_SET                            157
#define TK_KILL                           158
#define TK_CONNECTION                     159
#define TK_STREAM                         160
#define TK_COLON                          161
#define TK_DELETE                         162
#define TK_ABORT                          163
#define TK_AFTER                          164
#define TK_ATTACH                         165
#define TK_BEFORE                         166
#define TK_BEGIN                          167
#define TK_CASCADE                        168
#define TK_CLUSTER                        169
#define TK_CONFLICT                       170
#define TK_COPY                           171
#define TK_DEFERRED                       172
#define TK_DELIMITERS                     173
#define TK_DETACH                         174
#define TK_EACH                           175
#define TK_END                            176
#define TK_EXPLAIN                        177
#define TK_FAIL                           178
#define TK_FOR                            179
#define TK_IGNORE                         180
#define TK_IMMEDIATE                      181
#define TK_INITIALLY                      182
#define TK_INSTEAD                        183
#define TK_KEY                            184
#define TK_OF                             185
#define TK_RAISE                          186
#define TK_REPLACE                        187
#define TK_RESTRICT                       188
#define TK_ROW                            189
#define TK_STATEMENT                      190
#define TK_TRIGGER                        191
#define TK_VIEW                           192
#define TK_IPTOKEN                        193
#define TK_SEMI                           194
#define TK_NONE                           195
#define TK_PREV                           196
#define TK_LINEAR                         197
#define TK_IMPORT                         198
#define TK_TBNAME                         199
#define TK_JOIN                           200
#define TK_INSERT                         201
#define TK_INTO                           202
#define TK_VALUES                         203
#define TK_FILE                           204


#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



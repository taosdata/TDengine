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
#define TK_BITXOR                          35
#define TK_LSHIFT                          36
#define TK_RSHIFT                          37
#define TK_PLUS                            38
#define TK_MINUS                           39
#define TK_DIVIDE                          40
#define TK_TIMES                           41
#define TK_STAR                            42
#define TK_SLASH                           43
#define TK_REM                             44
#define TK_UMINUS                          45
#define TK_UPLUS                           46
#define TK_BITNOT                          47
#define TK_ARROW                           48
#define TK_SHOW                            49
#define TK_DATABASES                       50
#define TK_TOPICS                          51
#define TK_FUNCTIONS                       52
#define TK_MNODES                          53
#define TK_DNODES                          54
#define TK_ACCOUNTS                        55
#define TK_USERS                           56
#define TK_MODULES                         57
#define TK_QUERIES                         58
#define TK_CONNECTIONS                     59
#define TK_STREAMS                         60
#define TK_VARIABLES                       61
#define TK_SCORES                          62
#define TK_GRANTS                          63
#define TK_VNODES                          64
#define TK_DOT                             65
#define TK_CREATE                          66
#define TK_TABLE                           67
#define TK_STABLE                          68
#define TK_DATABASE                        69
#define TK_TABLES                          70
#define TK_STABLES                         71
#define TK_VGROUPS                         72
#define TK_ALIVE                           73
#define TK_CLUSTER                         74
#define TK_DROP                            75
#define TK_TOPIC                           76
#define TK_FUNCTION                        77
#define TK_DNODE                           78
#define TK_USER                            79
#define TK_ACCOUNT                         80
#define TK_USE                             81
#define TK_DESCRIBE                        82
#define TK_DESC                            83
#define TK_ALTER                           84
#define TK_PASS                            85
#define TK_PRIVILEGE                       86
#define TK_TAGS                            87
#define TK_LOCAL                           88
#define TK_COMPACT                         89
#define TK_LP                              90
#define TK_RP                              91
#define TK_IF                              92
#define TK_EXISTS                          93
#define TK_AS                              94
#define TK_OUTPUTTYPE                      95
#define TK_AGGREGATE                       96
#define TK_BUFSIZE                         97
#define TK_PPS                             98
#define TK_TSERIES                         99
#define TK_DBS                            100
#define TK_STORAGE                        101
#define TK_QTIME                          102
#define TK_CONNS                          103
#define TK_STATE                          104
#define TK_COMMA                          105
#define TK_KEEP                           106
#define TK_CACHE                          107
#define TK_REPLICA                        108
#define TK_QUORUM                         109
#define TK_DAYS                           110
#define TK_MINROWS                        111
#define TK_MAXROWS                        112
#define TK_BLOCKS                         113
#define TK_CTIME                          114
#define TK_WAL                            115
#define TK_FSYNC                          116
#define TK_COMP                           117
#define TK_PRECISION                      118
#define TK_UPDATE                         119
#define TK_CACHELAST                      120
#define TK_PARTITIONS                     121
#define TK_UNSIGNED                       122
#define TK_USING                          123
#define TK_TO                             124
#define TK_SPLIT                          125
#define TK_NULL                           126
#define TK_NOW                            127
#define TK_VARIABLE                       128
#define TK_SELECT                         129
#define TK_UNION                          130
#define TK_ALL                            131
#define TK_DISTINCT                       132
#define TK_FROM                           133
#define TK_RANGE                          134
#define TK_INTERVAL                       135
#define TK_EVERY                          136
#define TK_SESSION                        137
#define TK_STATE_WINDOW                   138
#define TK_FILL                           139
#define TK_SLIDING                        140
#define TK_ORDER                          141
#define TK_BY                             142
#define TK_ASC                            143
#define TK_GROUP                          144
#define TK_HAVING                         145
#define TK_LIMIT                          146
#define TK_OFFSET                         147
#define TK_SLIMIT                         148
#define TK_SOFFSET                        149
#define TK_WHERE                          150
#define TK_TODAY                          151
#define TK_RESET                          152
#define TK_QUERY                          153
#define TK_SYNCDB                         154
#define TK_ADD                            155
#define TK_COLUMN                         156
#define TK_MODIFY                         157
#define TK_TAG                            158
#define TK_CHANGE                         159
#define TK_SET                            160
#define TK_KILL                           161
#define TK_CONNECTION                     162
#define TK_STREAM                         163
#define TK_COLON                          164
#define TK_DELETE                         165
#define TK_ABORT                          166
#define TK_AFTER                          167
#define TK_ATTACH                         168
#define TK_BEFORE                         169
#define TK_BEGIN                          170
#define TK_CASCADE                        171
#define TK_CONFLICT                       172
#define TK_COPY                           173
#define TK_DEFERRED                       174
#define TK_DELIMITERS                     175
#define TK_DETACH                         176
#define TK_EACH                           177
#define TK_END                            178
#define TK_EXPLAIN                        179
#define TK_FAIL                           180
#define TK_FOR                            181
#define TK_IGNORE                         182
#define TK_IMMEDIATE                      183
#define TK_INITIALLY                      184
#define TK_INSTEAD                        185
#define TK_KEY                            186
#define TK_OF                             187
#define TK_RAISE                          188
#define TK_REPLACE                        189
#define TK_RESTRICT                       190
#define TK_ROW                            191
#define TK_STATEMENT                      192
#define TK_TRIGGER                        193
#define TK_VIEW                           194
#define TK_IPTOKEN                        195
#define TK_SEMI                           196
#define TK_NONE                           197
#define TK_PREV                           198
#define TK_LINEAR                         199
#define TK_IMPORT                         200
#define TK_TBNAME                         201
#define TK_JOIN                           202
#define TK_INSERT                         203
#define TK_INTO                           204
#define TK_VALUES                         205
#define TK_FILE                           206


#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



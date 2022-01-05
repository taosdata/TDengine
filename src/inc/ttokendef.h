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

#define TK_ID                              1
#define TK_BOOL                            2
#define TK_TINYINT                         3
#define TK_SMALLINT                        4
#define TK_INTEGER                         5
#define TK_BIGINT                          6
#define TK_FLOAT                           7
#define TK_DOUBLE                          8
#define TK_STRING                          9
#define TK_TIMESTAMP                      10
#define TK_BINARY                         11
#define TK_NCHAR                          12
#define TK_JSON                           13
#define TK_ABORT                          14
#define TK_AFTER                          15
#define TK_ASC                            16
#define TK_ATTACH                         17
#define TK_BEFORE                         18
#define TK_BEGIN                          19
#define TK_CASCADE                        20
#define TK_CLUSTER                        21
#define TK_CONFLICT                       22
#define TK_COPY                           23
#define TK_DATABASE                       24
#define TK_DEFERRED                       25
#define TK_DELIMITERS                     26
#define TK_DESC                           27
#define TK_DETACH                         28
#define TK_EACH                           29
#define TK_END                            30
#define TK_EXPLAIN                        31
#define TK_FAIL                           32
#define TK_FOR                            33
#define TK_GLOB                           34
#define TK_IGNORE                         35
#define TK_IMMEDIATE                      36
#define TK_INITIALLY                      37
#define TK_INSTEAD                        38
#define TK_LIKE                           39
#define TK_MATCH                          40
#define TK_NMATCH                         41
#define TK_KEY                            42
#define TK_OF                             43
#define TK_OFFSET                         44
#define TK_RAISE                          45
#define TK_REPLACE                        46
#define TK_RESTRICT                       47
#define TK_ROW                            48
#define TK_STATEMENT                      49
#define TK_TRIGGER                        50
#define TK_VIEW                           51
#define TK_ALL                            52
#define TK_NOW                            53
#define TK_IPTOKEN                        54
#define TK_SEMI                           55
#define TK_NONE                           56
#define TK_PREV                           57
#define TK_LINEAR                         58
#define TK_IMPORT                         59
#define TK_TBNAME                         60
#define TK_JOIN                           61
#define TK_STABLE                         62
#define TK_NULL                           63
#define TK_INSERT                         64
#define TK_INTO                           65
#define TK_VALUES                         66
#define TK_FILE                           67
#define TK_OR                             68
#define TK_AND                            69
#define TK_NOT                            70
#define TK_EQ                             71
#define TK_NE                             72
#define TK_ISNULL                         73
#define TK_NOTNULL                        74
#define TK_IS                             75
#define TK_CONTAINS                       76
#define TK_BETWEEN                        77
#define TK_IN                             78
#define TK_GT                             79
#define TK_GE                             80
#define TK_LT                             81
#define TK_LE                             82
#define TK_BITAND                         83
#define TK_BITOR                          84
#define TK_LSHIFT                         85
#define TK_RSHIFT                         86
#define TK_PLUS                           87
#define TK_MINUS                          88
#define TK_DIVIDE                         89
#define TK_TIMES                          90
#define TK_STAR                           91
#define TK_SLASH                          92
#define TK_REM                            93
#define TK_UMINUS                         94
#define TK_UPLUS                          95
#define TK_BITNOT                         96
#define TK_ARROW                          97
#define TK_SHOW                           98
#define TK_DATABASES                      99
#define TK_TOPICS                         100
#define TK_FUNCTIONS                      101
#define TK_MNODES                         102
#define TK_DNODES                         103
#define TK_ACCOUNTS                       104
#define TK_USERS                          105
#define TK_MODULES                        106
#define TK_QUERIES                        107
#define TK_CONNECTIONS                    108
#define TK_STREAMS                        109
#define TK_VARIABLES                      110
#define TK_SCORES                         111
#define TK_GRANTS                         112
#define TK_VNODES                         113
#define TK_DOT                            114
#define TK_BACKQUOTE                      115
#define TK_CREATE                         116
#define TK_TABLE                          117
#define TK_TABLES                         118
#define TK_STABLES                        119
#define TK_VGROUPS                        120
#define TK_DROP                           121
#define TK_TOPIC                          122
#define TK_FUNCTION                       123
#define TK_DNODE                          124
#define TK_USER                           125
#define TK_ACCOUNT                        126
#define TK_USE                            127
#define TK_DESCRIBE                       128
#define TK_ALTER                          129
#define TK_PASS                           130
#define TK_PRIVILEGE                      131
#define TK_LOCAL                          132
#define TK_COMPACT                        133
#define TK_LP                             134
#define TK_RP                             135
#define TK_IF                             136
#define TK_EXISTS                         137
#define TK_AS                             138
#define TK_OUTPUTTYPE                     139
#define TK_AGGREGATE                      140
#define TK_BUFSIZE                        141
#define TK_PPS                            142
#define TK_TSERIES                        143
#define TK_DBS                            144
#define TK_STORAGE                        145
#define TK_QTIME                          146
#define TK_CONNS                          147
#define TK_STATE                          148
#define TK_COMMA                          149
#define TK_KEEP                           150
#define TK_CACHE                          151
#define TK_REPLICA                        152
#define TK_QUORUM                         153
#define TK_DAYS                           154
#define TK_MINROWS                        155
#define TK_MAXROWS                        156
#define TK_BLOCKS                         157
#define TK_CTIME                          158
#define TK_WAL                            159
#define TK_FSYNC                          160
#define TK_COMP                           161
#define TK_PRECISION                      162
#define TK_UPDATE                         163
#define TK_CACHELAST                      164
#define TK_PARTITIONS                     165
#define TK_UNSIGNED                       166
#define TK_TAGS                           167
#define TK_USING                          168
#define TK_VARIABLE                       169
#define TK_SELECT                         170
#define TK_UNION                          171
#define TK_DISTINCT                       172
#define TK_FROM                           173
#define TK_RANGE                          174
#define TK_INTERVAL                       175
#define TK_EVERY                          176
#define TK_SESSION                        177
#define TK_STATE_WINDOW                   178
#define TK_FILL                           179
#define TK_SLIDING                        180
#define TK_ORDER                          181
#define TK_BY                             182
#define TK_GROUP                          183
#define TK_HAVING                         184
#define TK_LIMIT                          185
#define TK_SLIMIT                         186
#define TK_SOFFSET                        187
#define TK_WHERE                          188
#define TK_RESET                          189
#define TK_QUERY                          190
#define TK_SYNCDB                         191
#define TK_ADD                            192
#define TK_COLUMN                         193
#define TK_MODIFY                         194
#define TK_TAG                            195
#define TK_CHANGE                         196
#define TK_SET                            197
#define TK_KILL                           198
#define TK_CONNECTION                     199
#define TK_STREAM                         200
#define TK_COLON                          201


#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



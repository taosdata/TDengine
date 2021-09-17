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
#define TK_GLOB                            25
#define TK_BETWEEN                         26
#define TK_IN                              27
#define TK_GT                              28
#define TK_GE                              29
#define TK_LT                              30
#define TK_LE                              31
#define TK_BITAND                          32
#define TK_BITOR                           33
#define TK_LSHIFT                          34
#define TK_RSHIFT                          35
#define TK_PLUS                            36
#define TK_MINUS                           37
#define TK_DIVIDE                          38
#define TK_TIMES                           39
#define TK_STAR                            40
#define TK_SLASH                           41
#define TK_REM                             42
#define TK_CONCAT                          43
#define TK_UMINUS                          44
#define TK_UPLUS                           45
#define TK_BITNOT                          46
#define TK_QUESTION                        47
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
#define TK_DROP                            73
#define TK_TOPIC                           74
#define TK_FUNCTION                        75
#define TK_DNODE                           76
#define TK_USER                            77
#define TK_ACCOUNT                         78
#define TK_USE                             79
#define TK_DESCRIBE                        80
#define TK_DESC                            81
#define TK_ALTER                           82
#define TK_PASS                            83
#define TK_PRIVILEGE                       84
#define TK_LOCAL                           85
#define TK_COMPACT                         86
#define TK_LP                              87
#define TK_RP                              88
#define TK_IF                              89
#define TK_EXISTS                          90
#define TK_AS                              91
#define TK_OUTPUTTYPE                      92
#define TK_AGGREGATE                       93
#define TK_BUFSIZE                         94
#define TK_PPS                             95
#define TK_TSERIES                         96
#define TK_DBS                             97
#define TK_STORAGE                         98
#define TK_QTIME                           99
#define TK_CONNS                          100
#define TK_STATE                          101
#define TK_COMMA                          102
#define TK_KEEP                           103
#define TK_CACHE                          104
#define TK_REPLICA                        105
#define TK_QUORUM                         106
#define TK_DAYS                           107
#define TK_MINROWS                        108
#define TK_MAXROWS                        109
#define TK_BLOCKS                         110
#define TK_CTIME                          111
#define TK_WAL                            112
#define TK_FSYNC                          113
#define TK_COMP                           114
#define TK_PRECISION                      115
#define TK_UPDATE                         116
#define TK_CACHELAST                      117
#define TK_PARTITIONS                     118
#define TK_UNSIGNED                       119
#define TK_TAGS                           120
#define TK_USING                          121
#define TK_NULL                           122
#define TK_NOW                            123
#define TK_SELECT                         124
#define TK_UNION                          125
#define TK_ALL                            126
#define TK_DISTINCT                       127
#define TK_FROM                           128
#define TK_VARIABLE                       129
#define TK_INTERVAL                       130
#define TK_EVERY                          131
#define TK_SESSION                        132
#define TK_STATE_WINDOW                   133
#define TK_FILL                           134
#define TK_SLIDING                        135
#define TK_ORDER                          136
#define TK_BY                             137
#define TK_ASC                            138
#define TK_GROUP                          139
#define TK_HAVING                         140
#define TK_LIMIT                          141
#define TK_OFFSET                         142
#define TK_SLIMIT                         143
#define TK_SOFFSET                        144
#define TK_WHERE                          145
#define TK_RESET                          146
#define TK_QUERY                          147
#define TK_SYNCDB                         148
#define TK_ADD                            149
#define TK_COLUMN                         150
#define TK_MODIFY                         151
#define TK_TAG                            152
#define TK_CHANGE                         153
#define TK_SET                            154
#define TK_KILL                           155
#define TK_CONNECTION                     156
#define TK_STREAM                         157
#define TK_COLON                          158
#define TK_ABORT                          159
#define TK_AFTER                          160
#define TK_ATTACH                         161
#define TK_BEFORE                         162
#define TK_BEGIN                          163
#define TK_CASCADE                        164
#define TK_CLUSTER                        165
#define TK_CONFLICT                       166
#define TK_COPY                           167
#define TK_DEFERRED                       168
#define TK_DELIMITERS                     169
#define TK_DETACH                         170
#define TK_EACH                           171
#define TK_END                            172
#define TK_EXPLAIN                        173
#define TK_FAIL                           174
#define TK_FOR                            175
#define TK_IGNORE                         176
#define TK_IMMEDIATE                      177
#define TK_INITIALLY                      178
#define TK_INSTEAD                        179
#define TK_KEY                            180
#define TK_OF                             181
#define TK_RAISE                          182
#define TK_REPLACE                        183
#define TK_RESTRICT                       184
#define TK_ROW                            185
#define TK_STATEMENT                      186
#define TK_TRIGGER                        187
#define TK_VIEW                           188
#define TK_IPTOKEN                        189
#define TK_SEMI                           190
#define TK_NONE                           191
#define TK_PREV                           192
#define TK_LINEAR                         193
#define TK_IMPORT                         194
#define TK_TBNAME                         195
#define TK_JOIN                           196
#define TK_INSERT                         197
#define TK_INTO                           198
#define TK_VALUES                         199

#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_FILE                           306

#endif



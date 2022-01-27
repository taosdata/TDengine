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
#define TK_INTEGER                          3
#define TK_FLOAT                            4
#define TK_STRING                           5
#define TK_TIMESTAMP                        6
#define TK_OR                               7
#define TK_AND                              8
#define TK_NOT                              9
#define TK_EQ                              10
#define TK_NE                              11
#define TK_ISNULL                          12
#define TK_NOTNULL                         13
#define TK_IS                              14
#define TK_LIKE                            15
#define TK_MATCH                           16
#define TK_NMATCH                          17
#define TK_GLOB                            18
#define TK_BETWEEN                         19
#define TK_IN                              20
#define TK_GT                              21
#define TK_GE                              22
#define TK_LT                              23
#define TK_LE                              24
#define TK_BITAND                          25
#define TK_BITOR                           26
#define TK_LSHIFT                          27
#define TK_RSHIFT                          28
#define TK_PLUS                            29
#define TK_MINUS                           30
#define TK_DIVIDE                          31
#define TK_TIMES                           32
#define TK_STAR                            33
#define TK_SLASH                           34
#define TK_REM                             35
#define TK_CONCAT                          36
#define TK_UMINUS                          37
#define TK_UPLUS                           38
#define TK_BITNOT                          39
#define TK_SHOW                            40
#define TK_DATABASES                       41
#define TK_TOPICS                          42
#define TK_FUNCTIONS                       43
#define TK_MNODES                          44
#define TK_DNODES                          45
#define TK_ACCOUNTS                        46
#define TK_USERS                           47
#define TK_MODULES                         48
#define TK_QUERIES                         49
#define TK_CONNECTIONS                     50
#define TK_STREAMS                         51
#define TK_VARIABLES                       52
#define TK_SCORES                          53
#define TK_GRANTS                          54
#define TK_VNODES                          55
#define TK_DOT                             56
#define TK_CREATE                          57
#define TK_TABLE                           58
#define TK_STABLE                          59
#define TK_DATABASE                        60
#define TK_TABLES                          61
#define TK_STABLES                         62
#define TK_VGROUPS                         63
#define TK_DROP                            64
#define TK_TOPIC                           65
#define TK_FUNCTION                        66
#define TK_DNODE                           67
#define TK_USER                            68
#define TK_ACCOUNT                         69
#define TK_USE                             70
#define TK_DESCRIBE                        71
#define TK_DESC                            72
#define TK_ALTER                           73
#define TK_PASS                            74
#define TK_PRIVILEGE                       75
#define TK_LOCAL                           76
#define TK_COMPACT                         77
#define TK_LP                              78
#define TK_RP                              79
#define TK_IF                              80
#define TK_EXISTS                          81
#define TK_PORT                            82
#define TK_IPTOKEN                         83
#define TK_AS                              84
#define TK_OUTPUTTYPE                      85
#define TK_AGGREGATE                       86
#define TK_BUFSIZE                         87
#define TK_PPS                             88
#define TK_TSERIES                         89
#define TK_DBS                             90
#define TK_STORAGE                         91
#define TK_QTIME                           92
#define TK_CONNS                           93
#define TK_STATE                           94
#define TK_COMMA                           95
#define TK_KEEP                            96
#define TK_CACHE                           97
#define TK_REPLICA                         98
#define TK_QUORUM                          99
#define TK_DAYS                           100
#define TK_MINROWS                        101
#define TK_MAXROWS                        102
#define TK_BLOCKS                         103
#define TK_CTIME                          104
#define TK_WAL                            105
#define TK_FSYNC                          106
#define TK_COMP                           107
#define TK_PRECISION                      108
#define TK_UPDATE                         109
#define TK_CACHELAST                      110
#define TK_UNSIGNED                       111
#define TK_TAGS                           112
#define TK_USING                          113
#define TK_NULL                           114
#define TK_NOW                            115
#define TK_SELECT                         116
#define TK_UNION                          117
#define TK_ALL                            118
#define TK_DISTINCT                       119
#define TK_FROM                           120
#define TK_VARIABLE                       121
#define TK_INTERVAL                       122
#define TK_EVERY                          123
#define TK_SESSION                        124
#define TK_STATE_WINDOW                   125
#define TK_FILL                           126
#define TK_SLIDING                        127
#define TK_ORDER                          128
#define TK_BY                             129
#define TK_ASC                            130
#define TK_GROUP                          131
#define TK_HAVING                         132
#define TK_LIMIT                          133
#define TK_OFFSET                         134
#define TK_SLIMIT                         135
#define TK_SOFFSET                        136
#define TK_WHERE                          137
#define TK_RESET                          138
#define TK_QUERY                          139
#define TK_SYNCDB                         140
#define TK_ADD                            141
#define TK_COLUMN                         142
#define TK_MODIFY                         143
#define TK_TAG                            144
#define TK_CHANGE                         145
#define TK_SET                            146
#define TK_KILL                           147
#define TK_CONNECTION                     148
#define TK_STREAM                         149
#define TK_COLON                          150
#define TK_ABORT                          151
#define TK_AFTER                          152
#define TK_ATTACH                         153
#define TK_BEFORE                         154
#define TK_BEGIN                          155
#define TK_CASCADE                        156
#define TK_CLUSTER                        157
#define TK_CONFLICT                       158
#define TK_COPY                           159
#define TK_DEFERRED                       160
#define TK_DELIMITERS                     161
#define TK_DETACH                         162
#define TK_EACH                           163
#define TK_END                            164
#define TK_EXPLAIN                        165
#define TK_FAIL                           166
#define TK_FOR                            167
#define TK_IGNORE                         168
#define TK_IMMEDIATE                      169
#define TK_INITIALLY                      170
#define TK_INSTEAD                        171
#define TK_KEY                            172
#define TK_OF                             173
#define TK_RAISE                          174
#define TK_REPLACE                        175
#define TK_RESTRICT                       176
#define TK_ROW                            177
#define TK_STATEMENT                      178
#define TK_TRIGGER                        179
#define TK_VIEW                           180
#define TK_SEMI                           181
#define TK_NONE                           182
#define TK_PREV                           183
#define TK_LINEAR                         184
#define TK_IMPORT                         185
#define TK_TBNAME                         186
#define TK_JOIN                           187
#define TK_INSERT                         188
#define TK_INTO                           189
#define TK_VALUES                         190

#define NEW_TK_UNION                            1
#define NEW_TK_ALL                              2
#define NEW_TK_MINUS                            3
#define NEW_TK_EXCEPT                           4
#define NEW_TK_INTERSECT                        5
#define NEW_TK_NK_PLUS                          6
#define NEW_TK_NK_MINUS                         7
#define NEW_TK_NK_STAR                          8
#define NEW_TK_NK_SLASH                         9
#define NEW_TK_SHOW                            10
#define NEW_TK_DATABASES                       11
#define NEW_TK_NK_ID                           12
#define NEW_TK_NK_LP                           13
#define NEW_TK_NK_RP                           14
#define NEW_TK_NK_COMMA                        15
#define NEW_TK_NK_LITERAL                      16
#define NEW_TK_NK_DOT                          17
#define NEW_TK_SELECT                          18
#define NEW_TK_DISTINCT                        19
#define NEW_TK_AS                              20
#define NEW_TK_FROM                            21
#define NEW_TK_WITH                            22
#define NEW_TK_RECURSIVE                       23
#define NEW_TK_ORDER                           24
#define NEW_TK_BY                              25
#define NEW_TK_ASC                             26
#define NEW_TK_DESC                            27
#define NEW_TK_NULLS                           28
#define NEW_TK_FIRST                           29
#define NEW_TK_LAST                            30

#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_FILE                           306
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



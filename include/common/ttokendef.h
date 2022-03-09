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
#define TK_USER                            19
#define TK_PASS                            20
#define TK_NK_STRING                       21
#define TK_ALTER                           22
#define TK_PRIVILEGE                       23
#define TK_DROP                            24
#define TK_SHOW                            25
#define TK_USERS                           26
#define TK_DNODE                           27
#define TK_PORT                            28
#define TK_NK_INTEGER                      29
#define TK_DNODES                          30
#define TK_NK_ID                           31
#define TK_NK_IPTOKEN                      32
#define TK_DATABASE                        33
#define TK_DATABASES                       34
#define TK_USE                             35
#define TK_IF                              36
#define TK_NOT                             37
#define TK_EXISTS                          38
#define TK_BLOCKS                          39
#define TK_CACHE                           40
#define TK_CACHELAST                       41
#define TK_COMP                            42
#define TK_DAYS                            43
#define TK_FSYNC                           44
#define TK_MAXROWS                         45
#define TK_MINROWS                         46
#define TK_KEEP                            47
#define TK_PRECISION                       48
#define TK_QUORUM                          49
#define TK_REPLICA                         50
#define TK_TTL                             51
#define TK_WAL                             52
#define TK_VGROUPS                         53
#define TK_SINGLE_STABLE                   54
#define TK_STREAM_MODE                     55
#define TK_TABLE                           56
#define TK_NK_LP                           57
#define TK_NK_RP                           58
#define TK_STABLE                          59
#define TK_TABLES                          60
#define TK_STABLES                         61
#define TK_USING                           62
#define TK_TAGS                            63
#define TK_NK_DOT                          64
#define TK_NK_COMMA                        65
#define TK_COMMENT                         66
#define TK_BOOL                            67
#define TK_TINYINT                         68
#define TK_SMALLINT                        69
#define TK_INT                             70
#define TK_INTEGER                         71
#define TK_BIGINT                          72
#define TK_FLOAT                           73
#define TK_DOUBLE                          74
#define TK_BINARY                          75
#define TK_TIMESTAMP                       76
#define TK_NCHAR                           77
#define TK_UNSIGNED                        78
#define TK_JSON                            79
#define TK_VARCHAR                         80
#define TK_MEDIUMBLOB                      81
#define TK_BLOB                            82
#define TK_VARBINARY                       83
#define TK_DECIMAL                         84
#define TK_SMA                             85
#define TK_NK_FLOAT                        86
#define TK_NK_BOOL                         87
#define TK_NK_VARIABLE                     88
#define TK_BETWEEN                         89
#define TK_IS                              90
#define TK_NULL                            91
#define TK_NK_LT                           92
#define TK_NK_GT                           93
#define TK_NK_LE                           94
#define TK_NK_GE                           95
#define TK_NK_NE                           96
#define TK_NK_EQ                           97
#define TK_LIKE                            98
#define TK_MATCH                           99
#define TK_NMATCH                         100
#define TK_IN                             101
#define TK_FROM                           102
#define TK_AS                             103
#define TK_JOIN                           104
#define TK_ON                             105
#define TK_INNER                          106
#define TK_SELECT                         107
#define TK_DISTINCT                       108
#define TK_WHERE                          109
#define TK_PARTITION                      110
#define TK_BY                             111
#define TK_SESSION                        112
#define TK_STATE_WINDOW                   113
#define TK_INTERVAL                       114
#define TK_SLIDING                        115
#define TK_FILL                           116
#define TK_VALUE                          117
#define TK_NONE                           118
#define TK_PREV                           119
#define TK_LINEAR                         120
#define TK_NEXT                           121
#define TK_GROUP                          122
#define TK_HAVING                         123
#define TK_ORDER                          124
#define TK_SLIMIT                         125
#define TK_SOFFSET                        126
#define TK_LIMIT                          127
#define TK_OFFSET                         128
#define TK_ASC                            129
#define TK_DESC                           130
#define TK_NULLS                          131
#define TK_FIRST                          132
#define TK_LAST                           133

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

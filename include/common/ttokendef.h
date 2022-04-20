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
#define TK_BNODE                           44
#define TK_SNODE                           45
#define TK_MNODE                           46
#define TK_DATABASE                        47
#define TK_USE                             48
#define TK_IF                              49
#define TK_NOT                             50
#define TK_EXISTS                          51
#define TK_BLOCKS                          52
#define TK_CACHE                           53
#define TK_CACHELAST                       54
#define TK_COMP                            55
#define TK_DAYS                            56
#define TK_NK_VARIABLE                     57
#define TK_FSYNC                           58
#define TK_MAXROWS                         59
#define TK_MINROWS                         60
#define TK_KEEP                            61
#define TK_PRECISION                       62
#define TK_QUORUM                          63
#define TK_REPLICA                         64
#define TK_TTL                             65
#define TK_WAL                             66
#define TK_VGROUPS                         67
#define TK_SINGLE_STABLE                   68
#define TK_STREAM_MODE                     69
#define TK_RETENTIONS                      70
#define TK_STRICT                          71
#define TK_NK_COMMA                        72
#define TK_NK_COLON                        73
#define TK_TABLE                           74
#define TK_NK_LP                           75
#define TK_NK_RP                           76
#define TK_STABLE                          77
#define TK_ADD                             78
#define TK_COLUMN                          79
#define TK_MODIFY                          80
#define TK_RENAME                          81
#define TK_TAG                             82
#define TK_SET                             83
#define TK_NK_EQ                           84
#define TK_USING                           85
#define TK_TAGS                            86
#define TK_NK_DOT                          87
#define TK_COMMENT                         88
#define TK_BOOL                            89
#define TK_TINYINT                         90
#define TK_SMALLINT                        91
#define TK_INT                             92
#define TK_INTEGER                         93
#define TK_BIGINT                          94
#define TK_FLOAT                           95
#define TK_DOUBLE                          96
#define TK_BINARY                          97
#define TK_TIMESTAMP                       98
#define TK_NCHAR                           99
#define TK_UNSIGNED                       100
#define TK_JSON                           101
#define TK_VARCHAR                        102
#define TK_MEDIUMBLOB                     103
#define TK_BLOB                           104
#define TK_VARBINARY                      105
#define TK_DECIMAL                        106
#define TK_SMA                            107
#define TK_ROLLUP                         108
#define TK_FILE_FACTOR                    109
#define TK_NK_FLOAT                       110
#define TK_DELAY                          111
#define TK_SHOW                           112
#define TK_DATABASES                      113
#define TK_TABLES                         114
#define TK_STABLES                        115
#define TK_MNODES                         116
#define TK_MODULES                        117
#define TK_QNODES                         118
#define TK_FUNCTIONS                      119
#define TK_INDEXES                        120
#define TK_FROM                           121
#define TK_ACCOUNTS                       122
#define TK_APPS                           123
#define TK_CONNECTIONS                    124
#define TK_LICENCE                        125
#define TK_GRANTS                         126
#define TK_QUERIES                        127
#define TK_SCORES                         128
#define TK_TOPICS                         129
#define TK_VARIABLES                      130
#define TK_BNODES                         131
#define TK_SNODES                         132
#define TK_CLUSTER                        133
#define TK_LIKE                           134
#define TK_INDEX                          135
#define TK_FULLTEXT                       136
#define TK_FUNCTION                       137
#define TK_INTERVAL                       138
#define TK_TOPIC                          139
#define TK_AS                             140
#define TK_DESC                           141
#define TK_DESCRIBE                       142
#define TK_RESET                          143
#define TK_QUERY                          144
#define TK_EXPLAIN                        145
#define TK_ANALYZE                        146
#define TK_VERBOSE                        147
#define TK_NK_BOOL                        148
#define TK_RATIO                          149
#define TK_COMPACT                        150
#define TK_VNODES                         151
#define TK_IN                             152
#define TK_OUTPUTTYPE                     153
#define TK_AGGREGATE                      154
#define TK_BUFSIZE                        155
#define TK_STREAM                         156
#define TK_INTO                           157
#define TK_TRIGGER                        158
#define TK_AT_ONCE                        159
#define TK_WINDOW_CLOSE                   160
#define TK_WATERMARK                      161
#define TK_KILL                           162
#define TK_CONNECTION                     163
#define TK_MERGE                          164
#define TK_VGROUP                         165
#define TK_REDISTRIBUTE                   166
#define TK_SPLIT                          167
#define TK_SYNCDB                         168
#define TK_NULL                           169
#define TK_NK_QUESTION                    170
#define TK_NK_ARROW                       171
#define TK_ROWTS                          172
#define TK_TBNAME                         173
#define TK_QSTARTTS                       174
#define TK_QENDTS                         175
#define TK_WSTARTTS                       176
#define TK_WENDTS                         177
#define TK_WDURATION                      178
#define TK_CAST                           179
#define TK_NOW                            180
#define TK_TODAY                          181
#define TK_TIMEZONE                       182
#define TK_COUNT                          183
#define TK_FIRST                          184
#define TK_LAST                           185
#define TK_LAST_ROW                       186
#define TK_BETWEEN                        187
#define TK_IS                             188
#define TK_NK_LT                          189
#define TK_NK_GT                          190
#define TK_NK_LE                          191
#define TK_NK_GE                          192
#define TK_NK_NE                          193
#define TK_MATCH                          194
#define TK_NMATCH                         195
#define TK_CONTAINS                       196
#define TK_JOIN                           197
#define TK_INNER                          198
#define TK_SELECT                         199
#define TK_DISTINCT                       200
#define TK_WHERE                          201
#define TK_PARTITION                      202
#define TK_BY                             203
#define TK_SESSION                        204
#define TK_STATE_WINDOW                   205
#define TK_SLIDING                        206
#define TK_FILL                           207
#define TK_VALUE                          208
#define TK_NONE                           209
#define TK_PREV                           210
#define TK_LINEAR                         211
#define TK_NEXT                           212
#define TK_GROUP                          213
#define TK_HAVING                         214
#define TK_ORDER                          215
#define TK_SLIMIT                         216
#define TK_SOFFSET                        217
#define TK_LIMIT                          218
#define TK_OFFSET                         219
#define TK_ASC                            220
#define TK_NULLS                          221

#define TK_NK_SPACE                       300
#define TK_NK_COMMENT                     301
#define TK_NK_ILLEGAL                     302
#define TK_NK_HEX                         303   // hex number  0x123
#define TK_NK_OCT                         304   // oct number
#define TK_NK_BIN                         305   // bin format data 0b111
#define TK_NK_FILE                        306

#define TK_NK_BITNOT                      501
#define TK_INSERT                         502
#define TK_VALUES                         507
#define TK_IMPORT                         509
#define TK_NK_SEMI                        508

#define TK_NK_NIL                         65535

#endif /*_TD_COMMON_TOKEN_H_*/

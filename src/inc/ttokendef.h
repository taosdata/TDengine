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
#define TK_MNODES                          46
#define TK_DNODES                          47
#define TK_ACCOUNTS                        48
#define TK_USERS                           49
#define TK_MODULES                         50
#define TK_QUERIES                         51
#define TK_CONNECTIONS                     52
#define TK_STREAMS                         53
#define TK_VARIABLES                       54
#define TK_SCORES                          55
#define TK_GRANTS                          56
#define TK_VNODES                          57
#define TK_IPTOKEN                         58
#define TK_DOT                             59
#define TK_CREATE                          60
#define TK_TABLE                           61
#define TK_DATABASE                        62
#define TK_TABLES                          63
#define TK_STABLES                         64
#define TK_VGROUPS                         65
#define TK_DROP                            66
#define TK_STABLE                          67
#define TK_DNODE                           68
#define TK_USER                            69
#define TK_ACCOUNT                         70
#define TK_USE                             71
#define TK_DESCRIBE                        72
#define TK_ALTER                           73
#define TK_PASS                            74
#define TK_PRIVILEGE                       75
#define TK_LOCAL                           76
#define TK_IF                              77
#define TK_EXISTS                          78
#define TK_PPS                             79
#define TK_TSERIES                         80
#define TK_DBS                             81
#define TK_STORAGE                         82
#define TK_QTIME                           83
#define TK_CONNS                           84
#define TK_STATE                           85
#define TK_KEEP                            86
#define TK_CACHE                           87
#define TK_REPLICA                         88
#define TK_QUORUM                          89
#define TK_DAYS                            90
#define TK_MINROWS                         91
#define TK_MAXROWS                         92
#define TK_BLOCKS                          93
#define TK_CTIME                           94
#define TK_WAL                             95
#define TK_FSYNC                           96
#define TK_COMP                            97
#define TK_PRECISION                       98
#define TK_UPDATE                          99
#define TK_CACHELAST                      100
#define TK_LP                             101
#define TK_RP                             102
#define TK_UNSIGNED                       103
#define TK_TAGS                           104
#define TK_USING                          105
#define TK_COMMA                          106
#define TK_AS                             107
#define TK_NULL                           108
#define TK_SELECT                         109
#define TK_UNION                          110
#define TK_ALL                            111
#define TK_DISTINCT                       112
#define TK_FROM                           113
#define TK_VARIABLE                       114
#define TK_INTERVAL                       115
#define TK_FILL                           116
#define TK_SLIDING                        117
#define TK_ORDER                          118
#define TK_BY                             119
#define TK_ASC                            120
#define TK_DESC                           121
#define TK_GROUP                          122
#define TK_HAVING                         123
#define TK_LIMIT                          124
#define TK_OFFSET                         125
#define TK_SLIMIT                         126
#define TK_SOFFSET                        127
#define TK_WHERE                          128
#define TK_NOW                            129
#define TK_RESET                          130
#define TK_QUERY                          131
#define TK_ADD                            132
#define TK_COLUMN                         133
#define TK_TAG                            134
#define TK_CHANGE                         135
#define TK_SET                            136
#define TK_KILL                           137
#define TK_CONNECTION                     138
#define TK_STREAM                         139
#define TK_COLON                          140
#define TK_ABORT                          141
#define TK_AFTER                          142
#define TK_ATTACH                         143
#define TK_BEFORE                         144
#define TK_BEGIN                          145
#define TK_CASCADE                        146
#define TK_CLUSTER                        147
#define TK_CONFLICT                       148
#define TK_COPY                           149
#define TK_DEFERRED                       150
#define TK_DELIMITERS                     151
#define TK_DETACH                         152
#define TK_EACH                           153
#define TK_END                            154
#define TK_EXPLAIN                        155
#define TK_FAIL                           156
#define TK_FOR                            157
#define TK_IGNORE                         158
#define TK_IMMEDIATE                      159
#define TK_INITIALLY                      160
#define TK_INSTEAD                        161
#define TK_MATCH                          162
#define TK_KEY                            163
#define TK_OF                             164
#define TK_RAISE                          165
#define TK_REPLACE                        166
#define TK_RESTRICT                       167
#define TK_ROW                            168
#define TK_STATEMENT                      169
#define TK_TRIGGER                        170
#define TK_VIEW                           171
#define TK_COUNT                          172
#define TK_SUM                            173
#define TK_AVG                            174
#define TK_MIN                            175
#define TK_MAX                            176
#define TK_FIRST                          177
#define TK_LAST                           178
#define TK_TOP                            179
#define TK_BOTTOM                         180
#define TK_STDDEV                         181
#define TK_PERCENTILE                     182
#define TK_APERCENTILE                    183
#define TK_LEASTSQUARES                   184
#define TK_HISTOGRAM                      185
#define TK_DIFF                           186
#define TK_SPREAD                         187
#define TK_TWA                            188
#define TK_INTERP                         189
#define TK_LAST_ROW                       190
#define TK_RATE                           191
#define TK_IRATE                          192
#define TK_SUM_RATE                       193
#define TK_SUM_IRATE                      194
#define TK_AVG_RATE                       195
#define TK_AVG_IRATE                      196
#define TK_TBID                           197
#define TK_SEMI                           198
#define TK_NONE                           199
#define TK_PREV                           200
#define TK_LINEAR                         201
#define TK_IMPORT                         202
#define TK_METRIC                         203
#define TK_TBNAME                         204
#define TK_JOIN                           205
#define TK_METRICS                        206
#define TK_INSERT                         207
#define TK_INTO                           208
#define TK_VALUES                         209



#define TK_SPACE                          300
#define TK_COMMENT                        301
#define TK_ILLEGAL                        302
#define TK_HEX                            303   // hex number  0x123
#define TK_OCT                            304   // oct number
#define TK_BIN                            305   // bin format data 0b111
#define TK_FILE                           306
#define TK_QUESTION                       307   // denoting the placeholder of "?",when invoking statement bind query

#endif



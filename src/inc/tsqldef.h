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

#ifndef TDENGINE_TSQLDEF_H
#define TDENGINE_TSQLDEF_H

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
#define TK_USERS                           48
#define TK_MODULES                         49
#define TK_QUERIES                         50
#define TK_CONNECTIONS                     51
#define TK_STREAMS                         52
#define TK_CONFIGS                         53
#define TK_SCORES                          54
#define TK_GRANTS                          55
#define TK_DOT                             56
#define TK_TABLES                          57
#define TK_STABLES                         58
#define TK_VGROUPS                         59
#define TK_DROP                            60
#define TK_TABLE                           61
#define TK_DATABASE                        62
#define TK_USER                            63
#define TK_USE                             64
#define TK_DESCRIBE                        65
#define TK_ALTER                           66
#define TK_PASS                            67
#define TK_PRIVILEGE                       68
#define TK_DNODE                           69
#define TK_IP                              70
#define TK_LOCAL                           71
#define TK_IF                              72
#define TK_EXISTS                          73
#define TK_CREATE                          74
#define TK_KEEP                            75
#define TK_CACHE                           76
#define TK_REPLICA                         77
#define TK_DAYS                            78
#define TK_ROWS                            79
#define TK_ABLOCKS                         80
#define TK_TBLOCKS                         81
#define TK_CTIME                           82
#define TK_CLOG                            83
#define TK_COMP                            84
#define TK_PRECISION                       85
#define TK_LP                              86
#define TK_RP                              87
#define TK_TAGS                            88
#define TK_USING                           89
#define TK_AS                              90
#define TK_COMMA                           91
#define TK_NULL                            92
#define TK_SELECT                          93
#define TK_FROM                            94
#define TK_VARIABLE                        95
#define TK_INTERVAL                        96
#define TK_FILL                            97
#define TK_SLIDING                         98
#define TK_ORDER                           99
#define TK_BY                             100
#define TK_ASC                            101
#define TK_DESC                           102
#define TK_GROUP                          103
#define TK_HAVING                         104
#define TK_LIMIT                          105
#define TK_OFFSET                         106
#define TK_SLIMIT                         107
#define TK_SOFFSET                        108
#define TK_WHERE                          109
#define TK_NOW                            110
#define TK_INSERT                         111
#define TK_INTO                           112
#define TK_VALUES                         113
#define TK_RESET                          114
#define TK_QUERY                          115
#define TK_ADD                            116
#define TK_COLUMN                         117
#define TK_TAG                            118
#define TK_CHANGE                         119
#define TK_SET                            120
#define TK_KILL                           121
#define TK_CONNECTION                     122
#define TK_COLON                          123
#define TK_STREAM                         124
#define TK_ABORT                          125
#define TK_AFTER                          126
#define TK_ATTACH                         127
#define TK_BEFORE                         128
#define TK_BEGIN                          129
#define TK_CASCADE                        130
#define TK_CLUSTER                        131
#define TK_CONFLICT                       132
#define TK_COPY                           133
#define TK_DEFERRED                       134
#define TK_DELIMITERS                     135
#define TK_DETACH                         136
#define TK_EACH                           137
#define TK_END                            138
#define TK_EXPLAIN                        139
#define TK_FAIL                           140
#define TK_FOR                            141
#define TK_IGNORE                         142
#define TK_IMMEDIATE                      143
#define TK_INITIALLY                      144
#define TK_INSTEAD                        145
#define TK_MATCH                          146
#define TK_KEY                            147
#define TK_OF                             148
#define TK_RAISE                          149
#define TK_REPLACE                        150
#define TK_RESTRICT                       151
#define TK_ROW                            152
#define TK_STATEMENT                      153
#define TK_TRIGGER                        154
#define TK_VIEW                           155
#define TK_ALL                            156
#define TK_COUNT                          157
#define TK_SUM                            158
#define TK_AVG                            159
#define TK_MIN                            160
#define TK_MAX                            161
#define TK_FIRST                          162
#define TK_LAST                           163
#define TK_TOP                            164
#define TK_BOTTOM                         165
#define TK_STDDEV                         166
#define TK_PERCENTILE                     167
#define TK_APERCENTILE                    168
#define TK_LEASTSQUARES                   169
#define TK_HISTOGRAM                      170
#define TK_DIFF                           171
#define TK_SPREAD                         172
#define TK_WAVG                           173
#define TK_INTERP                         174
#define TK_LAST_ROW                       175
#define TK_SEMI                           176
#define TK_NONE                           177
#define TK_PREV                           178
#define TK_LINEAR                         179
#define TK_IMPORT                         180
#define TK_METRIC                         181
#define TK_TBNAME                         182
#define TK_JOIN                           183
#define TK_METRICS                        184
#define TK_STABLE                         185

#endif
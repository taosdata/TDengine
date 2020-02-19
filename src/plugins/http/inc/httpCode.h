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

#ifndef TDENGINE_HTTP_CODE_H
#define TDENGINE_HTTP_CODE_H

//for fixed msg info
#define HTTP_SUCCESS                  0
#define HTTP_SERVER_OFFLINE           1
#define HTTP_UNSUPPORT_URL            2
#define HTTP_PARSE_HTTP_METHOD_ERROR  3
#define HTTP_PARSE_HTTP_VERSION_ERROR 4
#define HTTP_PARSE_HEAD_ERROR         5
#define HTTP_REQUSET_TOO_BIG          6
#define HTTP_PARSE_BODY_ERROR         7
#define HTTP_PARSE_CHUNKED_BODY_ERROR 8
#define HTTP_PARSE_URL_ERROR          9
#define HTTP_INVALID_AUTH_TOKEN       10
#define HTTP_PARSE_USR_ERROR          11
#define HTTP_NO_SQL_INPUT             12  
#define HTTP_SESSION_FULL             13
#define HTTP_NO_ENOUGH_MEMORY         14
#define HTTP_GEN_TAOSD_TOKEN_ERR      15
#define HTTP_INVALID_DB_TABLE         16
#define HTTP_NO_EXEC_USEDB            17
#define HTTP_PARSE_GC_REQ_ERROR       18
#define HTTP_INVALID_MULTI_REQUEST    19
#define HTTP_NO_MSG_INPUT             20
#define HTTP_NO_ENOUGH_SESSIONS       21

//telegraf
#define HTTP_TG_DB_NOT_INPUT          22
#define HTTP_TG_DB_TOO_LONG           23
#define HTTP_TG_INVALID_JSON          24
#define HTTP_TG_METRICS_NULL          25
#define HTTP_TG_METRICS_SIZE          26
#define HTTP_TG_METRIC_NULL           27
#define HTTP_TG_METRIC_TYPE           28
#define HTTP_TG_METRIC_NAME_NULL      29
#define HTTP_TG_METRIC_NAME_LONG      30
#define HTTP_TG_TIMESTAMP_NULL        31
#define HTTP_TG_TIMESTAMP_TYPE        32
#define HTTP_TG_TIMESTAMP_VAL_NULL    33
#define HTTP_TG_TAGS_NULL             34
#define HTTP_TG_TAGS_SIZE_0           35
#define HTTP_TG_TAGS_SIZE_LONG        36
#define HTTP_TG_TAG_NULL              37
#define HTTP_TG_TAG_NAME_NULL         38
#define HTTP_TG_TAG_NAME_SIZE         39
#define HTTP_TG_TAG_VALUE_TYPE        40
#define HTTP_TG_TAG_VALUE_NULL        41
#define HTTP_TG_TABLE_NULL            42
#define HTTP_TG_TABLE_SIZE            43
#define HTTP_TG_FIELDS_NULL           44
#define HTTP_TG_FIELDS_SIZE_0         45
#define HTTP_TG_FIELDS_SIZE_LONG      46
#define HTTP_TG_FIELD_NULL            47
#define HTTP_TG_FIELD_NAME_NULL       48
#define HTTP_TG_FIELD_NAME_SIZE       49
#define HTTP_TG_FIELD_VALUE_TYPE      50
#define HTTP_TG_FIELD_VALUE_NULL      51
#define HTTP_INVALID_BASIC_AUTH_TOKEN 52
#define HTTP_INVALID_TAOSD_AUTH_TOKEN 53
#define HTTP_TG_HOST_NOT_STRING       54

//grafana
#define HTTP_GC_QUERY_NULL            55
#define HTTP_GC_QUERY_SIZE            56

//opentsdb
#define HTTP_OP_DB_NOT_INPUT          57
#define HTTP_OP_DB_TOO_LONG           58
#define HTTP_OP_INVALID_JSON          59
#define HTTP_OP_METRICS_NULL          60
#define HTTP_OP_METRICS_SIZE          61
#define HTTP_OP_METRIC_NULL           62
#define HTTP_OP_METRIC_TYPE           63
#define HTTP_OP_METRIC_NAME_NULL      64
#define HTTP_OP_METRIC_NAME_LONG      65
#define HTTP_OP_TIMESTAMP_NULL        66
#define HTTP_OP_TIMESTAMP_TYPE        67
#define HTTP_OP_TIMESTAMP_VAL_NULL    68
#define HTTP_OP_TAGS_NULL             69
#define HTTP_OP_TAGS_SIZE_0           70
#define HTTP_OP_TAGS_SIZE_LONG        71
#define HTTP_OP_TAG_NULL              72
#define HTTP_OP_TAG_NAME_NULL         73
#define HTTP_OP_TAG_NAME_SIZE         74
#define HTTP_OP_TAG_VALUE_TYPE        75
#define HTTP_OP_TAG_VALUE_NULL        76
#define HTTP_OP_TAG_VALUE_TOO_LONG    77
#define HTTP_OP_VALUE_NULL            78
#define HTTP_OP_VALUE_TYPE            79

//tgf
#define HTTP_TG_STABLE_NOT_EXIST     80

extern char *httpMsg[];

#endif
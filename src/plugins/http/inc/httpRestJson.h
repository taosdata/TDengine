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

#ifndef TDENGINE_REST_JSON_H
#define TDENGINE_REST_JSON_H
#include <stdbool.h>
#include "httpHandle.h"
#include "httpJson.h"
#include "taos.h"

#define REST_JSON_SUCCESS        "succ"
#define REST_JSON_SUCCESS_LEN    4
#define REST_JSON_FAILURE        "error"
#define REST_JSON_FAILURE_LEN    5
#define REST_JSON_STATUS         "status"
#define REST_JSON_STATUS_LEN     6
#define REST_JSON_CODE           "code"
#define REST_JSON_CODE_LEN       4
#define REST_JSON_DESC           "desc"
#define REST_JSON_DESC_LEN       4
#define REST_JSON_DATA           "data"
#define REST_JSON_DATA_LEN       4
#define REST_JSON_HEAD           "head"
#define REST_JSON_HEAD_LEN       4
#define REST_JSON_ROWS           "rows"
#define REST_JSON_ROWS_LEN       4
#define REST_JSON_AFFECT_ROWS    "affected_rows"
#define REST_JSON_AFFECT_ROWS_LEN 13

#define REST_TIMESTAMP_FMT_LOCAL_STRING 0
#define REST_TIMESTAMP_FMT_TIMESTAMP    1
#define REST_TIMESTAMP_FMT_UTC_STRING   2

void restBuildSqlAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows);

void restStartSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
bool restBuildSqlTimestampJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int numOfRows);
bool restBuildSqlLocalTimeStringJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int numOfRows);
bool restBuildSqlUtcTimeStringJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int numOfRows);
void restStopSqlJson(HttpContext *pContext, HttpSqlCmd *cmd);

#endif
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

#ifndef TDENGINE_ADMIN_JSON_H
#define TDENGINE_ADMIN_JSON_H

#include <stdbool.h>
#include "taos.h"
#include "httpJson.h"

#define ADMIN_JSON_SUCCESS        "succ"
#define ADMIN_JSON_SUCCESS_LEN     4
#define ADMIN_JSON_FAILURE         "error"
#define ADMIN_JSON_FAILURE_LEN     5
#define ADMIN_JSON_STATUS          "status"
#define ADMIN_JSON_STATUS_LEN      6
#define ADMIN_JSON_CODE            "code"
#define ADMIN_JSON_CODE_LEN        4
#define ADMIN_JSON_DESC            "desc"
#define ADMIN_JSON_DESC_LEN        4
#define ADMIN_JSON_DATA            "data"
#define ADMIN_JSON_DATA_LEN        4
#define ADMIN_JSON_HEAD            "head"
#define ADMIN_JSON_HEAD_LEN        4
#define ADMIN_JSON_ROWS            "rows"
#define ADMIN_JSON_ROWS_LEN        4
#define ADMIN_JSON_AFFECT_ROWS     "affect_rows"
#define ADMIN_JSON_AFFECT_ROWS_LEN 11
#define ADMIN_JSON_DBS             "dbs"
#define ADMIN_JSON_DBS_LEN         3
#define ADMIN_JSON_TABLES          "tables"
#define ADMIN_JSON_TABLES_LEN      6
#define ADMIN_JSON_USERS           "users"
#define ADMIN_JSON_USERS_LEN       5
#define ADMIN_JSON_MNODES          "mnodes"
#define ADMIN_JSON_MNODES_LEN      6
#define ADMIN_JSON_DNODES          "dnodes"
#define ADMIN_JSON_DNODES_LEN      6

#define ADMIN_JSON_COL_TYPE        "column type"
#define ADMIN_JSON_COL_TYPE_LEN    11
#define ADMIN_JSON_COL_NAME        "column name"
#define ADMIN_JSON_COL_NAME_LEN    11
#define ADMIN_JSON_COL_BYTES       "column bytes"
#define ADMIN_JSON_COL_BYTES_LEN   12

#define ADMIN_JSON_DBS_TYPE     1
#define ADMIN_JSON_TABLES_TYPE  2
#define ADMIN_JSON_USERS_TYPE   3
#define ADMIN_JSON_MNODES_TYPE  4
#define ADMIN_JSON_DNODES_TYPE  5

void adminStartSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
void adminBuildSqlAffectRowJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows);
bool adminBuildSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows);
bool adminBuildSqlAllJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows);
void adminStopSqlJson(HttpContext *pContext, HttpSqlCmd *cmd);

void adminInitInfoJson(HttpContext *pContext);
bool adminBuildInfoJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows);
void adminStopInfoJson(HttpContext *pContext, HttpSqlCmd *cmd);
void adminCleanInfoJson(HttpContext *pContext);

void adminStartMetaJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
bool adminBuildMetaJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows);
void adminStopMetaJson(HttpContext *pContext, HttpSqlCmd *cmd);

void adminInitSqlsJson(HttpContext *pContext);
void adminCleanSqlsJson(HttpContext *pContext);
void adminStartSqlsJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
void adminStopSqlsJson(HttpContext *pContext, HttpSqlCmd *cmd);

#endif
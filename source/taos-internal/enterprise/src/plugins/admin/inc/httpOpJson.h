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

#ifndef TDENGINE_OP_JSON_H
#define TDENGINE_OP_JSON_H

#include "taos.h"
#include "httpInt.h"
#include "httpJson.h"

void opInitPutDetailJson(HttpContext *pContext);
void opCleanPutDetailJson(HttpContext *pContext);
void opStartPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
void opStopPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd);
void opBuildPutDetailAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows);
bool opCheckPutDetailFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code);
void opSetPutDetailNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code);

void opInitPutSummaryJson(HttpContext *pContext);
void opCleanPutSummaryJson(HttpContext *pContext);
void opBuildPutSummaryAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows);
bool opCheckPutSummaryFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code);
void opSetPutSummaryNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code);

#endif
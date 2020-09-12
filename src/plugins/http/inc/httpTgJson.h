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

#ifndef TDENGINE_TG_JSON_H
#define TDENGINE_TG_JSON_H

#include "httpHandle.h"
#include "httpJson.h"
#include "taos.h"

void tgInitQueryJson(HttpContext *pContext);
void tgCleanQueryJson(HttpContext *pContext);
void tgStartQueryJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
void tgStopQueryJson(HttpContext *pContext, HttpSqlCmd *cmd);
void tgBuildSqlAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows);
bool tgCheckFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);
void tgSetNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);

#endif
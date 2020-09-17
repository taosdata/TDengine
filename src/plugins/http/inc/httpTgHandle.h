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

#ifndef TDENGINE_TG_HANDLE_H
#define TDENGINE_TG_HANDLE_H

#include "http.h"
#include "httpInt.h"
#include "httpUtil.h"
#include "httpResp.h"
#include "httpSql.h"

#define TG_ROOT_URL_POS   0
#define TG_DB_URL_POS     1
#define TG_USER_URL_POS   2
#define TG_PASS_URL_POS   3

void tgInitHandle(HttpServer *pServer);
void tgCleanupHandle();

bool tgProcessRquest(struct HttpContext *pContext);

#endif
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

#ifndef TDENGINE_OP_HANDLE_H
#define TDENGINE_OP_HANDLE_H

#include "http.h"
#include "httpInt.h"
#include "httpUtil.h"
#include "httpResp.h"
#include "httpSql.h"

#define OP_ROOT_URL_POS   0
#define OP_DB_URL_POS     1
#define OP_ACTION_URL_POS 2
#define OP_USER_URL_POS   3
#define OP_PASS_URL_POS   4

bool opProcessRequest(struct HttpContext* pContext);

#endif
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

#ifndef TDENGINE_REST_HANDLE_H
#define TDENGINE_REST_HANDLE_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

#define REST_ROOT_URL_POS   0
#define REST_ACTION_URL_POS 1
#define REST_USER_URL_POS   2
#define REST_PASS_URL_POS   3

void restInitHandle(HttpServer* pServer);
bool restProcessRequest(struct HttpContext* pContext);

#endif
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

#ifndef TDENGINE_HTTP_CONTEXT_H
#define TDENGINE_HTTP_CONTEXT_H

#include "httpInt.h"

bool        httpInitContexts();
void        httpCleanupContexts();
const char *httpContextStateStr(HttpContextState state);

HttpContext *httpCreateContext(SOCKET fd);
bool         httpInitContext(HttpContext *pContext);
HttpContext *httpGetContext(void * pContext);
void         httpReleaseContext(HttpContext *pContext, bool clearRes);
void         httpCloseContextByServer(HttpContext *pContext);
void         httpCloseContextByApp(HttpContext *pContext);
void         httpNotifyContextClose(HttpContext *pContext);
bool         httpAlterContextState(HttpContext *pContext, HttpContextState srcState, HttpContextState destState);

#endif

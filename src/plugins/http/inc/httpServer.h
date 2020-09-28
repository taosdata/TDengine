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

#ifndef TDENGINE_HTTP_SERVER_H
#define TDENGINE_HTTP_SERVER_H

#include "httpInt.h"

bool httpInitConnect();
void httpCleanUpConnect();

void *httpInitServer(char *ip, uint16_t port, char *label, int32_t numOfThreads, void *fp, void *shandle);
void  httpCleanUpServer(HttpServer *pServer);

#endif

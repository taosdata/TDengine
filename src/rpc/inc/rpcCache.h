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

#ifndef TDENGINE_RPC_CACHE_H
#define TDENGINE_RPC_CACHE_H

#ifdef __cplusplus
extern "C" {
#endif

void *rpcOpenConnCache(int maxSessions, void (*cleanFp)(void *), void *tmrCtrl, int64_t keepTimer);
void  rpcCloseConnCache(void *handle);
void  rpcAddConnIntoCache(void *handle, void *data, char *fqdn, uint16_t port, int8_t connType);
void *rpcGetConnFromCache(void *handle, char *fqdn, uint16_t port, int8_t connType);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RPC_CACHE_H

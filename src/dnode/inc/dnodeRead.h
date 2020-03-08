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

#ifndef TDENGINE_DNODE_READ_H
#define TDENGINE_DNODE_READ_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include "taosdef.h"
#include "taosmsg.h"

/*
 * handle query message, and the result is returned by callback function
 */
void dnodeQueryData(SQueryTableMsg *pQuery, void *pConn, void (*callback)(int32_t code, void *pQInfo, void *pConn));

/*
 * Dispose retrieve msg, and the result will passed through callback function
 */
typedef void (*SDnodeRetrieveCallbackFp)(int32_t code, void *pQInfo, void *pConn);
void dnodeRetrieveData(SRetrieveTableMsg *pRetrieve, void *pConn, SDnodeRetrieveCallbackFp callbackFp);

/*
 * Fill retrieve result according to query info
 */
int32_t dnodeGetRetrieveData(void *pQInfo, SRetrieveTableRsp *pRetrieve);

/*
 * Get the size of retrieve result according to query info
 */
int32_t dnodeGetRetrieveDataSize(void *pQInfo);

#ifdef __cplusplus
}
#endif

#endif

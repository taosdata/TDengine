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
#include "dnodeShell.h"

void dnodeFreeQInfoInQueue(SShellObj *pShellObj);

/*
 * Dnode handle read messages
 * The processing result is returned by callback function with pShellObj parameter
 */
int32_t dnodeReadData(SQueryMeterMsg *msg, void *pShellObj, void (*callback)(SQueryMeterRsp *rspMsg, void *pShellObj));

typedef void (*SDnodeRetrieveCallbackFp)(int32_t code, SRetrieveMeterRsp *pRetrieveRspMsg, void *pShellObj);

void dnodeRetrieveData(SRetrieveMeterMsg *pMsg, int32_t msgLen, void *pShellObj, SDnodeRetrieveCallbackFp callback);

#ifdef __cplusplus
}
#endif

#endif

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

#ifndef TDENGINE_MGMT_SHELL_H
#define TDENGINE_MGMT_SHELL_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mgmtDef.h"

int32_t mgmtInitShell();
void    mgmtCleanUpShell();
void    mgmtAddShellMsgHandle(uint8_t msgType, void (*fp)(SQueuedMsg *queuedMsg));

typedef int32_t (*SShowMetaFp)(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
void mgmtAddShellShowMetaHandle(uint8_t showType, SShowMetaFp fp);
void mgmtAddShellShowRetrieveHandle(uint8_t showType, SShowRetrieveFp fp);

void mgmtAddToShellQueue(SQueuedMsg *queuedMsg);
void mgmtDealyedAddToShellQueue(SQueuedMsg *queuedMsg);
void mgmtSendSimpleResp(void *thandle, int32_t code);

#ifdef __cplusplus
}
#endif

#endif

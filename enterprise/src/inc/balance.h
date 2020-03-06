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

#ifndef TDENGINE_MODULE_MNODE_PEER_H
#define TDENGINE_MODULE_MNODE_PEER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "mnode.h"

void balanceInit();

extern int32_t (*balanceInitResourceFp)();
extern void    (*balanceCleanUpResourceFp)();

extern void    (*balanceNotifyFp)();
extern int32_t (*balanceAllocVnodesFp)(SVgObj *pVgroup);

extern int32_t (*balanceSetDnodeRemoveStateFp)(SDnodeObj *pDnode);
extern void    (*balanceSetDnodeUnRemoveStateFp)(SDnodeObj *pDnode);

extern int32_t (*balanceGetScoresMetaFp)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
extern int32_t (*balanceRetrieveScoresFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);

#ifdef __cplusplus
}
#endif

#endif

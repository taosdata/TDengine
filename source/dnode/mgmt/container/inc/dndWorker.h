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

#ifndef _TD_DND_WORKER_H_
#define _TD_DND_WORKER_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dndInitWorker(void *param, SDnodeWorker *pWorker, EWorkerType type, const char *name, int32_t minNum,
                      int32_t maxNum, void *queueFp);
void    dndCleanupWorker(SDnodeWorker *pWorker);
int32_t dndWriteMsgToWorker(SDnodeWorker *pWorker, void *pCont, int32_t contLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_WORKER_H_*/
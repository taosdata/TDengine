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

#ifndef _TD_TQ_OFFSET_H_
#define _TD_TQ_OFFSET_H_

#include "tqInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STqOffsetCfg   STqOffsetCfg;
typedef struct STqOffsetStore STqOffsetStore;

STqOffsetStore* STqOffsetOpen(STqOffsetCfg*);
void            STqOffsetClose(STqOffsetStore*);

int64_t tqOffsetFetch(STqOffsetStore* pStore, const char* subscribeKey);
int32_t tqOffsetCommit(STqOffsetStore* pStore, const char* subscribeKey, int64_t offset);
int32_t tqOffsetPersist(STqOffsetStore* pStore, const char* subscribeKey);
int32_t tqOffsetPersistAll(STqOffsetStore* pStore);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_OFFSET_H_*/

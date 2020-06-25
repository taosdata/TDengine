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
#ifndef __TD_RWLATCH_H__
#define __TD_RWLATCH_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef int32_t SRWLatch;

void taosInitRWLatch(SRWLatch *pLatch);
void taosWLockLatch(SRWLatch *pLatch);
void taosWUnLockLatch(SRWLatch *pLatch);
void taosRLockLatch(SRWLatch *pLatch);
void taosRUnLockLatch(SRWLatch *pLatch);

#ifdef __cplusplus
}
#endif

#endif

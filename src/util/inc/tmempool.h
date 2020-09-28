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
#ifndef TDENGINE_TMEMPOOL_H
#define TDENGINE_TMEMPOOL_H

#ifdef __cplusplus
extern "C" {
#endif

#define mpool_h void *

mpool_h taosMemPoolInit(int maxNum, int blockSize);

char *taosMemPoolMalloc(mpool_h handle);

void taosMemPoolFree(mpool_h handle, char *p);

void taosMemPoolCleanUp(mpool_h handle);

#ifdef __cplusplus
}
#endif

#endif

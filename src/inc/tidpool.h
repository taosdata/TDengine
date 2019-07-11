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

#ifndef TDENGINE_TIDPOOL_H
#define TDENGINE_TIDPOOL_H

#ifdef __cplusplus
extern "C" {
#endif

void *taosInitIdPool(int maxId);

int taosAllocateId(void *handle);

void taosFreeId(void *handle, int id);

void taosIdPoolCleanUp(void *handle);

int taosIdPoolNumOfUsed(void *handle);

void taosIdPoolReinit(void *handle);

void taosIdPoolMarkStatus(void *handle, int id, int status);

void taosIdPoolSetFreeList(void *handle);

#ifdef __cplusplus
}
#endif

#endif

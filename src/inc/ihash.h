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

#ifndef TDENGINE_IHASH_H
#define TDENGINE_IHASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

void *taosInitIntHash(int32_t maxSessions, int32_t dataSize, int32_t (*fp)(void *, int32_t));

void taosCleanUpIntHash(void *handle);

char *taosGetIntHashData(void *handle, int32_t key);

void taosDeleteIntHash(void *handle, int32_t key);

char *taosAddIntHash(void *handle, int32_t key, char *pData);

int32_t taosHashInt(void *handle, int32_t key);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_IHASH_H

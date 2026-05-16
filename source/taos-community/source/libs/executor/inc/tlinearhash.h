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

#ifndef TDENGINE_TLINEARHASH_H
#define TDENGINE_TLINEARHASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "thash.h"

enum {
  LINEAR_HASH_STATIS = 0x1,
  LINEAR_HASH_DATA = 0x2,
};

typedef struct SLHashObj SLHashObj;

SLHashObj* tHashInit(int32_t inMemPages, int32_t pageSize, _hash_fn_t fn, int32_t numOfTuplePerPage);
void*      tHashCleanup(SLHashObj* pHashObj);

int32_t tHashPut(SLHashObj* pHashObj, const void* key, size_t keyLen, void* data, size_t size);
char*   tHashGet(SLHashObj* pHashObj, const void* key, size_t keyLen);
int32_t tHashRemove(SLHashObj* pHashObj, const void* key, size_t keyLen);

void tHashPrint(const SLHashObj* pHashObj, int32_t type);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_TLINEARHASH_H

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

#ifndef TDENGINE_HASHUTIL_H
#define TDENGINE_HASHUTIL_H

#include "os.h"

typedef uint32_t (*_hash_fn_t)(const char *, uint32_t);

typedef int32_t (*_equal_fn_t)(const void *a, const void *b, size_t sz); 

/**
 * murmur hash algorithm
 * @key  usually string
 * @len  key length
 * @seed hash seed
 * @out  an int32 value
 */
uint32_t MurmurHash3_32(const char *key, uint32_t len);

/**
 *
 * @param key
 * @param len
 * @return
 */
uint32_t taosIntHash_32(const char *key, uint32_t len);
uint32_t taosIntHash_64(const char *key, uint32_t len);


int32_t  taosFloatEqual(const void *a, const void *b, size_t sz);
int32_t  taosDoubleEqual(const void *a,const void *b, size_t sz); 

_hash_fn_t taosGetDefaultHashFunction(int32_t type);

_equal_fn_t taosGetDefaultEqualFunction(int32_t type);

#endif //TDENGINE_HASHUTIL_H

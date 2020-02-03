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

#ifndef TDENGINE_HASH_H
#define TDENGINE_HASH_H

#include "hashutil.h"

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_VALUE_IN_TRASH (-1)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

typedef struct SHashNode {
  char *key;  // null-terminated string
  union {
    struct SHashNode * prev;
    struct SHashEntry *prev1;
  };

  struct SHashNode *next;
  uint32_t          hashVal;  // the hash value of key, if hashVal == HASH_VALUE_IN_TRASH, this node is moved to trash
  uint32_t          keyLen;   // length of the key
  char              data[];
} SHashNode;

typedef struct SHashEntry {
  SHashNode *next;
  uint32_t   num;
} SHashEntry;

typedef struct HashObj {
  SHashEntry **hashList;
  uint32_t     capacity;
  int          size;
  _hash_fn_t   hashFp;
  bool         multithreadSafe;  // enable lock

#if defined LINUX
  pthread_rwlock_t lock;
#else
  pthread_mutex_t lock;
#endif

} HashObj;

void *taosInitHashTable(uint32_t capacity, _hash_fn_t fn, bool multithreadSafe);

int32_t taosAddToHashTable(HashObj *pObj, const char *key, uint32_t keyLen, void *data, uint32_t size);
void    taosDeleteFromHashTable(HashObj *pObj, const char *key, uint32_t keyLen);

char *taosGetDataFromHash(HashObj *pObj, const char *key, uint32_t keyLen);

void taosCleanUpHashTable(void *handle);

int32_t taosGetHashMaxOverflowLength(HashObj *pObj);

int32_t taosCheckHashTable(HashObj *pObj);

#endif  // TDENGINE_HASH_H

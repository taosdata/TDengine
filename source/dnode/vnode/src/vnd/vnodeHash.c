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

#include "vnodeHash.h"

#define VNODE_HASH_DEFAULT_NUM_BUCKETS 1024

typedef struct SVHashEntry SVHashEntry;

struct SVHashEntry {
  SVHashEntry* next;
  void*        obj;
};

static int32_t vHashRehash(SVHashTable* ht, uint32_t newNumBuckets) {
  SVHashEntry** newBuckets = (SVHashEntry**)taosMemoryCalloc(newNumBuckets, sizeof(SVHashEntry*));
  if (newBuckets == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < ht->numBuckets; i++) {
    SVHashEntry* entry = ht->buckets[i];
    while (entry != NULL) {
      SVHashEntry* next = entry->next;
      uint32_t     bucketIndex = ht->hash(entry->obj) % newNumBuckets;
      entry->next = newBuckets[bucketIndex];
      newBuckets[bucketIndex] = entry;
      entry = next;
    }
  }

  taosMemoryFree(ht->buckets);
  ht->buckets = newBuckets;
  ht->numBuckets = newNumBuckets;

  return 0;
}

int32_t vHashInit(SVHashTable** ht, uint32_t (*hash)(const void*), int32_t (*compare)(const void*, const void*)) {
  if (ht == NULL || hash == NULL || compare == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  (*ht) = (SVHashTable*)taosMemoryMalloc(sizeof(SVHashTable));
  if (*ht == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*ht)->hash = hash;
  (*ht)->compare = compare;
  (*ht)->numEntries = 0;
  (*ht)->numBuckets = VNODE_HASH_DEFAULT_NUM_BUCKETS;
  (*ht)->buckets = (SVHashEntry**)taosMemoryCalloc((*ht)->numBuckets, sizeof(SVHashEntry*));
  if ((*ht)->buckets == NULL) {
    taosMemoryFree(*ht);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

int32_t vHashDestroy(SVHashTable** ht) {
  if (ht == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (*ht) {
    ASSERT((*ht)->numEntries == 0);
    taosMemoryFree((*ht)->buckets);
    taosMemoryFree(*ht);
    (*ht) = NULL;
  }
  return 0;
}

int32_t vHashPut(SVHashTable* ht, void* obj) {
  if (ht == NULL || obj == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint32_t bucketIndex = ht->hash(obj) % ht->numBuckets;
  for (SVHashEntry* entry = ht->buckets[bucketIndex]; entry != NULL; entry = entry->next) {
    if (ht->compare(entry->obj, obj) == 0) {
      return TSDB_CODE_DUP_KEY;
    }
  }

  if (ht->numEntries >= ht->numBuckets) {
    vHashRehash(ht, ht->numBuckets * 2);
    bucketIndex = ht->hash(obj) % ht->numBuckets;
  }

  SVHashEntry* entry = (SVHashEntry*)taosMemoryMalloc(sizeof(SVHashEntry));
  if (entry == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  entry->obj = obj;
  entry->next = ht->buckets[bucketIndex];
  ht->buckets[bucketIndex] = entry;
  ht->numEntries++;

  return 0;
}

int32_t vHashGet(SVHashTable* ht, const void* obj, void** retObj) {
  if (ht == NULL || obj == NULL || retObj == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint32_t bucketIndex = ht->hash(obj) % ht->numBuckets;
  for (SVHashEntry* entry = ht->buckets[bucketIndex]; entry != NULL; entry = entry->next) {
    if (ht->compare(entry->obj, obj) == 0) {
      *retObj = entry->obj;
      return 0;
    }
  }

  *retObj = NULL;
  return TSDB_CODE_NOT_FOUND;
}

int32_t vHashDrop(SVHashTable* ht, const void* obj) {
  if (ht == NULL || obj == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint32_t bucketIndex = ht->hash(obj) % ht->numBuckets;
  for (SVHashEntry** entry = &ht->buckets[bucketIndex]; *entry != NULL; entry = &(*entry)->next) {
    if (ht->compare((*entry)->obj, obj) == 0) {
      SVHashEntry* tmp = *entry;
      *entry = (*entry)->next;
      taosMemoryFree(tmp);
      ht->numEntries--;
      if (ht->numBuckets > VNODE_HASH_DEFAULT_NUM_BUCKETS && ht->numEntries < ht->numBuckets / 4) {
        vHashRehash(ht, ht->numBuckets / 2);
      }
      return 0;
    }
  }

  return TSDB_CODE_NOT_FOUND;
}
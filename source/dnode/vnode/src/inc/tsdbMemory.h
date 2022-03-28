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

#ifndef _TD_TSDB_MEMORY_H_
#define _TD_TSDB_MEMORY_H_

#ifdef __cplusplus
extern "C" {
#endif

static void * taosTMalloc(size_t size);
static void * taosTCalloc(size_t nmemb, size_t size);
static void * taosTRealloc(void *ptr, size_t size);
static void * taosTZfree(void *ptr);
static size_t taosTSizeof(void *ptr);
static void   taosTMemset(void *ptr, int c);

static FORCE_INLINE void *taosTMalloc(size_t size) {
  if (size <= 0) return NULL;

  void *ret = taosMemoryMalloc(size + sizeof(size_t));
  if (ret == NULL) return NULL;

  *(size_t *)ret = size;

  return (void *)((char *)ret + sizeof(size_t));
}

static FORCE_INLINE void *taosTCalloc(size_t nmemb, size_t size) {
  size_t tsize = nmemb * size;
  void * ret = taosTMalloc(tsize);
  if (ret == NULL) return NULL;

  taosTMemset(ret, 0);
  return ret;
}

static FORCE_INLINE size_t taosTSizeof(void *ptr) { return (ptr) ? (*(size_t *)((char *)ptr - sizeof(size_t))) : 0; }

static FORCE_INLINE void taosTMemset(void *ptr, int c) { memset(ptr, c, taosTSizeof(ptr)); }

static FORCE_INLINE void * taosTRealloc(void *ptr, size_t size) {
  if (ptr == NULL) return taosTMalloc(size);

  if (size <= taosTSizeof(ptr)) return ptr;

  void * tptr = (void *)((char *)ptr - sizeof(size_t));
  size_t tsize = size + sizeof(size_t);
  void* tptr1 = taosMemoryRealloc(tptr, tsize);
  if (tptr1 == NULL) return NULL;
  tptr = tptr1;

  *(size_t *)tptr = size;

  return (void *)((char *)tptr + sizeof(size_t));
}

static FORCE_INLINE void* taosTZfree(void* ptr) {
  if (ptr) {
    taosMemoryFree((void*)((char*)ptr - sizeof(size_t)));
  }
  return NULL;
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_MEMORY_H_ */
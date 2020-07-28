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

#define _DEFAULT_SOURCE
#include "os.h"

#ifndef TAOS_OS_FUNC_MALLOC

void *tmalloc(size_t size) {
  if (size <= 0) return NULL;

  void *ret = malloc(size + sizeof(size_t));
  if (ret == NULL) return NULL;

  *(size_t *)ret = size;

  return (void *)((char *)ret + sizeof(size_t));
}

void *tcalloc(size_t nmemb, size_t size) {
  size_t tsize = nmemb * size;
  void * ret = tmalloc(tsize);
  if (ret == NULL) return NULL;

  tmemset(ret, 0);
  return ret;
}

size_t tsizeof(void *ptr) { return (ptr) ? (*(size_t *)((char *)ptr - sizeof(size_t))) : 0; }

void tmemset(void *ptr, int c) { memset(ptr, c, tsizeof(ptr)); }

void * trealloc(void *ptr, size_t size) {
  if (ptr == NULL) return tmalloc(size);

  if (size <= tsizeof(ptr)) return ptr;

  void * tptr = (void *)((char *)ptr - sizeof(size_t));
  size_t tsize = size + sizeof(size_t);
  tptr = realloc(tptr, tsize);
  if (tptr == NULL) return NULL;

  *(size_t *)tptr = size;

  return (void *)((char *)tptr + sizeof(size_t));
}

void tzfree(void *ptr) {
  if (ptr) {
    free((void *)((char *)ptr - sizeof(size_t)));
  }
}

#endif
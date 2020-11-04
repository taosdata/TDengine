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

#ifndef TDENGINE_OS_ALLOC_H
#define TDENGINE_OS_ALLOC_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef TAOS_OS_FUNC_ALLOC
  #define tmalloc(size) malloc(size)
  #define tcalloc(nmemb, size) calloc(nmemb, size)
  #define trealloc(p, size) realloc(p, size)
  #define tmemalign(alignment, size) malloc(size)
  #define tfree(p) free(p)
  #define tmemzero(p, size) memset(p, 0, size)
#else
  void *tmalloc(int32_t size);
  void *tcalloc(int32_t nmemb, int32_t size);
  void *trealloc(void *p, int32_t size);
  void *tmemalign(int32_t alignment, int32_t size);
  void  tfree(void *p);
  void  tmemzero(void *p, int32_t size);
#endif

#ifdef __cplusplus
}
#endif

#endif

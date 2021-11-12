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

#ifndef _TD_MALLOCATOR_H_
#define _TD_MALLOCATOR_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMemAllocator SMemAllocator;

struct SMemAllocator {
  char  name[16];
  void *impl;
  void *(*malloc)(SMemAllocator *, uint64_t size);
  void *(*calloc)(SMemAllocator *, uint64_t nmemb, uint64_t size);
  void *(*realloc)(SMemAllocator *, void *ptr, uint64_t size);
  void (*free)(SMemAllocator *, void *ptr);
  uint64_t (*usage)(SMemAllocator *);
};

typedef struct {
  void *impl;
  SMemAllocator *(*create)();
  void (*destroy)(SMemAllocator *);
} SMemAllocatorFactory;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MALLOCATOR_H_*/
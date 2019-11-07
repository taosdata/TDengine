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

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>

#include "tlog.h"

extern int32_t taosGetTimestampSec();
static int32_t startTime  = 0;
static int64_t m_curLimit = 100*1024;

bool isMallocMem(unsigned int size, char* _func) {
  if (0 == startTime) {
    startTime = taosGetTimestampSec();
    return true;
  } else {
    int32_t currentTime = taosGetTimestampSec();
    if (currentTime - startTime < 10) return true;
  }

  if (size > m_curLimit) {    
    if (3 == rand() % 20) {
      pTrace("====no alloc mem in func: %s, size:%d", _func, size);
      return false;
    }
  }

  return true;
}

void* taos_malloc(unsigned int size, char* _func) {

  if (false == isMallocMem(size, _func)) {    
    return NULL;
  }
  
  void *p = NULL;    
  p = malloc(size); 
  return p;
}

void* taos_calloc(unsigned int num, unsigned int size, char* _func) {
  
  if (false == isMallocMem(size, _func)) {
    return NULL;
  }
  
  void *p = NULL;    
  p = calloc(num, size); 
  return p;
}

void* taos_realloc(void* ptr, unsigned int size, char* _func) {
  
  if (false == isMallocMem(size, _func)) {
    return NULL;
  }
  
  void *p = NULL;  
  p = realloc(ptr, size); 
  return p;
}

void  taos_free(void* ptr) {
  free(ptr);
}


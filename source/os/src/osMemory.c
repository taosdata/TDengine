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

#define ALLOW_FORBID_FUNC
#include "os.h"

#define USE_TD_MEMORY

#define TD_MEMORY_SYMBOL ('T'<<24|'A'<<16|'O'<<8|'S')

#define TD_MEMORY_STACK_TRACE_DEPTH 10

typedef struct TdMemoryInfo
{
  int32_t symbol;
  void *stackTrace[TD_MEMORY_STACK_TRACE_DEPTH];     // gdb: disassemble /m 0xXXX
  int32_t memorySize; 
} *TdMemoryInfoPtr , TdMemoryInfo;

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

#else

#include<execinfo.h>
#define STACKCALL __attribute__((regparm(1), noinline))
void **STACKCALL taosGetEbp(void) {
  void **ebp = NULL;
  __asm__ __volatile__("mov %%rbp, %0;\n\t"
                       : "=m"(ebp)  /* output */
                       :            /* input */
                       : "memory"); /* not affect register */
  return (void **)(*ebp);
}
int32_t taosBackTrace(void **buffer, int32_t size) {
  int32_t frame = 0;
  void **ebp;
  void **ret = NULL;
  unsigned long long func_frame_distance = 0;
  if (buffer != NULL && size > 0) {
    ebp = taosGetEbp();
    func_frame_distance = (unsigned long long)(*ebp) - (unsigned long long)ebp;
    while (ebp && frame < size && (func_frame_distance < (1ULL << 24))  // assume function ebp more than 16M
           && (func_frame_distance > 0)) {
      ret = ebp + 1;
      buffer[frame++] = *ret;
      ebp = (void **)(*ebp);
      func_frame_distance = (unsigned long long)(*ebp) - (unsigned long long)ebp;
    }
  }
  return frame;
}
#endif

// char **taosBackTraceSymbols(int32_t *size) {
//   void  *buffer[20] = {NULL};
//   *size = taosBackTrace(buffer, 20);
//   return backtrace_symbols(buffer, *size);
// }

void *taosMemoryMalloc(int32_t size) {
  void *tmp = malloc(size + sizeof(TdMemoryInfo));
  if (tmp == NULL) return NULL;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)tmp;
  pTdMemoryInfo->memorySize = size;
  pTdMemoryInfo->symbol = TD_MEMORY_SYMBOL;
  taosBackTrace(pTdMemoryInfo->stackTrace,TD_MEMORY_STACK_TRACE_DEPTH);

  return (char*)tmp  + sizeof(TdMemoryInfo);
}

void *taosMemoryCalloc(int32_t num, int32_t size) {
  int32_t memorySize = num * size;
  char *tmp = calloc(memorySize + sizeof(TdMemoryInfo), 1);
  if (tmp == NULL) return NULL;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)tmp;
  pTdMemoryInfo->memorySize = memorySize;
  pTdMemoryInfo->symbol = TD_MEMORY_SYMBOL;
  taosBackTrace(pTdMemoryInfo->stackTrace,TD_MEMORY_STACK_TRACE_DEPTH);

  return (char*)tmp  + sizeof(TdMemoryInfo);
}

void *taosMemoryRealloc(void *ptr, int32_t size) {
  if (ptr == NULL) return taosMemoryMalloc(size);
  
  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char*)ptr - sizeof(TdMemoryInfo));
  assert(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL);

  TdMemoryInfo tdMemoryInfo;
  memcpy(&tdMemoryInfo, pTdMemoryInfo, sizeof(TdMemoryInfo));

  void *tmp = realloc(pTdMemoryInfo, size + sizeof(TdMemoryInfo));
  if (tmp == NULL) return NULL;
  
  memcpy(tmp, &tdMemoryInfo, sizeof(TdMemoryInfo));
  ((TdMemoryInfoPtr)tmp)->memorySize = size;

  return (char*)tmp  + sizeof(TdMemoryInfo);
}

void taosMemoryFree(const void *ptr) {
  if (ptr == NULL) return;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char*)ptr - sizeof(TdMemoryInfo));
  if(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL) {
    memset(pTdMemoryInfo, 0, sizeof(TdMemoryInfo));
    free(pTdMemoryInfo);
  } else {
    free((void*)ptr);
  }
}

int32_t taosMemorySize(void *ptr) {
  if (ptr == NULL) return 0;
  
  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char*)ptr - sizeof(TdMemoryInfo));
  assert(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL);

  return pTdMemoryInfo->memorySize;
}
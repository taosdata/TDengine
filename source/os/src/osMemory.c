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
#ifdef _TD_DARWIN_64
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include "os.h"

#if defined(USE_TD_MEMORY) || defined(USE_ADDR2LINE)

#define TD_MEMORY_SYMBOL ('T' << 24 | 'A' << 16 | 'O' << 8 | 'S')

#define TD_MEMORY_STACK_TRACE_DEPTH 10

typedef struct TdMemoryInfo *TdMemoryInfoPtr;

typedef struct TdMemoryInfo {
  int32_t symbol;
  int32_t memorySize;
  void   *stackTrace[TD_MEMORY_STACK_TRACE_DEPTH];  // gdb: disassemble /m 0xXXX
  // TdMemoryInfoPtr pNext;
  // TdMemoryInfoPtr pPrev;
} TdMemoryInfo;

// static TdMemoryInfoPtr GlobalMemoryPtr = NULL;

#ifdef WINDOWS
#define tstrdup(str) _strdup(str)

int32_t taosBackTrace(void **buffer, int32_t size) {
  int32_t frame = 0;
  return frame;
}

#ifdef USE_ADDR2LINE
#include <DbgHelp.h>
#pragma comment(lib, "dbghelp.lib")

void taosPrintBackTrace() {
#define MAX_STACK_FRAMES 20

  void *pStack[MAX_STACK_FRAMES];

  HANDLE process = GetCurrentProcess();
  SymInitialize(process, NULL, TRUE);
  WORD frames = CaptureStackBackTrace(1, MAX_STACK_FRAMES, pStack, NULL);

  char buf_tmp[1024];
  for (WORD i = 0; i < frames; ++i) {
    DWORD64 address = (DWORD64)(pStack[i]);

    DWORD64      displacementSym = 0;
    char         buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
    PSYMBOL_INFO pSymbol = (PSYMBOL_INFO)buffer;
    pSymbol->SizeOfStruct = sizeof(SYMBOL_INFO);
    pSymbol->MaxNameLen = MAX_SYM_NAME;

    DWORD           displacementLine = 0;
    IMAGEHLP_LINE64 line;
    // SymSetOptions(SYMOPT_LOAD_LINES);
    line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);

    if (SymFromAddr(process, address, &displacementSym, pSymbol) &&
        SymGetLineFromAddr64(process, address, &displacementLine, &line)) {
      snprintf(buf_tmp, sizeof(buf_tmp), "BackTrace %08" PRId64 " %s:%d %s\n", taosGetSelfPthreadId(), line.FileName,
               line.LineNumber, pSymbol->Name);
    } else {
      snprintf(buf_tmp, sizeof(buf_tmp), "BackTrace error: %d\n", GetLastError());
    }
    write(1, buf_tmp, strlen(buf_tmp));
  }
}
#endif
#else
#define tstrdup(str) strdup(str)

#include <execinfo.h>

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
  void  **ebp;
  void  **ret = NULL;
  size_t  func_frame_distance = 0;
  if (buffer != NULL && size > 0) {
    ebp = taosGetEbp();
    func_frame_distance = (size_t)*ebp - (size_t)ebp;
    while (ebp && frame < size && (func_frame_distance < (1ULL << 24))  // assume function ebp more than 16M
           && (func_frame_distance > 0)) {
      ret = ebp + 1;
      buffer[frame++] = *ret;
      ebp = (void **)(*ebp);
      func_frame_distance = (size_t)*ebp - (size_t)ebp;
    }
  }
  return frame;
}

// char **taosBackTraceSymbols(int32_t *size) {
//   void  *buffer[20] = {NULL};
//   *size = taosBackTrace(buffer, 20);
//   return backtrace_symbols(buffer, *size);
// }

#ifdef USE_ADDR2LINE

#include "dwarf.h"
#include "libdwarf.h"
#include "osThread.h"

#define DW_PR_DUu "llu"

typedef struct lookup_table {
  Dwarf_Line         *table;
  Dwarf_Line_Context *ctxts;
  int                 cnt;
  Dwarf_Addr          low;
  Dwarf_Addr          high;
} lookup_tableT;

extern int  create_lookup_table(Dwarf_Debug dbg, lookup_tableT *lookup_table);
extern void delete_lookup_table(lookup_tableT *lookup_table);

size_t              addr = 0;
lookup_tableT       lookup_table;
Dwarf_Debug         tDbg;
static TdThreadOnce traceThreadInit = PTHREAD_ONCE_INIT;

void endTrace() {
  TdThreadOnce tmp = PTHREAD_ONCE_INIT;
  if (memcmp(&traceThreadInit, &tmp, sizeof(TdThreadOnce)) != 0) {
    delete_lookup_table(&lookup_table);
    dwarf_finish(tDbg);
  }
}
void startTrace() {
  int       ret;
  Dwarf_Ptr errarg = 0;

  FILE *fp = fopen("/proc/self/maps", "r");
  fscanf(fp, "%lx-", &addr);
  fclose(fp);

  ret = dwarf_init_path("/proc/self/exe", NULL, 0, DW_GROUPNUMBER_ANY, NULL, errarg, &tDbg, NULL);
  if (ret == DW_DLV_NO_ENTRY) {
    printf("Unable to open file");
    return;
  }

  ret = create_lookup_table(tDbg, &lookup_table);
  if (ret != DW_DLV_OK) {
    printf("Unable to create lookup table");
    return;
  }
  atexit(endTrace);
}
static void print_line(Dwarf_Debug dbg, Dwarf_Line line, Dwarf_Addr pc) {
  char          *linesrc = "??";
  Dwarf_Unsigned lineno = 0;

  if (line) {
    dwarf_linesrc(line, &linesrc, NULL);
    dwarf_lineno(line, &lineno, NULL);
  }
  printf("BackTrace %08" PRId64 " %s:%" DW_PR_DUu "\n", taosGetSelfPthreadId(), linesrc, lineno);
  if (line) dwarf_dealloc(dbg, linesrc, DW_DLA_STRING);
}
void taosPrintBackTrace() {
  int        size = 20;
  void     **buffer[20];
  Dwarf_Addr pc;
  int32_t    frame = 0;
  void     **ebp;
  void     **ret = NULL;
  size_t     func_frame_distance = 0;

  taosThreadOnce(&traceThreadInit, startTrace);

  if (buffer != NULL && size > 0) {
    ebp = taosGetEbp();
    func_frame_distance = (size_t)*ebp - (size_t)ebp;
    while (ebp && frame < size && (func_frame_distance < (1ULL << 24)) && (func_frame_distance > 0)) {
      ret = ebp + 1;
      buffer[frame++] = *ret;
      ebp = (void **)(*ebp);
      func_frame_distance = (size_t)*ebp - (size_t)ebp;
    }
    for (size_t i = 0; i < frame; i++) {
      pc = (size_t)buffer[i] - addr;
      if (pc > 0) {
        if (pc >= lookup_table.low && pc < lookup_table.high) {
          Dwarf_Line line = lookup_table.table[pc - lookup_table.low];
          if (line) print_line(tDbg, line, pc);
        }
      }
    }
  }
}
#endif
#endif
#endif

#ifndef USE_ADDR2LINE
void taosPrintBackTrace() { return; }
#endif

int32_t taosMemoryDbgInit() {
#if defined(LINUX) && !defined(_ALPINE)
  int ret = mallopt(M_MMAP_THRESHOLD, 0);
  if (0 == ret) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  return 0;
#else
  return TSDB_CODE_FAILED;
#endif
}

int32_t taosMemoryDbgInitRestore() {
#if defined(LINUX) && !defined(_ALPINE)
  int ret = mallopt(M_MMAP_THRESHOLD, 128 * 1024);
  if (0 == ret) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  return 0;
#else
  return TSDB_CODE_FAILED;
#endif
}

void *taosMemoryMalloc(int64_t size) {
#ifdef USE_TD_MEMORY
  void *tmp = malloc(size + sizeof(TdMemoryInfo));
  if (tmp == NULL) return NULL;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)tmp;
  pTdMemoryInfo->memorySize = size;
  pTdMemoryInfo->symbol = TD_MEMORY_SYMBOL;
  taosBackTrace(pTdMemoryInfo->stackTrace, TD_MEMORY_STACK_TRACE_DEPTH);

  return (char *)tmp + sizeof(TdMemoryInfo);
#else
  return malloc(size);
#endif
}

void *taosMemoryCalloc(int64_t num, int64_t size) {
#ifdef USE_TD_MEMORY
  int32_t memorySize = num * size;
  char   *tmp = calloc(memorySize + sizeof(TdMemoryInfo), 1);
  if (tmp == NULL) return NULL;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)tmp;
  pTdMemoryInfo->memorySize = memorySize;
  pTdMemoryInfo->symbol = TD_MEMORY_SYMBOL;
  taosBackTrace(pTdMemoryInfo->stackTrace, TD_MEMORY_STACK_TRACE_DEPTH);

  return (char *)tmp + sizeof(TdMemoryInfo);
#else
  return calloc(num, size);
#endif
}

void *taosMemoryRealloc(void *ptr, int64_t size) {
#ifdef USE_TD_MEMORY
  if (ptr == NULL) return taosMemoryMalloc(size);

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char *)ptr - sizeof(TdMemoryInfo));
  ASSERT(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL);
  if (tpTdMemoryInfo->symbol != TD_MEMORY_SYMBOL) {
    +return NULL;
    +
  }

  TdMemoryInfo tdMemoryInfo;
  memcpy(&tdMemoryInfo, pTdMemoryInfo, sizeof(TdMemoryInfo));

  void *tmp = realloc(pTdMemoryInfo, size + sizeof(TdMemoryInfo));
  if (tmp == NULL) return NULL;

  memcpy(tmp, &tdMemoryInfo, sizeof(TdMemoryInfo));
  ((TdMemoryInfoPtr)tmp)->memorySize = size;

  return (char *)tmp + sizeof(TdMemoryInfo);
#else
  return realloc(ptr, size);
#endif
}

char *taosStrdup(const char *ptr) {
#ifdef USE_TD_MEMORY
  if (ptr == NULL) return NULL;

  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char *)ptr - sizeof(TdMemoryInfo));
  ASSERT(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL);
  if (pTdMemoryInfo->symbol != TD_MEMORY_SYMBOL) {
    return NULL;
  }
  void *tmp = tstrdup(pTdMemoryInfo);
  if (tmp == NULL) return NULL;

  memcpy(tmp, pTdMemoryInfo, sizeof(TdMemoryInfo));
  taosBackTrace(((TdMemoryInfoPtr)tmp)->stackTrace, TD_MEMORY_STACK_TRACE_DEPTH);

  return (char *)tmp + sizeof(TdMemoryInfo);
#else
  return tstrdup(ptr);
#endif
}

void taosMemoryFree(void *ptr) {
  if (NULL == ptr) return;
#ifdef USE_TD_MEMORY
  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char *)ptr - sizeof(TdMemoryInfo));
  if (pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL) {
    pTdMemoryInfo->memorySize = 0;
    // memset(pTdMemoryInfo, 0, sizeof(TdMemoryInfo));
    free(pTdMemoryInfo);
  } else {
    free(ptr);
  }
#else
  return free(ptr);
#endif
}

int64_t taosMemorySize(void *ptr) {
  if (ptr == NULL) return 0;

#ifdef USE_TD_MEMORY
  TdMemoryInfoPtr pTdMemoryInfo = (TdMemoryInfoPtr)((char *)ptr - sizeof(TdMemoryInfo));
  ASSERT(pTdMemoryInfo->symbol == TD_MEMORY_SYMBOL);
  if (pTdMemoryInfo->symbol != TD_MEMORY_SYMBOL) {
    +return NULL;
    +
  }

  return pTdMemoryInfo->memorySize;
#else
#ifdef WINDOWS
  return _msize(ptr);
#elif defined(_TD_DARWIN_64)
  return malloc_size(ptr);
#else
  return malloc_usable_size(ptr);
#endif
#endif
}

void taosMemoryTrim(int32_t size) {
#if defined(WINDOWS) || defined(DARWIN) || defined(_ALPINE)
  // do nothing
  return;
#else
  malloc_trim(size);
#endif
}

void *taosMemoryMallocAlign(uint32_t alignment, int64_t size) {
#ifdef USE_TD_MEMORY
  ASSERT(0);
#else
#if defined(LINUX)
  void *p = memalign(alignment, size);
  return p;
#else
  return taosMemoryMalloc(size);
#endif
#endif
}

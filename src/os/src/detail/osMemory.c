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
#include "tulog.h"

#ifdef TAOS_MEM_CHECK

static ETaosMemoryAllocMode allocMode = TAOS_ALLOC_MODE_DEFAULT;
static FILE* fpAllocLog = NULL;

////////////////////////////////////////////////////////////////////////////////
// memory allocator which fails randomly

extern int32_t taosGetTimestampSec();
static int32_t startTime = INT32_MAX;

static bool taosRandomAllocFail(size_t size, const char* file, uint32_t line) {
  if (taosGetTimestampSec() < startTime) {
    return false;
  }

  if (size < 100 * (size_t)1024) {
    return false;
  }

  if (rand() % 20 != 0) {
    return false;
  }

  if (fpAllocLog != NULL) {
    fprintf(fpAllocLog, "%s:%d: memory allocation of %" PRIzu " bytes will fail.\n", file, line, size);
  }

  return true;
}

static void* taosRandmoMalloc(size_t size, const char* file, uint32_t line) {
  return taosRandomAllocFail(size, file, line) ? NULL : malloc(size);
}

static void* taosRandomCalloc(size_t num, size_t size, const char* file, uint32_t line) {
  return taosRandomAllocFail(num * size, file, line) ? NULL : calloc(num, size);
}

static void* taosRandomRealloc(void* ptr, size_t size, const char* file, uint32_t line) {
  return taosRandomAllocFail(size, file, line) ? NULL : realloc(ptr, size);
}

static char* taosRandomStrdup(const char* str, const char* file, uint32_t line) {
  size_t len = strlen(str);
  return taosRandomAllocFail(len + 1, file, line) ? NULL : taosStrdupImp(str);
}

static char* taosRandomStrndup(const char* str, size_t size, const char* file, uint32_t line) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  return taosRandomAllocFail(len + 1, file, line) ? NULL : taosStrndupImp(str, len);
}

static ssize_t taosRandomGetline(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  return taosRandomAllocFail(*n, file, line) ? -1 : taosGetlineImp(lineptr, n, stream);
}

////////////////////////////////////////////////////////////////////////////////
// memory allocator with leak detection

#define MEMBLK_MAGIC 0x55AA

typedef struct SMemBlock {
  const char* file;
  uint16_t line;
  uint16_t magic;
  uint32_t size;
  struct SMemBlock* prev;
  struct SMemBlock* next;
  // TODO: need pading in 32bit platform
  char data[0];
} SMemBlock;

static SMemBlock *blocks = NULL;
static uintptr_t lock = 0;

static void taosAddMemBlock(SMemBlock* blk) {
  blk->prev = NULL;
  while (atomic_val_compare_exchange_ptr(&lock, 0, 1) != 0);
  blk->next = blocks;
  if (blocks != NULL) {
    blocks->prev = blk;
  }
  blocks = blk;
  atomic_store_ptr(&lock, 0);
}

static void taosRemoveMemBlock(SMemBlock* blk) {
  while (atomic_val_compare_exchange_ptr(&lock, 0, 1) != 0);

  if (blocks == blk) {
    blocks = blk->next;
  }
  if (blk->prev != NULL) {
    blk->prev->next = blk->next;
  }
  if (blk->next != NULL) {
    blk->next->prev = blk->prev;
  }

  atomic_store_ptr(&lock, 0);

  blk->prev = NULL;
  blk->next = NULL;
}

static void taosFreeDetectLeak(void* ptr, const char* file, uint32_t line) {
  if (ptr == NULL) {
    return;
  }

  SMemBlock* blk = (SMemBlock*)(((char*)ptr) - sizeof(SMemBlock));
  if (blk->magic != MEMBLK_MAGIC) {
    if (fpAllocLog != NULL) {
      fprintf(fpAllocLog, "%s:%d: memory is allocated by default allocator.\n", file, line);
    }
    free(ptr);
    return;
  }

  taosRemoveMemBlock(blk);
  free(blk);
}

static void* taosMallocDetectLeak(size_t size, const char* file, uint32_t line) {
  if (size == 0) {
    return NULL;
  }

  SMemBlock *blk = (SMemBlock*)malloc(size + sizeof(SMemBlock));
  if (blk == NULL) {
    return NULL;
  }

  if (line > UINT16_MAX && fpAllocLog != NULL) {
    fprintf(fpAllocLog, "%s:%d: line number too large.\n", file, line);
  }

  if (size > UINT32_MAX && fpAllocLog != NULL) {
    fprintf(fpAllocLog, "%s:%d: size too large: %" PRIzu ".\n", file, line, size);
  }

  blk->file = file;
  blk->line = (uint16_t)line;
  blk->magic = MEMBLK_MAGIC;
  blk->size = size;
  taosAddMemBlock(blk);

  return blk->data;
}

static void* taosCallocDetectLeak(size_t num, size_t size, const char* file, uint32_t line) {
  size *= num;
  void* p = taosMallocDetectLeak(size, file, line);
  if (p != NULL) {
    memset(p, 0, size);
  }
  return p;
}

static void* taosReallocDetectLeak(void* ptr, size_t size, const char* file, uint32_t line) {
  if (size == 0) {
    taosFreeDetectLeak(ptr, file, line);
    return NULL;
  }

  if (ptr == NULL) {
    return taosMallocDetectLeak(size, file, line);
  }

  SMemBlock* blk = (SMemBlock *)((char*)ptr) - sizeof(SMemBlock);
  if (blk->magic != MEMBLK_MAGIC) {
    if (fpAllocLog != NULL) {
      fprintf(fpAllocLog, "%s:%d: memory is allocated by default allocator.\n", file, line);
    }
    return realloc(ptr, size);
  }

  taosRemoveMemBlock(blk);

  void* p = realloc(blk, size + sizeof(SMemBlock));
  if (p == NULL) {
    taosAddMemBlock(blk);
    return NULL;
  }

  if (size > UINT32_MAX && fpAllocLog != NULL) {
    fprintf(fpAllocLog, "%s:%d: size too large: %" PRIzu ".\n", file, line, size);
  }

  blk = (SMemBlock*)p;
  blk->size = size;
  taosAddMemBlock(blk);
  return blk->data;
}

static char* taosStrdupDetectLeak(const char* str, const char* file, uint32_t line) {
  size_t len = strlen(str);
  char *p = taosMallocDetectLeak(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

static char* taosStrndupDetectLeak(const char* str, size_t size, const char* file, uint32_t line) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  char *p = taosMallocDetectLeak(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

static ssize_t taosGetlineDetectLeak(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  char* buf = NULL;
  size_t bufSize = 0;
  ssize_t size = taosGetlineImp(&buf, &bufSize, stream);
  if (size != -1) {
    if (*n < size + 1) {
      void* p = taosReallocDetectLeak(*lineptr, size + 1, file, line);
      if (p == NULL) {
        free(buf);
        return -1;
      }
      *lineptr = (char*)p;
      *n = size + 1;
    }
    memcpy(*lineptr, buf, size + 1);
  }

  free(buf);
  return size;
}

static void taosDumpMemoryLeakImp() {
  const char* hex = "0123456789ABCDEF";
  const char* fmt = ":%d: addr=%p, size=%d, content(first 16 bytes)=";
  size_t numOfBlk = 0, totalSize = 0;

  if (fpAllocLog == NULL) {
    return;
  }

  fputs("memory blocks allocated but not freed before exit:\n", fpAllocLog);

  while (atomic_val_compare_exchange_ptr(&lock, 0, 1) != 0);

  for (SMemBlock* blk = blocks; blk != NULL; blk = blk->next) {
    ++numOfBlk;
    totalSize += blk->size;

    fputs(blk->file, fpAllocLog);
    fprintf(fpAllocLog, fmt, blk->line, blk->data, blk->size);

    char sep = '\'';
    size_t size = blk->size > 16 ? 16 : blk->size;
    for (size_t i = 0; i < size; ++i) {
      uint8_t c = (uint8_t)(blk->data[i]);
      fputc(sep, fpAllocLog);
      sep = ' ';
      fputc(hex[c >> 4], fpAllocLog);
      fputc(hex[c & 0x0f], fpAllocLog);
    }

    fputs("'\n", fpAllocLog);
  }

  atomic_store_ptr(&lock, 0);

  fprintf(fpAllocLog, "\nnumber of blocks: %" PRIzu ", total bytes: %" PRIzu "\n", numOfBlk, totalSize);
  fflush(fpAllocLog);
}

static void taosDumpMemoryLeakOnSig(int sig) {
  fprintf(fpAllocLog, "signal %d received.\n", sig);

  // restore default signal handler
  struct sigaction act = {0};
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, NULL);

  taosDumpMemoryLeakImp();
}

////////////////////////////////////////////////////////////////////////////////
// interface functions

void* taosMallocMem(size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return malloc(size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandmoMalloc(size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosMallocDetectLeak(size, file, line);
  }
  return malloc(size);
}

void* taosCallocMem(size_t num, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return calloc(num, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandomCalloc(num, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosCallocDetectLeak(num, size, file, line);
  }
  return calloc(num, size);
}

void* taosReallocMem(void* ptr, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return realloc(ptr, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandomRealloc(ptr, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosReallocDetectLeak(ptr, size, file, line);
  }
  return realloc(ptr, size);
}

void  taosFreeMem(void* ptr, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return free(ptr);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return free(ptr);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosFreeDetectLeak(ptr, file, line);
  }
  return free(ptr);
}

char* taosStrdupMem(const char* str, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return taosStrdupImp(str);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandomStrdup(str, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosStrdupDetectLeak(str, file, line);
  }
  return taosStrdupImp(str);
}

char* taosStrndupMem(const char* str, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return taosStrndupImp(str, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandomStrndup(str, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosStrndupDetectLeak(str, size, file, line);
  }
  return taosStrndupImp(str, size);
}

ssize_t taosGetlineMem(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return taosGetlineImp(lineptr, n, stream);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return taosRandomGetline(lineptr, n, stream, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return taosGetlineDetectLeak(lineptr, n, stream, file, line);
  }
  return taosGetlineImp(lineptr, n, stream);
}

static void taosCloseAllocLog() {
  if (fpAllocLog != NULL) {
    if (fpAllocLog != stdout) {
      fclose(fpAllocLog);
    }
    fpAllocLog = NULL;
  }
}

void taosSetAllocMode(int mode, const char* path, bool autoDump) {
  assert(mode >= TAOS_ALLOC_MODE_DEFAULT);
  assert(mode <= TAOS_ALLOC_MODE_DETECT_LEAK);

  if (fpAllocLog != NULL || allocMode != TAOS_ALLOC_MODE_DEFAULT) {
    printf("memory allocation mode can only be set once.\n");
    return;
  }

  if (path == NULL || path[0] == 0) {
    fpAllocLog = stdout;
  } else if ((fpAllocLog = fopen(path, "w")) != NULL) {
    atexit(taosCloseAllocLog);
  } else {
    printf("failed to open memory allocation log file '%s', errno=%d\n", path, errno);
    return;
  }

  allocMode = mode;

  if (mode == TAOS_ALLOC_MODE_RANDOM_FAIL) {
    startTime = taosGetTimestampSec() + 10;
    return;
  }

  if (autoDump && mode == TAOS_ALLOC_MODE_DETECT_LEAK) {
    atexit(taosDumpMemoryLeakImp);

    struct sigaction act = {0};
    act.sa_handler = taosDumpMemoryLeakOnSig;
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGILL, &act, NULL);
  }
}

void taosDumpMemoryLeak() {
  taosDumpMemoryLeakImp();
  taosCloseAllocLog();
}

#else // 'TAOS_MEM_CHECK' not defined

void taosSetAllocMode(int mode, const char* path, bool autoDump) {
  // do nothing
}

void taosDumpMemoryLeak() {
  // do nothing
}

#endif // TAOS_MEM_CHECK

void *taosTMalloc(size_t size) {
  if (size <= 0) return NULL;

  void *ret = malloc(size + sizeof(size_t));
  if (ret == NULL) return NULL;

  *(size_t *)ret = size;

  return (void *)((char *)ret + sizeof(size_t));
}

void *taosTCalloc(size_t nmemb, size_t size) {
  size_t tsize = nmemb * size;
  void * ret = taosTMalloc(tsize);
  if (ret == NULL) return NULL;

  taosTMemset(ret, 0);
  return ret;
}

size_t taosTSizeof(void *ptr) { return (ptr) ? (*(size_t *)((char *)ptr - sizeof(size_t))) : 0; }

void taosTMemset(void *ptr, int c) { memset(ptr, c, taosTSizeof(ptr)); }

void * taosTRealloc(void *ptr, size_t size) {
  if (ptr == NULL) return taosTMalloc(size);

  if (size <= taosTSizeof(ptr)) return ptr;

  void * tptr = (void *)((char *)ptr - sizeof(size_t));
  size_t tsize = size + sizeof(size_t);
  tptr = realloc(tptr, tsize);
  if (tptr == NULL) return NULL;

  *(size_t *)tptr = size;

  return (void *)((char *)tptr + sizeof(size_t));
}

void taosTZfree(void *ptr) {
  if (ptr) {
    free((void *)((char *)ptr - sizeof(size_t)));
  }
}
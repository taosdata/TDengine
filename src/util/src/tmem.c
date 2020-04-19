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

#include "os.h"
#include "tulog.h"

#define TAOS_MEM_CHECK_IMPL
#include "tutil.h"


#ifdef TAOS_MEM_CHECK

static int allocMode = TAOS_ALLOC_MODE_DEFAULT;
static FILE* fpAllocLog = NULL;

////////////////////////////////////////////////////////////////////////////////
// memory allocator which fails randomly

extern int32_t taosGetTimestampSec();
static int32_t startTime = INT32_MAX;;

static bool random_alloc_fail(size_t size, const char* file, uint32_t line) {
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
    fprintf(fpAllocLog, "%s:%d: memory allocation of %zu bytes will fail.\n", file, line, size);
  }

  return true;
}

static void* malloc_random(size_t size, const char* file, uint32_t line) {
  return random_alloc_fail(size, file, line) ? NULL : malloc(size);
}

static void* calloc_random(size_t num, size_t size, const char* file, uint32_t line) {
  return random_alloc_fail(num * size, file, line) ? NULL : calloc(num, size);
}

static void* realloc_random(void* ptr, size_t size, const char* file, uint32_t line) {
  return random_alloc_fail(size, file, line) ? NULL : realloc(ptr, size);
}

static char* strdup_random(const char* str, const char* file, uint32_t line) {
  size_t len = strlen(str);
  return random_alloc_fail(len + 1, file, line) ? NULL : strdup(str);
}

static char* strndup_random(const char* str, size_t size, const char* file, uint32_t line) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  return random_alloc_fail(len + 1, file, line) ? NULL : strndup(str, len);
}

static ssize_t getline_random(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  return random_alloc_fail(*n, file, line) ? -1 : getline(lineptr, n, stream);
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

static void add_mem_block(SMemBlock* blk) {
  blk->prev = NULL;
  while (atomic_val_compare_exchange_ptr(&lock, 0, 1) != 0);
  blk->next = blocks;
  if (blocks != NULL) {
    blocks->prev = blk;
  }
  blocks = blk;
  atomic_store_ptr(&lock, 0);
}

static void remove_mem_block(SMemBlock* blk) {
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

static void free_detect_leak(void* ptr, const char* file, uint32_t line) {
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

  remove_mem_block(blk);
  free(blk);
}

static void* malloc_detect_leak(size_t size, const char* file, uint32_t line) {
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
    fprintf(fpAllocLog, "%s:%d: size too large: %zu.\n", file, line, size);
  }

  blk->file = file;
  blk->line = (uint16_t)line;
  blk->magic = MEMBLK_MAGIC;
  blk->size = size;
  add_mem_block(blk);

  return blk->data;
}

static void* calloc_detect_leak(size_t num, size_t size, const char* file, uint32_t line) {
  size *= num;
  void* p = malloc_detect_leak(size, file, line);
  if (p != NULL) {
    memset(p, 0, size);
  }
  return p;
}

static void* realloc_detect_leak(void* ptr, size_t size, const char* file, uint32_t line) {
  if (size == 0) {
    free_detect_leak(ptr, file, line);
    return NULL;
  }

  if (ptr == NULL) {
    return malloc_detect_leak(size, file, line);
  }

  SMemBlock* blk = ((char*)ptr) - sizeof(SMemBlock);
  if (blk->magic != MEMBLK_MAGIC) {
    if (fpAllocLog != NULL) {
      fprintf(fpAllocLog, "%s:%d: memory is allocated by default allocator.\n", file, line);
    }
    return realloc(ptr, size);
  }

  remove_mem_block(blk);

  void* p = realloc(blk, size + sizeof(SMemBlock));
  if (p == NULL) {
    add_mem_block(blk);
    return NULL;
  }

  if (size > UINT32_MAX && fpAllocLog != NULL) {
    fprintf(fpAllocLog, "%s:%d: size too large: %zu.\n", file, line, size);
  }

  blk = (SMemBlock*)p;
  blk->size = size;
  add_mem_block(blk);
  return blk->data;
}

static char* strdup_detect_leak(const char* str, const char* file, uint32_t line) {
  size_t len = strlen(str);
  char *p = malloc_detect_leak(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

static char* strndup_detect_leak(const char* str, size_t size, const char* file, uint32_t line) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  char *p = malloc_detect_leak(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

static ssize_t getline_detect_leak(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  char* buf = NULL;
  size_t bufSize = 0;
  ssize_t size = getline(&buf, &bufSize, stream);
  if (size != -1) {
    if (*n < size + 1) {
      void* p = realloc_detect_leak(*lineptr, size + 1, file, line);
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

static void dump_memory_leak() {
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

  fprintf(fpAllocLog, "\nnumber of blocks: %zu, total bytes: %zu\n", numOfBlk, totalSize);
  fflush(fpAllocLog);
}

static void dump_memory_leak_on_sig(int sig) {
  fprintf(fpAllocLog, "signal %d received.\n", sig);

  // restore default signal handler
  struct sigaction act = {0};
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, NULL);

  dump_memory_leak();
}

////////////////////////////////////////////////////////////////////////////////
// interface functions

void* taos_malloc(size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return malloc(size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return malloc_random(size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return malloc_detect_leak(size, file, line);
  }
  return malloc(size);
}

void* taos_calloc(size_t num, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return calloc(num, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return calloc_random(num, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return calloc_detect_leak(num, size, file, line);
  }
  return calloc(num, size);
}

void* taos_realloc(void* ptr, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return realloc(ptr, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return realloc_random(ptr, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return realloc_detect_leak(ptr, size, file, line);
  }
  return realloc(ptr, size);
}

void  taos_free(void* ptr, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return free(ptr);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return free(ptr);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return free_detect_leak(ptr, file, line);
  }
  return free(ptr);
}

char* taos_strdup(const char* str, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return strdup(str);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return strdup_random(str, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return strdup_detect_leak(str, file, line);
  }
  return strdup(str);
}

char* taos_strndup(const char* str, size_t size, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return strndup(str, size);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return strndup_random(str, size, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return strndup_detect_leak(str, size, file, line);
  }
  return strndup(str, size);
}

ssize_t taos_getline(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  switch (allocMode) {
    case TAOS_ALLOC_MODE_DEFAULT:
      return getline(lineptr, n, stream);

    case TAOS_ALLOC_MODE_RANDOM_FAIL:
      return getline_random(lineptr, n, stream, file, line);

    case TAOS_ALLOC_MODE_DETECT_LEAK:
      return getline_detect_leak(lineptr, n, stream, file, line);
  }
  return getline(lineptr, n, stream);
}

static void close_alloc_log() {
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
    atexit(close_alloc_log);
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
    atexit(dump_memory_leak);

    struct sigaction act = {0};
    act.sa_handler = dump_memory_leak_on_sig;
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGILL, &act, NULL);
  }
}

void taosDumpMemoryLeak() {
  dump_memory_leak();
  close_alloc_log();
}

#else // 'TAOS_MEM_CHECK' not defined

void taosSetAllocMode(int mode, const char* path, bool autoDump) {
  // do nothing
}

void taosDumpMemoryLeak() {
  // do nothing
}

#endif // TAOS_MEM_CHECK

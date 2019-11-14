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
#include "tlog.h"
#include "os.h"

#if TAOS_MEM_CHECK == 1

extern int32_t taosGetTimestampSec();
static int32_t startTime = 0;
static int64_t m_curLimit = 100 * 1024;

static bool isMallocMem(size_t size, const char* func) {
  if (0 == startTime) {
    startTime = taosGetTimestampSec();
    return true;
  } else {
    int32_t currentTime = taosGetTimestampSec();
    if (currentTime - startTime < 10) return true;
  }

  if (size > m_curLimit) {
    if (3 == rand() % 20) {
      pTrace("====no alloc mem in func: %s, size:%d", func, size);
      return false;
    }
  }

  return true;
}

void* taos_malloc(size_t size, const char* func) {
  if (false == isMallocMem(size, func)) {
    return NULL;
  }
  return malloc(size);
}

void* taos_calloc(size_t num, size_t size, const char* func) {
  if (false == isMallocMem(size, func)) {
    return NULL;
  }
  return calloc(num, size);
}

void* taos_realloc(void* ptr, size_t size, const char* func) {
  if (false == isMallocMem(size, func)) {
    return NULL;
  }
  return realloc(ptr, size);
}

void taos_free(void* ptr) { free(ptr); }

char* taos_strdup(const char* str, const char* func) {
  size_t len = strlen(str);
  return isMallocMem(len + 1, func) ? strdup(str) : NULL;
}

char* taos_strndup(const char* str, size_t size, const char* func) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  return isMallocMem(len + 1, func) ? strndup(str, len) : NULL;
}

#elif TAOS_MEM_CHECK == 2

#define MAGIC 0x55AA

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
static FILE* fpMemLeak = NULL;

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

void* taos_malloc(size_t size, const char* file, uint32_t line) {
  if (size == 0) {
    return NULL;
  }

  SMemBlock *blk = (SMemBlock*)malloc(size + sizeof(SMemBlock));
  if (blk == NULL) {
    return NULL;
  }

  if (line > UINT16_MAX && fpMemLeak != NULL) {
    fprintf(fpMemLeak, "%s:%d: line number too large.\n", file, line);
  }

  if (size > UINT32_MAX && fpMemLeak != NULL) {
    fprintf(fpMemLeak, "%s:%d: size too large: %lld.\n", file, line, size);
  }

  blk->file = file;
  blk->line = (uint16_t)line;
  blk->magic = MAGIC;
  blk->size = size;
  add_mem_block(blk);

  return blk->data;
}

void* taos_calloc(size_t num, size_t size, const char* file, uint32_t line) {
  size *= num;
  void* p = taos_malloc(size, file, line);
  if (p != NULL) {
    memset(p, 0, size);
  }
  return p;
}

void* taos_realloc(void* ptr, size_t size, const char* file, uint32_t line) {
  if (size == 0) {
    taos_free(ptr, file, line);
    return NULL;
  }

  if (ptr == NULL) {
    return taos_malloc(size, file, line);
  }

  SMemBlock* blk = ((char*)ptr) - sizeof(SMemBlock);
  if (blk->magic != MAGIC) {
    if (fpMemLeak != NULL) {
      fprintf(fpMemLeak, "%s:%d: memory not allocated by 'taos_malloc'.\n", file, line);
    }
    return realloc(ptr, size);
  }

  remove_mem_block(blk);

  void* p = realloc(blk, size + sizeof(SMemBlock));
  if (p == NULL) {
    add_mem_block(blk);
    return NULL;
  }

  if (size > UINT32_MAX && fpMemLeak != NULL) {
    fprintf(fpMemLeak, "%s:%d: size too large: %lld.\n", file, line, size);
  }

  blk = (SMemBlock*)p;
  blk->size = size;
  add_mem_block(blk);
  return blk->data;
}

void taos_free(void* ptr, const char* file, uint32_t line) {
  if (ptr == NULL) {
    return;
  }

  SMemBlock* blk = ((char*)ptr) - sizeof(SMemBlock);
  if (blk->magic != MAGIC) {
    if (fpMemLeak != NULL) {
      fprintf(fpMemLeak, "%s:%d: memory not allocated by 'taos_malloc'.\n", file, line);
    }
    free(ptr);
    return;
  }

  remove_mem_block(blk);
  free(blk);
}

char* taos_strdup(const char* str, const char* file, uint32_t line) {
  size_t len = strlen(str);
  char *p = taos_malloc(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

char* taos_strndup(const char* str, size_t size, const char* file, uint32_t line) {
  size_t len = strlen(str);
  if (len > size) {
    len = size;
  }
  char *p = taos_malloc(len + 1, file, line);
  if (p != NULL) {
    memcpy(p, str, len);
    p[len] = 0;
  }
  return p;
}

ssize_t taos_getline(char **lineptr, size_t *n, FILE *stream, const char* file, uint32_t line) {
  char* buf = NULL;
  size_t bufSize = 0;
  ssize_t size = getline(&buf, &bufSize, stream);
  if (size != -1) {
    if (*n < size + 1) {
      void* p = taos_realloc(*lineptr, size + 1, file, line);
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
  const char* fmt = ":%d: addr=0x%p, size=%d, content(first 16 bytes)='";
  size_t numOfBlk = 0, totalSize = 0;

  fputs("memory blocks allocated but not freed before exit:\n\n", fpMemLeak);

  while (atomic_val_compare_exchange_ptr(&lock, 0, 1) != 0);

  for (SMemBlock* blk = blocks; blk != NULL; blk = blk->next) {
    ++numOfBlk;
    totalSize += blk->size;

    fputs(blk->file, fpMemLeak);
    fprintf(fpMemLeak, fmt, blk->line, blk->data, blk->size);

    uint8_t c = (uint8_t)(blk->data[0]);
    fputc(hex[c >> 4], fpMemLeak);
    fputc(hex[c & 0x0f], fpMemLeak);

    size_t size = blk->size > 16 ? 16 : blk->size;
    for (size_t i = 1; i < size; ++i) {
      c = (uint8_t)(blk->data[i]);
      fputc(' ', fpMemLeak);
      fputc(hex[c >> 4], fpMemLeak);
      fputc(hex[c & 0x0f], fpMemLeak);
    }

    fputs("'\n", fpMemLeak);
  }

  atomic_store_ptr(&lock, 0);

  fprintf("\nnumber of blocks: %lld, total bytes: %lld\n", numOfBlk, totalSize);
  if (fpMemLeak != stdout) {
    fclose(fpMemLeak);
    fpMemLeak = NULL;
  }
}

static void dump_memory_leak_at_sig(int sig) {
  fprintf(fpMemLeak, "signal %d received, exiting...\n", sig);
  dump_memory_leak();
  struct sigaction act = {0};
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, NULL);
}

void taos_dump_memory_leak_at_exit(const char* path) {
  if (path == NULL || path[0] == 0) {
    fpMemLeak = stdout;
  } else if ((fpMemLeak = fopen(path, "w")) == NULL) {
    printf("failed to open memory leak dump file '%s', errno=%d\n", path, errno);
    return;
  }

  atexit(dump_memory_leak);

  struct sigaction act = {0};
  act.sa_handler = dump_memory_leak_at_sig;
  sigaction(SIGFPE, &act, NULL);
  sigaction(SIGSEGV, &act, NULL);
  sigaction(SIGILL, &act, NULL);
}

#endif

#if TAOS_MEM_CHECK != 2
void taos_dump_memory_leak_at_exit(const char* path) {
  printf("memory leak detection not enabled!")
}
#endif
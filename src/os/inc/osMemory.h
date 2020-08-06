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

#ifndef TDENGINE_OS_MEMORY_H
#define TDENGINE_OS_MEMORY_H

#include "osString.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_ALLOC_MODE_DEFAULT = 0,
  TAOS_ALLOC_MODE_RANDOM_FAIL = 1,
  TAOS_ALLOC_MODE_DETECT_LEAK = 2
} ETaosMemoryAllocMode;

void   taosSetAllocMode(int mode, const char *path, bool autoDump);
void   taosDumpMemoryLeak();

void * taosTMalloc(size_t size);
void * taosTCalloc(size_t nmemb, size_t size);
void * taosTRealloc(void *ptr, size_t size);
void   taosTZfree(void *ptr);
size_t taosTSizeof(void *ptr);
void   taosTMemset(void *ptr, int c);

#define taosTFree(x)     \
  do {                   \
    if (x) {             \
      free((void *)(x)); \
      x = 0;             \
    }                    \
  } while (0);

#define taosMalloc(size) malloc(size)
#define taosCalloc(num, size) calloc(num, size)
#define taosRealloc(ptr, size) realloc(ptr, size)
#define taosFree(ptr) free(ptr)
#define taosStrdup(str) taosStrdupImp(str)
#define taosStrndup(str, size) taosStrndupImp(str, size)
#define taosGetline(lineptr, n, stream) taosGetlineImp(lineptr, n, stream)

#ifdef TAOS_MEM_CHECK
  #ifdef TAOS_MEM_CHECK_TEST
    void *  taos_malloc(size_t size, const char *file, uint32_t line);
    void *  taos_calloc(size_t num, size_t size, const char *file, uint32_t line);
    void *  taos_realloc(void *ptr, size_t size, const char *file, uint32_t line);
    void    taos_free(void *ptr, const char *file, uint32_t line);
    char *  taos_strdup(const char *str, const char *file, uint32_t line);
    char *  taos_strndup(const char *str, size_t size, const char *file, uint32_t line);
    ssize_t taos_getline(char **lineptr, size_t *n, FILE *stream, const char *file, uint32_t line);
    #undef taosMalloc
    #undef taosCalloc
    #undef taosRealloc
    #undef taosFree
    #undef taosStrdup
    #undef taosStrndup
    #undef taosGetline
    #define taosMalloc(size) taos_malloc(size, __FILE__, __LINE__)
    #define taosCalloc(num, size) taos_calloc(num, size, __FILE__, __LINE__)
    #define taosRealloc(ptr, size) taos_realloc(ptr, size, __FILE__, __LINE__)
    #define taosFree(ptr) taos_free(ptr, __FILE__, __LINE__)
    //#define taosStrdup(str) taos_strdup(str, __FILE__, __LINE__)
    //#define taosStrndup(str, size) taos_strndup(str, size, __FILE__, __LINE__)
    //#define taosGetline(lineptr, n, stream) taos_getline(lineptr, n, stream, __FILE__, __LINE__)
  #endif  
#endif 

#ifdef __cplusplus
}
#endif

#endif

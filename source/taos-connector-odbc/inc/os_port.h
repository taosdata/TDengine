/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _os_port_h_
#define _os_port_h_

#include "macros.h"

EXTERN_C_BEGIN

#include <time.h>
#ifdef _WIN32            /* { */
struct tm* localtime_r(const time_t *clock, struct tm *result);
struct tm* gmtime_r(const time_t *clock, struct tm *result);
int gettimeofday(struct timeval *tp, void *tzp);
#else                    /* }{ */
#include <stdlib.h>
#include <sys/time.h>
#endif                   /* } */

#ifdef _WIN32            /* { */
#ifndef PATH_MAX         /* { */
#define PATH_MAX  MAX_PATH
#endif                   /* } */
#else                    /* }{ */
#include <limits.h>
#endif                   /* } */

char* tod_dirname(const char *path, char *buf, size_t sz) FA_HIDDEN;
char* tod_basename(const char *path, char *buf, size_t sz) FA_HIDDEN;
char* tod_strncpy(char *dest, const char *src, size_t n) FA_HIDDEN;

#ifdef _WIN32            /* { */
#define strdup _strdup
char* strndup(const char *s, size_t n);
#endif                   /* } */

#ifndef __cplusplus      /* { */
#ifdef _WIN32            /* { */
#define atomic_int               LONG
static inline LONG atomic_fetch_add(atomic_int *obj, LONG arg)
{
    LONG r = InterlockedAdd(obj, arg);
    return r - arg;
}

static inline LONG atomic_fetch_sub(atomic_int *obj, LONG arg)
{
    LONG r = InterlockedAdd(obj, -arg);
    return r + arg;
}

static inline LONG atomic_load(atomic_int *obj)
{
    return atomic_fetch_add(obj, 0);
}
#else                    /* }{ */
#include <stdatomic.h>
#endif                   /* } */
#endif                   /* } */

#ifdef _WIN32            /* { */
char* tod_getenv(const char *name);
int tod_setenv(const char *name, const char *value, int overwrite);
#else                    /* }{ */
#define tod_getenv     getenv
#define tod_setenv     setenv
#endif                   /* } */

#ifdef _WIN32            /* { */
typedef INIT_ONCE pthread_once_t;
#define PTHREAD_ONCE_INIT INIT_ONCE_STATIC_INIT
int pthread_once(pthread_once_t *once_control, void (*init_routine)(void));
#else                    /* }{ */
#include <pthread.h>
#endif                   /* } */

#ifdef _WIN32            /* { */
void* dlopen(const char* path, int mode);
int dlclose(void* handle);
void * dlsym(void *handle, const char *symbol);
const char * dlerror(void);
#define RTLD_NOW                            0
#ifndef RTLD_DEFAULT
#define RTLD_DEFAULT 0
#endif
#else                    /* }{ */
#include <dlfcn.h>
#endif                   /* } */

#ifdef _WIN32            /* { */
#ifndef SQL_NULL_DESC    /* { */
#define SQL_NULL_DESC NULL
#endif                   /* } */
#endif                   /* } */

EXTERN_C_END

#endif // _os_port_h_


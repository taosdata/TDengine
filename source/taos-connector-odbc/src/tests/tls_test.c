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

#include "logger.h"
#include "os_port.h"
#include "tls.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef _WIN32          /* { */
static void* _test_case1_routine(void *arg)
{
  (void)arg;
  mem_t *mem = tls_get_mem_intermediate();
  if (!mem) {
    E("tls_get_mem_intermediate failed");
    return (void*)(uintptr_t)-1;
  }
  const size_t sz = 1024 * 1024 * 64;
  int r = mem_keep(mem, sz);
  if (r) {
    E("mem_keep failed");
    return (void*)(uintptr_t)-2;
  }
  mem->base[0] = '0';

  mem_t *mem2 = tls_get_mem_intermediate();
  if (mem != mem2) {
    E("tls_get_mem_intermediate implementation failure");
    return (void*)(uintptr_t)-3;
  }

  mem->base[sz-1] = '1';

  return NULL;
}

static int test_case1(void)
{
  int ok = 1;
#define CASE7_NR 16

  pthread_t threads[CASE7_NR];
  size_t i = 0;

  for (i=0; i<sizeof(threads)/sizeof(threads[0]); ++i) {
    int r = pthread_create(threads + i, NULL, _test_case1_routine, NULL);
    if (r) {
      E("pthread_create failed");
      ok = 0;
      break;
    }
  }
  while (i>0) {
    void *p = NULL;
    int r = pthread_join(threads[i-1], &p);
    if (r) {
      E("pthread_join failed");
      ok = 0;
    }
    if (p) {
      E("_test_case1_routine failed");
      ok = 0;
    }
    --i;
  }

  if (!ok) return -1;

  return 0;
}
#endif                  /* } */

static int test(void)
{
  int r = 0;

#ifndef _WIN32          /* { */
  r = test_case1();
  if (r) return -1;
#endif                  /* }{ */

  return 0;
}

int main(void)
{
  int r = 0;
  r = test();

  fprintf(stderr,"==%s==\n", r ? "failure" : "success");

  return !!r;
}


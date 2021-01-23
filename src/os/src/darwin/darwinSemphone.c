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

struct tsem_s {
  dispatch_semaphore_t         sem;
  volatile unsigned int        valid:1;
};

int tsem_wait(tsem_t *sem);
int tsem_post(tsem_t *sem);
int tsem_destroy(tsem_t *sem);
int tsem_init(tsem_t *sem, int pshared, unsigned int value) {
  fprintf(stderr, "==%s[%d]%s():[%p]==creating\n", basename(__FILE__), __LINE__, __func__, sem);
  if (*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = (struct tsem_s*)calloc(1, sizeof(*p));
  if (!p) {
    fprintf(stderr, "==%s[%d]%s():[%p]==out of memory\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }

  p->sem = dispatch_semaphore_create(value);
  if (p->sem == NULL) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not created\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  p->valid = 1;

  *sem = p;

  return 0;
}

int tsem_wait(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  return dispatch_semaphore_wait(p->sem, DISPATCH_TIME_FOREVER);
}

int tsem_post(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  return dispatch_semaphore_signal(p->sem);
}

int tsem_destroy(tsem_t *sem) {
  fprintf(stderr, "==%s[%d]%s():[%p]==destroying\n", basename(__FILE__), __LINE__, __func__, sem);
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
    return 0;
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }

  p->valid = 0;
  free(p);

  *sem = NULL;
  return 0;
}


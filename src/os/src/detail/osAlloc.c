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
#include "taoserror.h"
#include "tulog.h"
#include "osAlloc.h"

#define TSDB_HAVE_MEMALIGN
#ifdef TAOS_OS_FUNC_ALLOC

void *tmalloc(int32_t size) {
  void *p = malloc(size);
  if (p == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to malloc memory, size:%d reason:%s", size, strerror(errno));
  }

  return p;
}

void *tcalloc(int32_t size) {
  void *p = calloc(1, size);
  if (p == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to calloc memory, size:%d reason:%s", size, strerror(errno));
  }

  return p;
}

void *trealloc(void *p, int32_t size) {
  p = realloc(p, size);
  if (p == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to realloc memory, size:%d reason:%s", size, strerror(errno));
  }

  return p;
}

void tfree(void *p) { free(p); }

void tmemzero(void *p, int32_t size) { memset(p, 0, size); }

#ifdef TSDB_HAVE_MEMALIGN

void *tmemalign(int32_t alignment, int32_t size) {
  void *p;

  int err = posix_memalign(&p, alignment, size);
  if (err) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to memalign memory, alignment:%d size:%d reason:%s", alignment, size, strerror(err));
    p = NULL;
  }

  return p;
}

#else

void *tmemalign(int32_t alignment, int32_t size) { return tmalloc(size); }

#endif
#endif
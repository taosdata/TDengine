/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define TAOS_ERROR_C
 
typedef struct {
    int32_t val;
    const char* str;
} STaosError;
 

#include "os.h"
#include "taoserror.h"


static threadlocal int32_t tsErrno;
int32_t* taosGetErrno() {
  return &tsErrno;
}


static int tsCompareTaosError(const void* a, const void* b) {
  const STaosError* x = (const STaosError*)a;
  const STaosError* y = (const STaosError*)b;
  if (x->val < y->val) {
    return -1;
  }
  if (x->val > y->val) {
    return 1;
  }
  return 0;
}

static pthread_once_t tsErrorInit = PTHREAD_ONCE_INIT;
static void tsSortError() {
  qsort(errors, sizeof(errors)/sizeof(errors[0]), sizeof(errors[0]), tsCompareTaosError);
}


const char* tstrerror(int32_t err) {
  pthread_once(&tsErrorInit, tsSortError);

  // this is a system errno
  if ((err & 0x00ff0000) == 0x00ff0000) {
    return strerror(err & 0x0000ffff);
  }

  size_t s = 0, e = sizeof(errors)/sizeof(errors[0]);
  while (s < e) {
    size_t mid = (s + e) / 2;
    int32_t val = errors[mid].val;
    if (err > val) {
      s = mid + 1;
    } else if (err < val) {
      e = mid;
    } else if (err == val) {
      return errors[mid].str;
    } else {
      break;
    }
  }

  return "";
}

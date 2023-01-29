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
#define _DEFAULT_SOURCE
#include "os.h"
#include <stdlib.h>

#ifdef WINDOWS
void swapStr(char* j, char* J, int width) {
  int  i;
  char tmp;
  for (i = 0; i < width; i++) {
    tmp = *j;
    *j = *J;
    *J = tmp;
    j++;
    J++;
  }
}
#endif

int qsortHelper(const void* p1, const void* p2, const void* param) {
  __compar_fn_t comparFn = param;
  return comparFn(p1, p2);
}

// todo refactor: 1) move away; 2) use merge sort instead; 3) qsort is not a stable sort actually.
void taosSort(void* base, int64_t sz, int64_t width, __compar_fn_t compar) {
#ifdef _ALPINE
  void* param = compar;
  taosqsort(base, width, sz, param, qsortHelper);
#else
  qsort(base, sz, width, compar);
#endif
}


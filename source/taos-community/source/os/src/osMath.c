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
#include <stdlib.h>
#include "talgo.h"

#if defined(WINDOWS_STASH) || defined(_ALPINE)
int32_t qsortHelper(const void* p1, const void* p2, const void* param) {
  __compar_fn_t comparFn = param;
  return comparFn(p1, p2);
}
#endif

// todo refactor: 1) move away; 2) use merge sort instead; 3) qsort is not a stable sort actually.
void taosSort(void* base, int64_t sz, int64_t width, __compar_fn_t compar) {
#if defined(WINDOWS_STASH) || defined(_ALPINE)
  void* param = compar;
  taosqsort(base, sz, width, param, qsortHelper);
#else
  qsort(base, sz, width, compar);
#endif
}

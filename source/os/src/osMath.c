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

void taosSort(void* arr, int64_t sz, int64_t width, __compar_fn_t compar) {
#ifdef WINDOWS
  int64_t i, j;
  for (i = 0; i < sz - 1; i++) {
    for (j = 0; j < sz - 1 - i; j++) {
      if (compar((char*)arr + j * width, (char*)arr + (j + 1) * width) > 0.00) {
        swapStr((char*)arr + j * width, (char*)arr + (j + 1) * width, width);
      }
    }
  }
#else
  qsort(arr, sz, width, compar);
#endif
}

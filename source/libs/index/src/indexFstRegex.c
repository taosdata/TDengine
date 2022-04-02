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

#include "indexFstRegex.h"
#include "indexFstSparse.h"

FstRegex *regexCreate(const char *str) {
  FstRegex *regex = taosMemoryCalloc(1, sizeof(FstRegex));
  if (regex == NULL) {
    return NULL;
  }
  int32_t sz = (int32_t)strlen(str);
  char *  orig = taosMemoryCalloc(1, sz);
  memcpy(orig, str, sz);

  regex->orig = orig;
  return regex;
}

void regexSetup(FstRegex *regex, uint32_t size, const char *str) {
  // return
  // return;
}

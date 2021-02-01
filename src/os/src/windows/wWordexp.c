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
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

int wordexp(char *words, wordexp_t *pwordexp, int flags) {
  pwordexp->we_offs = 0;
  pwordexp->we_wordc = 1;
  pwordexp->we_wordv[0] = pwordexp->wordPos;

  memset(pwordexp->wordPos, 0, 1025);
  if (_fullpath(words, pwordexp->wordPos, 1024) == NULL) {
    pwordexp->we_wordv[0] = words;
    uError("failed to parse relative path:%s to abs path", words);
    return -1;
  }

  uTrace("parse relative path:%s to abs path:%s", words, pwordexp->wordPos);
  return 0;
}

void wordfree(wordexp_t *pwordexp) {}

char *realpath(char *path, char *resolved_path) {
  return _fullpath(path, resolved_path, TSDB_FILENAME_LEN - 1);
}
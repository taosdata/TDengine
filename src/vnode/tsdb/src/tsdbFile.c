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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsdbFile.h"

const char *tsdbFileSuffix[] = {
    ".head",  // TSDB_FILE_TYPE_HEAD
    ".data",  // TSDB_FILE_TYPE_DATA
    ".last",  // TSDB_FILE_TYPE_LAST
    ".meta"   // TSDB_FILE_TYPE_META
};

char *tsdbGetFileName(char *dirName, char *fname, TSDB_FILE_TYPE type) {
  if (!IS_VALID_TSDB_FILE_TYPE(type)) return NULL;

  char *fileName = (char *)malloc(strlen(dirName) + strlen(fname) + strlen(tsdbFileSuffix[type]) + 5);
  if (fileName == NULL) return NULL;

  sprintf(fileName, "%s/%s%s", dirName, fname, tsdbFileSuffix[type]);
  return fileName;
}
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

#include "tsdb.h"

static const char *tsdbFileSuffix[] = {".tombstone", ".cache", ".index", ".data", ".last", ".sma", ""};

// .tombstone

struct STsdbIndexFile {
  int64_t size;
  int64_t offset;
  int32_t nRef;
};

struct STsdbDataFile {
  int64_t size;
  int32_t nRef;
};

struct STsdbLastFile {
  int64_t size;
  int32_t nRef;
};

struct STsdbSmaFile {
  int64_t size;
  int32_t nRef;
};

struct SDFileSet {
  STsdbIndexFile *pIndexF;
  STsdbDataFile  *pDataF;
  STsdbLastFile  *pLastF;
  STsdbSmaFile   *pSmaF;
};

// SDelFile ===============================================
char *tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile) {
  char   *pName = NULL;
  int32_t size;

  // TODO
  // sprintf(pName, "", pTsdb->path, );

  return pName;
}
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

struct STsdbFS {
  STsdb          *pTsdb;
  TdThreadRwlock  lock;
  int64_t         minVersion;
  int64_t         maxVersion;
  SDelFile       *pTombstoneF;
  STsdbCacheFile *pCacheF;
  SArray         *pArray;
};

int32_t tsdbFSOpen(STsdb *pTsdb, STsdbFS **ppFS) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbFSClose(STsdbFS *pFS) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbFSStart(STsdbFS *pFS) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbFSEnd(STsdbFS *pFS, int8_t rollback) {
  int32_t code = 0;
  // TODO
  return code;
}

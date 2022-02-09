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

#include "tdbInt.h"

struct STDbEnv {
  TDB *    dbList;      // TDB list
  SPgFile *pgFileList;  // SPgFile list
  struct {
  } pgfht;       // page file hash table;
  SPgCache pgc;  // page cache
};

int tdbEnvOpen(TENV **ppEnv) {
  // TODO
  return 0;
}

int tdbEnvClose(TENV *pEnv) {
  // TODO
  return 0;
}

SPgFile *tdbEnvGetPageFile(TENV *pEnv, const uint8_t fileid[]) {
  // TODO
  return NULL;
}

SPgCache *tdbEnvGetPgCache(TENV *pEnv) { return &(pEnv->pgc); }
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

#ifndef _TD_TDB_DB_H_
#define _TD_TDB_DB_H_

#include "tdb_mpool.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TDB TDB;

struct TDB {
  char *      fname;
  char *      dbname;
  TDB_MPFILE *mpf;
  // union {
  //   TDB_BTREE *btree;
  //   TDB_HASH * hash;
  //   TDB_HEAP * heap;
  // } dbam;  // db access method
};

int tdbOpen(TDB **dbpp, const char *fname, const char *dbname, uint32_t flags);
int tdbClose(TDB *dbp, uint32_t flags);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_DB_H_*/
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

#ifndef _TD_TKV_DB_H_
#define _TD_TKV_DB_H_

#include "tkvBtree.h"
#include "tkvHash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TDB_BTREE = 0,
  TDB_HASH,
  TDB_HEAP,
} tdb_db_t;

struct TDB {
  pgsize_t pageSize;
  tdb_db_t type;
  union {
    STkvBtree btree;
    STkvhash  hash;
  } dbimpl;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_DB_H_*/
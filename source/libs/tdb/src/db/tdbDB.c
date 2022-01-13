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

#include "tdbDB.h"
#include "tdb.h"

TDB_EXTERN int tdbCreateDB(TDB** dbpp) {
  TDB* dbp;

//   dbp = malloc
  return 0;
}

int tdbOpenDB(TDB* dbp, tdb_db_t type, uint32_t flags) {
  // TODO
  return 0;
}

int tdbCloseDB(TDB* dbp, uint32_t flags) {
  // TODO
  return 0;
}
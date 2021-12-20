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

#include "tsdbDef.h"

int tsdbPrepareCommit(STsdb *pTsdb) {
  if (pTsdb->mem == NULL) return 0;

  // tsem_wait(&(pTsdb->canCommit));
  ASSERT(pTsdb->imem == NULL);

  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
}

int tsdbCommit(STsdb *pTsdb) {
  // TODO
  pTsdb->imem = NULL;
  // tsem_post(&(pTsdb->canCommit));
  return 0;
}
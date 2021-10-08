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

#ifndef _TD_TSDB_H_
#define _TD_TSDB_H_

#include "os.h"
#include "taosMsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STsdb STsdb;
typedef struct {
  int32_t id; // TODO: use a global definition
  int32_t days;
  int32_t keep;
  int32_t keep1;
  int32_t keep2;
  int32_t minRows;
  int32_t maxRows;
  int8_t  precision;
  int8_t  update;
} STsdbCfg;

// Module init and clear
int tsdbInit();
int tsdbClear();

// Repository operations
int    tsdbCreateRepo(int id);
int    tsdbDropRepo(int id);
STsdb *tsdbOpenRepo(STsdbCfg *pCfg);
int    tsdbCloseRepo(STsdb *pTsdb);
int    tsdbForceCloseRepo(STsdb *pTsdb);

// Data commit
int tsdbInsert(STsdb *pTsdb, SSubmitReq *pMsg);
int tsdbCommit(STsdb *pTsdb);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_H_*/
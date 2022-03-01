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

#ifndef _TD_PLANN_NODES_H_
#define _TD_PLANN_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"

typedef struct SDatabaseOptions {
  int32_t numOfBlocks;
  int32_t cacheBlockSize;
  int8_t cachelast;
  int32_t compressionLevel;
  int32_t daysPerFile;
  int32_t fsyncPeriod;
  int32_t maxRowsPerBlock;
  int32_t minRowsPerBlock;
  int32_t keep;
  int32_t precision;
  int32_t quorum;
  int32_t replica;
  int32_t ttl;
  int32_t walLevel;
  int32_t numOfVgroups;
  int8_t singleStable;
  int8_t streamMode;
} SDatabaseOptions;

typedef struct SCreateDatabaseStmt {
  ENodeType type;
  char dbName[TSDB_DB_NAME_LEN];
  bool ignoreExists;
  SDatabaseOptions options;
} SCreateDatabaseStmt;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/

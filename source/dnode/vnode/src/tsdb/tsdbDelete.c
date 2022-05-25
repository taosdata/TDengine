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

typedef struct STombStoneItem STombStoneItem;
typedef struct STombStones    STombStones;

struct STombStones {
  TdThreadRwlock lock;
  SHashObj*      pTmStones;  // key: uid, value: tombstone items sorted by version
  T_REF_DECLARE()
};

struct STombStoneItem {
  int64_t tbUid;  // child/normal table
  int64_t version;
  int64_t gversion;  // global unique version assigned by mnode
  SArray* tsFilters;
};

// SBlock 中除了原有的 keyFirst/keyLast，应该不再需要记录　versionMin 和 versionMax
// 因为在查询时，新到的　query 的 version，一定大于原有写入的数据 version。

// mem   [ts1:v200, ts1:v201, ts2:v300,ts2:v310,ts2:320, ts5:400,ts5:v500,ts6:v600, ts7:v700, ts8:v800]

// imem  [ts2:80,ts2:v90,ts2:v100]

// file  [blk1: ts1:v10,ts1:v11,ts2:v20,ts2:v21
         [blk2: ts2:v30,ts2:v31,ts2:v35
         [blk3: ts2:v40,ts2:v45,ts2:v50,ts3:v60,ts4:v70

// delete [ctb1, ts2-ts5:v315] 
//        [ctb1, ts5:v450] 
//        [ctb1, ts6:v550]
// 根据 delete version, 逆序过滤。

// sql: select * from ctb1 where ts < ts8 and ts > ts1;  // query version = 460;

int32_t tsdbLoadBlockDataMV(SFileBlockIter *pBlockIter, SMemIter *pMemIter, SDataCols *pTarget, SArray *pTmStoneArray, int64_t queryVersion){
  
  //  首先，query executor 根据　查询的　ts 范围，找到最后一条记录的位置。
  //  1) 有可能只在　mem/imem
  //  2) 有可能只在 某一个文件
  //  3) 有可能 mem/imem 和　文件中均包含。



}

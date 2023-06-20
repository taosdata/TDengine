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
#ifndef TDENGINE_HASHJOIN_H
#define TDENGINE_HASHJOIN_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SHJoinCtx {
  bool         rowRemains;
  SBufRowInfo* pBuildRow;
  SSDataBlock* pProbeData;
  int32_t      probeIdx;
} SHJoinCtx;

typedef struct SRowLocation {
  SSDataBlock* pDataBlock;
  int32_t      pos;
} SRowLocation;

typedef struct SColBufInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
} SColBufInfo;

typedef struct SBufPageInfo {
  int32_t pageSize;
  int32_t offset;
  char*   data;
} SBufPageInfo;

#pragma pack(push, 1) 
typedef struct SBufRowInfo {
  void*    next;
  uint16_t pageId;
  int32_t  offset:31;
  int32_t  isNull:1;
} SBufRowInfo;
#pragma pack(pop)

typedef struct SGroupData {
  SBufRowInfo* rows;
} SGroupData;

typedef struct SJoinTableInfo {
  SOperatorInfo* downStream;
  int32_t        blkId;
  SQueryStat     inputStat;
  
  int32_t        keyNum;
  SColBufInfo*   keyCols;
  char*          keyBuf;
  
  int32_t        valNum;
  SColBufInfo*   valCols;
  int32_t        valBufSize;
  bool           valVarData;
} SJoinTableInfo;

typedef struct SHJoinOperatorInfo {
  SSDataBlock*    pRes;
  int32_t         joinType;
  SJoinTableInfo  tbs[2];
  SJoinTableInfo* pBuild;
  SJoinTableInfo* pProbe;
  int32_t         pResColNum;
  int8_t*         pResColMap;
  SArray*         pRowBufs;
  SNode*          pCondAfterJoin;
  SSHashObj*      pKeyHash;
  SHJoinCtx       ctx;
} SHJoinOperatorInfo;

static SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator);
static void         destroyHashJoinOperator(void* param);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H

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

#define HASH_JOIN_DEFAULT_PAGE_SIZE 10485760

#pragma pack(push, 1) 
typedef struct SBufRowInfo {
  void*    next;
  uint16_t pageId;
  int32_t  offset;
} SBufRowInfo;
#pragma pack(pop)

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

typedef struct SHJoinColInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     keyCol;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
  char*    bitMap;
} SHJoinColInfo;

typedef struct SBufPageInfo {
  int32_t pageSize;
  int32_t offset;
  char*   data;
} SBufPageInfo;


typedef struct SGroupData {
  SBufRowInfo* rows;
} SGroupData;

typedef struct SHJoinTableInfo {
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int32_t        blkId;
  SQueryStat     inputStat;
  
  int32_t        keyNum;
  SHJoinColInfo* keyCols;
  char*          keyBuf;
  char*          keyData;
  
  int32_t        valNum;
  SHJoinColInfo* valCols;
  char*          valData;
  int32_t        valBitMapSize;
  int32_t        valBufSize;
  SArray*        valVarCols;
  bool           valColExist;
} SHJoinTableInfo;

typedef struct SHJoinExecInfo {
  int64_t buildBlkNum;
  int64_t buildBlkRows;
  int64_t probeBlkNum;
  int64_t probeBlkRows;
  int64_t resRows;
  int64_t expectRows;
} SHJoinExecInfo;


typedef struct SHJoinOperatorInfo {
  int32_t          joinType;
  SHJoinTableInfo  tbs[2];
  SHJoinTableInfo* pBuild;
  SHJoinTableInfo* pProbe;
  SSDataBlock*     pRes;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  SArray*          pRowBufs;
  SNode*           pCond;
  SSHashObj*       pKeyHash;
  bool             keyHashBuilt;
  SHJoinCtx        ctx;
  SHJoinExecInfo   execInfo;
} SHJoinOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H

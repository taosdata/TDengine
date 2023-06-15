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

typedef struct SHJoinRowCtx {
  bool    rowRemains;
  int64_t ts;
  SArray* leftRowLocations;
  SArray* leftCreatedBlocks;
  SArray* rightCreatedBlocks;
  int32_t leftRowIdx;
  int32_t rightRowIdx;

  bool    rightUseBuildTable;
  SArray* rightRowLocations;
} SHJoinRowCtx;

typedef struct SRowLocation {
  SSDataBlock* pDataBlock;
  int32_t      pos;
} SRowLocation;

typedef struct SColBufInfo {
  int32_t  slotId;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
} SColBufInfo;

typedef struct SJoinTableInfo {
  int32_t      keyNum;
  SColBufInfo* keyCols;
  char*        keyBuf;
  int32_t      valNum;
  SColBufInfo* valCols;
  char*        valBuf;
} SJoinTableInfo;

typedef struct SHJoinOperatorInfo {
  SSDataBlock* pRes;
  int32_t      joinType;

  SJoinTableInfo tbs[2];

  SJoinTableInfo* pBuild;
  SJoinTableInfo* pProbe;
  
  int32_t      pLeftKeyNum;
  SColBufInfo* pLeftKeyInfo;
  char*        pLeftKeyBuf;
  int32_t      pLeftValNum;
  SColBufInfo* pLeftValInfo;
  char*        pLeftValBuf;

  int32_t      pRightKeyNum;  
  SColBufInfo* pRightKeyInfo;
  char*        pRightKeyBuf;
  int32_t      pRightValNum;  
  SColBufInfo* pRightValInfo;
  char*        pRightValBuf;
  
  SNode*       pCondAfterJoin;

  SSHashObj*   pKeyHash;

  SQueryStat   inputStat[2];
  
  SHJoinRowCtx  rowCtx;
} SHJoinOperatorInfo;
static SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator);
static void         destroyHashJoinOperator(void* param);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H

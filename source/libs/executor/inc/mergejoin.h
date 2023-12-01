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
#ifndef TDENGINE_MERGEJOIN_H
#define TDENGINE_MERGEJOIN_H

#ifdef __cplusplus
extern "C" {
#endif

typedef SSDataBlock* (*joinImplFp)(SOperatorInfo*);

typedef struct SMJoinColInfo {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     keyCol;
  bool     vardata;
  int32_t* offset;
  int32_t  bytes;
  char*    data;
  char*    bitMap;
} SMJoinColInfo;


typedef struct SMJoinTableInfo {
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int32_t        blkId;
  SQueryStat     inputStat;

  SMJoinColInfo* primCol;
  char*          primData;
  
  int32_t        keyNum;
  SMJoinColInfo* keyCols;
  char*          keyBuf;
  char*          keyData;
  
  int32_t        valNum;
  SMJoinColInfo* valCols;
  char*          valData;
  int32_t        valBitMapSize;
  int32_t        valBufSize;
  SArray*        valVarCols;
  bool           valColExist;
} SMJoinTableInfo;

typedef struct SMJoinOperatorInfo {
  int32_t          joinType;
  int32_t          inputTsOrder;  
  SMJoinTableInfo  tbs[2];
  SMJoinTableInfo* pBuild;
  SMJoinTableInfo* pProbe;
  SSDataBlock*     pRes;
  int32_t          pResColNum;
  int8_t*          pResColMap;
  SArray*          pRowBufs;
  SFilterInfo*     pPreFilter;
  SFilterInfo*     pFinFilter;
  SSHashObj*       pKeyHash;
  bool             keyHashBuilt;
  joinImplFp       joinFp;
  SHJoinCtx        ctx;
  SHJoinExecInfo   execInfo;
} SMJoinOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MERGEJOIN_H

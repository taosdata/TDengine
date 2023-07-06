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
#ifndef TDENGINE_GROUPCACHE_H
#define TDENGINE_GROUPCACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#define GROUP_CACHE_DEFAULT_PAGE_SIZE 10485760

typedef struct SGcOperatorParam {
  SOperatorParam*     pChild;
  int64_t             sessionId;
  int32_t             downstreamIdx;
  bool                needCache;
  void*               pGroupValue;
  int32_t             groupValueSize;
} SGcOperatorParam;

#pragma pack(push, 1) 
typedef struct SGcBlkBufInfo {
  void*    next;
  uint16_t pageId;
  int32_t  offset;
} SGcBlkBufInfo;
#pragma pack(pop)

typedef struct SGcBufPageInfo {
  int32_t pageSize;
  int32_t offset;
  char*   data;
} SGcBufPageInfo;

typedef struct SGroupData {
  SGcBlkBufInfo* blks;
} SGroupData;

typedef struct SGroupColInfo {
  int32_t  slot;
  bool     vardata;
  int32_t  bytes;
} SGroupColInfo;

typedef struct SGroupColsInfo {
  int32_t        colNum;
  bool           withNull;
  SGroupColInfo* pColsInfo;
  int32_t        bitMapSize;
  int32_t        bufSize;
  char*          pBuf;
  char*          pData;
} SGroupColsInfo;

typedef struct SGcSessionCtx {
  int32_t         downstreamIdx;
  bool            cacheHit;
  bool            needCache;
  SGcBlkBufInfo*  pLastBlk; 
} SGcSessionCtx;

typedef struct SGroupCacheOperatorInfo {
  SSHashObj*        pSessionHash;  
  SGroupColsInfo    groupColsInfo;
  SArray*           pBlkBufs;
  SSHashObj*        pBlkHash;  
  int64_t           pCurrentId;
  SGcSessionCtx*    pCurrent;
} SGroupCacheOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_GROUPCACHE_H

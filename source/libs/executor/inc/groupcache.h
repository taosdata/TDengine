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

typedef struct SGroupCacheData {
  TdThreadMutex  mutex;
  SArray*        waitQueue;
  bool           fetchDone;
  bool           needCache;
  SSDataBlock*   pBlock;
  SGcBlkBufInfo* pFirstBlk;
  SGcBlkBufInfo* pLastBlk;
} SGroupCacheData;

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

typedef struct SGcNewGroupInfo {
  int64_t         uid;
  SOperatorParam* pParam;
} SGcNewGroupInfo;

typedef struct SGcDownstreamCtx {
  SRWLatch      lock;
  int64_t       fetchSessionId;
  SArray*       pNewGrpList; // SArray<SGcNewGroupInfo>
  SArray*       pGrpUidList;
} SGcDownstreamCtx;

typedef struct SGcSessionCtx {
  int32_t           downstreamIdx;
  bool              needCache;
  SGcOperatorParam* pParam;
  SGroupCacheData*  pGroupData;
  SGcBlkBufInfo*    pLastBlk; 
  bool              semInit;
  tsem_t            waitSem;
} SGcSessionCtx;

typedef struct SGcExecInfo {
  int32_t  downstreamNum;
  int64_t* pDownstreamBlkNum;
} SGcExecInfo;

typedef struct SGroupCacheOperatorInfo {
  TdThreadMutex     sessionMutex;
  SSHashObj*        pSessionHash;  
  SGroupColsInfo    groupColsInfo;
  bool              grpByUid;
  SGcDownstreamCtx* pDownstreams;
  SArray*           pBlkBufs;
  SHashObj*         pBlkHash;  
  SGcExecInfo       execInfo;
} SGroupCacheOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_GROUPCACHE_H

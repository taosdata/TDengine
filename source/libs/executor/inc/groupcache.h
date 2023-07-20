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

#define GROUP_CACHE_DEFAULT_MAX_FILE_SIZE 104857600

#pragma pack(push, 1) 
typedef struct SGcBlkBufInfo {
  void*    prev;
  void*    next;
  int64_t  blkId;
  int64_t  offset;
  int64_t  bufSize;
  void*    pBuf;
  uint32_t fileId;
} SGcBlkBufInfo;
#pragma pack(pop)


typedef struct SGcVgroupCtx {
  SArray*  pTbList;
  uint64_t lastUid;
  int64_t  fileSize;
  uint32_t fileId;
} SGcVgroupCtx;

typedef struct SGroupSeqBlkList {
  int64_t        startBlkId;
  int64_t        endBlkId;
} SGroupSeqBlkList;

typedef struct SGroupBatchBlkList {
  SRWLatch         lock;
  SArray*          pBlkList;
} SGroupBatchBlkList;

typedef struct SGroupCacheData {
  TdThreadMutex  mutex;
  SArray*        waitQueue;
  bool           fetchDone;
  SSDataBlock*   pBlock;
  SGcVgroupCtx*  pVgCtx;
  int32_t        downstreamIdx;
  int32_t        vgId;
  union {
    SGroupSeqBlkList   seqList;
    SGroupBatchBlkList batchList;
  };
  uint32_t       fileId;
  int64_t        startOffset;
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
  int32_t          vgId;
  int64_t          uid;
  SGroupCacheData* pGroup;
  SOperatorParam*  pParam;
} SGcNewGroupInfo;

typedef struct SGcDownstreamCtx {
  SRWLatch      grpLock;
  int64_t       fetchSessionId;
  SArray*       pNewGrpList; // SArray<SGcNewGroupInfo>
  SSHashObj*    pVgTbHash;
  SHashObj*     pGrpHash;
  SRWLatch      blkLock;
  SSDataBlock*  pBaseBlock;
  SArray*       pFreeBlock;
  int64_t       lastBlkUid;
  SHashObj*     pSessions;  
  SHashObj*     pWaitSessions;  
} SGcDownstreamCtx;

typedef struct SGcSessionCtx {
  int32_t           downstreamIdx;
  SGcOperatorParam* pParam;
  SGroupCacheData*  pGroupData;
  int64_t           lastBlkId;
  int64_t           nextOffset;
  bool              semInit;
  tsem_t            waitSem;
  bool              newFetch;
} SGcSessionCtx;

typedef struct SGcExecInfo {
  int32_t  downstreamNum;
  int64_t* pDownstreamBlkNum;
} SGcExecInfo;

typedef struct SGcCacheFile {
  uint32_t grpNum;
  uint32_t grpDone;
  int64_t  fileSize;
} SGcCacheFile;

typedef struct SGcBlkCacheInfo {
  SRWLatch       dirtyLock;
  SSHashObj*     pCacheFile;
  SHashObj*      pDirtyBlk;
  SGcBlkBufInfo* pDirtyHead;
  SGcBlkBufInfo* pDirtyTail; 
  SHashObj*      pReadBlk;
  int64_t        blkCacheSize;
} SGcBlkCacheInfo;

typedef struct SGroupCacheOperatorInfo {
  TdThreadMutex     sessionMutex;
  int64_t           maxCacheSize;
  int64_t           currentBlkId;
  SGroupColsInfo    groupColsInfo;
  bool              globalGrp;
  bool              grpByUid;
  bool              batchFetch;
  bool              fetchDone;
  SGcDownstreamCtx* pDownstreams;
  SGcBlkCacheInfo   blkCache;
  SHashObj*         pGrpHash;  
  SGcExecInfo       execInfo;
} SGroupCacheOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_GROUPCACHE_H

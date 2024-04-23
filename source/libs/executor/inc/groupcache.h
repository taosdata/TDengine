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
#define GROUP_CACHE_MAX_FILE_FDS 10
#define GROUP_CACHE_DEFAULT_VGID 0

#pragma pack(push, 1) 
typedef struct SGcBlkBufBasic {
  int32_t           fileId;
  int64_t           blkId;
  int64_t           offset;
  int64_t           bufSize;
} SGcBlkBufBasic;
#pragma pack(pop)

typedef struct SGroupCacheFileFd {
  TdThreadMutex mutex;
  TdFilePtr     fd;
} SGroupCacheFileFd;

typedef struct SGroupCacheFileInfo {
  uint32_t          groupNum;
  bool              deleted;
  SGroupCacheFileFd fd;
} SGroupCacheFileInfo;

typedef struct SGcFileCacheCtx {
  int64_t         fileSize;
  int32_t         fileId;
  SHashObj*       pCacheFile;
  int32_t         baseNameLen;
  char            baseFilename[256];  
} SGcFileCacheCtx;

typedef struct SGcDownstreamCtx {
  int32_t         id;
  SRWLatch        grpLock;
  int64_t         fetchSessionId;
  SArray*         pNewGrpList; // SArray<SGcNewGroupInfo>
  SSHashObj*      pVgTbHash;   // SHash<SGcVgroupCtx>
  SHashObj*       pGrpHash;
  SRWLatch        blkLock;
  SSDataBlock*    pBaseBlock;
  SArray*         pFreeBlock;
  int64_t         lastBlkUid;
  SHashObj*       pSessions;  
  SHashObj*       pWaitSessions; 
  SGcFileCacheCtx fileCtx;
} SGcDownstreamCtx;

typedef struct SGcVgroupCtx {
  int32_t         id;
  SArray*         pTbList;
  uint64_t        lastBlkUid;
  SGcFileCacheCtx fileCtx;
} SGcVgroupCtx;

typedef struct SGcBlkList {
  SRWLatch         lock;
  SArray*          pList;
} SGcBlkList;

typedef struct SGroupCacheData {
  TdThreadMutex  mutex;
  SArray*        waitQueue;
  bool           fetchDone;
  bool           needCache;
  SSDataBlock*   pBlock;
  SGcVgroupCtx*  pVgCtx;
  int32_t        downstreamIdx;
  int32_t        vgId;
  SGcBlkList     blkList;
  int32_t        fileId;
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

typedef struct SGcSessionCtx {
  int32_t           downstreamIdx;
  SGcOperatorParam* pParam;
  SGroupCacheData*  pGroupData;
  int64_t           lastBlkId;
  bool              semInit;
  tsem_t            waitSem;
  bool              newFetch;
  int64_t           resRows;
} SGcSessionCtx;

typedef struct SGcBlkBufInfo {
  SGcBlkBufBasic    basic;
  void*             next;
  void*             pBuf;
  SGcDownstreamCtx* pCtx;
  int64_t           groupId;
} SGcBlkBufInfo;

typedef struct SGcExecInfo {
  int64_t* pDownstreamBlkNum;
} SGcExecInfo;

typedef struct SGcCacheFile {
  uint32_t grpNum;
  uint32_t grpDone;
  int64_t  fileSize;
} SGcCacheFile;

typedef struct SGcBlkCacheInfo {
  SRWLatch       dirtyLock;
  SHashObj*      pDirtyBlk;
  SGcBlkBufInfo* pDirtyHead;
  SGcBlkBufInfo* pDirtyTail; 
  SHashObj*      pReadBlk;
  int64_t        blkCacheSize;
  int32_t        writeDownstreamId;  
} SGcBlkCacheInfo;

typedef struct SGroupCacheOperatorInfo {
  int64_t           maxCacheSize;
  int64_t           currentBlkId;
  SGroupColsInfo    groupColsInfo;
  bool              globalGrp;
  bool              grpByUid;
  bool              batchFetch;
  int32_t           downstreamNum;
  SGcDownstreamCtx* pDownstreams;
  SGcBlkCacheInfo   blkCache;
  SHashObj*         pGrpHash;  
  SGcExecInfo       execInfo;
} SGroupCacheOperatorInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_GROUPCACHE_H

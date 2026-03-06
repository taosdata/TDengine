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
// Notes:
// - A group may map to multiple data blocks in one file.
// - Memory blocks are spilled to file when memory pressure is high.
// - For sliding mode, both in-memory and on-disk blocks may coexist and are read in order.

#ifndef TDENGINE_DATA_SINK_H
#define TDENGINE_DATA_SINK_H

#include <stdbool.h>
#include <stdint.h>
#include "tarray.h"
#include "tcommon.h"
#include "tdef.h"
#include "thash.h"
#include "trbtree.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DS_MEM_SIZE_RESERVED_FOR_WIRTE (20 * 1024 * 1024)
#define DS_MEM_SIZE_RESERVED (30 * 1024 * 1024)
#define DS_MEM_SIZE_ALTER_QUIT (300 * 1024 * 1024)
#define DS_FILE_BLOCK_SIZE (10 * 1024 * 1024)


typedef enum {
  DATA_SAVEMODE_BLOCK = 1,
  DATA_SAVEMODE_BUFF = 2,
  DATA_BLOCK_MOVED = 3,
} SSaveStatus;
typedef struct SFileBlockInfo {
  int64_t offset;  // offset in file
  int64_t size;
} SFileBlockInfo;

typedef struct SDataSinkFileMgr {
  char    fileName[FILENAME_MAX];
  int64_t fileSize;
  int64_t fileBlockCount;
  int64_t fileBlockUsedCount;
  int64_t fileGroupBlockMaxSize;
  SRBTree pFreeFileBlockList;  // <groupDataStartOffSet, SFileBlockInfo>

  int64_t   readingGroupId;
  int64_t   writingGroupId;
  TdFilePtr readFilePtr;
  TdFilePtr writeFilePtr;
} SDataSinkFileMgr;

typedef enum {
  DATA_CLEAN_NONE = 0,
  DATA_CLEAN_IMMEDIATE = 1,
  DATA_CLEAN_EXPIRED = 2,
} SCleanMode;

typedef struct SDataSinkManager2 {
  int64_t   memBufSize;
  int64_t   memAlterSize;
  int64_t   usedMemSize;
  int64_t   fileBlockSize;
  int64_t   readDataFromFileTimes;
  SHashObj* dsStreamTaskList;  // hash <streamId + taskId, SSlidingTaskDSMgr/SAlignTaskDSMgr>
} SDataSinkManager2;
extern SDataSinkManager2 g_pDataSinkManager;

typedef struct SMoveWindowInfo {
  int64_t      moveSize;
  SSDataBlock* pData;  // data block to move
} SMoveWindowInfo;

typedef struct SSlidingWindowInMem {
  int64_t startTime;
  int64_t endTime;
  int64_t dataLen;
  // char*   realDataBuf;    // realDataBuf == &pData + sizeof(SSlidingWindowInMem)
} SSlidingWindowInMem;

typedef struct SAlignBlocksInMem {
  int64_t capacity;
  int64_t dataLen;
  int32_t nWindow;
  // trailing bytes store serialized SSlidingWindowInMem items
} SAlignBlocksInMem;

typedef struct SBlocksInfoFile {
  int64_t groupOffset;  // offset in file
  int64_t dataLen;
  int64_t capacity;  // size in file
  // serialized SSlidingWindowInMem payload is stored in file at groupOffset
} SBlocksInfoFile;

typedef struct STaskDSMgr {
  int8_t cleanMode;  // 1 - immediate, 2 - expired
} STaskDSMgr;

typedef struct SAlignTaskDSMgr {
  int8_t            cleanMode;  // 1 - immediate, 2 - expired
  int64_t           streamId;
  int64_t           taskId;
  int64_t           sessionId;  // sessionId is used to distinguish different sessions in the same task
  int32_t           tsSlotId;
  SHashObj*         pAlignGrpList;  // hash <groupId, SAlignGrpMgr>
  SDataSinkFileMgr* pFileMgr;
} SAlignTaskDSMgr;

typedef struct SSlidingTaskDSMgr {
  int8_t            cleanMode;  // 1 - immediate, 2 - expired
  int64_t           streamId;
  int64_t           taskId;
  int64_t           sessionId;  // sessionId is used to distinguish different sessions in the same task
  int32_t           tsSlotId;
  int64_t           capacity;         // per-group block size in file
  SHashObj*         pSlidingGrpList;  // hash <groupId, SSlidingGrpMgr>
  SDataSinkFileMgr* pFileMgr;
} SSlidingTaskDSMgr;

typedef enum {
  GRP_DATA_IDLE = 0,
  GRP_DATA_WRITING = 1,
  GRP_DATA_WIAT_READ = 2,  // finished writing, but not reading
  GRP_DATA_READING = 3,
  GRP_DATA_MOVING = 4,
} EGroupStatus;

typedef enum {
  GRP_DATA_WAITREAD_MOVING = 0x01,  // waiting for read, can moving data
} EGroupStatusMode;

typedef struct SSlidingGrpMgr {
  int64_t groupId;
  int8_t  status;  // EGroupStatus
  int64_t usedMemSize;
  SArray* winDataInMem;  // array SSlidingWindowInMem
  SArray* blocksInFile;  // array SBlocksInfoFile
} SSlidingGrpMgr;

typedef struct SAlignGrpMgr {
  int64_t groupId;
  int8_t  status;        // EGroupStatus
  SArray* blocksInMem;   // array SAlignBlocksInMem <address, capacity, dataLen>
  SArray* blocksInFile;  // array SBlocksInfoFile <groupOffset, dataStartOffset, dataLen>
} SAlignGrpMgr;

typedef enum {
  DATA_SINK_MEM = 0,
  DATA_SINK_FILE,
  DATA_SINK_ALL_TMP,   // all in tmp file, not in mem
  DATA_SINK_PART_TMP,  // part in tmp file, part in other position
} SDataSinkPos;

typedef struct SResultIter {
  SCleanMode        cleanMode;    // 1 - immediate, 2 - expired
  void*             groupData;    // SAlignGrpMgr(data in mem) or SSlidingGrpMgr(data in file)
  SDataSinkFileMgr* pFileMgr;     // when has data in file, pFileMgr is not NULL
  int32_t           tsColSlotId;  // ts column slot id
  int32_t           winIndex;     // only for immediate clean mode, index of the window in the block
                     // when tmpBlocksInMem is not NULL, this is the index of the current tmpBlocksInMem's block
  int64_t      offset;          // array index, start from 0
  SArray*      tmpBlocksInMem;  // SSlidingWindowInMem, read from file,
  SDataSinkPos dataPos;         // 0 - data in mem, 1 - data in file
  int64_t      groupId;
  int64_t      reqStartTime;
  int64_t      reqEndTime;
} SResultIter;

typedef enum {
  DATA_NORMAL = 0,  // normal data, not moving
  DATA_MOVING = 1,  // data is moving from mem to file
} SDataMoveStatus;
typedef struct SSlidingGrpMemList {
  bool      enabled;
  int8_t    status;
  SHashObj* pSlidingGrpList;  // hash <SSlidingGrpMgr*, size>
  int64_t   waitMoveMemSize;  // used memory size in bytes
} SSlidingGrpMemList;
extern SSlidingGrpMemList g_slidigGrpMemList;

// ----------------- External DataSink APIs -----------------

// Create a data sink cache.
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t cleanMode, int32_t tsSlotId, void** ppCache);

// Destroy a data sink cache and associated resources.
void destroyStreamDataCache(void* pCache);

// Append data to cache.
int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex);

// Append data by transferring ownership of pBlock to cache on success.
int32_t moveStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock);

// Get an iterator for reading cached blocks by group and time range.
int32_t getStreamDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter);

// Read next block from iterator.
int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock);

// Clean cache data for a group.
int32_t cleanStreamDataCache(void* pCache, int64_t groupId);

// Cancel iterator traversal early.
void cancelStreamDataCacheIterate(void** pIter);

// Destroy all DataSink manager resources.
void destroyDataSinkMgr();

void setDataSinkMaxMemSize(int64_t maxMemSize);

// ----------------- Internal DataSink APIs -----------------
int32_t initDataSinkFileDir();
int32_t initStreamDataSink();
int32_t checkAndMoveMemCache(bool forWrite);
int32_t moveSlidingTaskMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr);
bool    hasEnoughMemSize();
int32_t moveSlidingGrpMemCache(SSlidingTaskDSMgr* pSlidingTaskMgr, SSlidingGrpMgr* pSlidingGrp);
int32_t moveMemFromWaitList(int8_t mode);

void* getWindowDataBuf(SSlidingWindowInMem* pWindowData);

int32_t buildSlidingWindowInMem(SSDataBlock* pBlock, int32_t tsColSlotId, int32_t startIndex, int32_t endIndex,
                                SSlidingWindowInMem** ppSlidingWinInMem);
void    destroySlidingWindowInMem(void* pSlidingWinInMem);
void    destroySlidingWindowInMemPP(void* ppSlidingWinInMem);

int32_t buildAlignWindowInMemBlock(SAlignGrpMgr* pAlignGrpMgr, SSDataBlock* pBlock, int32_t tsColSlotId, TSKEY wstart,
                                   TSKEY wend, int32_t startIndex, int32_t endIndex);
int32_t buildMoveAlignWindowInMem(SAlignGrpMgr* pAlignGrpMgr, SSDataBlock* pBlock, int32_t tsColSlotId, TSKEY wstart,
                                  TSKEY wend);

// Read data from in-memory cache.
int32_t readDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished);
int32_t readDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId);

// Find next iterator position from memory cache.
// return true: continue to file side, false: no file-side iteration needed
bool    setNextIteratorFromMem(SResultIter** ppResult);
bool    setNextIteratorFromFile(SResultIter** ppResult);
int32_t createDataResult(void** ppResult);
void    releaseDataResult(void** ppResult);
void    releaseDataResultAndResetMgrStatus(void** ppIter);

void slidingGrpMgrUsedMemAdd(SSlidingGrpMgr* pSlidingGrpCacheMgr, int64_t size);

int32_t initInserterGrpInfo();
void    destroyInserterGrpInfo();

void destroyAlignBlockInMem(void* pData);
void destroyAlignBlockInMemPP(void* ppData);

void destroyStreamDataSinkFile(SDataSinkFileMgr** ppDaSinkFileMgr);

bool changeMgrStatus(int8_t* pStatus, int8_t status);
bool changeMgrStatusToMoving(int8_t* pStatus, int8_t mode);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_DATA_SINK_H

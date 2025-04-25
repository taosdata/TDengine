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

#ifndef TDENGINE_DATA_SINK_H
#define TDENGINE_DATA_SINK_H

#include <stdint.h>
#include "tcommon.h"
#include "tdef.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DATA_SAVEMODE_BLOCK = 1,
  DATA_SAVEMODE_BUFF = 2,
  DATA_BLOCK_MOVED = 3,
} SSaveStatus;

typedef struct SGroupDSManager SGroupDSManager;
typedef struct SWindowData {
  int64_t          wstart;  // start time of the window
  int64_t          wend;    // end time of the window
  TSKEY            start;   // start time of the data
  TSKEY            end;     // end time of the data
  int64_t          dataLen;
  SSaveStatus      saveMode;
  SGroupDSManager* pGroupDataInfoMgr;
  void*            pDataBuf;
} SWindowData;

typedef struct SFileBlockInfo {
  int64_t          groupDataStartOffSet;  // offset in file
} SFileBlockInfo;

typedef struct SWindowDataInFile {
  int64_t          wstart;  // start time of the window
  int64_t          wend;    // end time of the window
  TSKEY            start;   // start time of the data
  TSKEY            end;     // end time of the data
  int64_t          dataLen;
  int64_t          offset;  // offset in file
  int64_t          groupDataStartOffSet;  // offset in file
} SWindowDataInFile;

typedef struct SGroupFileDataMgr {
  int64_t          groupId;
  int64_t          lastWstartInFile;
  bool             hasDataInFile;
  int64_t          allWindowDataLen;
  int64_t          groupDataStartOffSet;  // offset in file
  SArray*          windowDataInFile;  // array SWindowDataInFile <wstart, start block num in file>
} SGroupFileDataMgr;

typedef struct SDataSinkFileMgr {
  char      fileName[FILENAME_MAX];
  int64_t   fileSize;
  int64_t   fileBlockCount;
  int64_t   fileBlockUsedCount;
  int64_t   fileGroupBlockMaxSize;
  SArray*   freeBlockList;   // array: groupDataStartOffSet
  SHashObj* groupBlockList;  // hash <groupId, SGroupFileDataMgr>

  int64_t   readingGroupId;
  int64_t   writingGroupId;
  TdFilePtr readFilePtr;
  TdFilePtr writeFilePtr;
} SDataSinkFileMgr;

typedef enum {
  DATA_CLEAN_IMMEDIATE = 1,
  DATA_CLEAN_EXPIRED = 2,
} SCleanMode;

typedef struct SDataSinkManager2 {
  int8_t    status;  // 0 - init, 1 - running
  int64_t   usedMemSize;
  int64_t   maxMemSize;
  int64_t   fileBlockSize;
  int64_t   readDataFromMemTimes;
  int64_t   readDataFromFileTimes;
  SHashObj* DataSinkStreamTaskList;  // hash <streamId + taskId, SStreamTaskDSManager>
} SDataSinkManager2;
extern SDataSinkManager2 g_pDataSinkManager;

typedef struct SStreamTaskDSManager {
  int64_t           streamId;
  int64_t           taskId;
  int64_t           usedMemSize;
  SCleanMode        cleanMode;
  SHashObj*         DataSinkGroupList;  // hash <groupId, SGroupDSManager>
  SDataSinkFileMgr* pFileMgr;
} SStreamTaskDSManager;

struct SGroupDSManager {
  int64_t               groupId;
  int64_t               usedMemSize;
  int64_t               lastWstartInMem;
  SArray*               windowDataInMem;  // array SWindowData <wstart, SSDataBlock*>
  SStreamTaskDSManager* pSinkManager;
};

typedef enum {
  DATA_SINK_MEM = 0,
  DATA_SINK_FILE,
} SDataSinkPos;
typedef struct SResultIter {
  void*             groupData;
  SDataSinkFileMgr* pFileMgr;  // when dataPos is 1, pFileMgr is not NULL
  int64_t           offset;    // array index, start from 0
  SDataSinkPos      dataPos;   // 0 - data in mem, 1 - data in file
  int64_t           reqStartTime;
  int64_t           reqEndTime;
} SResultIter;

// @brief 创建一个数据缓存
// @param cleanMode 清理模式，具体含义如下:
//        1. 一行数据只会被读取一次，所以读取结束后可以立刻被清理
//        2. 一行数据可能被读取多次，所以等到下次读取时，才清理时间范围之前的数据
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int32_t cleanMode, void** ppCache);

// @brief 清理数据缓存，包括缓存的数据文件和内存
void destroyStreamDataCache(void* pCache);

// @brief 向数据缓存中添加数据
// @param pCache 数据缓存,使用 StreamDataCacheInit 创建
// @param wstart 当前数据集的起始时间戳
// @param wend 当前数据集的结束时间戳
// @param pBlock 数据块
// @param startIndex 数据块的起始索引
// @param endIndex 数据块的结束索引
// @note
//      1. 起始索引和结束索引是数据块数据的索引范围,从0开始计数
//      2. 可能会对同一个 {groupId, tableId, wstart} 进行多次调用,添加多个数据块,调用者保证这些数据是严格时间有序的
int32_t putStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock,
                           int32_t startIndex, int32_t endIndex);

// @brief 向数据缓存中添加数据
// @note 和 putStreamDataCache 区别是：
//        1. 会移交 pBlock 的所有权
//        2. 如果返回 success，pBlock 的内存释放由 Cache Sink 负责；
//        3. 如果返回 error，pBlock 的内存释放由调用者负责；
int32_t moveStreamDataCache(void *pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock *pBlock);

// @brief 从数据缓存中读取数据
// @param pCache 数据缓存,使用 StreamDataCacheInit 创建
// @param groupId 数据的分组ID,实际上是 "<streamid>_<taskid>_<groupid>" 格式的字符串
// @param start 读取数据的起始时间戳
// @param end 读取数据的结束时间戳
// @param pIter 迭代器,用于遍历数据块
// @note
//      1. 没有数据时，把 pIter 置为 NULL
//      2. 符合筛选条件的数据可能包含多个数据块,由 pIter 负责迭代遍历
//      3. 这里没有区分 tableId,后续对 pIter 遍历的结果应该是按照 tableId 有序,内部再以时间戳有序
//      4. start, end 一定是对齐到数据集边界的，即 [start, end] 包含若干个数据集，但不会包含任意数据集的一部分
int32_t getStreamDataCache(void* pCache, int64_t groupId, TSKEY start, TSKEY end, void** pIter);

// @brief 遍历获取所有符合条件的数据块
// @param pIter 迭代器,用于遍历数据块
// @param ppBlock 用于指向结果数据块，调用者不会释放指向的内存
// @note
//      1. 需要把 pIter 指向迭代器的下一位，如果没有数据了，返回 NULL
int32_t getNextStreamDataCache(void** pIter, SSDataBlock** ppBlock);

// @brief 取消对读取结果的遍历
// @note
//      1. 调用者在使用 pIter 遍历数据时，可以用这个接口提前结束遍历，通常用于异常情况
//      2. 取消数据遍历意味着读取操作结束，会触发底层 Cache Sink 的数据清理
void cancelStreamDataCacheIterate(void** pIter);

// @brief 释放 DataSink 相关所有资源
int32_t destroyDataSinkManager2();

void setDataSinkMaxMemSize(int64_t maxMemSize);

//----------------- **************************************   -----------------//
//----------------- 以下函数 DataSink 内部调用，不提供于其他模块   -----------------//
//----------------- **************************************   -----------------//
int32_t initDataSinkFileDir();

int32_t initStreamDataSinkOnce();

// @brief 写入数据到文件
int32_t writeToFile(SStreamTaskDSManager* pStreamDataSink, int64_t groupId, TSKEY wstart, TSKEY wend,
                    SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex);

// @brief 写入数据到内存
int32_t writeToCache(SStreamTaskDSManager* pStreamDataSink, SGroupDSManager* pGroupDataInfoMgr, TSKEY wstart, TSKEY wend,
                     SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex);
int32_t moveToCache(SStreamTaskDSManager* pStreamDataSink, SGroupDSManager* pGroupDataInfoMgr, TSKEY wstart, TSKEY wend,
                     SSDataBlock* pBlock);

// @brief 读取数据从内存
int32_t readDataFromCache(SResultIter* pResult, SSDataBlock** ppBlock);
int32_t getFirstDataIterFromCache(SStreamTaskDSManager* pStreamTaskDSMgr, int64_t groupId, TSKEY start, TSKEY end,
                                  void** ppResult);
int32_t readDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock);
int32_t getFirstDataIterFromFile(SStreamTaskDSManager* pStreamTaskDSMgr, int64_t groupId, TSKEY start, TSKEY end,
                                 void** ppResult);

// @brief 从内存查找下一组数据位置
// return false: 查询未结束, true: 查询已结束                                
bool    setNextIteratorFromCache(SResultIter** ppResult);
void    setNextIteratorFromFile(SResultIter** pIter);
void    releaseDataIterator(void** pIter);

// @brief 读取数据从文件
int32_t createSGroupDSManager(int64_t groupId, SGroupDSManager** ppGroupDataInfo);

int32_t getOrCreateSGroupDSManager(SStreamTaskDSManager* pStreamDataSink, int64_t groupId,
                                   SGroupDSManager** ppGroupDataInfoMgr);

void destorySWindowDataP(void* pData);
void destorySWindowDataPP(void* pData);

void clearGroupExpiredDataInMem(SGroupDSManager* pGroupData, TSKEY start);

void syncWindowDataMemAdd(SWindowData* pSWindowData);
void syncWindowDataMemSub(SWindowData* pSWindowData);

void destroyStreamDataSinkFile(SDataSinkFileMgr** ppDaSinkFileMgr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_DATA_SINK_H

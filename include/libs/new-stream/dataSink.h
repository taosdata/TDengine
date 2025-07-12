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
// 一个 group 无法避免有多个数据块在同一个文件中
// 需要有标记来记录已经 get 完成 但是没有开始 put 的 group， 这些是优先要淘汰的内存
// 上述内存淘汰完之后，内存还不足，代表这需要淘汰正在写的和正在读的的内存块了。

// 1. 非 sliding, 读取后立即释放内存，一个单独 list 存放 buff 信息，每个 buff 10 M，当前 buff 写满开始下一个
// buff，总大小超过则按时间升序将部分 buff 块写入文件，释放内存； 每次读取数据，会同时读取文件和 buff
// 中的数据，读取完成后会全部释放。内存和文件中数据会有一个 groupid 标签，目前非 sliding 模式同时只会存在一个 groupid
// 的数据，如果发现 有多个 groupid 数据在尝试同时写入，报错。 非 slidng
// 模式，内存中需保存管理信息(同时保存的group信息理论上只有一个)： groupid，usedMemSize  blocksInMem: address, capacity,
// size + (windows: startTime, endTime, dataLen) + dataBlock serialized data blocksInFile: offset, capacity, size
//                文件中保存信息：(windows: startTime, endTime, dataLen) + dataBlock serialized data
// 2. sliding 模式：每次写入数据，写入独立的内存段，内存不足时触发淘汰机制，将某些 group 的数据从内存淘汰，写入文件
// 内存不足时，先淘汰占用内存最多并且当前不活跃的 group ，淘汰后内存仍不足时，淘汰当前 group
// 的数据（理论上同一时间只有一个活跃的 group ） 当前不活跃的 group: 定义为 getdata 完成，没有进行新一轮的 putdata 的
// group 每个 task 首次需要写入文件时，计算要写入的 group data 的长度（考虑 +20% 做缓冲，最多 + 1M ），作为文件中每个
// group block 的大小，后续写入的 group block 均使用改大小，当大小不足时，申请新的 block 淘汰内存时，linux 下使用 writev
// 来写入文件, 可以将分散的内存连续一次写入，其他平台不支持，可以先逐个写入内存后再写入文件，后续优化 从 group
// 读取数据时，文件/内存中可能均有数据，先读文件后读取内存；
// 每次读取数据后，检查使用内存情况，如果需要触发淘汰机制，在一个新线程中进行内存淘汰。读取数据时的内存淘汰触发条件要比写入时内存淘汰更敏感，设置一个略小的值（例如比写入时限制小
// 10 M），尽量提前触发，避免在读取时需要阻塞读取进行释放 sliding 模式，内存中需保存的管理信息（多个
// group）：groupid，usedMemSize  blocksInMem: address, capacity, size + (windows: startTime, endTime, dataLen) +
// dataBlock serialized data blocksInFile: groupOffset, dataStartOffset, dataLen， capacity(same in a task)
//                  文件中保存信息：(windows: startTime, endTime, dataLen) + dataBlock serialized data
// 每个块写入的 window 保存：list, endtime, len

#ifndef TDENGINE_DATA_SINK_H
#define TDENGINE_DATA_SINK_H

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
  // void*   address;   // 后续地址存放的内容为 SSlidingWindowInMem 数组序列化后的内容
} SAlignBlocksInMem;

typedef struct SBlocksInfoFile {
  int64_t groupOffset;  // offset in file
  int64_t dataLen;
  int64_t capacity;  // size in file
  // SSlidingWindowInMem *windowDataInFile;  // array SSlidingWindowInMem 实际数据，反序列化保存至文件
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
  int64_t           capacity;         // group 在文件中的每个 block 块大小
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

//----------------- **************************************   -----------------//
//----------------- 以下函数 DataSink 对外提供接口   -----------------//
//----------------- **************************************   -----------------//

// @brief 创建一个数据缓存
// @param cleanMode 清理模式，具体含义如下:
//        1. 一行数据只会被读取一次，所以读取结束后可以立刻被清理
//        2. 一行数据可能被读取多次，所以等到下次读取时，才清理时间范围之前的数据
int32_t initStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int32_t cleanMode, int32_t tsSlotId, void** ppCache);

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
int32_t moveStreamDataCache(void* pCache, int64_t groupId, TSKEY wstart, TSKEY wend, SSDataBlock* pBlock);

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
int32_t destroyDataSinkMgr();

void setDataSinkMaxMemSize(int64_t maxMemSize);

//----------------- **************************************   -----------------//
//----------------- 以下函数 DataSink 内部调用，不提供于其他模块   -----------------//
//----------------- **************************************   -----------------//
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

// @brief 读取数据从内存
int32_t readDataFromMem(SResultIter* pResult, SSDataBlock** ppBlock, bool* finished);
int32_t readDataFromFile(SResultIter* pResult, SSDataBlock** ppBlock, int32_t tsColSlotId);

// @brief 从内存查找下一组数据位置
// return true: 需要继续查看文件, false: 不需要继续查看文件
bool    setNextIteratorFromMem(SResultIter** ppResult);
bool    setNextIteratorFromFile(SResultIter** ppResult);
int32_t createDataResult(void** ppResult);
void    releaseDataResult(void** ppResult);

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

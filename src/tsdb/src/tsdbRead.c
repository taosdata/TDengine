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

#include "os.h"

#include "talgo.h"
#include "tlog.h"
#include "tutil.h"

#include "../../../query/inc/qast.h"
#include "../../../query/inc/qextbuffer.h"
#include "../../../query/inc/tlosertree.h"
#include "../../../query/inc/tsqlfunction.h"
#include "tsdb.h"
#include "tsdbMain.h"

#define EXTRA_BYTES 2
#define PRIMARY_TSCOL_REQUIRED(c) (((SColumnInfoData*)taosArrayGet(c, 0))->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)
#define ASCENDING_ORDER_TRAVERSE(o)   (o == TSDB_ORDER_ASC)
#define QH_GET_NUM_OF_COLS(handle) ((size_t)(taosArrayGetSize((handle)->pColumns)))

enum {
  QUERY_RANGE_LESS_EQUAL = 0,
  QUERY_RANGE_GREATER_EQUAL = 1,
};

typedef struct SField {
  // todo need the definition
} SField;

typedef struct SQueryFilePos {
  int32_t fid;
  int32_t slot;
  int32_t pos;
  int64_t lastKey;
} SQueryFilePos;

typedef struct SDataBlockLoadInfo {
  SFileGroup* fileGroup;
  int32_t     slot;
  int32_t     sid;
  SArray*     pLoadedCols;
} SDataBlockLoadInfo;

typedef struct SLoadCompBlockInfo {
  int32_t sid; /* meter sid */
  int32_t fileId;
  int32_t fileListIndex;
} SLoadCompBlockInfo;

typedef struct STableCheckInfo {
  STableId   tableId;
  TSKEY      lastKey;
  STable*    pTableObj;
  int64_t    offsetInHeaderFile;
  int32_t    start;
  bool       checkFirstFileBlock;
  
  SCompInfo* pCompInfo;
  int32_t    compSize;
  
  int32_t    numOfBlocks;  // number of qualified data blocks not the original blocks

  SDataCols*         pDataCols;
  SSkipListIterator* iter;
} STableCheckInfo;

typedef struct {
  SCompBlock* compBlock;
  SField*     fields;
} SCompBlockFields;

typedef struct STableBlockInfo {
  SCompBlockFields pBlock;
  STableCheckInfo* pTableCheckInfo;
  int32_t          blockIndex;
  int32_t          groupIdx; /* number of group is less than the total number of tables */
} STableBlockInfo;

typedef struct SBlockOrderSupporter {
  int32_t             numOfTables;
  STableBlockInfo** pDataBlockInfo;
  int32_t*            blockIndexArray;
  int32_t*            numOfBlocksPerMeter;
} SBlockOrderSupporter;

typedef struct STsdbQueryHandle {
  STsdbRepo*    pTsdb;
  SQueryFilePos cur;    // current position
  SQueryFilePos start;  // the start position, used for secondary/third iteration

  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  int16_t     numOfRowsPerPage;
  uint16_t    flag;  // denotes reversed scan of data or not
  int16_t     order;
  STimeWindow window;  // the primary query time window that applies to all queries
  int32_t     blockBufferSize;
  SCompBlock* pBlock;
  int32_t     numOfBlocks;
  SField**    pFields;
  SArray*     pColumns;  // column list, SColumnInfoData array list
  bool        locateStart;
  int32_t     realNumOfRows;
  bool        loadDataAfterSeek;  // load data after seek.
  SArray*     pTableCheckInfo;
  int32_t     activeIndex;

  bool    checkFiles;  // check file stage
  int32_t tableIndex;
  bool    isFirstSlot;
  void*   qinfo;  // query info handle, for debug purpose

  STableBlockInfo* pDataBlockInfo;

  SFileGroup*    pFileGroup;
  SFileGroupIter fileIter;
  SCompIdx*      compIndex;
} STsdbQueryHandle;

static void tsdbInitDataBlockLoadInfo(SDataBlockLoadInfo* pBlockLoadInfo) {
  pBlockLoadInfo->slot = -1;
  pBlockLoadInfo->sid = -1;
  pBlockLoadInfo->fileGroup = NULL;
}

static void tsdbInitCompBlockLoadInfo(SLoadCompBlockInfo* pCompBlockLoadInfo) {
  pCompBlockLoadInfo->sid = -1;
  pCompBlockLoadInfo->fileId = -1;
  pCompBlockLoadInfo->fileListIndex = -1;
}

tsdb_query_handle_t* tsdbQueryTables(tsdb_repo_t* tsdb, STsdbQueryCond* pCond, SArray* groupList, SArray* pColumnInfo) {
  // todo 1. filter not exist table
  // todo 2. add the reference count for each table that is involved in query

  STsdbQueryHandle* pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  pQueryHandle->order  = pCond->order;
  pQueryHandle->window = pCond->twindow;
  pQueryHandle->pTsdb  = tsdb;
  pQueryHandle->compIndex = calloc(10000, sizeof(SCompIdx)),

  pQueryHandle->loadDataAfterSeek = false;
  pQueryHandle->isFirstSlot = true;
  pQueryHandle->cur.fid = -1;

  size_t size = taosArrayGetSize(groupList);
  assert(size >= 1);

  pQueryHandle->pTableCheckInfo = taosArrayInit(size, sizeof(STableCheckInfo));
  for (int32_t i = 0; i < size; ++i) {
    SArray* group = *(SArray**)taosArrayGet(groupList, i);

    size_t gsize = taosArrayGetSize(group);
    for (int32_t j = 0; j < gsize; ++j) {
      STable* pTable = *(STable**)taosArrayGet(group, j);
      assert(pTable != NULL);
      
      STableCheckInfo info = {
          .lastKey = pQueryHandle->window.skey,
          .tableId = pTable->tableId,
          .pTableObj = pTable,
      };

      taosArrayPush(pQueryHandle->pTableCheckInfo, &info);
    }
  }

  dTrace("%p total numOfTable:%d in query", pQueryHandle, taosArrayGetSize(pQueryHandle->pTableCheckInfo));
  
  /*
   * For ascending timestamp order query, query starts from data files. In contrast, buffer will be checked in the first place
   * in case of descending timestamp order query.
   */
  pQueryHandle->checkFiles  = ASCENDING_ORDER_TRAVERSE(pQueryHandle->order);
  pQueryHandle->activeIndex = 0;

  // allocate buffer in order to load data blocks from file
  int32_t numOfCols = taosArrayGetSize(pColumnInfo);
  size_t  bufferCapacity = 4096;

  pQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pColumnInfo, i);
    SColumnInfoData  pDest = {{0}, 0};

    pDest.pData = calloc(1, EXTRA_BYTES + bufferCapacity * pCol->info.bytes);
    pDest.info = pCol->info;
    taosArrayPush(pQueryHandle->pColumns, &pDest);
  }

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  return (tsdb_query_handle_t)pQueryHandle;
}

static bool hasMoreDataInCache(STsdbQueryHandle* pHandle) {
  size_t size = taosArrayGetSize(pHandle->pTableCheckInfo);
  assert(pHandle->activeIndex < size && pHandle->activeIndex >= 0 && size >= 1);
  pHandle->cur.fid = -1;
  
  STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);

  STable* pTable = pCheckInfo->pTableObj;
  assert(pTable != NULL);

  // no data in cache, abort
  if (pTable->mem == NULL && pTable->imem == NULL) {
    return false;
  }
  
  if (pCheckInfo->iter == NULL) {
    pCheckInfo->iter = tSkipListCreateIterFromVal(pTable->mem->pData, (const char*) &pCheckInfo->lastKey,
        TSDB_DATA_TYPE_TIMESTAMP, pHandle->order);
    
    if (pCheckInfo->iter == NULL) {
      return false;
    }
  }
  
  if (!tSkipListIterNext(pCheckInfo->iter)) {  // buffer is empty
    return false;
  }

  SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
  if (node == NULL) {
    return false;
  }

  SDataRow row = SL_GET_NODE_DATA(node);
  pCheckInfo->lastKey = dataRowKey(row);  // first timestamp in buffer
  dTrace("%p uid:%" PRId64", tid:%d check data in buffer from skey:%" PRId64 ", order:%d", pHandle,
      pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, pCheckInfo->lastKey, pHandle->order);
  
  // all data in mem are checked already.
  if ((pCheckInfo->lastKey > pHandle->window.ekey && ASCENDING_ORDER_TRAVERSE(pHandle->order)) ||
      (pCheckInfo->lastKey < pHandle->window.ekey && !ASCENDING_ORDER_TRAVERSE(pHandle->order))) {
    return false;
  }

  return true;
}

// todo dynamic get the daysperfile
static int32_t getFileIdFromKey(TSKEY key) {
  int64_t fid = (int64_t)(key / (10 * tsMsPerDay[0]));  // set the starting fileId
  if (fid > INT32_MAX) {
    fid = INT32_MAX;
  }
  
  return fid;
}

static int32_t binarySearchForBlockImpl(SCompBlock* pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
  int32_t firstSlot = 0;
  int32_t lastSlot = numOfBlocks - 1;
  
  int32_t midSlot = firstSlot;
  
  while (1) {
    numOfBlocks = lastSlot - firstSlot + 1;
    midSlot = (firstSlot + (numOfBlocks >> 1));
    
    if (numOfBlocks == 1) break;
    
    if (skey > pBlock[midSlot].keyLast) {
      if (numOfBlocks == 2) break;
      if ((order == TSDB_ORDER_DESC) && (skey < pBlock[midSlot + 1].keyFirst)) break;
      firstSlot = midSlot + 1;
    } else if (skey < pBlock[midSlot].keyFirst) {
      if ((order == TSDB_ORDER_ASC) && (skey > pBlock[midSlot - 1].keyLast)) break;
      lastSlot = midSlot - 1;
    } else {
      break;  // got the slot
    }
  }
  
  return midSlot;
}

static int32_t getFileCompInfo(STsdbQueryHandle* pQueryHandle, int32_t* numOfBlocks, int32_t type) {
  // todo check open file failed
  SFileGroup* fileGroup = pQueryHandle->pFileGroup;
  
  assert(fileGroup->files[TSDB_FILE_TYPE_HEAD].fname > 0);
  if (fileGroup->files[TSDB_FILE_TYPE_HEAD].fd == FD_INITIALIZER) {
    fileGroup->files[TSDB_FILE_TYPE_HEAD].fd = open(fileGroup->files[TSDB_FILE_TYPE_HEAD].fname, O_RDONLY);
  } else {
    assert(FD_VALID(fileGroup->files[TSDB_FILE_TYPE_HEAD].fd));
  }

  // load all the comp offset value for all tables in this file
  tsdbLoadCompIdx(fileGroup, pQueryHandle->compIndex, 10000);  // todo set dynamic max tables

  *numOfBlocks = 0;
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

    SCompIdx* compIndex = &pQueryHandle->compIndex[pCheckInfo->tableId.tid];
    if (compIndex->len == 0 || compIndex->numOfSuperBlocks == 0) {  // no data block in this file, try next file
      continue;//no data blocks in the file belongs to pCheckInfo->pTable
    } else {
      if (pCheckInfo->compSize < compIndex->len) {
        assert(compIndex->len > 0);
        
        char* t = realloc(pCheckInfo->pCompInfo, compIndex->len);
        assert(t != NULL);
        
        pCheckInfo->pCompInfo = (SCompInfo*) t;
        pCheckInfo->compSize = compIndex->len;
      }
      
      tsdbLoadCompBlocks(fileGroup, compIndex, pCheckInfo->pCompInfo);
  
      SCompInfo* pCompInfo = pCheckInfo->pCompInfo;
      
      TSKEY s = MIN(pCheckInfo->lastKey, pQueryHandle->window.ekey);
      TSKEY e = MAX(pCheckInfo->lastKey, pQueryHandle->window.ekey);
      
      // discard the unqualified data block based on the query time window
      int32_t start = binarySearchForBlockImpl(pCompInfo->blocks, compIndex->numOfSuperBlocks, s, TSDB_ORDER_ASC);
      int32_t end = start;
      
      if (s > pCompInfo->blocks[start].keyLast) {
        continue;
      }

      // todo speedup the procedure of located end block
      while (end < compIndex->numOfSuperBlocks && (pCompInfo->blocks[end].keyFirst <= e)) {
        end += 1;
      }

      pCheckInfo->numOfBlocks = (end - start);
      
      if (start > 0) {
        memmove(pCompInfo->blocks, &pCompInfo->blocks[start], pCheckInfo->numOfBlocks * sizeof(SCompBlock));
      }

      (*numOfBlocks) += pCheckInfo->numOfBlocks;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static SDataBlockInfo getTrueDataBlockInfo(STableCheckInfo* pCheckInfo, SCompBlock* pBlock) {
  SDataBlockInfo info = {
      .window = {.skey = pBlock->keyFirst, .ekey = pBlock->keyLast},
      .numOfCols = pBlock->numOfCols,
      .rows = pBlock->numOfPoints,
      .sid = pCheckInfo->tableId.tid,
      .uid = pCheckInfo->tableId.uid,
  };

  return info;
}

static SArray* getColumnIdList(STsdbQueryHandle* pQueryHandle) {
  size_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);
  assert(numOfCols <= TSDB_MAX_COLUMNS);
  
  SArray* pIdList = taosArrayInit(numOfCols, sizeof(int16_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
    taosArrayPush(pIdList, &pCol->info.colId);
  }
  
  return pIdList;
}

static SArray* getDefaultLoadColumns(STsdbQueryHandle* pQueryHandle, bool loadTS) {
  SArray* pLocalIdList = getColumnIdList(pQueryHandle);
  
  // check if the primary time stamp column needs to load
  int16_t colId = *(int16_t*)taosArrayGet(pLocalIdList, 0);
  
  // the primary timestamp column does not be included in the the specified load column list, add it
  if (loadTS && colId != 0) {
    int16_t columnId = 0;
    taosArrayInsert(pLocalIdList, 0, &columnId);
  }
  
  return pLocalIdList;
}

static void    filterDataInDataBlock(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SCompBlock* pBlock,
                                     SArray* sa);
static int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);

static bool doLoadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo) {
  SCompData* data = calloc(1, sizeof(SCompData) + sizeof(SCompCol) * pBlock->numOfCols);

  data->numOfCols = pBlock->numOfCols;
  data->uid = pCheckInfo->pTableObj->tableId.uid;

  bool    blockLoaded = false;
  SArray* sa = getDefaultLoadColumns(pQueryHandle, true);

  if (pCheckInfo->pDataCols == NULL) {
    pCheckInfo->pDataCols = tdNewDataCols(1000, 2, 4096);
  }

  tdInitDataCols(pCheckInfo->pDataCols, tsdbGetTableSchema(tsdbGetMeta(pQueryHandle->pTsdb), pCheckInfo->pTableObj));

  SFile* pFile = &pQueryHandle->pFileGroup->files[TSDB_FILE_TYPE_DATA];
  if (pFile->fd == FD_INITIALIZER) {
    pFile->fd = open(pFile->fname, O_RDONLY);
  }

  if (tsdbLoadDataBlock(pFile, pBlock, 1, pCheckInfo->pDataCols, data) == 0) {
    SDataBlockLoadInfo* pBlockLoadInfo = &pQueryHandle->dataBlockLoadInfo;

    pBlockLoadInfo->fileGroup = pQueryHandle->pFileGroup;
    pBlockLoadInfo->slot = pQueryHandle->cur.slot;
    pBlockLoadInfo->sid = pCheckInfo->pTableObj->tableId.tid;

    blockLoaded = true;
  }

  taosArrayDestroy(sa);
  tfree(data);

  TSKEY* d = (TSKEY*)pCheckInfo->pDataCols->cols[PRIMARYKEY_TIMESTAMP_COL_INDEX].pData;
  assert(d[0] == pBlock->keyFirst && d[pBlock->numOfPoints - 1] == pBlock->keyLast);

  return blockLoaded;
}

static bool loadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo) {
  SArray*        sa = getDefaultLoadColumns(pQueryHandle, true);
  SQueryFilePos* cur = &pQueryHandle->cur;

  if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
    // query ended in current block
    if (pQueryHandle->window.ekey < pBlock->keyLast || pCheckInfo->lastKey > pBlock->keyFirst) {
      if (!doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo)) {
        return false;
      }

      SDataCols* pDataCols = pCheckInfo->pDataCols;
      if (pCheckInfo->lastKey > pBlock->keyFirst) {
        cur->pos =
            binarySearchForKey(pDataCols->cols[0].pData, pBlock->numOfPoints, pCheckInfo->lastKey, pQueryHandle->order);
      } else {
        cur->pos = 0;
      }

      filterDataInDataBlock(pQueryHandle, pCheckInfo, pBlock, sa);
    } else {  // the whole block is loaded in to buffer
      pQueryHandle->realNumOfRows = pBlock->numOfPoints;
    }
  } else {
    // query ended in current block
    if (pQueryHandle->window.ekey > pBlock->keyFirst) {
      if (!doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo)) {
        return false;
      }
      
      SDataCols* pDataCols = pCheckInfo->pDataCols;
      if (pCheckInfo->lastKey < pBlock->keyLast) {
        cur->pos =
            binarySearchForKey(pDataCols->cols[0].pData, pBlock->numOfPoints, pCheckInfo->lastKey, pQueryHandle->order);
      } else {
        cur->pos = pBlock->numOfPoints - 1;
      }
      
      filterDataInDataBlock(pQueryHandle, pCheckInfo, pBlock, sa);
    } else {
      pQueryHandle->realNumOfRows = pBlock->numOfPoints;
    }
  }

  taosArrayDestroy(sa);
  return pQueryHandle->realNumOfRows > 0;
}

static int vnodeBinarySearchKey(char* pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    numOfPoints;
  TSKEY* keyList;

  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);
  
  if (num <= 0) return -1;

  keyList = (TSKEY*)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

// only return the qualified data to client in terms of query time window, data rows in the same block but do not
// be included in the query time window will be discarded
static void filterDataInDataBlock(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SCompBlock* pBlock,
                                  SArray* sa) {
  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataBlockInfo blockInfo = getTrueDataBlockInfo(pCheckInfo, pBlock);

  SDataCols* pCols = pCheckInfo->pDataCols;

  int32_t endPos = cur->pos;
  if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey > blockInfo.window.ekey) {
    endPos = blockInfo.rows - 1;
    pQueryHandle->realNumOfRows = endPos - cur->pos + 1;
    pCheckInfo->lastKey = blockInfo.window.ekey + 1;
  } else if (!ASCENDING_ORDER_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey < blockInfo.window.skey) {
    endPos = 0;
    pQueryHandle->realNumOfRows = cur->pos + 1;
    pCheckInfo->lastKey = blockInfo.window.ekey - 1;
  } else {
    int32_t order = (pQueryHandle->order == TSDB_ORDER_ASC)? TSDB_ORDER_DESC:TSDB_ORDER_ASC;
    endPos = vnodeBinarySearchKey(pCols->cols[0].pData, pCols->numOfPoints, pQueryHandle->window.ekey, order);

    if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
      if (endPos < cur->pos) {
        pQueryHandle->realNumOfRows = 0;
        return;
      } else {
        pQueryHandle->realNumOfRows = endPos - cur->pos + 1;
      }

      pCheckInfo->lastKey = ((int64_t*)(pCols->cols[0].pData))[endPos] + 1;
    } else {
      if (endPos > cur->pos) {
        pQueryHandle->realNumOfRows = 0;
        return;
      } else {
        pQueryHandle->realNumOfRows = cur->pos - endPos + 1;
      }
    }
  }

  int32_t start = MIN(cur->pos, endPos);

  // move the data block in the front to data block if needed
  int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);

  for (int32_t i = 0; i < taosArrayGetSize(sa); ++i) {
    int16_t colId = *(int16_t*)taosArrayGet(sa, i);

    for (int32_t j = 0; j < numOfCols; ++j) {
      SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, j);

      if (pCol->info.colId == colId) {
        SDataCol* pDataCol = &pCols->cols[i];
        memmove(pCol->pData, pDataCol->pData + pCol->info.bytes * start,
                pQueryHandle->realNumOfRows * pCol->info.bytes);
        break;
      }
    }
  }

  assert(pQueryHandle->realNumOfRows <= blockInfo.rows);

  // forward(backward) the position for cursor
  cur->pos = endPos;
}

int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    numOfPoints;
  TSKEY* keyList;

  if (num <= 0) return -1;

  keyList = (TSKEY*)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == 0) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfPoints = lastPos - firstPos + 1;
      midPos = (numOfPoints >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

static void cleanBlockOrderSupporter(SBlockOrderSupporter* pSupporter, int32_t numOfTables) {
  tfree(pSupporter->numOfBlocksPerMeter);
  tfree(pSupporter->blockIndexArray);

  for (int32_t i = 0; i < numOfTables; ++i) {
    tfree(pSupporter->pDataBlockInfo[i]);
  }

  tfree(pSupporter->pDataBlockInfo);
}

static int32_t dataBlockOrderCompar(const void* pLeft, const void* pRight, void* param) {
  int32_t leftTableIndex = *(int32_t*)pLeft;
  int32_t rightTableIndex = *(int32_t*)pRight;

  SBlockOrderSupporter* pSupporter = (SBlockOrderSupporter*)param;

  int32_t leftTableBlockIndex = pSupporter->blockIndexArray[leftTableIndex];
  int32_t rightTableBlockIndex = pSupporter->blockIndexArray[rightTableIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerMeter[leftTableIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerMeter[rightTableIndex]) {
    /* right block is empty */
    return -1;
  }

  STableBlockInfo* pLeftBlockInfoEx = &pSupporter->pDataBlockInfo[leftTableIndex][leftTableBlockIndex];
  STableBlockInfo* pRightBlockInfoEx = &pSupporter->pDataBlockInfo[rightTableIndex][rightTableBlockIndex];

  //    assert(pLeftBlockInfoEx->pBlock.compBlock->offset != pRightBlockInfoEx->pBlock.compBlock->offset);
  if (pLeftBlockInfoEx->pBlock.compBlock->offset == pRightBlockInfoEx->pBlock.compBlock->offset &&
      pLeftBlockInfoEx->pBlock.compBlock->last == pRightBlockInfoEx->pBlock.compBlock->last) {
    // todo add more information
    dError("error in header file, two block with same offset:%p", pLeftBlockInfoEx->pBlock.compBlock->offset);
  }

  return pLeftBlockInfoEx->pBlock.compBlock->offset > pRightBlockInfoEx->pBlock.compBlock->offset ? 1 : -1;
}

static int32_t createDataBlocksInfo(STsdbQueryHandle* pQueryHandle, int32_t numOfBlocks, int32_t* numOfAllocBlocks) {
  char* tmp = realloc(pQueryHandle->pDataBlockInfo, sizeof(STableBlockInfo) * numOfBlocks);
  if (tmp == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  pQueryHandle->pDataBlockInfo = (STableBlockInfo*)tmp;
  memset(pQueryHandle->pDataBlockInfo, 0, sizeof(STableBlockInfo) * numOfBlocks);
  *numOfAllocBlocks = numOfBlocks;

  int32_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  SBlockOrderSupporter sup = {0};
  sup.numOfTables = numOfTables;
  sup.numOfBlocksPerMeter = calloc(1, sizeof(int32_t) * numOfTables);
  sup.blockIndexArray = calloc(1, sizeof(int32_t) * numOfTables);
  sup.pDataBlockInfo = calloc(1, POINTER_BYTES * numOfTables);

  if (sup.numOfBlocksPerMeter == NULL || sup.blockIndexArray == NULL || sup.pDataBlockInfo == NULL) {
    cleanBlockOrderSupporter(&sup, 0);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t cnt = 0;
  int32_t numOfQualTables = 0;
  for (int32_t j = 0; j < numOfTables; ++j) {
    STableCheckInfo* pTableCheck = (STableCheckInfo*)taosArrayGet(pQueryHandle->pTableCheckInfo, j);
    if (pTableCheck->numOfBlocks <= 0) {
      continue;
    }
    
    SCompBlock* pBlock = pTableCheck->pCompInfo->blocks;
    sup.numOfBlocksPerMeter[numOfQualTables] = pTableCheck->numOfBlocks;

    char* buf = calloc(1, sizeof(STableBlockInfo) * pTableCheck->numOfBlocks);
    if (buf == NULL) {
      cleanBlockOrderSupporter(&sup, numOfQualTables);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[numOfQualTables] = (STableBlockInfo*)buf;

    for (int32_t k = 0; k < pTableCheck->numOfBlocks; ++k) {
      STableBlockInfo* pBlockInfoEx = &sup.pDataBlockInfo[numOfQualTables][k];

      pBlockInfoEx->pBlock.compBlock = &pBlock[k];
      pBlockInfoEx->pBlock.fields = NULL;

      pBlockInfoEx->pTableCheckInfo = pTableCheck;
      //      pBlockInfoEx->groupIdx = pTableCheckInfo[j]->groupIdx;     // set the group index
      //      pBlockInfoEx->blockIndex = pTableCheckInfo[j]->start + k;    // set the block index in original meter
      cnt++;
    }

    numOfQualTables++;
  }

  dTrace("%p create data blocks info struct completed, %d blocks in %d tables", pQueryHandle, cnt, numOfQualTables);

  assert(cnt <= numOfBlocks && numOfQualTables <= numOfTables);  // the pMeterDataInfo[j]->numOfBlocks may be 0
  sup.numOfTables = numOfQualTables;
  SLoserTreeInfo* pTree = NULL;

  uint8_t ret = tLoserTreeCreate(&pTree, sup.numOfTables, &sup, dataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanBlockOrderSupporter(&sup, numOfTables);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t pos = pTree->pNode[0].index;
    int32_t index = sup.blockIndexArray[pos]++;

    STableBlockInfo* pBlocksInfoEx = sup.pDataBlockInfo[pos];
    pQueryHandle->pDataBlockInfo[numOfTotal++] = pBlocksInfoEx[index];

    // set data block index overflow, in order to disable the offset comparator
    if (sup.blockIndexArray[pos] >= sup.numOfBlocksPerMeter[pos]) {
      sup.blockIndexArray[pos] = sup.numOfBlocksPerMeter[pos] + 1;
    }

    tLoserTreeAdjust(pTree, pos + sup.numOfTables);
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfo)[i].pBlock.compBlock->offset < (*pDataBlockInfo)[i+1].pBlock.compBlock->offset);
   * }
   */

  dTrace("%p %d data blocks sort completed", pQueryHandle, cnt);
  cleanBlockOrderSupporter(&sup, numOfTables);
  free(pTree);

  return TSDB_CODE_SUCCESS;
}

// todo opt for only one table case
static bool getDataBlocksInFilesImpl(STsdbQueryHandle* pQueryHandle) {
  pQueryHandle->numOfBlocks = 0;
  SQueryFilePos* cur = &pQueryHandle->cur;
  
  int32_t numOfBlocks = 0;
  int32_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  
  while ((pQueryHandle->pFileGroup = tsdbGetFileGroupNext(&pQueryHandle->fileIter)) != NULL) {
    int32_t type = ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)? QUERY_RANGE_GREATER_EQUAL:QUERY_RANGE_LESS_EQUAL;
    if (getFileCompInfo(pQueryHandle, &numOfBlocks, type) != TSDB_CODE_SUCCESS) {
      break;
    }
    
    assert(numOfBlocks >= 0);
    dTrace("%p %d blocks found in file for %d table(s), fid:%d", pQueryHandle, numOfBlocks,
           numOfTables, pQueryHandle->pFileGroup->fileId);
    
    // todo return error code to query engine
    if (createDataBlocksInfo(pQueryHandle, numOfBlocks, &pQueryHandle->numOfBlocks) != TSDB_CODE_SUCCESS) {
      break;
    }
    
    assert(numOfBlocks >= pQueryHandle->numOfBlocks);
    if (pQueryHandle->numOfBlocks > 0) {
      break;
    }
  }
  
  // no data in file anymore
  if (pQueryHandle->numOfBlocks <= 0) {
    assert(pQueryHandle->pFileGroup == NULL);
    cur->fid = -1;
    
    return false;
  }
  
  cur->slot = ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)? 0:pQueryHandle->numOfBlocks-1;
  cur->fid = pQueryHandle->pFileGroup->fileId;
  
  STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
  STableCheckInfo* pCheckInfo = pBlockInfo->pTableCheckInfo;
  SCompBlock*      pBlock = pBlockInfo->pBlock.compBlock;
  
  return loadFileDataBlock(pQueryHandle, pBlock, pCheckInfo);
}

static bool getDataBlocksInFiles(STsdbQueryHandle* pQueryHandle) {
  STsdbFileH*    pFileHandle = tsdbGetFile(pQueryHandle->pTsdb);
  SQueryFilePos* cur = &pQueryHandle->cur;

  // find the start data block in file
  if (!pQueryHandle->locateStart) {
    pQueryHandle->locateStart = true;

    int32_t fid = getFileIdFromKey(pQueryHandle->window.skey);
    
    tsdbInitFileGroupIter(pFileHandle, &pQueryHandle->fileIter, pQueryHandle->order);
    tsdbSeekFileGroupIter(&pQueryHandle->fileIter, fid);

    return getDataBlocksInFilesImpl(pQueryHandle);
  } else {
    if ((cur->slot == pQueryHandle->numOfBlocks - 1 && ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) ||
        (cur->slot == 0 && !ASCENDING_ORDER_TRAVERSE(pQueryHandle->order))) { // all blocks
      
      return getDataBlocksInFilesImpl(pQueryHandle);
    } else {  // next block of the same file
      int32_t step = ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)? 1:-1;
      cur->slot += step;
      
      STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
      if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
        cur->pos = 0;
      } else {
        cur->pos = pBlockInfo->pBlock.compBlock->numOfPoints - 1;
      }

      return loadFileDataBlock(pQueryHandle, pBlockInfo->pBlock.compBlock, pBlockInfo->pTableCheckInfo);
    }
  }
}

static bool doHasDataInBuffer(STsdbQueryHandle* pQueryHandle) {
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  // todo add assert, the value of numOfTables should be less than the maximum value for each vnode capacity
  
  while (pQueryHandle->activeIndex < numOfTables) {
    if (hasMoreDataInCache(pQueryHandle)) {
      return true;
    }
    
    pQueryHandle->activeIndex += 1;
  }
  
  return false;
}

// handle data in cache situation
bool tsdbNextDataBlock(tsdb_query_handle_t* pqHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pqHandle;
  
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  assert(numOfTables > 0);
  
  if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
    if (pQueryHandle->checkFiles) {
      if (getDataBlocksInFiles(pQueryHandle)) {
        return true;
      }

      pQueryHandle->activeIndex = 0;
      pQueryHandle->checkFiles  = false;
    }
    
    return doHasDataInBuffer(pQueryHandle);
  } else {  // starts from the buffer in case of descending timestamp order check data blocks
    if (!pQueryHandle->checkFiles) {
      if (doHasDataInBuffer(pQueryHandle)) {
        return true;
      }
      
      pQueryHandle->checkFiles = true;
    }

    return getDataBlocksInFiles(pQueryHandle);
  }
  
}

static int tsdbReadRowsFromCache(SSkipListIterator* pIter, TSKEY maxKey, int maxRowsToRead, TSKEY* skey, TSKEY* ekey,
                                 STsdbQueryHandle* pQueryHandle) {
  int     numOfRows = 0;
  int32_t numOfCols = taosArrayGetSize(pQueryHandle->pColumns);
  *skey = INT64_MIN;

  do {
    SSkipListNode* node = tSkipListIterGet(pIter);
    if (node == NULL) {
      break;
    }

    SDataRow row = SL_GET_NODE_DATA(node);
    TSKEY key = dataRowKey(row);
    
    if ((key > maxKey && ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) ||
        (key < maxKey && !ASCENDING_ORDER_TRAVERSE(pQueryHandle->order))) {
      
      dTrace("%p key:%"PRIu64" beyond qrange:%"PRId64" - %"PRId64", no more data in buffer", pQueryHandle, key, pQueryHandle->window.skey,
          pQueryHandle->window.ekey);
      
      break;
    }

    if (*skey == INT64_MIN) {
      *skey = dataRowKey(row);
    }

    *ekey = dataRowKey(row);

    int32_t offset = 0;
    char* pData = NULL;
    
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      
      if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
        pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
      } else {
        pData = pColInfo->pData + (maxRowsToRead - numOfRows - 1) * pColInfo->info.bytes;
      }
      
      memcpy(pData, dataRowTuple(row) + offset, pColInfo->info.bytes);
      offset += pColInfo->info.bytes;
    }

    numOfRows++;
    if (numOfRows >= maxRowsToRead) {
      break;
    }
    
  } while(tSkipListIterNext(pIter));

  assert(numOfRows <= maxRowsToRead);
  
  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  if (!ASCENDING_ORDER_TRAVERSE(pQueryHandle->order) && numOfRows < maxRowsToRead) {
    int32_t emptySize = maxRowsToRead - numOfRows;
    
    for(int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      memmove(pColInfo->pData, pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
    }
  }
  
  return numOfRows;
}

// copy data from cache into data block
SDataBlockInfo tsdbRetrieveDataBlockInfo(tsdb_query_handle_t* pQueryHandle) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;

  STable* pTable = NULL;

  TSKEY   skey = 0, ekey = 0;
  int32_t rows = 0;

  int32_t step = ASCENDING_ORDER_TRAVERSE(pHandle->order)? 1:-1;
  
  // data in file
  if (pHandle->cur.fid >= 0) {
    STableBlockInfo* pBlockInfo = &pHandle->pDataBlockInfo[pHandle->cur.slot];

    pTable = pBlockInfo->pTableCheckInfo->pTableObj;

    SDataBlockInfo binfo = getTrueDataBlockInfo(pBlockInfo->pTableCheckInfo, pBlockInfo->pBlock.compBlock);
    if (binfo.rows == pHandle->realNumOfRows) {
      pBlockInfo->pTableCheckInfo->lastKey = pBlockInfo->pBlock.compBlock->keyLast + 1;
      return binfo;
    } else {
      /* not a whole disk block, only the qualified rows, so this block is loaded in to buffer during the
       * block next function
       */
      SColumnInfoData* pColInfoEx = taosArrayGet(pHandle->pColumns, 0);

      rows = pHandle->realNumOfRows;
      skey = *(TSKEY*)pColInfoEx->pData;
      ekey = *(TSKEY*)((char*)pColInfoEx->pData + TSDB_KEYSIZE * (rows - 1));

      // update the last key value
      pBlockInfo->pTableCheckInfo->lastKey = ekey + step;
    }
  } else {
    STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);
    pTable = pCheckInfo->pTableObj;

    if (pTable->mem != NULL) {
      // create mem table iterator if it is not created yet
      assert(pCheckInfo->iter != NULL);
      rows = tsdbReadRowsFromCache(pCheckInfo->iter, pHandle->window.ekey, 2, &skey, &ekey, pHandle);

      // update the last key value
      pCheckInfo->lastKey = ekey + step;
    }
  }

  SDataBlockInfo blockInfo = {
      .uid = pTable->tableId.uid,
      .sid = pTable->tableId.tid,
      .rows = rows,
      .window = {.skey = MIN(skey, ekey), .ekey = MAX(skey, ekey)}
  };

  return blockInfo;
}

// return null for data block in cache
int32_t tsdbRetrieveDataBlockStatisInfo(tsdb_query_handle_t* pQueryHandle, SDataStatis** pBlockStatis) {
  *pBlockStatis = NULL;
  return TSDB_CODE_SUCCESS;
}

SArray* tsdbRetrieveDataBlock(tsdb_query_handle_t* pQueryHandle, SArray* pIdList) {
  /**
   * In the following two cases, the data has been loaded to SColumnInfoData.
   * 1. data is from cache, 2. data block is not completed qualified to query time range
   */
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;

  if (pHandle->cur.fid < 0) {
    return pHandle->pColumns;
  } else {
    STableBlockInfo* pBlockInfoEx = &pHandle->pDataBlockInfo[pHandle->cur.slot];
    STableCheckInfo*   pCheckInfo = pBlockInfoEx->pTableCheckInfo;

    SDataBlockInfo binfo = getTrueDataBlockInfo(pCheckInfo, pBlockInfoEx->pBlock.compBlock);
    assert(pHandle->realNumOfRows <= binfo.rows);

    if (pHandle->realNumOfRows < binfo.rows) {
      return pHandle->pColumns;
    } else {
      // data block has been loaded, todo extract method
      SDataBlockLoadInfo* pBlockLoadInfo = &pHandle->dataBlockLoadInfo;
      if (pBlockLoadInfo->slot == pHandle->cur.slot && pBlockLoadInfo->sid == pCheckInfo->pTableObj->tableId.tid) {
        return pHandle->pColumns;
      } else {
        SCompBlock* pBlock = pBlockInfoEx->pBlock.compBlock;
        doLoadFileDataBlock(pHandle, pBlock, pCheckInfo);

        SArray* sa = getDefaultLoadColumns(pHandle, true);
        filterDataInDataBlock(pHandle, pCheckInfo, pBlock, sa);
        taosArrayDestroy(sa);

        return pHandle->pColumns;
      }
    }
  }
}

int32_t tsdbResetQuery(tsdb_query_handle_t* pQueryHandle, STimeWindow* window, tsdbpos_t position, int16_t order) {
  return 0;
}

int32_t tsdbDataBlockSeek(tsdb_query_handle_t* pQueryHandle, tsdbpos_t pos) { return 0; }

tsdbpos_t tsdbDataBlockTell(tsdb_query_handle_t* pQueryHandle) { return NULL; }

SArray* tsdbRetrieveDataRow(tsdb_query_handle_t* pQueryHandle, SArray* pIdList, SQueryRowCond* pCond) { return NULL; }

tsdb_query_handle_t* tsdbQueryFromTagConds(STsdbQueryCond* pCond, int16_t stableId, const char* pTagFilterStr) {
  return NULL;
}

SArray* tsdbGetTableList(tsdb_query_handle_t* pQueryHandle) { return NULL; }

static int32_t getAllTableIdList(STsdbRepo* tsdb, int64_t uid, SArray* list) {
  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  assert(pTable != NULL);  // assert pTable is a super table

  SSkipListIterator* iter = tSkipListCreateIter(pTable->pIndex);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* pNode = tSkipListIterGet(iter);
    
    STable* t = *(STable**)SL_GET_NODE_DATA(pNode);
    taosArrayPush(list, t);
  }
  
  return TSDB_CODE_SUCCESS;
}

typedef struct SExprTreeSupporter {
  SSchema* pTagSchema;
  int32_t  numOfTags;
  int32_t  optr;
} SExprTreeSupporter;

/**
 * convert the result pointer to table id instead of table object pointer
 * @param pRes
 */
static void convertQueryResult(SArray* pRes, SArray* pTableList) {
  if (pTableList == NULL || taosArrayGetSize(pTableList) == 0) {
    return;
  }

  size_t size = taosArrayGetSize(pTableList);
  for (int32_t i = 0; i < size; ++i) {
    STable* pTable = taosArrayGetP(pTableList, i);
    taosArrayPush(pRes, &pTable->tableId);
  }
}

static void destroyHelper(void* param) {
  if (param == NULL) {
    return;
  }

  tQueryInfo* pInfo = (tQueryInfo*)param;
  tVariantDestroy(&(pInfo->q));
  free(param);
}

static void getTagColumnInfo(SExprTreeSupporter* pSupporter, SSchema* pSchema, int32_t* index, int32_t* offset) {
  *index = 0;
  *offset = 0;

  // filter on table name(TBNAME)
  if (strcasecmp(pSchema->name, TSQL_TBNAME_L) == 0) {
    *index = TSDB_TBNAME_COLUMN_INDEX;
    *offset = TSDB_TBNAME_COLUMN_INDEX;
    return;
  }

  while ((*index) < pSupporter->numOfTags) {
    if (pSupporter->pTagSchema[*index].bytes == pSchema->bytes &&
        pSupporter->pTagSchema[*index].type == pSchema->type &&
        pSupporter->pTagSchema[*index].colId == pSchema->colId) {
      break;
    } else {
      (*offset) += pSupporter->pTagSchema[(*index)++].bytes;
    }
  }
}

void filterPrepare(void* expr, void* param) {
  tExprNode* pExpr = (tExprNode*)expr;
  if (pExpr->_node.info != NULL) {
    return;
  }

  int32_t i = 0, offset = 0;
  pExpr->_node.info = calloc(1, sizeof(tQueryInfo));

  tQueryInfo* pInfo = pExpr->_node.info;

  SExprTreeSupporter* pSupporter = (SExprTreeSupporter*)param;

  tVariant* pCond = pExpr->_node.pRight->pVal;
  SSchema*  pSchema = pExpr->_node.pLeft->pSchema;

  getTagColumnInfo(pSupporter, pSchema, &i, &offset);
  assert((i >= 0 && i < TSDB_MAX_TAGS) || (i == TSDB_TBNAME_COLUMN_INDEX));
  assert((offset >= 0 && offset < TSDB_MAX_TAGS_LEN) || (offset == TSDB_TBNAME_COLUMN_INDEX));

  pInfo->sch = *pSchema;
  pInfo->colIndex = i;
  pInfo->optr = pExpr->_node.optr;
  pInfo->offset = offset;
  //  pInfo->compare  = getFilterComparator(pSchema->type, pCond->nType, pInfo->optr);

  tVariantAssign(&pInfo->q, pCond);
  tVariantTypeSetType(&pInfo->q, pInfo->sch.type);
}

int32_t doCompare(const char* f1, const char* f2, int32_t type, size_t size) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:        DEFAULT_COMP(GET_INT32_VAL(f1), GET_INT32_VAL(f2));
    case TSDB_DATA_TYPE_DOUBLE:     DEFAULT_COMP(GET_DOUBLE_VAL(f1), GET_DOUBLE_VAL(f2));
    case TSDB_DATA_TYPE_FLOAT:      DEFAULT_COMP(GET_FLOAT_VAL(f1), GET_FLOAT_VAL(f2));
    case TSDB_DATA_TYPE_BIGINT:     DEFAULT_COMP(GET_INT64_VAL(f1), GET_INT64_VAL(f2));
    case TSDB_DATA_TYPE_SMALLINT:   DEFAULT_COMP(GET_INT16_VAL(f1), GET_INT16_VAL(f2));
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:       DEFAULT_COMP(GET_INT8_VAL(f1), GET_INT8_VAL(f2));
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t ret = wcsncmp((wchar_t*) f1, (wchar_t*) f2, size/TSDB_NCHAR_SIZE);
      if (ret == 0) {
        return ret;
      }
      return (ret < 0) ? -1 : 1;
    }
    default: {
      int32_t ret = strncmp(f1, f2, (size_t)size);
      if (ret == 0) {
        return ret;
      }
      
      return (ret < 0) ? -1 : 1;
    }
  }
}

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  STSchema*  pTagSchema;
} STableGroupSupporter;

int32_t tableGroupComparFn(const void *p1, const void *p2, const void *param) {
  STableGroupSupporter* pTableGroupSupp = (STableGroupSupporter*) param;
  
  STable *pTable1 = *(STable **) p1;
  STable *pTable2 = *(STable **) p2;
  
  for (int32_t i = 0; i < pTableGroupSupp->numOfCols; ++i) {
    SColIndex* pColIndex = &pTableGroupSupp->pCols[i];
    int32_t colIndex = pColIndex->colIndex;
    
    char *  f1 = NULL;
    char *  f2 = NULL;
    int32_t type = 0;
    int32_t bytes = 0;
    
    if (colIndex == -1) { // table name, todo fix me
//      f1 = s1->tags;
//      f2 = s2->tags;
      type = TSDB_DATA_TYPE_BINARY;
      bytes = TSDB_TABLE_NAME_LEN;
    } else {
      f1 = dataRowTuple(pTable1->tagVal);
      f2 = dataRowTuple(pTable2->tagVal);

      type = schemaColAt(pTableGroupSupp->pTagSchema, colIndex)->type;
      bytes = schemaColAt(pTableGroupSupp->pTagSchema, colIndex)->bytes;
    }
    
    int32_t ret = doCompare(f1, f2, type, bytes);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }
  
  return 0;
}

void createTableGroupImpl(SArray* pGroups, STable** pTables, size_t numOfTables, STableGroupSupporter* pSupp, __ext_compar_fn_t compareFn) {
  SArray* g = taosArrayInit(16, POINTER_BYTES);
  taosArrayPush(g, &pTables[0]);
  
  for (int32_t i = 1; i < numOfTables; ++i) {
    int32_t ret = compareFn(&pTables[i - 1], &pTables[i], pSupp);
    assert(ret == 0 || ret == -1);
    
    if (ret == 0) {
      taosArrayPush(g, &pTables[i]);
    } else {
      taosArrayPush(pGroups, &g);  // current group is ended, start a new group
      
      g = taosArrayInit(16, POINTER_BYTES);
      taosArrayPush(g, &pTables[i]);
    }
  }
}

SArray* createTableGroup(SArray* pTableList, STSchema* pTagSchema, SColIndex* pCols, int32_t numOfOrderCols) {
  assert(pTableList != NULL && taosArrayGetSize(pTableList) > 0);
  SArray* pTableGroup = taosArrayInit(1, POINTER_BYTES);
  
  size_t size = taosArrayGetSize(pTableList);
  if (size == 0) {
    pTrace("no qualified tables");
    return pTableGroup;
  }
  
  if (numOfOrderCols == 0 || size == 1) { // no group by tags clause or only one table
    taosArrayPush(pTableGroup, pTableList);
    pTrace("all %d tables belong to one group", size);
    
#ifdef _DEBUG_VIEW
    tSidSetDisplay(pTableGroup);
#endif
  } else {
    STableGroupSupporter *pSupp = (STableGroupSupporter *) calloc(1, sizeof(STableGroupSupporter));
    pSupp->numOfCols = numOfOrderCols;
    pSupp->pTagSchema = pTagSchema;
    pSupp->pCols = pCols;
    
    taosqsort(pTableList->pData, size, POINTER_BYTES, pSupp, tableGroupComparFn);
    createTableGroupImpl(pTableGroup, pTableList->pData, size, pSupp, tableGroupComparFn);

#ifdef _DEBUG_VIEW
    tSidSetDisplay(pTableGroup);
#endif
    tfree(pSupp);
  }
  
  return pTableGroup;
}

bool tSkipListNodeFilterCallback(const void* pNode, void* param) {
  tQueryInfo* pInfo = (tQueryInfo*)param;

  STable* pTable = (STable*)(SL_GET_NODE_DATA((SSkipListNode*)pNode));

  char*  val = dataRowTuple(pTable->tagVal);  // todo not only the first column
  int8_t type = pInfo->sch.type;

  int32_t ret = 0;
  if (pInfo->q.nType == TSDB_DATA_TYPE_BINARY || pInfo->q.nType == TSDB_DATA_TYPE_NCHAR) {
    ret = pInfo->compare(val, pInfo->q.pz);
  } else {
    tVariant t = {0};
    tVariantCreateFromBinary(&t, val, (uint32_t)pInfo->sch.bytes, type);

    ret = pInfo->compare(&t.i64Key, &pInfo->q.i64Key);
  }

  switch (pInfo->optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_GREATER_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_GREATER: {
      return ret > 0;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      return ret <= 0;
    }
    case TSDB_RELATION_LESS: {
      return ret < 0;
    }
    case TSDB_RELATION_LIKE: {
      return ret == 0;
    }

    default:
      assert(false);
  }
  return true;
}

static int32_t doQueryTableList(STable* pSTable, SArray* pRes, tExprNode* pExpr) {
  // query according to the binary expression
  STSchema* pSchema = pSTable->tagSchema;
  SSchema*  schema = calloc(schemaNCols(pSchema), sizeof(SSchema));
  for (int32_t i = 0; i < schemaNCols(pSchema); ++i) {
    schema[i].colId = schemaColAt(pSchema, i)->colId;
    schema[i].type = schemaColAt(pSchema, i)->type;
    schema[i].bytes = schemaColAt(pSchema, i)->bytes;
  }

  SExprTreeSupporter s = {.pTagSchema = schema, .numOfTags = schemaNCols(pSTable->tagSchema)};

  SBinaryFilterSupp supp = {
      .fp = (__result_filter_fn_t)tSkipListNodeFilterCallback, .setupInfoFn = filterPrepare, .pExtInfo = &s};

  SArray* pTableList = taosArrayInit(8, POINTER_BYTES);

  tExprTreeTraverse(pExpr, pSTable->pIndex, pTableList, &supp);
  tExprTreeDestroy(&pExpr, destroyHelper);

  convertQueryResult(pRes, pTableList);
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbQueryTags(tsdb_repo_t* tsdb, int64_t uid, const char* pTagCond, size_t len, SArray** pGroupList,
    SColIndex* pColIndex, int32_t numOfCols) {
  
  STable* pSTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pSTable == NULL) {
    dError("failed to get stable, uid:%" PRIu64, uid);
    return TSDB_CODE_INVALID_TABLE_ID;
  }
  
  SArray* res = taosArrayInit(8, POINTER_BYTES);
  STSchema* pTagSchema = tsdbGetTableTagSchema(tsdbGetMeta(tsdb), pSTable);
  
  if (pTagCond == NULL || len == 0) {  // no tags condition, all tables created according to this stable are involved
    int32_t ret = getAllTableIdList(tsdb, uid, res);
    if (ret != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(res);
      return ret;
    }
    
    *pGroupList = createTableGroup(res, pTagSchema, pColIndex, numOfCols);
    taosArrayDestroy(res);
    return ret;
  }

  tExprNode* pExprNode = NULL;
  int32_t    ret = TSDB_CODE_SUCCESS;

  // failed to build expression, no result, return immediately
  if ((ret = exprTreeFromBinary(pTagCond, len, &pExprNode) != TSDB_CODE_SUCCESS) || (pExprNode == NULL)) {
    dError("stable:%" PRIu64 ", failed to deserialize expression tree, error exists", uid);
    taosArrayDestroy(res);
    return ret;
  }

  doQueryTableList(pSTable, res, pExprNode);
  *pGroupList = createTableGroup(res, pTagSchema, pColIndex, numOfCols);

  taosArrayDestroy(res);
  return ret;
}

int32_t tsdbGetOneTableGroup(tsdb_repo_t* tsdb, int64_t uid, SArray** pGroupList) {
  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pTable == NULL) {
    return TSDB_CODE_INVALID_TABLE_ID;
  }
  
  //todo assert table type, add the table ref count
  
  *pGroupList = taosArrayInit(1, POINTER_BYTES);
  SArray* group = taosArrayInit(1, POINTER_BYTES);
  
  taosArrayPush(group, &pTable);
  taosArrayPush(*pGroupList, &group);
  
  return TSDB_CODE_SUCCESS;
}
void tsdbCleanupQueryHandle(tsdb_query_handle_t queryHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*)queryHandle;
  if (pQueryHandle == NULL) {
    return;
  }
  
  size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableCheckInfo* pTableCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    tSkipListDestroyIter(pTableCheckInfo->iter);

    if (pTableCheckInfo->pDataCols != NULL) {
      tfree(pTableCheckInfo->pDataCols->buf);
    }

    tfree(pTableCheckInfo->pDataCols);

    tfree(pTableCheckInfo->pCompInfo);
  }

  taosArrayDestroy(pQueryHandle->pTableCheckInfo);
  tfree(pQueryHandle->compIndex);

  size_t cols = taosArrayGetSize(pQueryHandle->pColumns);
  for (int32_t i = 0; i < cols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
    tfree(pColInfo->pData);
  }

  taosArrayDestroy(pQueryHandle->pColumns);
  
  tfree(pQueryHandle->pDataBlockInfo);
  tfree(pQueryHandle);
}

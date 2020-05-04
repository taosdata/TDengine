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
#include "tulog.h"
#include "talgo.h"
#include "tutil.h"
#include "tcompare.h"
#include "exception.h"

#include "../../../query/inc/qast.h"  // todo move to common module
#include "../../../query/inc/tlosertree.h"  // todo move to util module
#include "tsdb.h"
#include "tsdbMain.h"

#define EXTRA_BYTES 2
#define ASCENDING_ORDER_TRAVERSE(o)   (o == TSDB_ORDER_ASC)
#define QH_GET_NUM_OF_COLS(handle) ((size_t)(taosArrayGetSize((handle)->pColumns)))

enum {
  QUERY_RANGE_LESS_EQUAL = 0,
  QUERY_RANGE_GREATER_EQUAL = 1,
};

enum {
  TSDB_QUERY_TYPE_ALL      = 1,
  TSDB_QUERY_TYPE_LAST     = 2,
  TSDB_QUERY_TYPE_EXTERNAL = 3,
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
  int32_t sid; /* table sid */
  int32_t fileId;
  int32_t fileListIndex;
} SLoadCompBlockInfo;

typedef struct STableCheckInfo {
  STableId   tableId;
  TSKEY      lastKey;
  STable*    pTableObj;
  int32_t    start;
  SCompInfo* pCompInfo;
  int32_t    compSize;
  int32_t    numOfBlocks;  // number of qualified data blocks not the original blocks
  SDataCols* pDataCols;
  
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
  int32_t*            numOfBlocksPerTable;
} SBlockOrderSupporter;

typedef struct STsdbQueryHandle {
  STsdbRepo*    pTsdb;
  SQueryFilePos cur;    // current position

  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  int16_t     order;
  STimeWindow window;  // the primary query time window that applies to all queries
  SCompBlock* pBlock;
  int32_t     numOfBlocks;
  SField**    pFields;
  SArray*     pColumns;    // column list, SColumnInfoData array list
  bool        locateStart;
  int32_t     realNumOfRows;
  SArray*     pTableCheckInfo;  //SArray<STableCheckInfo>
  int32_t     activeIndex;
  bool        checkFiles;  // check file stage
  void*       qinfo;       // query info handle, for debug purpose
  int32_t     type;        // query type: retrieve all data blocks, 2. retrieve only last row, 3. retrieve direct prev|next rows
  STableBlockInfo* pDataBlockInfo;

  SFileGroup*    pFileGroup;
  SFileGroupIter fileIter;
  SRWHelper      rhelper;
} STsdbQueryHandle;

static void changeQueryHandleForLastrowQuery(TsdbQueryHandleT pqHandle);

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

TsdbQueryHandleT* tsdbQueryTables(TsdbRepoT* tsdb, STsdbQueryCond* pCond, STableGroupInfo* groupList) {
  // todo 1. filter not exist table
  // todo 2. add the reference count for each table that is involved in query

  STsdbQueryHandle* pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  pQueryHandle->order  = pCond->order;
  pQueryHandle->window = pCond->twindow;
  pQueryHandle->pTsdb  = tsdb;
  pQueryHandle->type   = TSDB_QUERY_TYPE_ALL;
  tsdbInitReadHelper(&pQueryHandle->rhelper, (STsdbRepo*) tsdb);

  pQueryHandle->cur.fid = -1;

  size_t sizeOfGroup = taosArrayGetSize(groupList->pGroupList);
  assert(sizeOfGroup >= 1 && pCond != NULL && pCond->numOfCols > 0);

  pQueryHandle->pTableCheckInfo = taosArrayInit(groupList->numOfTables, sizeof(STableCheckInfo));
  
  for (int32_t i = 0; i < sizeOfGroup; ++i) {
    SArray* group = *(SArray**) taosArrayGet(groupList->pGroupList, i);

    size_t gsize = taosArrayGetSize(group);
    assert(gsize > 0);
    
    for (int32_t j = 0; j < gsize; ++j) {
      STableId* id = (STableId*) taosArrayGet(group, j);
      
      STableCheckInfo info = {
          .lastKey = pQueryHandle->window.skey,
          .tableId = *id,
          .pTableObj = tsdbGetTableByUid(tsdbGetMeta(tsdb), id->uid),
      };

      assert(info.pTableObj != NULL && info.pTableObj->tableId.tid == id->tid);
      taosArrayPush(pQueryHandle->pTableCheckInfo, &info);
    }
  }

  uTrace("%p total numOfTable:%d in query", pQueryHandle, taosArrayGetSize(pQueryHandle->pTableCheckInfo));
  
  /*
   * For ascending timestamp order query, query starts from data files. In contrast, buffer will be checked in the first place
   * in case of descending timestamp order query.
   */
  pQueryHandle->checkFiles  = ASCENDING_ORDER_TRAVERSE(pQueryHandle->order);
  pQueryHandle->activeIndex = 0;

  // allocate buffer in order to load data blocks from file
  int32_t numOfCols = pCond->numOfCols;
  size_t  bufferCapacity = 4096;

  pQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfoData  pDest = {{0}, 0};

    pDest.info = pCond->colList[i];
    pDest.pData = calloc(1, EXTRA_BYTES + bufferCapacity * pCond->colList[i].bytes);
    taosArrayPush(pQueryHandle->pColumns, &pDest);
  }

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  return (TsdbQueryHandleT) pQueryHandle;
}

TsdbQueryHandleT tsdbQueryLastRow(TsdbRepoT *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList);
  
  pQueryHandle->type = TSDB_QUERY_TYPE_LAST;
  pQueryHandle->order = TSDB_ORDER_DESC;
  
  changeQueryHandleForLastrowQuery(pQueryHandle);
  return pQueryHandle;
}

TsdbQueryHandleT tsdbQueryRowsInExternalWindow(TsdbRepoT *tsdb, STsdbQueryCond* pCond, STableGroupInfo *groupList) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList);
  
  pQueryHandle->type = TSDB_QUERY_TYPE_EXTERNAL;
  pQueryHandle->order = TSDB_ORDER_ASC;
  
//  changeQueryHandleForLastrowQuery(pQueryHandle);
  return pQueryHandle;
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
  
  if (pCheckInfo->iter == NULL && pTable->mem) {
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
  uTrace("%p uid:%" PRId64", tid:%d check data in buffer from skey:%" PRId64 ", order:%d, %p", pHandle,
      pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, pCheckInfo->lastKey, pHandle->order, pHandle->qinfo);
  
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
  tsdbSetAndOpenHelperFile(&pQueryHandle->rhelper, fileGroup);

  // load all the comp offset value for all tables in this file
  *numOfBlocks = 0;
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

    SCompIdx* compIndex = &pQueryHandle->rhelper.pCompIdx[pCheckInfo->tableId.tid];
    if (compIndex->len == 0 || compIndex->numOfBlocks == 0) {  // no data block in this file, try next file
      continue;//no data blocks in the file belongs to pCheckInfo->pTable
    } else {
      if (pCheckInfo->compSize < compIndex->len) {
        assert(compIndex->len > 0);
        
        char* t = realloc(pCheckInfo->pCompInfo, compIndex->len);
        assert(t != NULL);
        
        pCheckInfo->pCompInfo = (SCompInfo*) t;
        pCheckInfo->compSize = compIndex->len;
      }
      
      // tsdbLoadCompBlocks(fileGroup, compIndex, pCheckInfo->pCompInfo);
      STable* pTable = tsdbGetTableByUid(tsdbGetMeta(pQueryHandle->pTsdb), pCheckInfo->tableId.uid);
      assert(pTable != NULL);
      
      tsdbSetHelperTable(&pQueryHandle->rhelper, pTable, pQueryHandle->pTsdb);

      tsdbLoadCompInfo(&(pQueryHandle->rhelper), (void *)(pCheckInfo->pCompInfo));
      SCompInfo* pCompInfo = pCheckInfo->pCompInfo;
      
      TSKEY s = MIN(pCheckInfo->lastKey, pQueryHandle->window.ekey);
      TSKEY e = MAX(pCheckInfo->lastKey, pQueryHandle->window.ekey);
      
      // discard the unqualified data block based on the query time window
      int32_t start = binarySearchForBlockImpl(pCompInfo->blocks, compIndex->numOfBlocks, s, TSDB_ORDER_ASC);
      int32_t end = start;
      
      if (s > pCompInfo->blocks[start].keyLast) {
        continue;
      }

      // todo speedup the procedure of located end block
      while (end < compIndex->numOfBlocks && (pCompInfo->blocks[end].keyFirst <= e)) {
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
      .tid = pCheckInfo->tableId.tid,
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
    pCheckInfo->pDataCols = tdNewDataCols(1000, 100, 4096);  //todo fix me
  }

  tdInitDataCols(pCheckInfo->pDataCols, tsdbGetTableSchema(tsdbGetMeta(pQueryHandle->pTsdb), pCheckInfo->pTableObj));

  if (tsdbLoadBlockData(&(pQueryHandle->rhelper), pBlock, NULL) == 0) {
    SDataBlockLoadInfo* pBlockLoadInfo = &pQueryHandle->dataBlockLoadInfo;

    pBlockLoadInfo->fileGroup = pQueryHandle->pFileGroup;
    pBlockLoadInfo->slot = pQueryHandle->cur.slot;
    pBlockLoadInfo->sid = pCheckInfo->pTableObj->tableId.tid;

    blockLoaded = true;
  }

  taosArrayDestroy(sa);
  tfree(data);

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
  
      SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
      assert(pCols->numOfPoints == pBlock->numOfPoints);
      
      if (pCheckInfo->lastKey > pBlock->keyFirst) {
        cur->pos =
            binarySearchForKey(pCols->cols[0].pData, pBlock->numOfPoints, pCheckInfo->lastKey, pQueryHandle->order);
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
  
  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];

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
  
//  if (start > 0) {
//    tdPopDataColsPoints(pQueryHandle->rhelper.pDataCols[0], start);
//  }

  // move the data block in the front to data block if needed
  int32_t numOfCols = pQueryHandle->rhelper.pDataCols[0]->numOfCols;
  int32_t reqCols = taosArrayGetSize(pQueryHandle->pColumns);
  
  for (int32_t i = 0; i < reqCols; ++i) {
//    int16_t colId = *(int16_t*)taosArrayGet(sa, i);
    SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
    int32_t bytes = pCol->info.bytes;

    for (int32_t j = 0; j < numOfCols; ++j) {
      SDataCol* src = &pQueryHandle->rhelper.pDataCols[0]->cols[j];

      if (pCol->info.colId == src->colId) {
        if (pCol->info.type != TSDB_DATA_TYPE_BINARY && pCol->info.type != TSDB_DATA_TYPE_NCHAR) {
          memmove(pCol->pData, src->pData + bytes * start, bytes * pQueryHandle->realNumOfRows);
        } else { // handle the var-string
          for(int32_t k = start; k < pQueryHandle->realNumOfRows + start; ++k) {
            char* p = tdGetColDataOfRow(src, k);
            memcpy(pCol->pData + k * bytes, p, varDataTLen(p));  // todo refactor
          }
        }
        
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
  tfree(pSupporter->numOfBlocksPerTable);
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

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerTable[leftTableIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerTable[rightTableIndex]) {
    /* right block is empty */
    return -1;
  }

  STableBlockInfo* pLeftBlockInfoEx = &pSupporter->pDataBlockInfo[leftTableIndex][leftTableBlockIndex];
  STableBlockInfo* pRightBlockInfoEx = &pSupporter->pDataBlockInfo[rightTableIndex][rightTableBlockIndex];

  //    assert(pLeftBlockInfoEx->pBlock.compBlock->offset != pRightBlockInfoEx->pBlock.compBlock->offset);
  if (pLeftBlockInfoEx->pBlock.compBlock->offset == pRightBlockInfoEx->pBlock.compBlock->offset &&
      pLeftBlockInfoEx->pBlock.compBlock->last == pRightBlockInfoEx->pBlock.compBlock->last) {
    // todo add more information
    uError("error in header file, two block with same offset:%p", pLeftBlockInfoEx->pBlock.compBlock->offset);
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
  sup.numOfBlocksPerTable = calloc(1, sizeof(int32_t) * numOfTables);
  sup.blockIndexArray = calloc(1, sizeof(int32_t) * numOfTables);
  sup.pDataBlockInfo = calloc(1, POINTER_BYTES * numOfTables);

  if (sup.numOfBlocksPerTable == NULL || sup.blockIndexArray == NULL || sup.pDataBlockInfo == NULL) {
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
    sup.numOfBlocksPerTable[numOfQualTables] = pTableCheck->numOfBlocks;

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
      //      pBlockInfoEx->blockIndex = pTableCheckInfo[j]->start + k;    // set the block index in original table
      cnt++;
    }

    numOfQualTables++;
  }

  uTrace("%p create data blocks info struct completed, %d blocks in %d tables", pQueryHandle, cnt, numOfQualTables);

  assert(cnt <= numOfBlocks && numOfQualTables <= numOfTables);  // the pTableQueryInfo[j]->numOfBlocks may be 0
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
    if (sup.blockIndexArray[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.blockIndexArray[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    tLoserTreeAdjust(pTree, pos + sup.numOfTables);
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfo)[i].pBlock.compBlock->offset < (*pDataBlockInfo)[i+1].pBlock.compBlock->offset);
   * }
   */

  uTrace("%p %d data blocks sort completed", pQueryHandle, cnt);
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
    
    uTrace("%p %d blocks found in file for %d table(s), fid:%d", pQueryHandle, numOfBlocks,
           numOfTables, pQueryHandle->pFileGroup->fileId);
    
    assert(numOfBlocks >= 0);
    if (numOfBlocks == 0) {
      continue;
    }
    
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
bool tsdbNextDataBlock(TsdbQueryHandleT* pqHandle) {
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

void changeQueryHandleForLastrowQuery(TsdbQueryHandleT pqHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pqHandle;
  assert(!ASCENDING_ORDER_TRAVERSE(pQueryHandle->order));
  
  // starts from the buffer in case of descending timestamp order check data blocks
  
  // todo consider the query time window, current last_row does not apply the query time window
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  
  TSKEY key = 0;
  int32_t index = -1;
  
  for(int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    if (pCheckInfo->pTableObj->lastKey > key) {  //todo lastKey should not be 0 by default
      key = pCheckInfo->pTableObj->lastKey;
      index = i;
    }
  }
  
  // todo, there are no data in all the tables. opt performance
  if (index == -1) {
    return;
  }
  
  // erase all other elements in array list, todo refactor
  size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    if (i == index) {
      continue;
    }
    
    STableCheckInfo* pTableCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    tSkipListDestroyIter(pTableCheckInfo->iter);
    
    if (pTableCheckInfo->pDataCols != NULL) {
      tfree(pTableCheckInfo->pDataCols->buf);
    }
    
    tfree(pTableCheckInfo->pDataCols);
    tfree(pTableCheckInfo->pCompInfo);
  }
  
  STableCheckInfo info = *(STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, index);
  taosArrayDestroy(pQueryHandle->pTableCheckInfo);
  
  pQueryHandle->pTableCheckInfo = taosArrayInit(1, sizeof(STableCheckInfo));
  taosArrayPush(pQueryHandle->pTableCheckInfo, &info);
  
  // update the query time window according to the chosen last timestamp
  pQueryHandle->window = (STimeWindow) {key, key};
}

static int tsdbReadRowsFromCache(SSkipListIterator* pIter, STable* pTable, TSKEY maxKey, int maxRowsToRead, TSKEY* skey, TSKEY* ekey,
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
      
      uTrace("%p key:%"PRIu64" beyond qrange:%"PRId64" - %"PRId64", no more data in buffer", pQueryHandle, key, pQueryHandle->window.skey,
          pQueryHandle->window.ekey);
      
      break;
    }

    if (*skey == INT64_MIN) {
      *skey = dataRowKey(row);
    }

    *ekey = dataRowKey(row);

    int32_t offset = -1;
    char* pData = NULL;
  
    STSchema* pSchema = tsdbGetTableSchema(tsdbGetMeta(pQueryHandle->pTsdb), pTable);
    int32_t numOfTableCols = schemaNCols(pSchema);
    
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      
      if (ASCENDING_ORDER_TRAVERSE(pQueryHandle->order)) {
        pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
      } else {
        pData = pColInfo->pData + (maxRowsToRead - numOfRows - 1) * pColInfo->info.bytes;
      }
      
      for(int32_t j = 0; j < numOfTableCols; ++j) {
        if (pColInfo->info.colId == pSchema->columns[j].colId) {
          offset = pSchema->columns[j].offset;
          break;
        }
      }
      
      assert(offset != -1); // todo handle error
      
      if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
        void *value = tdGetRowDataOfCol(row, pColInfo->info.type, TD_DATA_ROW_HEAD_SIZE + offset);
        memcpy(pData, value, varDataTLen(value));
  
        offset += sizeof(int32_t);
      } else {
        memcpy(pData, dataRowTuple(row) + offset, pColInfo->info.bytes);
        offset += pColInfo->info.bytes;
      }
      
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
SDataBlockInfo tsdbRetrieveDataBlockInfo(TsdbQueryHandleT* pQueryHandle) {
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
      rows = tsdbReadRowsFromCache(pCheckInfo->iter, pCheckInfo->pTableObj, pHandle->window.ekey, 4000, &skey, &ekey, pHandle);

      // update the last key value
      pCheckInfo->lastKey = ekey + step;
    }
  }

  SDataBlockInfo blockInfo = {
      .uid = pTable->tableId.uid,
      .tid = pTable->tableId.tid,
      .rows = rows,
      .window = {.skey = MIN(skey, ekey), .ekey = MAX(skey, ekey)}
  };

  return blockInfo;
}

// return null for data block in cache
int32_t tsdbRetrieveDataBlockStatisInfo(TsdbQueryHandleT* pQueryHandle, SDataStatis** pBlockStatis) {
  *pBlockStatis = NULL;
  return TSDB_CODE_SUCCESS;
}

SArray* tsdbRetrieveDataBlock(TsdbQueryHandleT* pQueryHandle, SArray* pIdList) {
  /**
   * In the following two cases, the data has been loaded to SColumnInfoData.
   * 1. data is from cache, 2. data block is not completed qualified to query time range
   */
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;

  if (pHandle->cur.fid < 0) {
    return pHandle->pColumns;
  } else {
    STableBlockInfo* pBlockInfoEx = &pHandle->pDataBlockInfo[pHandle->cur.slot];
    STableCheckInfo* pCheckInfo = pBlockInfoEx->pTableCheckInfo;

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

SArray* tsdbRetrieveDataRow(TsdbQueryHandleT* pQueryHandle, SArray* pIdList, SQueryRowCond* pCond) { return NULL; }

TsdbQueryHandleT* tsdbQueryFromTagConds(STsdbQueryCond* pCond, int16_t stableId, const char* pTagFilterStr) {
  return NULL;
}

SArray* tsdbGetTableList(TsdbQueryHandleT* pQueryHandle) { return NULL; }

static int32_t getAllTableIdList(STable* pSuperTable, SArray* list) {
  SSkipListIterator* iter = tSkipListCreateIter(pSuperTable->pIndex);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* pNode = tSkipListIterGet(iter);
    
    STable* t = *(STable**)SL_GET_NODE_DATA(pNode);
    taosArrayPush(list, &t->tableId);
  }
  
  tSkipListDestroyIter(iter);
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
  if (pInfo->optr != TSDB_RELATION_IN) {
    tfree(pInfo->q);
  }
  
//  tVariantDestroy(&(pInfo->q));
  free(param);
}

static int32_t getTagColumnInfo(SExprTreeSupporter* pSupporter, SSchema* pSchema) {
  // filter on table name(TBNAME)
  if (strcasecmp(pSchema->name, TSQL_TBNAME_L) == 0) {
    return TSDB_TBNAME_COLUMN_INDEX;
  }

  for(int32_t i = 0; i < pSupporter->numOfTags; ++i) {
    if (pSupporter->pTagSchema[i].bytes == pSchema->bytes &&
        pSupporter->pTagSchema[i].type  == pSchema->type  &&
        pSupporter->pTagSchema[i].colId == pSchema->colId) {
      return i;
    }
  }
  
  return -2;
}

void filterPrepare(void* expr, void* param) {
  tExprNode* pExpr = (tExprNode*)expr;
  if (pExpr->_node.info != NULL) {
    return;
  }

  int32_t i = 0;
  pExpr->_node.info = calloc(1, sizeof(tQueryInfo));

  SExprTreeSupporter* pSupporter = (SExprTreeSupporter*)param;

  tQueryInfo* pInfo = pExpr->_node.info;
  tVariant*   pCond = pExpr->_node.pRight->pVal;
  SSchema*    pSchema = pExpr->_node.pLeft->pSchema;

  // todo : if current super table does not change schema yet, this function may failed, add test case
  int32_t index = getTagColumnInfo(pSupporter, pSchema);
  assert((index >= 0 && i < TSDB_MAX_TAGS) || (index == TSDB_TBNAME_COLUMN_INDEX));

  pInfo->sch      = *pSchema;
  pInfo->colIndex = index;
  pInfo->optr     = pExpr->_node.optr;
  pInfo->compare  = getComparFunc(pSchema->type, pInfo->optr);
  
  if (pInfo->optr == TSDB_RELATION_IN) {
    pInfo->q = (char*) pCond->arr;
  } else {
    pInfo->q = calloc(1, pSchema->bytes);
    tVariantDump(pCond, pInfo->q, pSchema->type);
  }
}

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  STSchema*  pTagSchema;
  void*      tsdbMeta;
} STableGroupSupporter;

int32_t tableGroupComparFn(const void *p1, const void *p2, const void *param) {
  STableGroupSupporter* pTableGroupSupp = (STableGroupSupporter*) param;
  STableId* id1 = (STableId*) p1;
  STableId* id2 = (STableId*) p2;
  
  STable *pTable1 = tsdbGetTableByUid(pTableGroupSupp->tsdbMeta, id1->uid);
  STable *pTable2 = tsdbGetTableByUid(pTableGroupSupp->tsdbMeta, id2->uid);
  
  for (int32_t i = 0; i < pTableGroupSupp->numOfCols; ++i) {
    SColIndex* pColIndex = &pTableGroupSupp->pCols[i];
    int32_t colIndex = pColIndex->colIndex;
    
    assert((colIndex >= 0 && colIndex < schemaNCols(pTableGroupSupp->pTagSchema)) ||
           (colIndex == TSDB_TBNAME_COLUMN_INDEX));
    
    char *  f1 = NULL;
    char *  f2 = NULL;
    int32_t type = 0;
    int32_t bytes = 0;
    
    if (colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      f1 = pTable1->name;
      f2 = pTable2->name;
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

void createTableGroupImpl(SArray* pGroups, SArray* pTableIdList, size_t numOfTables, STableGroupSupporter* pSupp,
    __ext_compar_fn_t compareFn) {
  STableId* pId = taosArrayGet(pTableIdList, 0);
  
  SArray* g = taosArrayInit(16, sizeof(STableId));
  taosArrayPush(g, pId);
  
  for (int32_t i = 1; i < numOfTables; ++i) {
    STableId* prev = taosArrayGet(pTableIdList, i - 1);
    STableId* p = taosArrayGet(pTableIdList, i);
    
    int32_t ret = compareFn(prev, p, pSupp);
    assert(ret == 0 || ret == -1);
    
    if (ret == 0) {
      taosArrayPush(g, p);
    } else {
      taosArrayPush(pGroups, &g);  // current group is ended, start a new group
      g = taosArrayInit(16, sizeof(STableId));
      
      taosArrayPush(g, p);
    }
  }
  
  taosArrayPush(pGroups, &g);
}

SArray* createTableGroup(SArray* pTableList, STSchema* pTagSchema, SColIndex* pCols, int32_t numOfOrderCols,
    TsdbRepoT* tsdb) {
  assert(pTableList != NULL);
  SArray* pTableGroup = taosArrayInit(1, POINTER_BYTES);
  
  size_t size = taosArrayGetSize(pTableList);
  if (size == 0) {
    uTrace("no qualified tables");
    return pTableGroup;
  }
  
  if (numOfOrderCols == 0 || size == 1) { // no group by tags clause or only one table
    SArray* sa = taosArrayInit(size, sizeof(STableId));
    for(int32_t i = 0; i < size; ++i) {
      STableId* tableId = taosArrayGet(pTableList, i);
      taosArrayPush(sa, tableId);
    }
    
    taosArrayPush(pTableGroup, &sa);
    uTrace("all %d tables belong to one group", size);
  } else {
    STableGroupSupporter *pSupp = (STableGroupSupporter *) calloc(1, sizeof(STableGroupSupporter));
    pSupp->tsdbMeta = tsdbGetMeta(tsdb);
    pSupp->numOfCols = numOfOrderCols;
    pSupp->pTagSchema = pTagSchema;
    pSupp->pCols = pCols;
    
    taosqsort(pTableList->pData, size, sizeof(STableId), pSupp, tableGroupComparFn);
    createTableGroupImpl(pTableGroup, pTableList, size, pSupp, tableGroupComparFn);
    tfree(pSupp);
  }
  
  return pTableGroup;
}

bool tSkipListNodeFilterCallback(const void* pNode, void* param) {
  tQueryInfo* pInfo = (tQueryInfo*)param;

  STable* pTable = *(STable**)(SL_GET_NODE_DATA((SSkipListNode*)pNode));

  char*  val = NULL;
  int8_t type = pInfo->sch.type;

  if (pInfo->colIndex == TSDB_TBNAME_COLUMN_INDEX) {
    val = pTable->name;
    type = TSDB_DATA_TYPE_BINARY;
  } else {
    val = dataRowTuple(pTable->tagVal);  // todo not only the first column
  }

  int32_t ret = 0;
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    if (pInfo->optr == TSDB_RELATION_IN) {
      ret = pInfo->compare(val, pInfo->q);
    } else {
      ret = pInfo->compare(val, pInfo->q);
    }
  } else {
//    tVariant t = {0};
//    tVariantCreateFromBinary(&t, val, (uint32_t)pInfo->sch.bytes, type);
    ret = pInfo->compare(val, pInfo->q);
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
    case TSDB_RELATION_IN: {
      return ret == 1;
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
    schema[i].type  = schemaColAt(pSchema, i)->type;
    schema[i].bytes = schemaColAt(pSchema, i)->bytes;
  }

  SExprTreeSupporter s = {.pTagSchema = schema, .numOfTags = schemaNCols(pSTable->tagSchema)};

  SBinaryFilterSupp supp = {
      .fp = (__result_filter_fn_t)tSkipListNodeFilterCallback, .setupInfoFn = filterPrepare, .pExtInfo = &s,
      };

  SArray* pTableList = taosArrayInit(8, POINTER_BYTES);

  tExprTreeTraverse(pExpr, pSTable->pIndex, pTableList, &supp);
  tExprTreeDestroy(&pExpr, destroyHelper);

  convertQueryResult(pRes, pTableList);
  taosArrayDestroy(pTableList);
  free(schema);
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbQuerySTableByTagCond(TsdbRepoT *tsdb, int64_t uid, const char *pTagCond, size_t len, int16_t tagNameRelType,
  const char* tbnameCond, STableGroupInfo *pGroupInfo, SColIndex *pColIndex, int32_t numOfCols) {
  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pTable == NULL) {
    uError("failed to get stable, uid:%, %p" PRIu64, uid);
    return TSDB_CODE_INVALID_TABLE_ID;
  }
  
  SArray* res = taosArrayInit(8, sizeof(STableId));
  STSchema* pTagSchema = tsdbGetTableTagSchema(tsdbGetMeta(tsdb), pTable);
  
  // no tags and tbname condition, all child tables of this stable are involved
  if (tbnameCond == NULL && (pTagCond == NULL || len == 0)) {
    int32_t ret = getAllTableIdList(pTable, res);
    if (ret == TSDB_CODE_SUCCESS) {
      pGroupInfo->numOfTables = taosArrayGetSize(res);
      pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, tsdb);
      
      uTrace("no tbname condition or tagcond, all tables belongs to one group, numOfTables:%d", pGroupInfo->numOfTables);
    } else {
      // todo add error
    }
    
    taosArrayDestroy(res);
    return ret;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  tExprNode* expr = NULL;

  TRY(32) {
    expr = exprTreeFromTableName(tbnameCond);
    if (expr == NULL) {
      expr = exprTreeFromBinary(pTagCond, len);
    } else {
      CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, expr, NULL);
      tExprNode* tagExpr = exprTreeFromBinary(pTagCond, len);
      if (tagExpr != NULL) {
        CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, tagExpr, NULL);
        tExprNode* tbnameExpr = expr;
        expr = calloc(1, sizeof(tExprNode));
        if (expr == NULL) {
          THROW( TSDB_CODE_SERV_OUT_OF_MEMORY );
        }
        expr->nodeType = TSQL_NODE_EXPR;
        expr->_node.optr = tagNameRelType;
        expr->_node.pLeft = tagExpr;
        expr->_node.pRight = tbnameExpr;
      }
    }
    CLEANUP_EXECUTE();

  } CATCH( code ) {
    CLEANUP_EXECUTE();
    ret = code;
    // TODO: more error handling
  } END_TRY

  doQueryTableList(pTable, res, expr);
  pGroupInfo->numOfTables = taosArrayGetSize(res);
  pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, tsdb);

  taosArrayDestroy(res);
  return ret;
}

int32_t tsdbGetOneTableGroup(TsdbRepoT* tsdb, int64_t uid, STableGroupInfo* pGroupInfo) {
  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pTable == NULL) {
    return TSDB_CODE_INVALID_TABLE_ID;
  }
  
  //todo assert table type, add the table ref count
  pGroupInfo->numOfTables = 1;
  pGroupInfo->pGroupList = taosArrayInit(1, POINTER_BYTES);
  
  SArray* group = taosArrayInit(1, sizeof(STableId));
  
  taosArrayPush(group, &pTable->tableId);
  taosArrayPush(pGroupInfo->pGroupList, &group);
  
  return TSDB_CODE_SUCCESS;
}

void tsdbCleanupQueryHandle(TsdbQueryHandleT queryHandle) {
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

   size_t cols = taosArrayGetSize(pQueryHandle->pColumns);
   for (int32_t i = 0; i < cols; ++i) {
     SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      tfree(pColInfo->pData);
   }

  taosArrayDestroy(pQueryHandle->pColumns);
  
  tfree(pQueryHandle->pDataBlockInfo);
  tsdbDestroyHelper(&pQueryHandle->rhelper);
  
  tfree(pQueryHandle);
}

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

#include <tlog.h>
#include "os.h"
#include "tutil.h"

#include "../../../query/inc/qast.h"
#include "../../../query/inc/tsqlfunction.h"
#include "tsdb.h"
#include "tsdbFile.h"
#include "tsdbMeta.h"

#define EXTRA_BYTES 2
#define PRIMARY_TSCOL_REQUIRED(c) (((SColumnInfoEx *)taosArrayGet(c, 0))->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)
#define QUERY_IS_ASC_QUERY(o) (o == TSQL_SO_ASC)
#define QH_GET_NUM_OF_COLS(handle) (taosArrayGetSize((handle)->pColumns))

typedef struct SField {
  // todo need the definition
} SField;

typedef struct SHeaderFileInfo {
  int32_t fileId;
} SHeaderFileInfo;

typedef struct SQueryHandlePos {
  int32_t fileId;
  int32_t slot;
  int32_t pos;
  int32_t fileIndex;
} SQueryHandlePos;

typedef struct SDataBlockLoadInfo {
  int32_t fileListIndex;
  int32_t fileId;
  int32_t slotIdx;
  int32_t sid;
  SArray *pLoadedCols;
} SDataBlockLoadInfo;

typedef struct SLoadCompBlockInfo {
  int32_t sid; /* meter sid */
  int32_t fileId;
  int32_t fileListIndex;
} SLoadCompBlockInfo;

typedef struct SQueryFilesInfo {
  SArray *pFileInfo;
  int32_t current;  // the memory mapped header file, NOTE: only one header file can be mmap.
  int32_t vnodeId;

  int32_t headerFd;  // header file fd
  int64_t headerFileSize;
  int32_t dataFd;
  int32_t lastFd;

  char headerFilePath[PATH_MAX];  // current opened header file name
  char dataFilePath[PATH_MAX];    // current opened data file name
  char lastFilePath[PATH_MAX];    // current opened last file path
  char dbFilePathPrefix[PATH_MAX];
} SQueryFilesInfo;

typedef struct STableQueryRec {
  TSKEY       lastKey;
  STable *    pTableObj;
  int64_t     offsetInHeaderFile;
  int32_t     numOfBlocks;
  int32_t     start;
  SCompBlock *pBlock;
} STableQueryRec;

typedef struct {
  SCompBlock *compBlock;
  SField *    fields;
} SCompBlockFields;

typedef struct STableDataBlockInfoEx {
  SCompBlockFields pBlock;
  STableQueryRec * pMeterDataInfo;
  int32_t          blockIndex;
  int32_t          groupIdx; /* number of group is less than the total number of meters */
} STableDataBlockInfoEx;

typedef struct STsdbQueryHandle {
  struct STsdbRepo* pTsdb;
  
  SQueryHandlePos cur;    // current position
  SQueryHandlePos start;  // the start position, used for secondary/third iteration
  int32_t         unzipBufSize;
  char *unzipBuffer;
  char *secondaryUnzipBuffer;

  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  SQueryFilesInfo vnodeFileInfo;

  int16_t     numOfRowsPerPage;
  uint16_t    flag;  // denotes reversed scan of data or not
  int16_t     order;
  STimeWindow window;  // the primary query time window that applies to all queries
  int32_t     blockBufferSize;
  SCompBlock *pBlock;
  int32_t     numOfBlocks;
  SField **   pFields;
  SArray *    pColumns;         // column list, SColumnInfoEx array list
  SArray *    pTableIdList;     // table id object list
  bool        locateStart;
  int32_t     realNumOfRows;
  bool        loadDataAfterSeek;  // load data after seek.

  STableDataBlockInfoEx *pDataBlockInfoEx;
  STableQueryRec *       pTableQueryInfo;
  int32_t                tableIndex;
  bool                   isFirstSlot;
  void *                 qinfo;  // query info handle, for debug purpose
  SSkipListIterator*     memIter;
} STsdbQueryHandle;

int32_t doAllocateBuf(STsdbQueryHandle *pQueryHandle, int32_t rowsPerFileBlock) {
  // record the maximum column width among columns of this meter/metric
  SColumnInfoEx *pColumn = taosArrayGet(pQueryHandle->pColumns, 0);

  int32_t maxColWidth = pColumn->info.bytes;
  for (int32_t i = 1; i < QH_GET_NUM_OF_COLS(pQueryHandle); ++i) {
    int32_t bytes = pColumn[i].info.bytes;
    if (bytes > maxColWidth) {
      maxColWidth = bytes;
    }
  }

  // only one unzip buffer required, since we can unzip each column one by one
  pQueryHandle->unzipBufSize = (size_t)(maxColWidth * rowsPerFileBlock + EXTRA_BYTES);  // plus extra_bytes
  pQueryHandle->unzipBuffer = (char *)calloc(1, pQueryHandle->unzipBufSize);

  pQueryHandle->secondaryUnzipBuffer = (char *)calloc(1, pQueryHandle->unzipBufSize);

  if (pQueryHandle->unzipBuffer == NULL || pQueryHandle->secondaryUnzipBuffer == NULL) {
    goto _error_clean;
  }

  return TSDB_CODE_SUCCESS;

_error_clean:
  tfree(pQueryHandle->unzipBuffer);
  tfree(pQueryHandle->secondaryUnzipBuffer);

  return TSDB_CODE_SERV_OUT_OF_MEMORY;
}

static void initQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo) {
  pVnodeFilesInfo->current = -1;
  pVnodeFilesInfo->headerFileSize = -1;
  
  pVnodeFilesInfo->headerFd = FD_INITIALIZER;  // set the initial value
  pVnodeFilesInfo->dataFd = FD_INITIALIZER;
  pVnodeFilesInfo->lastFd = FD_INITIALIZER;
}

static void vnodeInitDataBlockLoadInfo(SDataBlockLoadInfo *pBlockLoadInfo) {
  pBlockLoadInfo->slotIdx = -1;
  pBlockLoadInfo->fileId = -1;
  pBlockLoadInfo->sid = -1;
  pBlockLoadInfo->fileListIndex = -1;
}

static void vnodeInitCompBlockLoadInfo(SLoadCompBlockInfo *pCompBlockLoadInfo) {
  pCompBlockLoadInfo->sid = -1;
  pCompBlockLoadInfo->fileId = -1;
  pCompBlockLoadInfo->fileListIndex = -1;
}

static int fileOrderComparFn(const void *p1, const void *p2) {
  SHeaderFileInfo *pInfo1 = (SHeaderFileInfo *)p1;
  SHeaderFileInfo *pInfo2 = (SHeaderFileInfo *)p2;
  
  if (pInfo1->fileId == pInfo2->fileId) {
    return 0;
  }
  
  return (pInfo1->fileId > pInfo2->fileId) ? 1 : -1;
}

void vnodeRecordAllFiles(int32_t vnodeId, SQueryFilesInfo *pVnodeFilesInfo) {
  char suffix[] = ".head";
  pVnodeFilesInfo->pFileInfo = taosArrayInit(4, sizeof(int32_t));
  
  struct dirent *pEntry = NULL;
  pVnodeFilesInfo->vnodeId = vnodeId;
  char* tsDirectory = "";
  
  sprintf(pVnodeFilesInfo->dbFilePathPrefix, "%s/vnode%d/db/", tsDirectory, vnodeId);
  DIR *pDir = opendir(pVnodeFilesInfo->dbFilePathPrefix);
  if (pDir == NULL) {
    //    dError("QInfo:%p failed to open directory:%s, %s", pQInfo, pVnodeFilesInfo->dbFilePathPrefix,
    //    strerror(errno));
    return;
  }
  
  while ((pEntry = readdir(pDir)) != NULL) {
    if ((pEntry->d_name[0] == '.' && pEntry->d_name[1] == '\0') || (strcmp(pEntry->d_name, "..") == 0)) {
      continue;
    }
    
    if (pEntry->d_type & DT_DIR) {
      continue;
    }
    
    size_t len = strlen(pEntry->d_name);
    if (strcasecmp(&pEntry->d_name[len - 5], suffix) != 0) {
      continue;
    }
    
    int32_t vid = 0;
    int32_t fid = 0;
    sscanf(pEntry->d_name, "v%df%d", &vid, &fid);
    if (vid != vnodeId) { /* ignore error files */
      //      dError("QInfo:%p error data file:%s in vid:%d, ignore", pQInfo, pEntry->d_name, vnodeId);
      continue;
    }
    
//    int32_t firstFid = pVnode->fileId - pVnode->numOfFiles + 1;
//    if (fid > pVnode->fileId || fid < firstFid) {
//           dError("QInfo:%p error data file:%s in vid:%d, fid:%d, fid range:%d-%d", pQInfo, pEntry->d_name, vnodeId,
//           fid, firstFid, pVnode->fileId);
//      continue;
//    }
    
    assert(fid >= 0 && vid >= 0);
    taosArrayPush(pVnodeFilesInfo->pFileInfo, &fid);
  }
  
  closedir(pDir);
  
  //  dTrace("QInfo:%p find %d data files in %s to be checked", pQInfo, pVnodeFilesInfo->numOfFiles,
  //         pVnodeFilesInfo->dbFilePathPrefix);
  
  // order the files information according their names */
  size_t numOfFiles = taosArrayGetSize(pVnodeFilesInfo->pFileInfo);
  qsort(pVnodeFilesInfo->pFileInfo->pData, numOfFiles, sizeof(SHeaderFileInfo), fileOrderComparFn);
}

tsdb_query_handle_t *tsdbQueryByTableId(tsdb_repo_t* tsdb, STsdbQueryCond *pCond, SArray *idList, SArray *pColumnInfo) {
  // todo 1. filter not exist table

  // todo 2. add the reference count for each table that is involved in query

  STsdbQueryHandle *pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  pQueryHandle->order = pCond->order;
  pQueryHandle->window = pCond->twindow;
  pQueryHandle->pTsdb = tsdb;

  pQueryHandle->pTableIdList = idList;
  pQueryHandle->pColumns = pColumnInfo;
  pQueryHandle->loadDataAfterSeek = false;
  pQueryHandle->isFirstSlot = true;
  
  // only support table query
  assert(taosArrayGetSize(idList) == 1);
  
  pQueryHandle->pTableQueryInfo = calloc(1, sizeof(STableQueryRec));
  STableQueryRec* pTableQRec = pQueryHandle->pTableQueryInfo;
  
  pTableQRec->lastKey = pQueryHandle->window.skey;
  
  STableIdInfo* idInfo = taosArrayGet(pQueryHandle->pTableIdList, 0);
  
  STable *pTable = tsdbGetTableByUid(tsdbGetMeta(pQueryHandle->pTsdb), idInfo->uid);
  assert(pTable != NULL);
  
  pTableQRec->pTableObj = pTable;
  
  // malloc buffer in order to load data from file
  int32_t numOfCols = taosArrayGetSize(pColumnInfo);
  size_t  bufferCapacity = 4096;
  
  pQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoEx));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoEx *pCol = taosArrayGet(pColumnInfo, i);
    SColumnInfoEx  pDest = {{0}, 0};

    pDest.pData = calloc(1, EXTRA_BYTES + bufferCapacity * pCol->info.bytes);
    pDest.info = pCol->info;
    taosArrayPush(pQueryHandle->pColumns, &pDest);
  }

  if (doAllocateBuf(pQueryHandle, bufferCapacity) != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  initQueryFileInfoFD(&pQueryHandle->vnodeFileInfo);
  vnodeInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  vnodeInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  int32_t vnodeId = 1;
  vnodeRecordAllFiles(vnodeId, &pQueryHandle->vnodeFileInfo);

  return (tsdb_query_handle_t)pQueryHandle;
}

bool tsdbNextDataBlock(tsdb_query_handle_t *pQueryHandle) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*) pQueryHandle;
  STable *pTable = pHandle->pTableQueryInfo->pTableObj;
  
  // no data in cache, abort
  if (pTable->mem == NULL && pTable->imem == NULL) {
    return false;
  }
  
  // all data in mem are checked already.
  if (pHandle->pTableQueryInfo->lastKey > pTable->mem->keyLast) {
    return false;
  }
  
  return true;
}

static int tsdbReadRowsFromCache(SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead,
    TSKEY* skey, TSKEY* ekey, STsdbQueryHandle* pHandle) {
  int numOfRows = 0;
  int32_t numOfCols = taosArrayGetSize(pHandle->pColumns);
  *skey = INT64_MIN;
  
  while(tSkipListIterNext(pIter)) {
    SSkipListNode *node = tSkipListIterGet(pIter);
    if (node == NULL) break;
    
    SDataRow row = SL_GET_NODE_DATA(node);
    if (dataRowKey(row) > maxKey) break;
    
    if (*skey == INT64_MIN) {
      *skey = dataRowKey(row);
    }
    
    *ekey = dataRowKey(row);
    
    int32_t offset = 0;
    for(int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoEx* pColInfo = taosArrayGet(pHandle->pColumns, i);
      memcpy(pColInfo->pData + numOfRows*pColInfo->info.bytes, dataRowTuple(row) + offset, pColInfo->info.bytes);
      offset += pColInfo->info.bytes;
    }
    
    numOfRows++;
    if (numOfRows >= maxRowsToRead) break;
  };
  
  return numOfRows;
}

// copy data from cache into data block
SDataBlockInfo tsdbRetrieveDataBlockInfo(tsdb_query_handle_t *pQueryHandle) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*) pQueryHandle;
  STableIdInfo* idInfo = taosArrayGet(pHandle->pTableIdList, 0);
  
  STable *pTable = pHandle->pTableQueryInfo->pTableObj;
  
  TSKEY skey = 0, ekey = 0;
  int32_t rows = 0;
  
  if (pTable->mem != NULL) {
    
    // create mem table iterator if it is not created yet
    if (pHandle->memIter == NULL) {
      pHandle->memIter = tSkipListCreateIter(pTable->mem->pData);
    }
    
    rows = tsdbReadRowsFromCache(pHandle->memIter, INT64_MAX, 2, &skey, &ekey, pHandle);
  }
  
  SDataBlockInfo blockInfo = {
      .uid = idInfo->uid,
      .sid = idInfo->sid,
      .size = rows,
      .window = {.skey = skey, .ekey = ekey}
  };
  
  // update the last key value
  pHandle->pTableQueryInfo->lastKey = ekey + 1;
  
  return blockInfo;
}

// return null for data block in cache
int32_t tsdbRetrieveDataBlockStatisInfo(tsdb_query_handle_t *pQueryHandle, SDataStatis **pBlockStatis) {
  *pBlockStatis = NULL;
  return TSDB_CODE_SUCCESS;
}

SArray *tsdbRetrieveDataBlock(tsdb_query_handle_t *pQueryHandle, SArray *pIdList) {
  // in case of data in cache, all data has been kept in column info object.
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*) pQueryHandle;
  return pHandle->pColumns;
}

int32_t tsdbResetQuery(tsdb_query_handle_t *pQueryHandle, STimeWindow *window, tsdbpos_t position, int16_t order) {}

int32_t tsdbDataBlockSeek(tsdb_query_handle_t *pQueryHandle, tsdbpos_t pos) {}

tsdbpos_t tsdbDataBlockTell(tsdb_query_handle_t *pQueryHandle) { return NULL; }

SArray *tsdbRetrieveDataRow(tsdb_query_handle_t *pQueryHandle, SArray *pIdList, SQueryRowCond *pCond) {}

tsdb_query_handle_t *tsdbQueryFromTagConds(STsdbQueryCond *pCond, int16_t stableId, const char *pTagFilterStr) {}

SArray *tsdbGetTableList(tsdb_query_handle_t *pQueryHandle) {}

static SArray* createTableIdArrayList(struct STsdbRepo* tsdb, int64_t uid) {
  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  assert(pTable != NULL);  //assert pTable is a super table
  
  size_t size = tSkipListGetSize(pTable->pIndex);
  SArray* pList = taosArrayInit(size, sizeof(STableId));
  
  SSkipListIterator* iter = tSkipListCreateIter(pTable->pIndex);
  while(tSkipListIterNext(iter)) {
    STable* t = *(STable**) tSkipListIterGet(iter);
    taosArrayPush(pList, &t->tableId);
  }
  
  return pList;
}

typedef struct SSyntaxTreeFilterSupporter {
  SSchema* pTagSchema;
  int32_t  numOfTags;
  int32_t  optr;
} SSyntaxTreeFilterSupporter;

/**
 * convert the result pointer to STabObj instead of tSkipListNode
 * @param pRes
 */
static void tansformQueryResult(SArray* pRes) {
  if (pRes == NULL || taosArrayGetSize(pRes) == 0) {
    return;
  }
  
  size_t size = taosArrayGetSize(pRes);
  for (int32_t i = 0; i < size; ++i) {
//    pRes->pRes[i] = ((tSkipListNode*)(pRes->pRes[i]))->pData;
  }
}

void tSQLListTraverseDestroyInfo(void* param) {
  if (param == NULL) {
    return;
  }
  
  tQueryInfo* pInfo = (tQueryInfo*)param;
  tVariantDestroy(&(pInfo->q));
  free(param);
}

static char* convertTagQueryStr(const wchar_t* str, size_t len) {
  char* mbs = NULL;
  
  if (len > 0) {
    mbs = calloc(1, (len + 1) * TSDB_NCHAR_SIZE);
    taosUcs4ToMbs((void*) str, len * TSDB_NCHAR_SIZE, mbs); //todo add log
  }
  
  return mbs;
}

static int32_t compareStrVal(const void* pLeft, const void* pRight) {
  int32_t ret = strcmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareWStrVal(const void* pLeft, const void* pRight) {
  int32_t ret = wcscmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareIntVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_INT64_VAL(pRight));
}

static int32_t compareIntDoubleVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleVal(const void* pLeft, const void* pRight) {
  DEFAULT_COMP(GET_DOUBLE_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleIntVal(const void* pLeft, const void* pRight) {
  double ret = (*(double*)pLeft) - (*(int64_t*)pRight);
  if (fabs(ret) < DBL_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  const char* pattern = pRight;
  const char* str = pLeft;
  
  int32_t ret = patternMatch(pattern, str, strlen(str), &pInfo);
  
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

static int32_t compareWStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  const wchar_t* pattern = pRight;
  const wchar_t* str = pLeft;
  
  int32_t ret = WCSPatternMatch(pattern, str, wcslen(str), &pInfo);
  
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

static __compar_fn_t getFilterComparator(int32_t type, int32_t filterType, int32_t optr) {
  __compar_fn_t comparator = NULL;
  
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareIntDoubleVal;
      }
      break;
    }
    
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareDoubleIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareDoubleVal;
      }
      break;
    }
    
    case TSDB_DATA_TYPE_BINARY: {
      assert(filterType == TSDB_DATA_TYPE_BINARY);
      
      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparator = compareStrPatternComp;
      } else { /* normal relational comparator */
        comparator = compareStrVal;
      }
      
      break;
    }
    
    case TSDB_DATA_TYPE_NCHAR: {
      assert(filterType == TSDB_DATA_TYPE_NCHAR);
      
      if (optr == TSDB_RELATION_LIKE) {
        comparator = compareWStrPatternComp;
      } else {
        comparator = compareWStrVal;
      }
      
      break;
    }
    default:
      comparator = compareIntVal;
      break;
  }
  
  return comparator;
}

static void getTagColumnInfo(SSyntaxTreeFilterSupporter* pSupporter, SSchema* pSchema, int32_t* index,
                             int32_t* offset) {
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
        strcmp(pSupporter->pTagSchema[*index].name, pSchema->name) == 0) {
      break;
    } else {
      (*offset) += pSupporter->pTagSchema[(*index)++].bytes;
    }
  }
}

void filterPrepare(void* expr, void* param) {
  tSQLBinaryExpr *pExpr = (tSQLBinaryExpr*) expr;
  if (pExpr->info != NULL) {
    return;
  }
  
  int32_t i = 0, offset = 0;
  pExpr->info = calloc(1, sizeof(tQueryInfo));
  
  tQueryInfo*                 pInfo = pExpr->info;
  SSyntaxTreeFilterSupporter* pSupporter = (SSyntaxTreeFilterSupporter*)param;
  
  tVariant* pCond = pExpr->pRight->pVal;
  SSchema*  pSchema = pExpr->pLeft->pSchema;
  
  getTagColumnInfo(pSupporter, pSchema, &i, &offset);
  assert((i >= 0 && i < TSDB_MAX_TAGS) || (i == TSDB_TBNAME_COLUMN_INDEX));
  assert((offset >= 0 && offset < TSDB_MAX_TAGS_LEN) || (offset == TSDB_TBNAME_COLUMN_INDEX));
  
  pInfo->sch = *pSchema;
  pInfo->colIdx = i;
  pInfo->optr = pExpr->nSQLBinaryOptr;
  pInfo->offset = offset;
  pInfo->compare = getFilterComparator(pSchema->type, pCond->nType, pInfo->optr);
  
  tVariantAssign(&pInfo->q, pCond);
  tVariantTypeSetType(&pInfo->q, pInfo->sch.type);
}

bool tSkipListNodeFilterCallback(const void* pNode, void* param) {
  tQueryInfo* pInfo = (tQueryInfo*)param;
  
  STable* pTable = (STable*)(SL_GET_NODE_DATA((SSkipListNode*)pNode));
  
  char   buf[TSDB_MAX_TAGS_LEN] = {0};
  
  char* val = dataRowTuple(pTable->tagVal);  // todo not only the first column
  int8_t type = pInfo->sch.type;
  
  int32_t ret = 0;
  if (pInfo->q.nType == TSDB_DATA_TYPE_BINARY || pInfo->q.nType == TSDB_DATA_TYPE_NCHAR) {
    ret = pInfo->compare(val, pInfo->q.pz);
  } else {
    tVariant t = {0};
    tVariantCreateFromBinary(&t, val, (uint32_t) pInfo->sch.bytes, type);
    
    ret = pInfo->compare(&t.i64Key, &pInfo->q.i64Key);
  }
  
  switch (pInfo->optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_LARGE_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_LARGE: {
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

static int32_t mgmtFilterMeterByIndex(STable* pSTable, SArray* pRes, const char* pCond) {
  STColumn* stcol = schemaColAt(pSTable->tagSchema, 0);
  
  tSQLBinaryExpr* pExpr = NULL;
  tSQLBinaryExprFromString(&pExpr, stcol, schemaNCols(pSTable->tagSchema), pCond, strlen(pCond));
  
  // failed to build expression, no result, return immediately
  if (pExpr == NULL) {
    mError("table:%" PRIu64 ", no result returned, error in super table query expression:%s", pSTable->tableId.uid, pCond);
    tfree(pCond);
    
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }
  
  // query according to the binary expression
  SSyntaxTreeFilterSupporter s = {.pTagSchema = stcol, .numOfTags = schemaNCols(pSTable->tagSchema)};
  
  SBinaryFilterSupp supp = {.fp = (__result_filter_fn_t)tSkipListNodeFilterCallback,
      .setupInfoFn = (__do_filter_suppl_fn_t)filterPrepare,
      .pExtInfo = &s};
  
  tSQLBinaryExprTraverse(pExpr, pSTable->pIndex, pRes, &supp);
  tSQLBinaryExprDestroy(&pExpr, tSQLListTraverseDestroyInfo);
  
  tansformQueryResult(pRes);
  
  return TSDB_CODE_SUCCESS;
}

SArray *tsdbQueryTableList(struct STsdbRepo* tsdb, int64_t uid, const wchar_t *pTagCond, size_t len) {
  // no condition, all tables created according to the stable will involved in querying
  if (pTagCond == NULL || wcslen(pTagCond) == 0) {
    return createTableIdArrayList(tsdb, uid);
  } else {
    char* str = convertTagQueryStr(pTagCond, len);
    SArray* result = taosArrayInit(8, POINTER_BYTES);
    
    STable* pSTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
    assert(pSTable != NULL);
    
    if (mgmtFilterMeterByIndex(pSTable, result, str) == TSDB_CODE_SUCCESS) {
      return result;
    }
  }
}

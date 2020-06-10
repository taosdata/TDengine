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

#ifndef TDENGINE_VNODEREAD_H
#define TDENGINE_VNODEREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tresultBuf.h"

#include "tinterpolation.h"
#include "vnodeTagMgmt.h"

/*
 * use to keep the first point position, consisting of position in blk and block
 * id, file id
 */
typedef struct {
  int32_t pos;
  int32_t slot;
  int32_t fileId;
} SPositionInfo;

typedef struct SLoadDataBlockInfo {
  int32_t fileListIndex; /* index of this file in files list of this vnode */
  int32_t fileId;
  int32_t slotIdx;
  int32_t sid;
  bool    tsLoaded;      // if timestamp column of current block is loaded or not
} SLoadDataBlockInfo;

typedef struct SLoadCompBlockInfo {
  int32_t sid; /* meter sid */
  int32_t fileId;
  int32_t fileListIndex;
} SLoadCompBlockInfo;

/*
 * the header file info for one vnode
 */
typedef struct SHeaderFileInfo {
  int32_t fileID;  // file id
} SHeaderFileInfo;

typedef struct SQueryCostSummary {
  double cacheTimeUs;
  double fileTimeUs;

  int64_t numOfFiles;   // opened files during query
  int64_t numOfTables;  // num of queries tables
  int64_t numOfSeek;    // number of seek operation

  int64_t readDiskBlocks;     // accessed disk block
  int64_t skippedFileBlocks;  // skipped blocks
  int64_t blocksInCache;      // accessed cache blocks

  int64_t readField;       // field size
  int64_t totalFieldSize;  // total read fields size
  double  loadFieldUs;     // total elapsed time to read fields info

  int64_t totalBlockSize;  // read data blocks
  double  loadBlocksUs;    // total elapsed time to read data blocks

  int64_t totalGenData;  // in-memory generated data

  int64_t readCompInfo;       // read compblock info
  int64_t totalCompInfoSize;  // total comp block size
  double  loadCompInfoUs;     // total elapsed time to read comp block info

  int64_t tmpBufferInDisk;  // size of buffer for intermediate result
} SQueryCostSummary;

typedef struct SPosInfo {
  int16_t pageId;
  int16_t rowId;
} SPosInfo;

typedef struct STimeWindow {
  TSKEY skey;
  TSKEY ekey;
} STimeWindow;

typedef struct SWindowStatus {
  bool closed;
} SWindowStatus;

typedef struct SWindowResult {
  uint16_t     numOfRows;
  SPosInfo     pos;         // Position of current result in disk-based output buffer
  SResultInfo* resultInfo;  // For each result column, there is a resultInfo
  STimeWindow  window;      // The time window that current result covers.
  SWindowStatus status;
} SWindowResult;

/*
 * header files info, avoid to iterate the directory, the data is acquired
 * during in query preparation function
 */
typedef struct SQueryFilesInfo {
  SHeaderFileInfo* pFileInfo;
  uint32_t         numOfFiles;  // the total available number of files for this virtual node during query execution
  int32_t          current;     // the memory mapped header file, NOTE: only one header file can be mmap.
  int32_t          vnodeId;
  
  int32_t          headerFd;         // header file fd
  int64_t          headerFileSize;
  int32_t          dataFd;
  int32_t          lastFd;

  char headerFilePath[PATH_MAX];  // current opened header file name
  char dataFilePath[PATH_MAX];    // current opened data file name
  char lastFilePath[PATH_MAX];    // current opened last file path
  char dbFilePathPrefix[PATH_MAX];
} SQueryFilesInfo;

typedef struct SWindowResInfo {
  SWindowResult*      pResult;    // reference to SQuerySupporter->pResult
  void*               hashList;   // hash list for quick access
  int16_t             type;       // data type for hash key
  int32_t             capacity;   // max capacity
  int32_t             curIndex;   // current start active index
  int32_t             size;
  
  int64_t             startTime;  // start time of the first time window for sliding query
  int64_t             prevSKey;   // previous (not completed) sliding window start key
  int64_t             threshold;  // threshold for return completed results.
} SWindowResInfo;

typedef struct SPointInterpoSupporter {
  int32_t numOfCols;
  char**  pPrevPoint;
  char**  pNextPoint;
} SPointInterpoSupporter;

typedef struct SQueryRuntimeEnv {
  SPositionInfo       startPos; /* the start position, used for secondary/third iteration */
  SPositionInfo       endPos;   /* the last access position in query, served as the start pos of reversed order query */
  SPositionInfo       nextPos;  /* start position of the next scan */
  SData*              colDataBuffer[TSDB_MAX_COLUMNS];
  SResultInfo*        resultInfo;   // todo refactor to merge with SWindowResInfo
  uint8_t             blockStatus;  // Indicate if data block is loaded, the block is first/last/internal block
  int32_t             unzipBufSize;
  SData*              primaryColBuffer;
  char*               unzipBuffer;
  char*               secondaryUnzipBuffer;
  SQuery*             pQuery;
  SMeterObj*          pMeterObj;
  SQLFunctionCtx*     pCtx;
  SLoadDataBlockInfo  loadBlockInfo;         /* record current block load information */
  SLoadCompBlockInfo  loadCompBlockInfo; /* record current compblock information in SQuery */
  SQueryFilesInfo     vnodeFileInfo;
  int16_t             numOfRowsPerPage;
  int16_t             offset[TSDB_MAX_COLUMNS];
  uint16_t            scanFlag;  // denotes reversed scan of data or not
  SInterpolationInfo  interpoInfo;
  SData**             pInterpoBuf;
  
  SWindowResInfo      windowResInfo;
  
  STSBuf*             pTSBuf;
  STSCursor           cur;
  SQueryCostSummary   summary;
  bool                stableQuery;  // is super table query or not
  SQueryDiskbasedResultBuf*    pResultBuf;   // query result buffer based on blocked-wised disk file
  
  bool hasTimeWindow;
  char** lastRowInBlock;
  bool interpoSearch;
  
  /*
   * Temporarily hold the in-memory cache block info during scan cache blocks
   * Here we do not use the cache block info from pMeterObj, simple because it may change anytime
   * during the query by the submit/insert handling threads.
   * So we keep a copy of the support structure as well as the cache block data itself.
   */
  SCacheBlock         cacheBlock;
} SQueryRuntimeEnv;

/* intermediate pos during multimeter query involves interval */
typedef struct SMeterQueryInfo {
  int64_t      lastKey;
  int64_t      skey;
  int64_t      ekey;
  int32_t      numOfRes;
  int16_t      queryRangeSet;   // denote if the query range is set, only available for interval query
  tVariant      tag;
  STSCursor    cur;
  int32_t      sid; // for retrieve the page id list
  
  SWindowResInfo windowResInfo;
} SMeterQueryInfo;

typedef struct SMeterDataInfo {
  uint64_t     offsetInHeaderFile;
  int32_t      numOfBlocks;
  int32_t      start;  // start block index
  SCompBlock*  pBlock;
  int32_t      meterOrderIdx;
  SMeterObj*   pMeterObj;
  int32_t      groupIdx;    // group id in meter list
  SMeterQueryInfo* pMeterQInfo;
} SMeterDataInfo;

typedef struct STableQuerySupportObj {
  void* pMetersHashTable;   // meter table hash list

  SMeterSidExtInfo** pMeterSidExtInfo;
  int32_t            numOfMeters;

  /*
   * multimeter query resultset.
   * In multimeter queries, the result is temporarily stored on this structure, instead of
   * directly put result into output buffer, since we have no idea how many number of
   * rows may be generated by a specific subgroup. When query on all subgroups is executed,
   * the result is copy to output buffer. This attribution is not used during single meter query processing.
   */
  SQueryRuntimeEnv runtimeEnv;
  int64_t          rawSKey;
  int64_t          rawEKey;
  int32_t          subgroupIdx;
  int32_t          offset; /* offset in group result set of subgroup */
  tSidSet* pSidSet;

  /*
   * the query is executed position on which meter of the whole list.
   * when the index reaches the last one of the list, it means the query is completed.
   * We later may refactor to remove this attribution by using another flag to denote
   * whether a multimeter query is completed or not.
   */
  int32_t meterIdx;

  int32_t numOfGroupResultPages;
  int32_t groupResultSize;
  SMeterDataInfo* pMeterDataInfo;

  TSKEY* tsList;
} STableQuerySupportObj;

typedef struct _qinfo {
  uint64_t       signature;
  int32_t        refCount;  // QInfo reference count, when the value is 0, it can be released safely
  char           user[TSDB_METER_ID_LEN + 1];
  char           sql[TSDB_SHOW_SQL_LEN];
  uint8_t        stream;
  uint16_t       port;
  uint32_t       ip;
  uint64_t       startTime;
  int64_t        useconds;
  int            killed;
  struct _qinfo *prev, *next;
  SQuery         query;
  int            totalPoints;
  int            pointsRead;
  int            pointsReturned;
  int            pointsInterpo;
  int            code;
  char           bufIndex;
  char           changed;
  char           over;
  SMeterObj*     pObj;
  sem_t          dataReady;

  STableQuerySupportObj* pTableQuerySupporter;
  int (*fp)(SMeterObj*, SQuery*);
} SQInfo;

int32_t vnodeQueryTablePrepare(SQInfo* pQInfo, SMeterObj* pMeterObj, STableQuerySupportObj* pSMultiMeterObj,
                                     void* param);

void vnodeQueryFreeQInfoEx(SQInfo* pQInfo);

bool vnodeParametersSafetyCheck(SQuery* pQuery);

int32_t vnodeSTableQueryPrepare(SQInfo* pQInfo, SQuery* pQuery, void* param);

/**
 * decrease the numofQuery of each table that is queried, enable the
 * remove/close operation can be executed
 * @param pQInfo
 */
void vnodeDecMeterRefcnt(SQInfo* pQInfo);

/* sql query handle in dnode */
void vnodeSingleTableQuery(SSchedMsg* pMsg);

/*
 * handle multi-meter query process
 */
void vnodeMultiMeterQuery(SSchedMsg* pMsg);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEREAD_H

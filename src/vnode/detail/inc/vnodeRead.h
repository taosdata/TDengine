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

typedef struct SQueryLoadBlockInfo {
  int32_t fileListIndex; /* index of this file in files list of this vnode */
  int32_t fileId;
  int32_t slotIdx;
  int32_t sid;
  bool    tsLoaded;      // if timestamp column of current block is loaded or not
} SQueryLoadBlockInfo;

typedef struct SQueryLoadCompBlockInfo {
  int32_t sid; /* meter sid */
  int32_t fileId;
  int32_t fileListIndex;
} SQueryLoadCompBlockInfo;

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

typedef struct SOutputRes {
  uint16_t     numOfRows;
  int32_t      nAlloc;
  tFilePage**  result;
  SResultInfo* resultInfo;
} SOutputRes;

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

typedef struct RuntimeEnvironment {
  SPositionInfo       startPos; /* the start position, used for secondary/third iteration */
  SPositionInfo       endPos;   /* the last access position in query, served as the start pos of reversed order query */
  SPositionInfo       nextPos;  /* start position of the next scan */
  SData*              colDataBuffer[TSDB_MAX_COLUMNS];
  SResultInfo*        resultInfo;
  uint8_t             blockStatus;  // Indicate if data block is loaded, the block is first/last/internal block
  int32_t             unzipBufSize;
  SData*              primaryColBuffer;
  char*               unzipBuffer;
  char*               secondaryUnzipBuffer;
  SQuery*             pQuery;
  SMeterObj*          pMeterObj;
  SQLFunctionCtx*     pCtx;
  SQueryLoadBlockInfo loadBlockInfo;         /* record current block load information */
  SQueryLoadCompBlockInfo loadCompBlockInfo; /* record current compblock information in SQuery */
  SQueryFilesInfo         vnodeFileInfo;
  int16_t                 numOfRowsPerPage;
  int16_t                 offset[TSDB_MAX_COLUMNS];
  int16_t                 scanFlag;  // denotes reversed scan of data or not
  SInterpolationInfo      interpoInfo;
  SData**                 pInterpoBuf;
  SOutputRes*             pResult;  // reference to SQuerySupporter->pResult
  void*                   hashList;
  int32_t                 usedIndex;  // assigned SOutputRes in list
  STSBuf*                 pTSBuf;
  STSCursor               cur;
  SQueryCostSummary       summary;
} SQueryRuntimeEnv;

/* intermediate result during multimeter query involves interval */
typedef struct SMeterQueryInfo {
  int64_t      lastKey;
  int64_t      skey;
  int64_t      ekey;
  int32_t      numOfRes;
  uint32_t     numOfPages;
  uint32_t     numOfAlloc;
  int32_t      reverseIndex;    // reversed output indicator, start from (numOfRes-1)
  int16_t      reverseFillRes;  // denote if reverse fill the results in supplementary scan required or not
  int16_t      queryRangeSet;   // denote if the query range is set, only available for interval query
  int16_t      lastResRows;
  int64_t      tag;
  STSCursor    cur;
  SResultInfo* resultInfo;
  uint32_t*    pageList;
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

typedef struct SMeterQuerySupportObj {
  void* pMeterObj;

  SMeterSidExtInfo** pMeterSidExtInfo;
  int32_t            numOfMeters;

  /*
   * multimeter query resultset.
   * In multimeter queries, the result is temporarily stored on this structure, instead of
   * directly put result into output buffer, since we have no idea how many number of
   * rows may be generated by a specific subgroup. When query on all subgroups is executed,
   * the result is copy to output buffer. This attribution is not used during single meter query processing.
   */
  SOutputRes*      pResult;
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

  int32_t meterOutputFd;
  int32_t lastPageId;
  int32_t numOfPages;
  int32_t numOfGroupResultPages;
  int32_t groupResultSize;

  char*   meterOutputMMapBuf;
  int64_t bufSize;
  char    extBufFile[256];  // external file name

  SMeterDataInfo* pMeterDataInfo;

  TSKEY* tsList;
} SMeterQuerySupportObj;

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
  int            num;
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

  SMeterQuerySupportObj* pMeterQuerySupporter;
  int (*fp)(SMeterObj*, SQuery*);
} SQInfo;

int32_t vnodeQuerySingleMeterPrepare(SQInfo* pQInfo, SMeterObj* pMeterObj, SMeterQuerySupportObj* pSMultiMeterObj,
                                     void* param);

void vnodeQueryFreeQInfoEx(SQInfo* pQInfo);

bool vnodeParametersSafetyCheck(SQuery* pQuery);

int32_t vnodeMultiMeterQueryPrepare(SQInfo* pQInfo, SQuery* pQuery, void* param);

/**
 * decrease the numofQuery of each table that is queried, enable the
 * remove/close operation can be executed
 * @param pQInfo
 */
void vnodeDecMeterRefcnt(SQInfo* pQInfo);

/* sql query handle in dnode */
void vnodeSingleMeterQuery(SSchedMsg* pMsg);

/*
 * handle multi-meter query process
 */
void vnodeMultiMeterQuery(SSchedMsg* pMsg);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEREAD_H

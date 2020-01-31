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

#ifndef TDENGINE_TSCUTIL_H
#define TDENGINE_TSCUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * @date   2018/09/30
 */
#include "os.h"
#include "textbuffer.h"
#include "tscSecondaryMerge.h"
#include "tsclient.h"
#include "tsdb.h"

#define UTIL_METER_IS_METRIC(metaInfo) \
  (((metaInfo)->pMeterMeta != NULL) && ((metaInfo)->pMeterMeta->meterType == TSDB_METER_METRIC))
#define UTIL_METER_IS_NOMRAL_METER(metaInfo) (!(UTIL_METER_IS_METRIC(metaInfo)))
#define UTIL_METER_IS_CREATE_FROM_METRIC(metaInfo) \
  (((metaInfo)->pMeterMeta != NULL) && ((metaInfo)->pMeterMeta->meterType == TSDB_METER_MTABLE))

#define TSDB_COL_IS_TAG(f) (((f)&TSDB_COL_TAG) != 0)

typedef struct SParsedColElem {
  int16_t colIndex;
  int16_t offset;
} SParsedColElem;

typedef struct SParsedDataColInfo {
  int16_t        numOfCols;
  int16_t        numOfAssignedCols;
  SParsedColElem elems[TSDB_MAX_COLUMNS];
  bool           hasVal[TSDB_MAX_COLUMNS];
} SParsedDataColInfo;

typedef struct SJoinSubquerySupporter {
  SSubqueryState* pState;
  SSqlObj*        pObj;           // parent SqlObj
  int32_t         subqueryIndex;  // index of sub query
  int64_t         interval;       // interval time
  SLimitVal       limit;          // limit info
  uint64_t        uid;            // query meter uid
  SColumnBaseInfo colList;        // previous query information
  SSqlExprInfo    exprsInfo;
  SFieldInfo      fieldsInfo;
  STagCond        tagCond;
  SSqlGroupbyExpr groupbyExpr;
  struct STSBuf*  pTSBuf;          // the TSBuf struct that holds the compressed timestamp array
  FILE*           f;               // temporary file in order to create TSBuf
  char            path[PATH_MAX];  // temporary file path
} SJoinSubquerySupporter;

int32_t tscCreateDataBlock(size_t initialSize, int32_t rowSize, int32_t startOffset, const char* name,
                           STableDataBlocks** dataBlocks);
void    tscAppendDataBlock(SDataBlockList* pList, STableDataBlocks* pBlocks);
void    tscDestroyDataBlock(STableDataBlocks* pDataBlock);

SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, short bytes,
                                   uint32_t offset);

SDataBlockList* tscCreateBlockArrayList();

void*   tscDestroyBlockArrayList(SDataBlockList* pList);
int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock);
void    tscFreeUnusedDataBlocks(SDataBlockList* pList);
int32_t tscMergeTableDataBlocks(SSqlObj* pSql, SDataBlockList* pDataList);
int32_t tscGetDataBlockFromList(void* pHashList, SDataBlockList* pDataBlockList, int64_t id, int32_t size,
                                int32_t startOffset, int32_t rowSize, const char* tableId,
                                STableDataBlocks** dataBlocks);

SVnodeSidList*    tscGetVnodeSidList(SMetricMeta* pMetricmeta, int32_t vnodeIdx);
SMeterSidExtInfo* tscGetMeterSidInfo(SVnodeSidList* pSidList, int32_t idx);

/**
 *
 * for the projection query on metric or point interpolation query on metric,
 * we iterate all the meters, instead of invoke query on all qualified meters simultaneously.
 *
 * @param pSql  sql object
 * @return
 */
bool tscIsPointInterpQuery(SSqlCmd* pCmd);
bool tscIsTWAQuery(SSqlCmd* pCmd);
bool tscProjectionQueryOnMetric(SSqlCmd* pCmd);
bool tscProjectionQueryOnTable(SSqlCmd* pCmd);

bool tscIsTwoStageMergeMetricQuery(SSqlCmd* pCmd);
bool tscQueryOnMetric(SSqlCmd* pCmd);
bool tscQueryMetricTags(SSqlCmd* pCmd);
bool tscIsSelectivityWithTagQuery(SSqlCmd* pCmd);

void tscAddSpecialColumnForSelect(SSqlCmd* pCmd, int32_t outputColIndex, int16_t functionId, SColumnIndex* pIndex,
                                  SSchema* pColSchema, int16_t isTag);

void addRequiredTagColumn(SSqlCmd* pCmd, int32_t tagColIndex, int32_t tableIndex);

int32_t setMeterID(SSqlObj* pSql, SSQLToken* pzTableName, int32_t tableIndex);
void    tscClearInterpInfo(SSqlCmd* pCmd);

bool tscIsInsertOrImportData(char* sqlstr);

/* use for keep current db info temporarily, for handle table with db prefix */
void tscGetDBInfoFromMeterId(char* meterId, char* db);

int tscAllocPayload(SSqlCmd* pCmd, int size);

void tscFieldInfoSetValFromSchema(SFieldInfo* pFieldInfo, int32_t index, SSchema* pSchema);
void tscFieldInfoSetValFromField(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* pField);
void tscFieldInfoSetValue(SFieldInfo* pFieldInfo, int32_t index, int8_t type, const char* name, int16_t bytes);
void tscFieldInfoUpdateVisible(SFieldInfo* pFieldInfo, int32_t index, bool visible);

void tscFieldInfoCalOffset(SSqlCmd* pCmd);
void tscFieldInfoUpdateOffset(SSqlCmd* pCmd);
void tscFieldInfoCopy(SFieldInfo* src, SFieldInfo* dst, const int32_t* indexList, int32_t size);
void tscFieldInfoCopyAll(SFieldInfo* src, SFieldInfo* dst);

TAOS_FIELD* tscFieldInfoGetField(SSqlCmd* pCmd, int32_t index);
int16_t     tscFieldInfoGetOffset(SSqlCmd* pCmd, int32_t index);
int32_t     tscGetResRowLength(SSqlCmd* pCmd);
void        tscClearFieldInfo(SFieldInfo* pFieldInfo);

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes, int16_t tableIndex);

SSqlExpr* tscSqlExprInsert(SSqlCmd* pCmd, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t interSize);
SSqlExpr* tscSqlExprInsertEmpty(SSqlCmd* pCmd, int32_t index, int16_t functionId);

SSqlExpr* tscSqlExprUpdate(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size);

SSqlExpr* tscSqlExprGet(SSqlCmd* pCmd, int32_t index);
void      tscSqlExprCopy(SSqlExprInfo* dst, const SSqlExprInfo* src, uint64_t uid);

SColumnBase* tscColumnBaseInfoInsert(SSqlCmd* pCmd, SColumnIndex* colIndex);
void         tscColumnFilterInfoCopy(SColumnFilterInfo* dst, const SColumnFilterInfo* src);
void         tscColumnBaseCopy(SColumnBase* dst, const SColumnBase* src);

void         tscColumnBaseInfoCopy(SColumnBaseInfo* dst, const SColumnBaseInfo* src, int16_t tableIndex);
SColumnBase* tscColumnBaseInfoGet(SColumnBaseInfo* pColumnBaseInfo, int32_t index);
void         tscColumnBaseInfoUpdateTableIndex(SColumnBaseInfo* pColList, int16_t tableIndex);

void tscColumnBaseInfoReserve(SColumnBaseInfo* pColumnBaseInfo, int32_t size);
void tscColumnBaseInfoDestroy(SColumnBaseInfo* pColumnBaseInfo);

int32_t tscValidateName(SSQLToken* pToken);

void tscIncStreamExecutionCount(void* pStream);

bool tscValidateColumnId(SSqlCmd* pCmd, int32_t colId);

// get starter position of metric query condition (query on tags) in SSqlCmd.payload
SCond* tsGetMetricQueryCondPos(STagCond* pCond, uint64_t tableIndex);
void   tsSetMetricQueryCond(STagCond* pTagCond, uint64_t uid, const char* str);

void tscTagCondCopy(STagCond* dest, const STagCond* src);
void tscTagCondRelease(STagCond* pCond);

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SSqlCmd* pCmd);

void tscSetFreeHeatBeat(STscObj* pObj);
bool tscShouldFreeHeatBeat(SSqlObj* pHb);
void tscCleanSqlCmd(SSqlCmd* pCmd);
bool tscShouldFreeAsyncSqlObj(SSqlObj* pSql);

void            tscRemoveAllMeterMetaInfo(SSqlCmd* pCmd, bool removeFromCache);
SMeterMetaInfo* tscGetMeterMetaInfo(SSqlCmd* pCmd, int32_t index);
SMeterMetaInfo* tscGetMeterMetaInfoByUid(SSqlCmd* pCmd, uint64_t uid, int32_t* index);
void            tscClearMeterMetaInfo(SMeterMetaInfo* pMeterMetaInfo, bool removeFromCache);

SMeterMetaInfo* tscAddMeterMetaInfo(SSqlCmd* pCmd, const char* name, SMeterMeta* pMeterMeta, SMetricMeta* pMetricMeta,
                                    int16_t numOfTags, int16_t* tags);
SMeterMetaInfo* tscAddEmptyMeterMetaInfo(SSqlCmd* pCmd);

void tscGetMetricMetaCacheKey(SSqlCmd* pCmd, char* keyStr, uint64_t uid);
int  tscGetMetricMeta(SSqlObj* pSql);
int  tscGetMeterMeta(SSqlObj* pSql, char* meterId, int32_t tableIndex);
int  tscGetMeterMetaEx(SSqlObj* pSql, char* meterId, bool createIfNotExists);

void tscResetForNextRetrieve(SSqlRes* pRes);

void tscAddTimestampColumn(SSqlCmd* pCmd, int16_t functionId, int16_t tableIndex);
void tscDoQuery(SSqlObj* pSql);

/**
 * The create object function must be successful expect for the out of memory issue.
 *
 * Therefore, the metermeta/metricmeta object is directly passed to the newly created subquery object from the
 * previous sql object, instead of retrieving the metermeta/metricmeta from cache.
 *
 * Because the metermeta/metricmeta may have been released by other threads, resulting in the retrieving failed as
 * well as the create function.
 *
 * @param pSql
 * @param vnodeIndex
 * @param tableIndex
 * @param fp
 * @param param
 * @param pPrevSql
 * @return
 */
SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, void (*fp)(), void* param, SSqlObj* pPrevSql);
void     addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t tableIndex);

void doAddGroupColumnForSubquery(SSqlCmd* pCmd, int32_t tagIndex);

int16_t tscGetJoinTagColIndexByUid(STagCond* pTagCond, uint64_t uid);

TAOS* taos_connect_a(char* ip, char* user, char* pass, char* db, uint16_t port, void (*fp)(void*, TAOS_RES*, int),
                     void* param, void** taos);

void sortRemoveDuplicates(STableDataBlocks* dataBuf);

void tscPrintSelectClause(SSqlCmd* pCmd);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCUTIL_H

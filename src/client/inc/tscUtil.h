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
#include "tbuffer.h"
#include "qextbuffer.h"
#include "taosdef.h"
#include "tscSecondaryMerge.h"
#include "tsclient.h"

#define UTIL_TABLE_IS_SUPERTABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_SUPER_TABLE))
#define UTIL_TABLE_IS_NOMRAL_TABLE(metaInfo) (!(UTIL_TABLE_IS_SUPERTABLE(metaInfo)))
#define UTIL_TABLE_IS_CHILD_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_CHILD_TABLE))

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
                           STableMeta* pTableMeta, STableDataBlocks** dataBlocks);
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
                                int32_t startOffset, int32_t rowSize, const char* tableId, STableMeta* pTableMeta,
                                STableDataBlocks** dataBlocks);

SVnodeSidList*    tscGetVnodeSidList(SSuperTableMeta* pMetricmeta, int32_t vnodeIdx);
STableIdInfo* tscGetMeterSidInfo(SVnodeSidList* pSidList, int32_t idx);

/**
 *
 * for the projection query on metric or point interpolation query on metric,
 * we iterate all the meters, instead of invoke query on all qualified meters simultaneously.
 *
 * @param pSql  sql object
 * @return
 */
bool tscIsPointInterpQuery(SQueryInfo* pQueryInfo);
bool tscIsTWAQuery(SQueryInfo* pQueryInfo);

bool tscNonOrderedProjectionQueryOnSTable(SQueryInfo *pQueryInfo, int32_t tableIndex);
bool tscOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex);
bool tscIsProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex);

bool tscProjectionQueryOnTable(SQueryInfo* pQueryInfo);

bool tscIsTwoStageSTableQuery(SQueryInfo* pQueryInfo, int32_t tableIndex);
bool tscQueryOnMetric(SSqlCmd* pCmd);
bool tscQueryTags(SQueryInfo* pQueryInfo);
bool tscIsSelectivityWithTagQuery(SSqlCmd* pCmd);

void tscAddSpecialColumnForSelect(SQueryInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId, SColumnIndex* pIndex,
                                  SSchema* pColSchema, int16_t isTag);

void addRequiredTagColumn(SQueryInfo* pQueryInfo, int32_t tagColIndex, int32_t tableIndex);

int32_t setMeterID(STableMetaInfo* pTableMetaInfo, SSQLToken* pzTableName, SSqlObj* pSql);
void    tscClearInterpInfo(SQueryInfo* pQueryInfo);

bool tscIsInsertOrImportData(char* sqlstr);

/* use for keep current db info temporarily, for handle table with db prefix */
void tscGetDBInfoFromMeterId(char* tableId, char* db);

int tscAllocPayload(SSqlCmd* pCmd, int size);

void tscFieldInfoSetValFromSchema(SFieldInfo* pFieldInfo, int32_t index, SSchema* pSchema);
void tscFieldInfoSetValFromField(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* pField);
void tscFieldInfoSetValue(SFieldInfo* pFieldInfo, int32_t index, int8_t type, const char* name, int16_t bytes);
void tscFieldInfoUpdateVisible(SFieldInfo* pFieldInfo, int32_t index, bool visible);
void tscFieldInfoSetExpr(SFieldInfo* pFieldInfo, int32_t index, SSqlExpr* pExpr);
void tscFieldInfoSetBinExpr(SFieldInfo* pFieldInfo, int32_t index, SSqlFunctionExpr* pExpr);

void tscFieldInfoCalOffset(SQueryInfo* pQueryInfo);
void tscFieldInfoCopy(SFieldInfo* src, SFieldInfo* dst, const int32_t* indexList, int32_t size);
void tscFieldInfoCopyAll(SFieldInfo* dst, SFieldInfo* src);
void tscFieldInfoUpdateOffsetForInterResult(SQueryInfo* pQueryInfo);

TAOS_FIELD* tscFieldInfoGetField(SQueryInfo* pQueryInfo, int32_t index);
int16_t     tscFieldInfoGetOffset(SQueryInfo* pQueryInfo, int32_t index);
int32_t     tscGetResRowLength(SQueryInfo* pQueryInfo);
void        tscClearFieldInfo(SFieldInfo* pFieldInfo);
int32_t tscNumOfFields(SQueryInfo* pQueryInfo);
int32_t tscFieldInfoCompare(SFieldInfo* pFieldInfo1, SFieldInfo* pFieldInfo2);

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes, int16_t tableIndex);

SSqlExpr* tscSqlExprInsert(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t interSize);
SSqlExpr* tscSqlExprInsertEmpty(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId);

SSqlExpr* tscSqlExprUpdate(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size);
int32_t   tscSqlExprNumOfExprs(SQueryInfo* pQueryInfo);

SSqlExpr* tscSqlExprGet(SQueryInfo* pQueryInfo, int32_t index);
void      tscSqlExprCopy(SSqlExprInfo* dst, const SSqlExprInfo* src, uint64_t uid, bool deepcopy);
void*     tscSqlExprDestroy(SSqlExpr* pExpr);
void      tscSqlExprInfoDestroy(SSqlExprInfo* pExprInfo);

SColumnBase* tscColumnBaseInfoInsert(SQueryInfo* pQueryInfo, SColumnIndex* colIndex);
void         tscColumnFilterInfoCopy(SColumnFilterInfo* dst, const SColumnFilterInfo* src);
void         tscColumnBaseCopy(SColumnBase* dst, const SColumnBase* src);

void         tscColumnBaseInfoCopy(SColumnBaseInfo* dst, const SColumnBaseInfo* src, int16_t tableIndex);
SColumnBase* tscColumnBaseInfoGet(SColumnBaseInfo* pColumnBaseInfo, int32_t index);
void         tscColumnBaseInfoUpdateTableIndex(SColumnBaseInfo* pColList, int16_t tableIndex);

void tscColumnBaseInfoReserve(SColumnBaseInfo* pColumnBaseInfo, int32_t size);
void tscColumnBaseInfoDestroy(SColumnBaseInfo* pColumnBaseInfo);

int32_t tscValidateName(SSQLToken* pToken);

void tscIncStreamExecutionCount(void* pStream);

bool tscValidateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId);

// get starter position of metric query condition (query on tags) in SSqlCmd.payload
SCond* tsGetSTableQueryCond(STagCond* pCond, uint64_t uid);
void   tsSetSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBuffer* pBuf);

void tscTagCondCopy(STagCond* dest, const STagCond* src);
void tscTagCondRelease(STagCond* pCond);

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryInfo* pQueryInfo);

void tscSetFreeHeatBeat(STscObj* pObj);
bool tscShouldFreeHeatBeat(SSqlObj* pHb);
void tscCleanSqlCmd(SSqlCmd* pCmd);
bool tscShouldFreeAsyncSqlObj(SSqlObj* pSql);

void            tscRemoveAllMeterMetaInfo(SQueryInfo* pQueryInfo, const char* address, bool removeFromCache);
STableMetaInfo* tscGetTableMetaInfoFromCmd(SSqlCmd *pCmd, int32_t subClauseIndex, int32_t tableIndex);
STableMetaInfo* tscGetMetaInfo(SQueryInfo *pQueryInfo, int32_t tableIndex);

SQueryInfo *tscGetQueryInfoDetail(SSqlCmd* pCmd, int32_t subClauseIndex);
int32_t tscGetQueryInfoDetailSafely(SSqlCmd *pCmd, int32_t subClauseIndex, SQueryInfo** pQueryInfo);

STableMetaInfo* tscGetMeterMetaInfoByUid(SQueryInfo* pQueryInfo, uint64_t uid, int32_t* index);
void            tscClearMeterMetaInfo(STableMetaInfo* pTableMetaInfo, bool removeFromCache);

STableMetaInfo* tscAddTableMetaInfo(SQueryInfo* pQueryInfo, const char* name, STableMeta* pTableMeta,
    SVgroupsInfo* vgroupList, int16_t numOfTags, int16_t* tags);

STableMetaInfo* tscAddEmptyMetaInfo(SQueryInfo *pQueryInfo);
int32_t tscAddSubqueryInfo(SSqlCmd *pCmd);
void tscFreeSubqueryInfo(SSqlCmd* pCmd);
void tscClearSubqueryInfo(SSqlCmd* pCmd);

void tscGetMetricMetaCacheKey(SQueryInfo* pQueryInfo, char* keyStr, uint64_t uid);
int  tscGetSTableVgroupInfo(SSqlObj* pSql, int32_t clauseIndex);
int  tscGetTableMeta(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo);
int  tscGetMeterMetaEx(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, bool createIfNotExists);

void tscResetForNextRetrieve(SSqlRes* pRes);

void tscAddTimestampColumn(SQueryInfo* pQueryInfo, int16_t functionId, int16_t tableIndex);
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
SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, void (*fp)(), void* param, int32_t cmd, SSqlObj* pPrevSql);
void     addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t subClauseIndex, int32_t tableIndex);

void doAddGroupColumnForSubquery(SQueryInfo* pQueryInfo, int32_t tagIndex);

int16_t tscGetJoinTagColIndexByUid(STagCond* pTagCond, uint64_t uid);

TAOS* taos_connect_a(char* ip, char* user, char* pass, char* db, uint16_t port, void (*fp)(void*, TAOS_RES*, int),
                     void* param, void** taos);

void sortRemoveDuplicates(STableDataBlocks* dataBuf);

void tscPrintSelectClause(SSqlObj* pSql, int32_t subClauseIndex);

bool hasMoreVnodesToTry(SSqlObj *pSql);
void tscTryQueryNextVnode(SSqlObj *pSql, __async_cb_func_t fp);
void tscAsyncQuerySingleRowForNextVnode(void *param, TAOS_RES *tres, int numOfRows);
void tscTryQueryNextClause(SSqlObj* pSql, void (*queryFp)());

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCUTIL_H

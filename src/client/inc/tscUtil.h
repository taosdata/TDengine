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

#include "exception.h"
#include "os.h"
#include "qExtbuffer.h"
#include "taosdef.h"
#include "tbuffer.h"
#include "tscLocalMerge.h"
#include "tsclient.h"

#define UTIL_TABLE_IS_SUPER_TABLE(metaInfo)  \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_SUPER_TABLE))
#define UTIL_TABLE_IS_CHILD_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_CHILD_TABLE))
  
#define UTIL_TABLE_IS_NORMAL_TABLE(metaInfo)\
  (!(UTIL_TABLE_IS_SUPER_TABLE(metaInfo) || UTIL_TABLE_IS_CHILD_TABLE(metaInfo)))


typedef struct SParsedColElem {
  int16_t colIndex;
  uint16_t offset;
} SParsedColElem;

typedef struct SParsedDataColInfo {
  int16_t        numOfCols;
  int16_t        numOfAssignedCols;
  SParsedColElem elems[TSDB_MAX_COLUMNS];
  bool           hasVal[TSDB_MAX_COLUMNS];
} SParsedDataColInfo;

#pragma pack(push,1)
// this struct is transfered as binary, padding two bytes to avoid
// an 'uid' whose low bytes is 0xff being recoginized as NULL,
// and set 'pack' to 1 to avoid break existing code.
typedef struct STidTags {
  int16_t  padding;
  int64_t  uid;
  int32_t  tid;
  int32_t  vgId;
  char     tag[];
} STidTags;
#pragma pack(pop)

typedef struct SJoinSupporter {
  SSqlObj*        pObj;           // parent SqlObj
  int32_t         subqueryIndex;  // index of sub query
  SInterval       interval;
  SLimitVal       limit;          // limit info
  uint64_t        uid;            // query table uid
  SArray*         colList;        // previous query information, no need to use this attribute, and the corresponding attribution
  SArray*         exprList;
  SFieldInfo      fieldsInfo;
  STagCond        tagCond;
  SSqlGroupbyExpr groupInfo;       // group by info
  struct STSBuf*  pTSBuf;          // the TSBuf struct that holds the compressed timestamp array
  FILE*           f;               // temporary file in order to create TSBuf
  char            path[PATH_MAX];  // temporary file path, todo dynamic allocate memory
  int32_t         tagSize;         // the length of each in the first filter stage
  char*           pIdTagList;      // result of first stage tags
  int32_t         totalLen;
  int32_t         num;
  SArray*         pVgroupTables;
} SJoinSupporter;

typedef struct SVgroupTableInfo {
  SVgroupInfo vgInfo;
  SArray*     itemList;   //SArray<STableIdInfo>
} SVgroupTableInfo;

static FORCE_INLINE SQueryNodeInfo* tscGetQueryInfo(SSqlCmd* pCmd, int32_t subClauseIndex) {
  assert(pCmd != NULL && subClauseIndex >= 0);
  if (pCmd->pQueryInfo == NULL || subClauseIndex >= pCmd->numOfClause) {
    return NULL;
  }

  return pCmd->pQueryInfo[subClauseIndex];
}

SQueryNodeInfo* tscGetActiveQueryInfo(SSqlCmd* pCmd);

int32_t tscCreateDataBlock(size_t initialSize, int32_t rowSize, int32_t startOffset, SName* name, STableMeta* pTableMeta, STableDataBlocks** dataBlocks);
void tscDestroyDataBlock(STableDataBlocks* pDataBlock, bool removeMeta);
void tscSortRemoveDataBlockDupRows(STableDataBlocks* dataBuf);

SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, int16_t bytes,
                                   uint32_t offset);

void*   tscDestroyBlockArrayList(SArray* pDataBlockList);
void*   tscDestroyBlockHashTable(SHashObj* pBlockHashTable, bool removeMeta);

int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock);
int32_t tscMergeTableDataBlocks(SSqlObj* pSql, bool freeBlockMap);
int32_t tscGetDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize, SName* pName, STableMeta* pTableMeta,
                                STableDataBlocks** dataBlocks, SArray* pBlockList);

/**
 * for the projection query on metric or point interpolation query on metric,
 * we iterate all the meters, instead of invoke query on all qualified meters simultaneously.
 *
 * @param pSql  sql object
 * @return
 */
bool tscIsPointInterpQuery(SQueryNodeInfo* pQueryInfo);
bool tscIsTWAQuery(SQueryNodeInfo* pQueryInfo);
bool tscIsSecondStageQuery(SQueryNodeInfo* pQueryInfo);
bool tscGroupbyColumn(SQueryNodeInfo* pQueryInfo);
bool tscIsTopBotQuery(SQueryNodeInfo* pQueryInfo);
bool hasTagValOutput(SQueryNodeInfo* pQueryInfo);
bool timeWindowInterpoRequired(SQueryNodeInfo *pQueryNodeInfo);
bool isStabledev(SQueryNodeInfo* pQueryInfo);
bool isTsCompQuery(SQueryNodeInfo* pQueryNodeInfo);
bool isSimpleAggregate(SQueryNodeInfo* pQueryNodeInfo);

bool tscNonOrderedProjectionQueryOnSTable(SQueryNodeInfo *pQueryInfo, int32_t tableIndex);
bool tscOrderedProjectionQueryOnSTable(SQueryNodeInfo* pQueryInfo, int32_t tableIndex);
bool tscIsProjectionQueryOnSTable(SQueryNodeInfo* pQueryInfo, int32_t tableIndex);

bool tscIsProjectionQuery(SQueryNodeInfo* pQueryInfo);

bool tscIsTwoStageSTableQuery(SQueryNodeInfo* pQueryInfo, int32_t tableIndex);
bool tscQueryTags(SQueryNodeInfo* pQueryInfo);
bool tscMultiRoundQuery(SQueryNodeInfo* pQueryInfo, int32_t tableIndex);
bool tscQueryBlockInfo(SQueryNodeInfo* pQueryInfo);

SExprInfo* tscAddFuncInSelectClause(SQueryNodeInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId,
                                       SColumnIndex* pIndex, SSchema* pColSchema, int16_t colType);

int32_t tscSetTableFullName(STableMetaInfo* pTableMetaInfo, SStrToken* pzTableName, SSqlObj* pSql);
void    tscClearInterpInfo(SQueryNodeInfo* pQueryInfo);

bool tscIsInsertData(char* sqlstr);

int tscAllocPayload(SSqlCmd* pCmd, int size);

TAOS_FIELD tscCreateField(int8_t type, const char* name, int16_t bytes);

SInternalField* tscFieldInfoAppend(SFieldInfo* pFieldInfo, TAOS_FIELD* pField);
SInternalField* tscFieldInfoInsert(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* field);

SInternalField* tscFieldInfoGetInternalField(SFieldInfo* pFieldInfo, int32_t index);
TAOS_FIELD* tscFieldInfoGetField(SFieldInfo* pFieldInfo, int32_t index);

void tscFieldInfoUpdateOffset(SQueryNodeInfo* pQueryInfo);

int16_t tscFieldInfoGetOffset(SQueryNodeInfo* pQueryInfo, int32_t index);
void    tscFieldInfoClear(SFieldInfo* pFieldInfo);

static FORCE_INLINE int32_t tscNumOfFields(SQueryNodeInfo* pQueryInfo) { return pQueryInfo->fieldsInfo.numOfOutput; }

int32_t tscFieldInfoCompare(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2);

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes);

int32_t   tscGetResRowLength(SArray* pExprList);

SExprInfo* tscSqlExprInsert(SQueryNodeInfo* pQueryInfo, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
    int16_t size, int16_t resColId, int16_t interSize, bool isTagCol);

SExprInfo* tscSqlExprAppend(SQueryNodeInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t resColId, int16_t interSize, bool isTagCol);

SExprInfo* tscSqlExprUpdate(SQueryNodeInfo* pQueryInfo, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size);
size_t   tscSqlExprNumOfExprs(SQueryNodeInfo* pQueryInfo);
void tscInsertPrimaryTsSourceColumn(SQueryNodeInfo* pQueryInfo, SColumnIndex* pIndex);

SExprInfo* tscSqlExprGet(SQueryNodeInfo* pQueryInfo, int32_t index);
int32_t   tscSqlExprCopy(SArray* dst, const SArray* src, uint64_t uid, bool deepcopy);
void      tscSqlExprAssign(SExprInfo* dst, const SExprInfo* src);
void      tscSqlExprInfoDestroy(SArray* pExprInfo);

SColumn* tscColumnClone(const SColumn* src);
SColumn* tscColumnListInsert(SArray* pColList, SColumnIndex* colIndex, SSchema* pSchema);
void tscColumnListDestroy(SArray* pColList);

void tscDequoteAndTrimToken(SStrToken* pToken);
int32_t tscValidateName(SStrToken* pToken);

void tscIncStreamExecutionCount(void* pStream);

bool tscValidateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId, int32_t numOfParams);

// get starter position of metric query condition (query on tags) in SSqlCmd.payload
SCond* tsGetSTableQueryCond(STagCond* pCond, uint64_t uid);
void   tsSetSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBufferWriter* bw);

int32_t tscTagCondCopy(STagCond* dest, const STagCond* src);
void tscTagCondRelease(STagCond* pCond);

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryNodeInfo* pQueryInfo);

bool tscShouldBeFreed(SSqlObj* pSql);

STableMetaInfo* tscGetTableMetaInfoFromCmd(SSqlCmd *pCmd, int32_t subClauseIndex, int32_t tableIndex);
STableMetaInfo* tscGetMetaInfo(SQueryNodeInfo *pQueryInfo, int32_t tableIndex);

void        tscInitQueryInfo(SQueryNodeInfo* pQueryInfo);
void        tscClearSubqueryInfo(SSqlCmd* pCmd);
int32_t     tscAddQueryInfo(SSqlCmd *pCmd);
SQueryNodeInfo *tscGetQueryInfo(SSqlCmd* pCmd, int32_t subClauseIndex);
SQueryNodeInfo *tscGetQueryInfoS(SSqlCmd *pCmd, int32_t subClauseIndex);

void tscClearTableMetaInfo(STableMetaInfo* pTableMetaInfo);

STableMetaInfo* tscAddTableMetaInfo(SQueryNodeInfo* pQueryInfo, SName* name, STableMeta* pTableMeta,
                                    SVgroupsInfo* vgroupList, SArray* pTagCols, SArray* pVgroupTables);

STableMetaInfo* tscAddEmptyMetaInfo(SQueryNodeInfo *pQueryInfo);

void tscFreeVgroupTableInfo(SArray* pVgroupTables);
SArray* tscVgroupTableInfoDup(SArray* pVgroupTables);
void tscRemoveVgroupTableGroup(SArray* pVgroupTable, int32_t index);
void tscVgroupTableCopy(SVgroupTableInfo* info, SVgroupTableInfo* pInfo);

int  tscGetSTableVgroupInfo(SSqlObj* pSql, int32_t clauseIndex);
int  tscGetTableMeta(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo);
int  tscGetTableMetaEx(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, bool createIfNotExists);

void tscResetForNextRetrieve(SSqlRes* pRes);
void tscDoQuery(SSqlObj* pSql);
void executeQuery(SSqlObj* pSql, SQueryNodeInfo* pQueryInfo);
void doExecuteQuery(SSqlObj* pSql, SQueryNodeInfo* pQueryInfo);

SVgroupsInfo* tscVgroupInfoClone(SVgroupsInfo *pInfo);
void* tscVgroupInfoClear(SVgroupsInfo *pInfo);
void tscSVgroupInfoCopy(SVgroupInfo* dst, const SVgroupInfo* src);
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
SSqlObj* createSimpleSubObj(SSqlObj* pSql, __async_cb_func_t fp, void* param, int32_t cmd);

void registerSqlObj(SSqlObj* pSql);

SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, __async_cb_func_t fp, void* param, int32_t cmd, SSqlObj* pPrevSql);
void     addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t subClauseIndex, int32_t tableIndex);

void doAddGroupColumnForSubquery(SQueryNodeInfo* pQueryInfo, int32_t tagIndex);

int16_t tscGetJoinTagColIdByUid(STagCond* pTagCond, uint64_t uid);
int16_t tscGetTagColIndexById(STableMeta* pTableMeta, int16_t colId);

void tscPrintSelNodeList(SSqlObj* pSql, int32_t subClauseIndex);

bool hasMoreVnodesToTry(SSqlObj *pSql);
bool hasMoreClauseToTry(SSqlObj* pSql);

void tscFreeQueryInfo(SSqlCmd* pCmd, bool removeMeta);

void tscTryQueryNextVnode(SSqlObj *pSql, __async_cb_func_t fp);
void tscAsyncQuerySingleRowForNextVnode(void *param, TAOS_RES *tres, int numOfRows);
void tscTryQueryNextClause(SSqlObj* pSql, __async_cb_func_t fp);
int  tscSetMgmtEpSetFromCfg(const char *first, const char *second, SRpcCorEpSet *corEpSet);

bool tscSetSqlOwner(SSqlObj* pSql);
void tscClearSqlOwner(SSqlObj* pSql);
int32_t doArithmeticCalculate(SQueryNodeInfo* pQueryInfo, tFilePage* pOutput, int32_t rowSize, int32_t finalRowSize);

char*   serializeTagData(STagData* pTagData, char* pMsg);
int32_t copyTagData(STagData* dst, const STagData* src);

STableMeta* createSuperTableMeta(STableMetaMsg* pChild);
uint32_t tscGetTableMetaSize(STableMeta* pTableMeta);
CChildTableMeta* tscCreateChildMeta(STableMeta* pTableMeta);
uint32_t tscGetTableMetaMaxSize();
int32_t tscCreateTableMetaFromCChildMeta(STableMeta* pChild, const char* name);
STableMeta* tscTableMetaDup(STableMeta* pTableMeta);
SQuery* tscCreateQueryFromQueryNodeInfo(SQueryNodeInfo* pQueryNodeInfo);

void tsCreateSQLFunctionCtx(SQueryNodeInfo* pQueryInfo, SQLFunctionCtx* pCtx);
void* createQueryInfoFromQueryNode(SQueryNodeInfo* pQueryNodeInfo, SExprInfo* pExprs, STableGroupInfo* pTableGroupInfo,
                                   uint64_t* qId, char* sql);

void* malloc_throw(size_t size);
void* calloc_throw(size_t nmemb, size_t size);
char* strdup_throw(const char* str);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCUTIL_H

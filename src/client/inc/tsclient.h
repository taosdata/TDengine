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

#ifndef TDENGINE_TSCLIENT_H
#define TDENGINE_TSCLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "qAggMain.h"
#include "taos.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"
#include "tcache.h"
#include "tglobal.h"
#include "tref.h"
#include "tutil.h"

#include "qExecutor.h"
#include "qSqlparser.h"
#include "qTsbuf.h"
#include "qUtil.h"
#include "tcmdtype.h"

// forward declaration
struct SSqlInfo;

typedef void (*__async_cb_func_t)(void *param, TAOS_RES *tres, int32_t numOfRows);

typedef struct SNewVgroupInfo {
  int32_t    vgId;
  int8_t     inUse;
  int8_t     numOfEps;
  SEpAddrMsg ep[TSDB_MAX_REPLICA];
} SNewVgroupInfo;

typedef struct CChildTableMeta {
  int32_t        vgId;
  STableId       id;
  uint8_t        tableType;
  char           sTableName[TSDB_TABLE_FNAME_LEN];  // TODO: refactor super table name, not full name
  uint64_t       suid;                              // super table id
} CChildTableMeta;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
} SColumnIndex;

typedef struct SColumn {
  uint64_t     tableUid;
  int32_t      columnIndex;
  SColumnInfo  info;
} SColumn;

typedef struct SInternalField {
  TAOS_FIELD      field;
  bool            visible;
  SExprInfo      *pExpr;
} SInternalField;

typedef struct SParamInfo {
  int32_t  idx;
  uint8_t  type;
  uint8_t  timePrec;
  int16_t  bytes;
  uint32_t offset;
} SParamInfo;

typedef struct SBoundColumn {
  int32_t offset;   // all column offset value
  int32_t toffset;  // first part offset for SDataRow TODO: get offset from STSchema on future
  uint8_t valStat;  // denote if current column bound or not(0 means has val, 1 means no val)
} SBoundColumn;
typedef enum {
  VAL_STAT_HAS = 0x0,    // 0 means has val
  VAL_STAT_NONE = 0x01,  // 1 means no val
} EValStat;

typedef struct {
  uint16_t schemaColIdx;
  uint16_t boundIdx;
  uint16_t finalIdx;
} SBoundIdxInfo;

typedef enum _COL_ORDER_STATUS {
  ORDER_STATUS_UNKNOWN = 0,
  ORDER_STATUS_ORDERED = 1,
  ORDER_STATUS_DISORDERED = 2,
} EOrderStatus;
typedef struct SParsedDataColInfo {
  int16_t        numOfCols;
  int16_t        numOfBound;
  uint16_t       flen;        // TODO: get from STSchema
  uint16_t       allNullLen;  // TODO: get from STSchema
  uint16_t       extendedVarLen;
  int32_t *      boundedColumns;  // bound column idx according to schema
  SBoundColumn * cols;
  SBoundIdxInfo *colIdxInfo;
  int8_t         orderStatus;  // bound columns
} SParsedDataColInfo;

#define IS_DATA_COL_ORDERED(spd) ((spd->orderStatus) == (int8_t)ORDER_STATUS_ORDERED)

typedef struct {
  int32_t dataLen;  // len of SDataRow
  int32_t kvLen;    // len of SKVRow
} SMemRowInfo;
typedef struct {
  uint8_t      memRowType;
  uint8_t      compareStat;  // 0 unknown, 1 need compare, 2 no need
  TDRowTLenT   dataRowInitLen;
  TDRowTLenT   kvRowInitLen;
  SMemRowInfo *rowInfo;
} SMemRowBuilder;

typedef enum {
  ROW_COMPARE_UNKNOWN = 0,
  ROW_COMPARE_NEED = 1,
  ROW_COMPARE_NO_NEED = 2,
} ERowCompareStat;

int tsParseTime(SStrToken *pToken, int64_t *time, char **next, char *error, int16_t timePrec);

int  initMemRowBuilder(SMemRowBuilder *pBuilder, uint32_t nRows, uint32_t nCols, uint32_t nBoundCols,
                       int32_t allNullLen);
void destroyMemRowBuilder(SMemRowBuilder *pBuilder);

/**
 * @brief
 *
 * @param memRowType
 * @param spd
 * @param idx   the absolute bound index of columns
 * @return FORCE_INLINE
 */
static FORCE_INLINE void tscGetMemRowAppendInfo(SSchema *pSchema, uint8_t memRowType, SParsedDataColInfo *spd,
                                                int32_t idx, int32_t *toffset, int16_t *colId) {
  int32_t schemaIdx = 0;
  if (IS_DATA_COL_ORDERED(spd)) {
    schemaIdx = spd->boundedColumns[idx];
    if (isDataRowT(memRowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;  // the offset of firstPart
    } else {
      *toffset = idx * sizeof(SColIdx);  // the offset of SColIdx
    }
  } else {
    ASSERT(idx == (spd->colIdxInfo + idx)->boundIdx);
    schemaIdx = (spd->colIdxInfo + idx)->schemaColIdx;
    if (isDataRowT(memRowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;
    } else {
      *toffset = ((spd->colIdxInfo + idx)->finalIdx) * sizeof(SColIdx);
    }
  }
  *colId = pSchema[schemaIdx].colId;
}

/**
 * @brief Applicable to consume by multi-columns
 *
 * @param row
 * @param value
 * @param isCopyVarData In some scenario, the varVal is copied to row directly before calling tdAppend***ColVal()
 * @param colId
 * @param colType
 * @param idx index in SSchema
 * @param pBuilder
 * @param spd
 * @return FORCE_INLINE
 */
static FORCE_INLINE void tscAppendMemRowColVal(SMemRow row, const void *value, bool isCopyVarData, int16_t colId,
                                               int8_t colType, int32_t toffset, SMemRowBuilder *pBuilder,
                                               int32_t rowNum) {
  tdAppendMemRowColVal(row, value, isCopyVarData, colId, colType, toffset);
  if (pBuilder->compareStat == ROW_COMPARE_NEED) {
    SMemRowInfo *pRowInfo = pBuilder->rowInfo + rowNum;
    tdGetColAppendDeltaLen(value, colType, &pRowInfo->dataLen, &pRowInfo->kvLen);
  }
}

// Applicable to consume by one row
static FORCE_INLINE void tscAppendMemRowColValEx(SMemRow row, const void *value, bool isCopyVarData, int16_t colId,
                                                 int8_t colType, int32_t toffset, int32_t *dataLen, int32_t *kvLen,
                                                 uint8_t compareStat) {
  tdAppendMemRowColVal(row, value, isCopyVarData, colId, colType, toffset);
  if (compareStat == ROW_COMPARE_NEED) {
    tdGetColAppendDeltaLen(value, colType, dataLen, kvLen);
  }
}
typedef struct STableDataBlocks {
  SName       tableName;
  int8_t      tsSource;     // where does the UNIX timestamp come from, server or client
  bool        ordered;      // if current rows are ordered or not
  int64_t     vgId;         // virtual group id
  int64_t     prevTS;       // previous timestamp, recorded to decide if the records array is ts ascending
  int32_t     numOfTables;  // number of tables in current submit block
  int32_t     rowSize;      // row size for current table
  uint32_t    nAllocSize;
  uint32_t    headerSize;   // header for table info (uid, tid, submit metadata)
  uint32_t    size;
  STableMeta *pTableMeta;   // the tableMeta of current table, the table meta will be used during submit, keep a ref to avoid to be removed from cache
  char       *pData;
  bool        cloned;
  
  SParsedDataColInfo boundColumnInfo;

  // for parameter ('?') binding
  uint32_t       numOfAllocedParams;
  uint32_t       numOfParams;
  SParamInfo *   params;
  SMemRowBuilder rowBuilder;
} STableDataBlocks;

typedef struct {
  STableMeta   *pTableMeta;
  SArray       *vgroupIdList;
//  SVgroupsInfo *pVgroupsInfo;
} STableMetaVgroupInfo;

typedef struct SInsertStatementParam {
  SName      **pTableNameList;          // all involved tableMeta list of current insert sql statement.
  int32_t      numOfTables;             // number of tables in table name list
  SHashObj    *pTableBlockHashList;     // data block for each table
  SArray      *pDataBlocks;             // SArray<STableDataBlocks*>. Merged submit block for each vgroup
  int8_t       schemaAttached;          // denote if submit block is built with table schema or not
  uint8_t      payloadType;             // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  STagData     tagData;                 // NOTE: pTagData->data is used as a variant length array

  int32_t      batchSize;               // for parameter ('?') binding and batch processing
  int32_t      numOfParams;

  char         msg[512];                // error message
  uint32_t     insertType;              // insert data from [file|sql statement| bound statement]
  uint64_t     objectId;                // sql object id
  char        *sql;                     // current sql statement position
} SInsertStatementParam;

typedef enum {
  PAYLOAD_TYPE_KV = 0,
  PAYLOAD_TYPE_RAW = 1,
} EPayloadType;

#define IS_RAW_PAYLOAD(t) \
  (((int)(t)) == PAYLOAD_TYPE_RAW)  // 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert

// TODO extract sql parser supporter
typedef struct {
  int     command;
  uint8_t msgType;
  SInsertStatementParam insertParam;
  char    reserve1[3];        // fix bus error on arm32
  int32_t count;   // todo remove it
  bool    subCmd;

  char         reserve2[3];        // fix bus error on arm32
  int16_t      numOfCols;
  char         reserve3[2];        // fix bus error on arm32
  uint32_t     allocSize;
  char *       payload;
  int32_t      payloadLen;

  SHashObj    *pTableMetaMap;  // local buffer to keep the queried table meta, before validating the AST
  SQueryInfo  *pQueryInfo;
  SQueryInfo  *active;         // current active query info
  int32_t      batchSize;      // for parameter ('?') binding and batch processing
  int32_t      resColumnId;
} SSqlCmd;

typedef struct SResRec {
  int numOfRows;
  int numOfTotal;
} SResRec;

typedef struct {
  int32_t        numOfRows;                  // num of results in current retrieval
  int64_t        numOfRowsGroup;             // num of results of current group
  int64_t        numOfTotal;                 // num of total results
  int64_t        numOfClauseTotal;           // num of total result in current subclause
  char *         pRsp;
  int32_t        rspType;
  int32_t        rspLen;
  uint64_t       qId;
  int64_t        useconds;
  int64_t        offset;  // offset value from vnode during projection query of stable
  int32_t        row;
  int16_t        numOfCols;
  int16_t        precision;
  bool           completed;
  int32_t        code;
  int32_t        numOfGroups;
  SResRec *      pGroupRec;
  char *         data;
  TAOS_ROW       tsrow;
  TAOS_ROW       urow;
  int32_t*       length;  // length for each field for current row
  char **        buffer;  // Buffer used to put multibytes encoded using unicode (wchar_t)
  SColumnIndex*  pColumnIndex;

  TAOS_FIELD*           final;
  SArithmeticSupport   *pArithSup;   // support the arithmetic expression calculation on agg functions
  struct SGlobalMerger *pMerger;
} SSqlRes;

typedef struct {
  char         key[512]; 
  void         *pDnodeConn; 
} SRpcObj;

typedef struct STscObj {
  void *             signature;
  void *             pTimer;
  char               user[TSDB_USER_LEN];
  char               pass[TSDB_KEY_LEN];
  char               acctId[TSDB_ACCT_ID_LEN];
  char               db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  char               sversion[TSDB_VERSION_LEN];
  char               writeAuth : 1;
  char               superAuth : 1;
  uint32_t           connId;
  uint64_t           rid;      // ref ID returned by taosAddRef
  int64_t            hbrid;
  struct SSqlObj *   sqlList;
  struct SSqlStream *streamList;
  SRpcObj           *pRpcObj;
  SRpcCorEpSet      *tscCorMgmtEpSet;
  pthread_mutex_t    mutex;
  int32_t            numOfObj; // number of sqlObj from this tscObj
} STscObj;

typedef struct SSubqueryState {
  pthread_mutex_t mutex;
  int8_t  *states;
  int32_t  numOfSub;            // the number of total sub-queries
  uint64_t numOfRetrievedRows;  // total number of points in this query
} SSubqueryState;

typedef struct SSqlObj {
  void            *signature;
  int64_t          owner;        // owner of sql object, by which it is executed
  STscObj         *pTscObj;
  int64_t          rpcRid;
  __async_cb_func_t  fp;
  __async_cb_func_t  fetchFp;
  void            *param;
  int64_t          stime;
  uint32_t         queryId;
  void *           pStream;
  void *           pSubscription;
  char *           sqlstr;
  void *           pBuf;  // table meta buffer
  char             parseRetry;
  char             retry;
  char             maxRetry;
  SRpcEpSet        epSet;
  char             listed;
  tsem_t           rspSem;
  SSqlCmd          cmd;
  SSqlRes          res;
  bool             isBind;

  SSubqueryState   subState;
  struct SSqlObj **pSubs;

  int64_t          metaRid;
  int64_t          svgroupRid;

  int64_t          squeryLock;
  int32_t          retryReason;  // previous error code
  struct SSqlObj  *prev, *next;
  int64_t          self;
} SSqlObj;

typedef struct SSqlStream {
  SSqlObj *pSql;
  void *  cqhandle;  // stream belong to SCQContext handle
  const char* dstTable;
  uint32_t streamId;
  char     listed;
  bool     isProject;
  int16_t  precision;
  int64_t  num;  // number of computing count

  /*
   * keep the number of current result in computing,
   * the value will be set to 0 before set timer for next computing
   */
  int64_t numOfRes;

  int64_t useconds;  // total  elapsed time
  int64_t ctime;     // stream created time
  int64_t stime;     // stream next executed time
  int64_t etime;     // stream end query time, when time is larger then etime, the stream will be closed
  int64_t ltime;     // stream last row time in stream table
  SInterval interval;
  void *  pTimer;

  void (*fp)();
  void *param;

  void (*callback)(void *);  // Callback function when stream is stopped from client level
  struct SSqlStream *prev, *next;
} SSqlStream;

void tscSetStreamDestTable(SSqlStream* pStream, const char* dstTable);

int  tscAcquireRpc(const char *key, const char *user, const char *secret,void **pRpcObj);
void tscReleaseRpc(void *param);
void tscInitMsgsFp();

int tsParseSql(SSqlObj *pSql, bool initial);

void tscProcessMsgFromServer(SRpcMsg *rpcMsg, SRpcEpSet *pEpSet);
int  tscBuildAndSendRequest(SSqlObj *pSql, SQueryInfo* pQueryInfo);

int  tscRenewTableMeta(SSqlObj *pSql, int32_t tableIndex);
void tscAsyncResultOnError(SSqlObj *pSql);

void tscQueueAsyncError(void(*fp), void *param, int32_t code);

int tscProcessLocalCmd(SSqlObj *pSql);
int tscCfgDynamicOptions(char *msg);

int32_t tscTansformFuncForSTableQuery(SQueryInfo *pQueryInfo);
void    tscRestoreFuncForSTableQuery(SQueryInfo *pQueryInfo);

int32_t tscCreateResPointerInfo(SSqlRes *pRes, SQueryInfo *pQueryInfo);
void tscSetResRawPtr(SSqlRes* pRes, SQueryInfo* pQueryInfo);
void tscSetResRawPtrRv(SSqlRes* pRes, SQueryInfo* pQueryInfo, SSDataBlock* pBlock, bool convertNchar);

void handleDownstreamOperator(SSqlObj** pSqlList, int32_t numOfUpstream, SQueryInfo* px, SSqlObj* pParent);
void destroyTableNameList(SInsertStatementParam* pInsertParam);

void tscResetSqlCmd(SSqlCmd *pCmd, bool removeMeta, uint64_t id);

/**
 * free query result of the sql object
 * @param pObj
 */
void tscFreeSqlResult(SSqlObj *pSql);

void* tscCleanupTableMetaMap(SHashObj* pTableMetaMap);

/**
 * free sql object, release allocated resource
 * @param pObj
 */
void tscFreeSqlObj(SSqlObj *pSql);
void tscFreeSubobj(SSqlObj* pSql);

void tscFreeRegisteredSqlObj(void *pSql);

void tscCloseTscObj(void *pObj);

// todo move to taos? or create a new file: taos_internal.h
TAOS *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, TAOS **taos);
TAOS_RES* taos_query_h(TAOS* taos, const char *sqlstr, int64_t* res);
TAOS_RES * taos_query_ra(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param);

void waitForQueryRsp(void *param, TAOS_RES *tres, int code);

void doAsyncQuery(STscObj *pObj, SSqlObj *pSql, __async_cb_func_t fp, void *param, const char *sqlstr, size_t sqlLen);

void tscImportDataFromFile(SSqlObj *pSql);
struct SGlobalMerger* tscInitResObjForLocalQuery(int32_t numOfRes, int32_t rowLen, uint64_t id);
bool tscIsUpdateQuery(SSqlObj* pSql);
char* tscGetSqlStr(SSqlObj* pSql);
bool tscIsQueryWithLimit(SSqlObj* pSql);

bool tscHasReachLimitation(SQueryInfo *pQueryInfo, SSqlRes *pRes);
void tscSetBoundColumnInfo(SParsedDataColInfo *pColInfo, SSchema *pSchema, int32_t numOfCols);

char *tscGetErrorMsgPayload(SSqlCmd *pCmd);

int32_t tscInvalidOperationMsg(char *msg, const char *additionalInfo, const char *sql);
int32_t tscSQLSyntaxErrMsg(char* msg, const char* additionalInfo,  const char* sql);

int32_t tscValidateSqlInfo(SSqlObj *pSql, struct SSqlInfo *pInfo);

int32_t tsSetBlockInfo(SSubmitBlk *pBlocks, const STableMeta *pTableMeta, int32_t numOfRows);
extern int32_t    sentinel;
extern SHashObj  *tscVgroupMap;
extern SHashObj  *tscTableMetaMap;
extern SCacheObj *tscVgroupListBuf;

extern int   tscObjRef;
extern void *tscTmr;
extern void *tscQhandle;
extern int   tscKeepConn[];
extern int   tscRefId;
extern int   tscNumOfObj;     // number of existed sqlObj in current process.

extern int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo);
 
void tscBuildVgroupTableInfo(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, SArray* tables);
int16_t getNewResColId(SSqlCmd* pCmd);

int32_t schemaIdxCompar(const void *lhs, const void *rhs);
int32_t boundIdxCompar(const void *lhs, const void *rhs);
static FORCE_INLINE int32_t getExtendedRowSize(STableDataBlocks *pBlock) {
  ASSERT(pBlock->rowSize == pBlock->pTableMeta->tableInfo.rowSize);
  return pBlock->rowSize + TD_MEM_ROW_DATA_HEAD_SIZE + pBlock->boundColumnInfo.extendedVarLen;
}

static FORCE_INLINE void checkAndConvertMemRow(SMemRow row, int32_t dataLen, int32_t kvLen) {
  if (isDataRow(row)) {
    if (kvLen < (dataLen * KVRatioConvert)) {
      memRowSetConvert(row);
    }
  } else if (kvLen > dataLen) {
    memRowSetConvert(row);
  }
}

static FORCE_INLINE void initSMemRow(SMemRow row, uint8_t memRowType, STableDataBlocks *pBlock, int16_t nBoundCols) {
  memRowSetType(row, memRowType);
  if (isDataRowT(memRowType)) {
    dataRowSetVersion(memRowDataBody(row), pBlock->pTableMeta->sversion);
    dataRowSetLen(memRowDataBody(row), (TDRowLenT)(TD_DATA_ROW_HEAD_SIZE + pBlock->boundColumnInfo.flen));
  } else {
    ASSERT(nBoundCols > 0);
    memRowSetKvVersion(row, pBlock->pTableMeta->sversion);
    kvRowSetNCols(memRowKvBody(row), nBoundCols);
    kvRowSetLen(memRowKvBody(row), (TDRowLenT)(TD_KV_ROW_HEAD_SIZE + sizeof(SColIdx) * nBoundCols));
  }
}
/**
 * TODO: Move to tdataformat.h and refactor when STSchema available.
 *    - fetch flen and toffset from STSChema and remove param spd
 */
static FORCE_INLINE void convertToSDataRow(SMemRow dest, SMemRow src, SSchema *pSchema, int nCols,
                                           SParsedDataColInfo *spd) {
  ASSERT(isKvRow(src));
  SKVRow   kvRow = memRowKvBody(src);
  SDataRow dataRow = memRowDataBody(dest);

  memRowSetType(dest, SMEM_ROW_DATA);
  dataRowSetVersion(dataRow, memRowKvVersion(src));
  dataRowSetLen(dataRow, (TDRowLenT)(TD_DATA_ROW_HEAD_SIZE + spd->flen));

  int32_t kvIdx = 0;
  for (int i = 0; i < nCols; ++i) {
    SSchema *schema = pSchema + i;
    void *   val = tdGetKVRowValOfColEx(kvRow, schema->colId, &kvIdx);
    tdAppendDataColVal(dataRow, val != NULL ? val : getNullValue(schema->type), true, schema->type,
                       (spd->cols + i)->toffset);
  }
}

// TODO: Move to tdataformat.h and refactor when STSchema available.
static FORCE_INLINE void convertToSKVRow(SMemRow dest, SMemRow src, SSchema *pSchema, int nCols, int nBoundCols,
                                         SParsedDataColInfo *spd) {
  ASSERT(isDataRow(src));

  SDataRow dataRow = memRowDataBody(src);
  SKVRow   kvRow = memRowKvBody(dest);

  memRowSetType(dest, SMEM_ROW_KV);
  memRowSetKvVersion(kvRow, dataRowVersion(dataRow));
  kvRowSetNCols(kvRow, nBoundCols);
  kvRowSetLen(kvRow, (TDRowLenT)(TD_KV_ROW_HEAD_SIZE + sizeof(SColIdx) * nBoundCols));

  int32_t toffset = 0, kvOffset = 0;
  for (int i = 0; i < nCols; ++i) {
    if ((spd->cols + i)->valStat == VAL_STAT_HAS) {
      SSchema *schema = pSchema + i;
      toffset = (spd->cols + i)->toffset;
      void *val = tdGetRowDataOfCol(dataRow, schema->type, toffset + TD_DATA_ROW_HEAD_SIZE);
      tdAppendKvColVal(kvRow, val, true, schema->colId, schema->type, kvOffset);
      kvOffset += sizeof(SColIdx);
    }
  }
}

// TODO: Move to tdataformat.h and refactor when STSchema available.
static FORCE_INLINE void convertSMemRow(SMemRow dest, SMemRow src, STableDataBlocks *pBlock) {
  STableMeta *        pTableMeta = pBlock->pTableMeta;
  STableComInfo       tinfo = tscGetTableInfo(pTableMeta);
  SSchema *           pSchema = tscGetTableSchema(pTableMeta);
  SParsedDataColInfo *spd = &pBlock->boundColumnInfo;

  ASSERT(dest != src);

  if (isDataRow(src)) {
    // TODO: Can we use pBlock -> numOfParam directly?
    ASSERT(spd->numOfBound > 0);
    convertToSKVRow(dest, src, pSchema, tinfo.numOfColumns, spd->numOfBound, spd);
  } else {
    convertToSDataRow(dest, src, pSchema, tinfo.numOfColumns, spd);
  }
}

static bool isNullStr(SStrToken *pToken) {
  return (pToken->type == TK_NULL) || ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                                       (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0));
}

static FORCE_INLINE int32_t tscToDouble(SStrToken *pToken, double *value, char **endPtr) {
  errno = 0;
  *value = strtold(pToken->z, endPtr);

  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_ILLEGAL;
  }

  return pToken->type;
}

static uint8_t TRUE_VALUE = (uint8_t)TSDB_TRUE;
static uint8_t FALSE_VALUE = (uint8_t)TSDB_FALSE;

static FORCE_INLINE int32_t tsParseOneColumnKV(SSchema *pSchema, SStrToken *pToken, SMemRow row, char *msg, char **str,
                                               bool primaryKey, int16_t timePrec, int32_t toffset, int16_t colId,
                                               int32_t *dataLen, int32_t *kvLen, uint8_t compareStat) {
  int64_t iv;
  int32_t ret;
  char *  endptr = NULL;

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return tscInvalidOperationMsg(msg, "invalid numeric data", pToken->z);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
          if (strncmp(pToken->z, "true", pToken->n) == 0) {
            tscAppendMemRowColValEx(row, &TRUE_VALUE, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
          } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
            tscAppendMemRowColValEx(row, &FALSE_VALUE, true, colId, pSchema->type, toffset, dataLen, kvLen,
                                    compareStat);
          } else {
            return tscSQLSyntaxErrMsg(msg, "invalid bool data", pToken->z);
          }
        } else if (pToken->type == TK_INTEGER) {
          iv = strtoll(pToken->z, NULL, 10);
          tscAppendMemRowColValEx(row, ((iv == 0) ? &FALSE_VALUE : &TRUE_VALUE), true, colId, pSchema->type, toffset,
                                  dataLen, kvLen, compareStat);
        } else if (pToken->type == TK_FLOAT) {
          double dv = strtod(pToken->z, NULL);
          tscAppendMemRowColValEx(row, ((dv == 0) ? &FALSE_VALUE : &TRUE_VALUE), true, colId, pSchema->type, toffset,
                                  dataLen, kvLen, compareStat);
        } else {
          return tscInvalidOperationMsg(msg, "invalid bool data", pToken->z);
        }
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid tinyint data", pToken->z);
        } else if (!IS_VALID_TINYINT(iv)) {
          return tscInvalidOperationMsg(msg, "data overflow", pToken->z);
        }

        uint8_t tmpVal = (uint8_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_UTINYINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned tinyint data", pToken->z);
        } else if (!IS_VALID_UTINYINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned tinyint data overflow", pToken->z);
        }

        uint8_t tmpVal = (uint8_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_SMALLINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid smallint data", pToken->z);
        } else if (!IS_VALID_SMALLINT(iv)) {
          return tscInvalidOperationMsg(msg, "smallint data overflow", pToken->z);
        }

        int16_t tmpVal = (int16_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_USMALLINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned smallint data", pToken->z);
        } else if (!IS_VALID_USMALLINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned smallint data overflow", pToken->z);
        }

        uint16_t tmpVal = (uint16_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_INT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid int data", pToken->z);
        } else if (!IS_VALID_INT(iv)) {
          return tscInvalidOperationMsg(msg, "int data overflow", pToken->z);
        }

        int32_t tmpVal = (int32_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_UINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned int data", pToken->z);
        } else if (!IS_VALID_UINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned int data overflow", pToken->z);
        }

        uint32_t tmpVal = (uint32_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid bigint data", pToken->z);
        } else if (!IS_VALID_BIGINT(iv)) {
          return tscInvalidOperationMsg(msg, "bigint data overflow", pToken->z);
        }

        tscAppendMemRowColValEx(row, &iv, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned bigint data", pToken->z);
        } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
          return tscInvalidOperationMsg(msg, "unsigned bigint data overflow", pToken->z);
        }

        uint64_t tmpVal = (uint64_t)iv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidOperationMsg(msg, "illegal float data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) ||
            isnan(dv)) {
          return tscInvalidOperationMsg(msg, "illegal float data", pToken->z);
        }

        float tmpVal = (float)dv;
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (isNullStr(pToken)) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidOperationMsg(msg, "illegal double data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
          return tscInvalidOperationMsg(msg, "illegal double data", pToken->z);
        }

        tscAppendMemRowColValEx(row, &dv, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_BINARY:
      // binary data cannot be null-terminated char string, otherwise the last char of the string is lost
      if (pToken->type == TK_NULL) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {  // too long values will return invalid sql, not be truncated automatically
        if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {  // todo refactor
          return tscInvalidOperationMsg(msg, "string data overflow", pToken->z);
        }
        // STR_WITH_SIZE_TO_VARSTR(payload, pToken->z, pToken->n);
        char *rowEnd = memRowEnd(row);
        STR_WITH_SIZE_TO_VARSTR(rowEnd, pToken->z, pToken->n);
        tscAppendMemRowColValEx(row, rowEnd, false, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_NCHAR:
      if (pToken->type == TK_NULL) {
        tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
        int32_t output = 0;
        char *  rowEnd = memRowEnd(row);
        if (!taosMbsToUcs4(pToken->z, pToken->n, (char *)varDataVal(rowEnd), pSchema->bytes - VARSTR_HEADER_SIZE,
                           &output)) {
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), "%s", strerror(errno));
          return tscInvalidOperationMsg(msg, buf, pToken->z);
        }
        varDataSetLen(rowEnd, output);
        tscAppendMemRowColValEx(row, rowEnd, false, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (pToken->type == TK_NULL) {
        if (primaryKey) {
          // When building SKVRow primaryKey, we should not skip even with NULL value.
          int64_t tmpVal = 0;
          tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
        } else {
          tscAppendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                  compareStat);
        }
      } else {
        int64_t tmpVal;
        if (tsParseTime(pToken, &tmpVal, str, msg, timePrec) != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid timestamp", pToken->z);
        }
        tscAppendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

#ifdef __cplusplus
}
#endif

#endif

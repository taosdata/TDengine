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
  bool    hasVal;  // denote if current column has bound or not
  int32_t offset;  // all column offset value
} SBoundColumn;

typedef struct SParsedDataColInfo {
  int16_t         numOfCols;
  int16_t         numOfBound;
  int32_t        *boundedColumns;
  SBoundColumn   *cols;
} SParsedDataColInfo;

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

  SParsedDataColInfo  boundColumnInfo;

  // for parameter ('?') binding
  uint32_t    numOfAllocedParams;
  uint32_t    numOfParams;
  SParamInfo *params;
} STableDataBlocks;

typedef struct {
  STableMeta   *pTableMeta;
  SVgroupsInfo *pVgroupInfo;
} STableMetaVgroupInfo;

typedef struct SInsertStatementParam {
  SName      **pTableNameList;          // all involved tableMeta list of current insert sql statement.
  int32_t      numOfTables;             // number of tables in table name list
  SHashObj    *pTableBlockHashList;     // data block for each table
  SArray      *pDataBlocks;             // SArray<STableDataBlocks*>. Merged submit block for each vgroup
  int8_t       schemaAttached;          // denote if submit block is built with table schema or not
  STagData     tagData;                 // NOTE: pTagData->data is used as a variant length array

  int32_t      batchSize;               // for parameter ('?') binding and batch processing
  int32_t      numOfParams;

  char         msg[512];                // error message
  uint32_t     insertType;              // insert data from [file|sql statement| bound statement]
  uint64_t     objectId;                // sql object id
  char        *sql;                     // current sql statement position
} SInsertStatementParam;

// TODO extract sql parser supporter
typedef struct {
  int     command;
  uint8_t msgType;
  SInsertStatementParam insertParam;
  char    reserve1[3];        // fix bus error on arm32
  int32_t count;   // todo remove it

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

void tscResetSqlCmd(SSqlCmd *pCmd, bool removeMeta);

/**
 * free query result of the sql object
 * @param pObj
 */
void tscFreeSqlResult(SSqlObj *pSql);

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
extern SHashObj  *tscTableMetaInfo;

extern int   tscObjRef;
extern void *tscTmr;
extern void *tscQhandle;
extern int   tscKeepConn[];
extern int   tscRefId;
extern int   tscNumOfObj;     // number of existed sqlObj in current process.

extern int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo);

void tscBuildVgroupTableInfo(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, SArray* tables);
int16_t getNewResColId(SSqlCmd* pCmd);

#ifdef __cplusplus
}
#endif

#endif

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
#include "tcmdtype.h"

// forward declaration
struct SSqlInfo;
struct SLocalMerger;

// data source from sql string or from file
enum {
  DATA_FROM_SQL_STRING = 1,
  DATA_FROM_DATA_FILE  = 2,
};

typedef void (*__async_cb_func_t)(void *param, TAOS_RES *tres, int32_t numOfRows);

typedef struct STableComInfo {
  uint8_t numOfTags;
  uint8_t precision;
  int16_t numOfColumns;
  int32_t rowSize;
} STableComInfo;

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
  char           sTableName[TSDB_TABLE_FNAME_LEN];  //super table name, not full name
} CChildTableMeta;

typedef struct STableMeta {
  int32_t        vgId;
  STableId       id;
  uint8_t        tableType;
  char           sTableName[TSDB_TABLE_FNAME_LEN];
  int16_t        sversion;
  int16_t        tversion;
  STableComInfo  tableInfo;
  SSchema        schema[];  // if the table is TSDB_CHILD_TABLE, schema is acquired by super table meta info
} STableMeta;

typedef struct STableMetaInfo {
  STableMeta   *pTableMeta;      // table meta, cached in client side and acquired by name
  SVgroupsInfo *vgroupList;
  SArray       *pVgroupTables;   // SArray<SVgroupTableInfo>
  
  /*
   * 1. keep the vgroup index during the multi-vnode super table projection query
   * 2. keep the vgroup index for multi-vnode insertion
   */
  int32_t       vgroupIndex;
  SName         name;
  char          aliasName[TSDB_TABLE_NAME_LEN];    // alias name of table specified in query sql
  SArray       *tagColList;                        // SArray<SColumn*>, involved tag columns
} STableMetaInfo;

/* the structure for sql function in select clause */
typedef struct SSqlExpr {
  char      aliasName[TSDB_COL_NAME_LEN];  // as aliasName
  SColIndex colInfo;
  uint64_t  uid;            // refactor use the pointer
  int16_t   functionId;     // function id in aAgg array
  int16_t   resType;        // return value type
  int16_t   resBytes;       // length of return value
  int32_t   interBytes;     // inter result buffer size
  int16_t   numOfParams;    // argument value of each function
  tVariant  param[3];       // parameters are not more than 3
  int32_t   offset;         // sub result column value of arithmetic expression.
  int16_t   resColId;       // result column id
} SSqlExpr;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
} SColumnIndex;

typedef struct SInternalField {
  TAOS_FIELD      field;
  bool            visible;
  SExprInfo      *pArithExprInfo;
  SSqlExpr       *pSqlExpr;
} SInternalField;

typedef struct SFieldInfo {
  int16_t      numOfOutput;   // number of column in result
  TAOS_FIELD*  final;
  SArray      *internalField; // SArray<SInternalField>
} SFieldInfo;

typedef struct SColumn {
  SColumnIndex       colIndex;
  int32_t            numOfFilters;
  SColumnFilterInfo *filterInfo;
} SColumn;

typedef struct SCond {
  uint64_t uid;
  int32_t  len;  // length of tag query condition data
  char *   cond;
} SCond;

typedef struct SJoinNode {
  uint64_t uid;
  int16_t  tagColId;
  SArray*  tsJoin;
  SArray*  tagJoin;
} SJoinNode;

typedef struct SJoinInfo {
  bool      hasJoin;  
  SJoinNode*  joinTables[TSDB_MAX_JOIN_TABLE_NUM];
} SJoinInfo;

typedef struct STagCond {
  // relation between tbname list and query condition, including : TK_AND or TK_OR
  int16_t relType;

  // tbname query condition, only support tbname query condition on one table
  SCond tbnameCond;

  // join condition, only support two tables join currently
  SJoinInfo joinInfo;

  // for different table, the query condition must be seperated
  SArray *pCond;
} STagCond;

typedef struct SParamInfo {
  int32_t  idx;
  char     type;
  uint8_t  timePrec;
  int16_t  bytes;
  uint32_t offset;
} SParamInfo;

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

  // for parameter ('?') binding
  uint32_t    numOfAllocedParams;
  uint32_t    numOfParams;
  SParamInfo *params;
} STableDataBlocks;

typedef struct SQueryInfo {
  int16_t          command;       // the command may be different for each subclause, so keep it seperately.
  uint32_t         type;          // query/insert type
  STimeWindow      window;        // the whole query time window

  SInterval        interval;      // tumble time window
  SSessionWindow   sessionWindow; // session time window

  SSqlGroupbyExpr  groupbyExpr;   // group by tags info
  SArray *         colList;       // SArray<SColumn*>
  SFieldInfo       fieldsInfo;
  SArray *         exprList;      // SArray<SSqlExpr*>
  SLimitVal        limit;
  SLimitVal        slimit;
  STagCond         tagCond;
  SOrderVal        order;
  int16_t          fillType;      // final result fill type
  int16_t          numOfTables;
  STableMetaInfo **pTableMetaInfo;
  struct STSBuf   *tsBuf;
  int64_t *        fillVal;       // default value for fill
  char *           msg;           // pointer to the pCmd->payload to keep error message temporarily
  int64_t          clauseLimit;   // limit for current sub clause

  int64_t          prjOffset;     // offset value in the original sql expression, only applied at client side
  int64_t          vgroupLimit;    // table limit in case of super table projection query + global order + limit

  int32_t          udColumnId;    // current user-defined constant output field column id, monotonically decreases from TSDB_UD_COLUMN_INDEX
  int16_t          resColumnId;   // result column id
  bool             distinctTag;   // distinct tag or not
  int32_t          round;         // 0/1/....
  int32_t          bufLen;
  char*            buf;
} SQueryInfo;

typedef struct {
  int     command;
  uint8_t msgType;
  char    reserve1[3];        // fix bus error on arm32
  bool    autoCreated;        // create table if it is not existed during retrieve table meta in mnode

  union {
    int32_t count;
    int32_t numOfTablesInSubmit;
  };

  uint32_t     insertType;   // TODO remove it
  int32_t      clauseIndex;  // index of multiple subclause query

  char *       curSql;       // current sql, resume position of sql after parsing paused
  int8_t       parseFinished;
  char    reserve2[3];        // fix bus error on arm32

  int16_t      numOfCols;
  char    reserve3[2];        // fix bus error on arm32
  uint32_t     allocSize;
  char *       payload;
  int32_t      payloadLen;
  SQueryInfo **pQueryInfo;
  int32_t      numOfClause;
  int32_t      batchSize;    // for parameter ('?') binding and batch processing
  int32_t      numOfParams;

  int8_t       dataSourceType;     // load data from file or not
  char    reserve4[3];        // fix bus error on arm32
  int8_t       submitSchema;   // submit block is built with table schema
  char    reserve5[3];        // fix bus error on arm32
  STagData     tagData;        // NOTE: pTagData->data is used as a variant length array

  SName      **pTableNameList; // all involved tableMeta list of current insert sql statement.
  int32_t      numOfTables;

  SHashObj    *pTableBlockHashList;     // data block for each table
  SArray      *pDataBlocks;    // SArray<STableDataBlocks*>. Merged submit block for each vgroup
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

  SArithmeticSupport   *pArithSup;   // support the arithmetic expression calculation on agg functions
  struct SLocalMerger  *pLocalMerger;
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
int  tscProcessSql(SSqlObj *pSql);

int  tscRenewTableMeta(SSqlObj *pSql, int32_t tableIndex);
void tscAsyncResultOnError(SSqlObj *pSql);

void tscQueueAsyncError(void(*fp), void *param, int32_t code);

int tscProcessLocalCmd(SSqlObj *pSql);
int tscCfgDynamicOptions(char *msg);

int32_t tscTansformFuncForSTableQuery(SQueryInfo *pQueryInfo);
void    tscRestoreFuncForSTableQuery(SQueryInfo *pQueryInfo);

int32_t tscCreateResPointerInfo(SSqlRes *pRes, SQueryInfo *pQueryInfo);
void tscSetResRawPtr(SSqlRes* pRes, SQueryInfo* pQueryInfo);

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
void tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen);
bool tscIsUpdateQuery(SSqlObj* pSql);
char* tscGetSqlStr(SSqlObj* pSql);
bool tscIsQueryWithLimit(SSqlObj* pSql);

bool tscHasReachLimitation(SQueryInfo *pQueryInfo, SSqlRes *pRes);

char *tscGetErrorMsgPayload(SSqlCmd *pCmd);

int32_t tscInvalidSQLErrMsg(char *msg, const char *additionalInfo, const char *sql);
int32_t tscSQLSyntaxErrMsg(char* msg, const char* additionalInfo,  const char* sql);

int32_t tscToSQLCmd(SSqlObj *pSql, struct SSqlInfo *pInfo);

static FORCE_INLINE void tscGetResultColumnChr(SSqlRes* pRes, SFieldInfo* pFieldInfo, int32_t columnIndex, int32_t offset) {
  SInternalField* pInfo = (SInternalField*) TARRAY_GET_ELEM(pFieldInfo->internalField, columnIndex);

  int32_t type = pInfo->field.type;
  int32_t bytes = pInfo->field.bytes;

  char* pData = pRes->data + (int32_t)(offset * pRes->numOfRows + bytes * pRes->row);
  UNUSED(pData);

//   user defined constant value output columns
  if (pInfo->pSqlExpr != NULL && TSDB_COL_IS_UD_COL(pInfo->pSqlExpr->colInfo.flag)) {
    if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
      pData = pInfo->pSqlExpr->param[1].pz;
      pRes->length[columnIndex] = pInfo->pSqlExpr->param[1].nLen;
      pRes->tsrow[columnIndex] = (pInfo->pSqlExpr->param[1].nType == TSDB_DATA_TYPE_NULL) ? NULL : (unsigned char*)pData;
    } else {
      assert(bytes == tDataTypes[type].bytes);

      pRes->tsrow[columnIndex] = isNull(pData, type) ? NULL : (unsigned char*)&pInfo->pSqlExpr->param[1].i64;
      pRes->length[columnIndex] = bytes;
    }
  } else {
    if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
      int32_t realLen = varDataLen(pData);
      assert(realLen <= bytes - VARSTR_HEADER_SIZE);

      pRes->tsrow[columnIndex] = (isNull(pData, type)) ? NULL : (unsigned char*)((tstr *)pData)->data;
      if (realLen < pInfo->pSqlExpr->resBytes - VARSTR_HEADER_SIZE) {  // todo refactor
        *(pData + realLen + VARSTR_HEADER_SIZE) = 0;
      }

      pRes->length[columnIndex] = realLen;
    } else {
      assert(bytes == tDataTypes[type].bytes);

      pRes->tsrow[columnIndex] = isNull(pData, type) ? NULL : (unsigned char*)pData;
      pRes->length[columnIndex] = bytes;
    }
  }
}

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
int16_t getNewResColId(SQueryInfo* pQueryInfo);

#ifdef __cplusplus
}
#endif

#endif

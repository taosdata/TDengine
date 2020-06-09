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
#include "taos.h"
#include "taosmsg.h"
#include "tglobalcfg.h"
#include "tlog.h"
#include "tscCache.h"
#include "tscSQLParser.h"
#include "tsdb.h"
#include "tsqlfunction.h"
#include "tutil.h"

#define TSC_GET_RESPTR_BASE(res, _queryinfo, col) (res->data + ((_queryinfo)->fieldsInfo.pSqlExpr[col]->offset) * res->numOfRows)

// forward declaration
struct SSqlInfo;

typedef struct SSqlGroupbyExpr {
  int16_t     tableIndex;
  int16_t     numOfGroupCols;
  SColIndexEx columnInfo[TSDB_MAX_TAGS];  // group by columns information
  int16_t     orderIndex;                 // order by column index
  int16_t     orderType;                  // order by type: asc/desc
} SSqlGroupbyExpr;

typedef struct SMeterMetaInfo {
  SMeterMeta * pMeterMeta;   // metermeta
  SMetricMeta *pMetricMeta;  // metricmeta

  /*
   * 1. keep the vnode index during the multi-vnode super table projection query
   * 2. keep the vnode index for multi-vnode insertion
   */
  int32_t vnodeIndex;
  char    name[TSDB_METER_ID_LEN + 1];         // table(super table) name
  char    aliasName[TSDB_METER_ID_LEN + 1];    // alias name
  int16_t numOfTags;                           // total required tags in query, including groupby tags
  int16_t tagColumnIndex[TSDB_MAX_TAGS];       // clause + tag projection
} SMeterMetaInfo;

/* the structure for sql function in select clause */
typedef struct SSqlExpr {
  char        aliasName[TSDB_COL_NAME_LEN + 1];  // as aliasName
  SColIndexEx colInfo;
  int64_t     uid;            // refactor use the pointer
  int16_t     functionId;     // function id in aAgg array
  int16_t     resType;        // return value type
  int16_t     resBytes;       // length of return value
  int16_t     interResBytes;  // inter result buffer size
  int16_t     numOfParams;    // argument value of each function
  tVariant    param[3];       // parameters are not more than 3
  int32_t     offset;         // sub result column value of arithmetic expression.
} SSqlExpr;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
} SColumnIndex;

typedef struct SFieldInfo {
  int16_t     numOfOutputCols;  // number of column in result
  int16_t     numOfAlloc;       // allocated size
  TAOS_FIELD *pFields;
//  short *     pOffset;

  /*
   * define if this column is belong to the queried result, it may be add by parser to faciliate
   * the query process
   *
   * NOTE: these hidden columns always locate at the end of the output columns
   */
  bool *  pVisibleCols;
  int32_t numOfHiddenCols;   // the number of column not belongs to the queried result columns
  SSqlFunctionExpr** pExpr;  // used for aggregation arithmetic express,such as count(*)+count(*)
  SSqlExpr** pSqlExpr;
} SFieldInfo;

typedef struct SSqlExprInfo {
  int16_t    numOfAlloc;
  int16_t    numOfExprs;
  SSqlExpr** pExprs;
} SSqlExprInfo;

typedef struct SColumnBase {
  SColumnIndex       colIndex;
  int32_t            numOfFilters;
  SColumnFilterInfo *filterInfo;
} SColumnBase;

typedef struct SColumnBaseInfo {
  int16_t      numOfAlloc;
  int16_t      numOfCols;
  SColumnBase *pColList;
} SColumnBaseInfo;

struct SLocalReducer;

typedef struct SCond {
  uint64_t uid;
  char *   cond;
} SCond;

typedef struct SJoinNode {
  char     meterId[TSDB_METER_ID_LEN];
  uint64_t uid;
  int16_t  tagCol;
} SJoinNode;

typedef struct SJoinInfo {
  bool      hasJoin;
  SJoinNode left;
  SJoinNode right;
} SJoinInfo;

typedef struct STagCond {
  // relation between tbname list and query condition, including : TK_AND or TK_OR
  int16_t relType;

  // tbname query condition, only support tbname query condition on one table
  SCond tbnameCond;

  // join condition, only support two tables join currently
  SJoinInfo joinInfo;

  // for different table, the query condition must be seperated
  SCond   cond[TSDB_MAX_JOIN_TABLE_NUM];
  int16_t numOfTagCond;
} STagCond;

typedef struct SParamInfo {
  int32_t  idx;
  char     type;
  uint8_t  timePrec;
  short    bytes;
  uint32_t offset;
} SParamInfo;

typedef struct STableDataBlocks {
  char    meterId[TSDB_METER_ID_LEN];
  int8_t  tsSource;     // where does the UNIX timestamp come from, server or client
  bool    ordered;      // if current rows are ordered or not
  int64_t vgid;         // virtual group id
  int64_t prevTS;       // previous timestamp, recorded to decide if the records array is ts ascending
  int32_t numOfMeters;  // number of tables in current submit block

  int32_t  rowSize;  // row size for current table
  uint32_t nAllocSize;
  uint32_t headerSize;  // header for metadata (submit metadata)
  uint32_t size;

  /*
   * the metermeta for current table, the metermeta will be used during submit stage, keep a ref
   * to avoid it to be removed from cache
   */
  SMeterMeta *pMeterMeta;

  union {
    char *filename;
    char *pData;
  };

  // for parameter ('?') binding
  uint32_t    numOfAllocedParams;
  uint32_t    numOfParams;
  SParamInfo *params;
} STableDataBlocks;

typedef struct SDataBlockList {
  int32_t            idx;
  uint32_t           nSize;
  uint32_t           nAlloc;
  char *             userParam; /* user assigned parameters for async query */
  void *             udfp;      /* user defined function pointer, used in async model */
  STableDataBlocks **pData;
} SDataBlockList;

typedef struct SQueryInfo {
  int16_t  command;  // the command may be different for each subclause, so keep it seperately.
  uint32_t type;     // query/insert/import type
  char     slidingTimeUnit;

  int64_t         etime, stime;
  int64_t         intervalTime;  // aggregation time interval
  int64_t         slidingTime;   // sliding window in mseconds
  SSqlGroupbyExpr groupbyExpr;   // group by tags info

  SColumnBaseInfo  colList;
  SFieldInfo       fieldsInfo;
  SSqlExprInfo     exprsInfo;
  SLimitVal        limit;
  SLimitVal        slimit;
  STagCond         tagCond;
  SOrderVal        order;
  int16_t          interpoType;  // interpolate type
  int16_t          numOfTables;
  SMeterMetaInfo **pMeterInfo;
  struct STSBuf *  tsBuf;
  int64_t *        defaultVal;   // default value for interpolation
  char *           msg;          // pointer to the pCmd->payload to keep error message temporarily
  int64_t          clauseLimit;  // limit for current sub clause

  // offset value in the original sql expression, NOT sent to virtual node, only applied at client side
  int64_t prjOffset;
} SQueryInfo;

// data source from sql string or from file
enum {
  DATA_FROM_SQL_STRING = 1,
  DATA_FROM_DATA_FILE = 2,
};

typedef struct {
  int     command;
  uint8_t msgType;

  union {
    bool   existsCheck;     // check if the table exists or not
    bool   inStream;        // denote if current sql is executed in stream or not
    bool   createOnDemand;  // if the table is missing, on-the-fly create it. during getmeterMeta
    int8_t dataSourceType;  // load data from file or not
  };

  union {
    int32_t count;
    int32_t numOfTablesInSubmit;
  };

  int32_t  clauseIndex;  // index of multiple subclause query
  int8_t   isParseFinish;
  short    numOfCols;
  uint32_t allocSize;
  char *   payload;
  int      payloadLen;

  SQueryInfo **pQueryInfo;
  int32_t      numOfClause;

  // submit data blocks branched according to vnode
  SDataBlockList *pDataBlocks;

  // for parameter ('?') binding and batch processing
  int32_t batchSize;
  int32_t numOfParams;
} SSqlCmd;

typedef struct SResRec {
  int numOfRows;
  int numOfTotal;
} SResRec;

struct STSBuf;

typedef struct {
  uint8_t       code;
  int64_t       numOfRows;                  // num of results in current retrieved
  int64_t       numOfTotal;                 // num of total results
  int64_t       numOfTotalInCurrentClause;  // num of total result in current subclause
  char *        pRsp;
  int           rspType;
  int           rspLen;
  uint64_t      qhandle;
  int64_t       uid;
  int64_t       useconds;
  int64_t       offset;  // offset value from vnode during projection query of stable
  int           row;
  int16_t       numOfCols;
  int16_t       precision;
  int32_t       numOfGroups;
  SResRec *     pGroupRec;
  char *        data;
  void **       tsrow;
  char **       buffer;  // Buffer used to put multibytes encoded using unicode (wchar_t)
  SColumnIndex *pColumnIndex;

  struct SLocalReducer *pLocalReducer;
} SSqlRes;

typedef struct _tsc_obj {
  void *           signature;
  void *           pTimer;
  char             mgmtIp[TSDB_USER_LEN];
  uint16_t         mgmtPort;
  char             user[TSDB_USER_LEN];
  char             pass[TSDB_KEY_LEN];
  char             acctId[TSDB_DB_NAME_LEN];
  char             db[TSDB_METER_ID_LEN];
  char             sversion[TSDB_VERSION_LEN];
  char             writeAuth : 1;
  char             superAuth : 1;
  struct _sql_obj *pSql;
  struct _sql_obj *pHb;
  struct _sql_obj *sqlList;
  struct _sstream *streamList;
  pthread_mutex_t  mutex;
} STscObj;

typedef struct _sql_obj {
  void *   signature;
  STscObj *pTscObj;
  void (*fp)();
  void (*fetchFp)();
  void *            param;
  uint32_t          ip;
  short             vnode;
  int64_t           stime;
  uint32_t          queryId;
  void *            thandle;
  void *            pStream;
  void *            pSubscription;
  char *            sqlstr;
  char              retry;
  char              maxRetry;
  uint8_t           index;
  char              freed : 4;
  char              listed : 4;
  tsem_t            rspSem;
  tsem_t            emptyRspSem;
  SSqlCmd           cmd;
  SSqlRes           res;
  uint8_t           numOfSubs;
  char *            asyncTblPos;
  void *            pTableHashList;
  struct _sql_obj **pSubs;
  struct _sql_obj * prev, *next;
} SSqlObj;

typedef struct _sstream {
  SSqlObj *pSql;
  uint32_t streamId;
  char     listed;
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
  int64_t interval;
  int64_t slidingTime;
  int16_t precision;
  void *  pTimer;

  void (*fp)();
  void *param;

  void (*callback)(void *);  // Callback function when stream is stopped from client level
  struct _sstream *prev, *next;
} SSqlStream;

typedef struct {
  char     numOfIps;
  uint32_t ip[TSDB_MAX_MGMT_IPS];
  char     ipstr[TSDB_MAX_MGMT_IPS][TSDB_IPv4ADDR_LEN];
} SIpStrList;

// tscSql API
int tsParseSql(SSqlObj *pSql, bool multiVnodeInsertion);

void tscInitMsgs();
extern int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo);

void *tscProcessMsgFromServer(char *msg, void *ahandle, void *thandle);
int   tscProcessSql(SSqlObj *pSql);

void tscAsyncInsertMultiVnodesProxy(void *param, TAOS_RES *tres, int numOfRows);

int  tscRenewMeterMeta(SSqlObj *pSql, char *meterId);
void tscQueueAsyncRes(SSqlObj *pSql);

void tscQueueAsyncError(void(*fp), void *param);

int tscProcessLocalCmd(SSqlObj *pSql);
int tscCfgDynamicOptions(char *msg);
int taos_retrieve(TAOS_RES *res);

/*
 * transfer function for metric query in stream computing, the function need to be change
 * before send query message to vnode
 */
int32_t tscTansformSQLFunctionForSTableQuery(SQueryInfo *pQueryInfo);
void    tscRestoreSQLFunctionForMetricQuery(SQueryInfo *pQueryInfo);

int32_t tscCreateResPointerInfo(SSqlRes *pRes, SQueryInfo *pQueryInfo);
void    tscDestroyResPointerInfo(SSqlRes *pRes);

void tscFreeSqlCmdData(SSqlCmd *pCmd);
void tscFreeResData(SSqlObj *pSql);

/**
 * free query result of the sql object
 * @param pObj
 */
void tscFreeSqlResult(SSqlObj *pSql);

/**
 * only free part of resources allocated during query.
 * Note: this function is multi-thread safe.
 * @param pObj
 */
void tscFreeSqlObjPartial(SSqlObj *pObj);

/**
 * free sql object, release allocated resource
 * @param pObj  Free metric/meta information, dynamically allocated payload, and
 * response buffer, object itself
 */
void tscFreeSqlObj(SSqlObj *pObj);

void tscCloseTscObj(STscObj *pObj);

void tscProcessMultiVnodesInsert(SSqlObj *pSql);
void tscProcessMultiVnodesInsertFromFile(SSqlObj *pSql);
void tscKillMetricQuery(SSqlObj *pSql);
void tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen);
bool tscIsUpdateQuery(STscObj *pObj);
bool tscHasReachLimitation(SQueryInfo *pQueryInfo, SSqlRes *pRes);

char *tscGetErrorMsgPayload(SSqlCmd *pCmd);

int32_t tscInvalidSQLErrMsg(char *msg, const char *additionalInfo, const char *sql);

// transfer SSqlInfo to SqlCmd struct
int32_t tscToSQLCmd(SSqlObj *pSql, struct SSqlInfo *pInfo);

void tscQueueAsyncFreeResult(SSqlObj *pSql);

extern void *     pVnodeConn;
extern void *     pTscMgmtConn;
extern void *     tscCacheHandle;
extern uint8_t    globalCode;
extern int        slaveIndex;
extern void *     tscTmr;
extern void *     tscConnCache;
extern void *     tscQhandle;
extern int        tscKeepConn[];
extern int        tsInsertHeadSize;
extern int        tscNumOfThreads;
extern SIpStrList tscMgmtIpList;

typedef void (*__async_cb_func_t)(void *param, TAOS_RES *tres, int numOfRows);

#ifdef __cplusplus
}
#endif

#endif

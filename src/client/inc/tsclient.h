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

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

#define TSC_GET_RESPTR_BASE(res, cmd, col, ord)                     \
  ((res->data + tscFieldInfoGetOffset(cmd, col) * res->numOfRows) + \
   (1 - ord.order) * (res->numOfRows - 1) * tscFieldInfoGetField(cmd, col)->bytes)

enum _sql_cmd {
  TSDB_SQL_SELECT,
  TSDB_SQL_FETCH,
  TSDB_SQL_INSERT,

  TSDB_SQL_MGMT,  // the SQL below is for mgmt node
  TSDB_SQL_CREATE_DB,
  TSDB_SQL_CREATE_TABLE,
  TSDB_SQL_DROP_DB,
  TSDB_SQL_DROP_TABLE,
  TSDB_SQL_CREATE_ACCT,
  TSDB_SQL_CREATE_USER,
  TSDB_SQL_DROP_ACCT,  // 10
  TSDB_SQL_DROP_USER,
  TSDB_SQL_ALTER_USER,
  TSDB_SQL_ALTER_ACCT,
  TSDB_SQL_ALTER_TABLE,
  TSDB_SQL_ALTER_DB,
  TSDB_SQL_CREATE_MNODE,
  TSDB_SQL_DROP_MNODE,
  TSDB_SQL_CREATE_DNODE,
  TSDB_SQL_DROP_DNODE,
  TSDB_SQL_CFG_DNODE,  // 20
  TSDB_SQL_CFG_MNODE,
  TSDB_SQL_SHOW,
  TSDB_SQL_RETRIEVE,
  TSDB_SQL_KILL_QUERY,
  TSDB_SQL_KILL_STREAM,
  TSDB_SQL_KILL_CONNECTION,

  TSDB_SQL_READ,  // SQL below is for read operation
  TSDB_SQL_CONNECT,
  TSDB_SQL_USE_DB,
  TSDB_SQL_META,  // 30
  TSDB_SQL_METRIC,
  TSDB_SQL_MULTI_META,
  TSDB_SQL_HB,

  TSDB_SQL_LOCAL,  // SQL below for client local
  TSDB_SQL_DESCRIBE_TABLE,
  TSDB_SQL_RETRIEVE_METRIC,
  TSDB_SQL_METRIC_JOIN_RETRIEVE,
  TSDB_SQL_RETRIEVE_TAGS,
  /*
   * build empty result instead of accessing dnode to fetch result
   * reset the client cache
   */
  TSDB_SQL_RETRIEVE_EMPTY_RESULT,

  TSDB_SQL_RESET_CACHE,  // 40
  TSDB_SQL_SERV_STATUS,
  TSDB_SQL_CURRENT_DB,
  TSDB_SQL_SERV_VERSION,
  TSDB_SQL_CLI_VERSION,
  TSDB_SQL_CURRENT_USER,
  TSDB_SQL_CFG_LOCAL,

  TSDB_SQL_MAX
};

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
  SMeterMeta * pMeterMeta;                     // metermeta
  SMetricMeta *pMetricMeta;                    // metricmeta
  
  /*
   * 1. keep the vnode index during the multi-vnode super table projection query
   * 2. keep the vnode index for multi-vnode insertion
   */
  int32_t      vnodeIndex;
  char         name[TSDB_METER_ID_LEN + 1];    // table(super table) name
  int16_t      numOfTags;                      // total required tags in query, including groupby tags
  int16_t      tagColumnIndex[TSDB_MAX_TAGS];  // clause + tag projection
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
} SSqlExpr;

typedef struct SFieldInfo {
  int16_t     numOfOutputCols;  // number of column in result
  int16_t     numOfAlloc;       // allocated size
  TAOS_FIELD *pFields;
  short *     pOffset;

  /*
   * define if this column is belong to the queried result, it may be add by parser to faciliate
   * the query process
   *
   * NOTE: these hidden columns always locate at the end of the output columns
   */
  bool *  pVisibleCols;
  int32_t numOfHiddenCols;  // the number of column not belongs to the queried result columns
} SFieldInfo;

typedef struct SSqlExprInfo {
  int16_t   numOfAlloc;
  int16_t   numOfExprs;
  SSqlExpr *pExprs;
} SSqlExprInfo;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
} SColumnIndex;

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

// todo move to utility
typedef struct SString {
  int32_t alloc;
  int32_t n;
  char *  z;
} SString;

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
  char   meterId[TSDB_METER_ID_LEN];
  int8_t tsSource;        // where does the UNIX timestamp come from, server or client
  bool   ordered;         // if current rows are ordered or not
  int64_t vgid;           // virtual group id
  int64_t prevTS;         // previous timestamp, recorded to decide if the records array is ts ascending
  int32_t numOfMeters;    // number of tables in current submit block

  int32_t  rowSize;       // row size for current table
  uint32_t nAllocSize;
  uint32_t size;
  
  /*
   * the metermeta for current table, the metermeta will be used during submit stage, keep a ref
   * to avoid it to be removed from cache
   */
  SMeterMeta* pMeterMeta;
  
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

typedef struct {
  SOrderVal order;
  int       command;
  int       count;  // TODO refactor

  union {
    bool   existsCheck;  // check if the table exists
    int8_t showType;     // show command type
  };

  int8_t          isInsertFromFile;  // load data from file or not
  bool            import;            // import/insert type
  uint8_t         msgType;
  uint16_t        type;  // query type
  char            intervalTimeUnit;
  int64_t         etime, stime;
  int64_t         nAggTimeInterval;  // aggregation time interval
  int64_t         nSlidingTime;      // sliding window in mseconds
  SSqlGroupbyExpr groupbyExpr;       // group by tags info

  /*
   * use to keep short request msg and error msg, in such case, SSqlCmd->payload == SSqlCmd->ext;
   * create table/query/insert operations will exceed the TSDB_SQLCMD_SIZE.
   *
   * In such cases, allocate the memory dynamically, and need to free the memory
   */
  uint32_t        allocSize;
  char *          payload;
  int             payloadLen;
  short           numOfCols;
  SColumnBaseInfo colList;
  SFieldInfo      fieldsInfo;
  SSqlExprInfo    exprsInfo;
  SLimitVal       limit;
  SLimitVal       slimit;
  int64_t         globalLimit;
  STagCond        tagCond;
  int16_t         interpoType;  // interpolate type
  int16_t         numOfTables;

  // submit data blocks branched according to vnode
  SDataBlockList * pDataBlocks;
  SMeterMetaInfo **pMeterInfo;
  struct STSBuf *  tsBuf;
  // todo use dynamic allocated memory for defaultVal
  int64_t defaultVal[TSDB_MAX_COLUMNS];  // default value for interpolation

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
  uint8_t               code;
  int                   numOfRows;   // num of results in current retrieved
  int                   numOfTotal;  // num of total results
  char *                pRsp;
  int                   rspType;
  int                   rspLen;
  uint64_t              qhandle;
  int64_t               uid;
  int64_t               useconds;
  int64_t               offset;  // offset value from vnode during projection query of stable
  int                   row;
  int16_t               numOfnchar;
  int16_t               precision;
  int32_t               numOfGroups;
  SResRec *             pGroupRec;
  char *                data;
  short *               bytes;
  void **               tsrow;
  char **               buffer;  // Buffer used to put multibytes encoded using unicode (wchar_t)
  struct SLocalReducer *pLocalReducer;
  SColumnIndex *        pColumnIndex;
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
  char*             asyncTblPos;
  void*             pTableHashList;
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
int tsParseSql(SSqlObj *pSql, char *acct, char *db, bool multiVnodeInsertion);

void  tscInitMsgs();
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
int32_t tscTansformSQLFunctionForMetricQuery(SSqlCmd *pCmd);
void    tscRestoreSQLFunctionForMetricQuery(SSqlCmd *pCmd);

void tscClearSqlMetaInfoForce(SSqlCmd *pCmd);

int32_t tscCreateResPointerInfo(SSqlCmd *pCmd, SSqlRes *pRes);
void    tscDestroyResPointerInfo(SSqlRes *pRes);

void tscFreeSqlCmdData(SSqlCmd *pCmd);

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

void    tscProcessMultiVnodesInsert(SSqlObj *pSql);
void    tscProcessMultiVnodesInsertForFile(SSqlObj *pSql);
void    tscKillMetricQuery(SSqlObj *pSql);
void    tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen);
bool    tscIsUpdateQuery(STscObj *pObj);
bool    tscHasReachLimitation(SSqlObj* pSql);

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

#ifdef __cplusplus
}
#endif

#endif

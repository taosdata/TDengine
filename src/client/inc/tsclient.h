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
#include "tsdb.h"
#include "tsql.h"
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
  TSDB_SQL_CREATE_PNODE,
  TSDB_SQL_DROP_PNODE,
  TSDB_SQL_CFG_PNODE,  // 20
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
  TSDB_SQL_HB,

  TSDB_SQL_LOCAL,  // SQL below for client local
  TSDB_SQL_DESCRIBE_TABLE,
  TSDB_SQL_RETRIEVE_METRIC,
  TSDB_SQL_RETRIEVE_TAGS,
  TSDB_SQL_RETRIEVE_EMPTY_RESULT,  // build empty result instead of accessing
                                   // dnode to fetch result
  TSDB_SQL_RESET_CACHE,            // reset the client cache
  TSDB_SQL_CFG_LOCAL,

  TSDB_SQL_MAX
};

// forward declaration
struct SSqlInfo;

typedef struct SSqlGroupbyExpr {
  int16_t numOfGroupbyCols;
  int16_t tagIndex[TSDB_MAX_TAGS]; /* group by columns information */
  int16_t orderIdx;                /* order by column index        */
  int16_t orderType;               /* order by type: asc/desc      */
} SSqlGroupbyExpr;

/* the structure for sql function in select clause */
typedef struct SSqlExpr {
  char aliasName[TSDB_COL_NAME_LEN + 1];  // as aliasName

  SColIndex colInfo;
  int16_t   sqlFuncId;  // function id in aAgg array

  int16_t resType;      // return value type
  int16_t resBytes;     // length of return value

  int16_t  numOfParams;  // argument value of each function
  tVariant param[3];     // parameters are not more than 3
} SSqlExpr;

typedef struct SFieldInfo {
  int16_t     numOfOutputCols;  // number of column in result
  int16_t     numOfAlloc;       // allocated size
  TAOS_FIELD *pFields;
  short *     pOffset;
} SFieldInfo;

typedef struct SSqlExprInfo {
  int16_t   numOfAlloc;
  int16_t   numOfExprs;
  SSqlExpr *pExprs;
} SSqlExprInfo;

typedef struct SColumnBase {
  int16_t colIndex;

  /* todo refactor: the following data is belong to one struct */
  int16_t filterOn; /* denote if the filter is active       */
  int16_t lowerRelOptr;
  int16_t upperRelOptr;
  int16_t filterOnBinary; /* denote if current column is binary   */

  union {
    struct {
      int64_t lowerBndi;
      int64_t upperBndi;
    };
    struct {
      double lowerBndd;
      double upperBndd;
    };
    struct {
      int64_t pz;
      int64_t len;
    };
  };
} SColumnBase;

typedef struct SColumnsInfo {
  int16_t      numOfAlloc;
  int16_t      numOfCols;
  SColumnBase *pColList;
} SColumnsInfo;

struct SLocalReducer;

typedef struct STagCond {
  int32_t len;
  int32_t allocSize;
  int16_t type;
  char *  pData;
} STagCond;

typedef struct STableDataBlocks {
  char        meterId[TSDB_METER_ID_LEN];
  int64_t     vgid;
  int64_t     size;

  int64_t     prevTS;
  bool        ordered;

  int32_t     numOfMeters;
  int32_t     rowSize;
  uint32_t    nAllocSize;
  union {
    char *filename;
    char *pData;
  };
} STableDataBlocks;

typedef struct SDataBlockList {
  int32_t               idx;
  int32_t               nSize;
  int32_t               nAlloc;
  char *                userParam; /* user assigned parameters for async query */
  void *                udfp;      /* user defined function pointer, used in async model */
  STableDataBlocks **pData;
} SDataBlockList;

typedef struct {
  char            name[TSDB_METER_ID_LEN];
  SOrderVal       order;
  int             command;
  int             count;
  int16_t         isInsertFromFile;  // load data from file or not
  int16_t         metricQuery;       // metric query or not
  bool            existsCheck;
  char            msgType;
  char            type;
  char            intervalTimeUnit;
  int64_t         etime;
  int64_t         stime;
  int64_t         nAggTimeInterval;  // aggregation time interval
  int64_t         nSlidingTime;      // sliding window in mseconds
  SSqlGroupbyExpr groupbyExpr;       // group by tags info

  /*
   * use to keep short request msg and error msg, in such case, SSqlCmd->payload == SSqlCmd->ext;
   * create table/query/insert operations will exceed the TSDB_SQLCMD_SIZE.
   *
   * In such cases, allocate the memory dynamically, and need to free the memory
   */
  uint32_t     allocSize;
  char *       payload;
  int          payloadLen;
  short        numOfCols;
  SColumnsInfo colList;
  SFieldInfo   fieldsInfo;
  SSqlExprInfo exprsInfo;
  int16_t      numOfReqTags;  // total required tags in query, inlcuding groupby clause + tag projection
  int16_t      tagColumnIndex[TSDB_MAX_TAGS + 1];
  SLimitVal    limit;
  int64_t      globalLimit;
  SLimitVal    glimit;
  STagCond     tagCond;
  int16_t      vnodeIdx;     // vnode index in pMetricMeta for metric query
  int16_t      interpoType;  // interpolate type

  SDataBlockList *pDataBlocks;  // submit data blocks branched according to vnode
  SMeterMeta *    pMeterMeta;   // metermeta
  SMetricMeta *   pMetricMeta;  // metricmeta

  // todo use dynamic allocated memory for defaultVal
  int64_t defaultVal[TSDB_MAX_COLUMNS];  // default value for interpolation
} SSqlCmd;

typedef struct SResRec {
  int numOfRows;
  int numOfTotal;
} SResRec;

typedef struct {
  uint8_t  code;
  int      numOfRows;   // num of results in current retrieved
  int      numOfTotal;  // num of total results
  char *   pRsp;
  int      rspType;
  int      rspLen;
  uint64_t qhandle;
  int64_t  useconds;
  int64_t  offset;  // offset value from vnode during projection query of stable
  int      row;
  int16_t  numOfnchar;
  int16_t  precision;
  int32_t  numOfGroups;
  SResRec *pGroupRec;
  char *   data;
  short *  bytes;
  void **  tsrow;

  // Buffer used to put multibytes encoded using unicode (wchar_t)
  char **               buffer;
  struct SLocalReducer *pLocalReducer;
} SSqlRes;

typedef struct _tsc_obj {
  void *           signature;
  void *           pTimer;
  char             mgmtIp[TSDB_USER_LEN];
  short            mgmtPort;
  char             user[TSDB_USER_LEN];
  char             pass[TSDB_KEY_LEN];
  char             acctId[TSDB_DB_NAME_LEN];
  char             db[TSDB_DB_NAME_LEN];
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
  void *   param;
  uint32_t ip;
  short    vnode;
  int64_t  stime;
  uint32_t queryId;
  void *   thandle;
  void *   pStream;
  char *   sqlstr;
  char     retry;
  char     maxRetry;
  char     index;
  char     freed : 4;
  char     listed : 4;
  sem_t    rspSem;
  sem_t    emptyRspSem;

  SSqlCmd cmd;
  SSqlRes res;

  char              numOfSubs;
  struct _sql_obj **pSubs;
  struct _sql_obj * prev, *next;
} SSqlObj;

typedef struct _sstream {
  SSqlObj *pSql;
  uint32_t streamId;
  char     listed;
  int64_t  num;  // number of computing count

  /*
   * bookmark the current number of result in computing,
   * the value will be set to 0 before set timer for next computing
   */
  int64_t numOfRes;

  int64_t useconds;  // total  elapsed time
  int64_t ctime;     // stream created time
  int64_t stime;     // stream next executed time
  int64_t etime;     // stream end query time, when time is larger then etime, the
                     // stream will be closed
  int64_t interval;
  int64_t slidingTime;
  int16_t precision;
  void *  pTimer;

  void (*fp)();
  void *param;

  // Call backfunction when stream is stopped from client level
  void (*callback)(void *);
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
int tscProcessSql(SSqlObj *pSql);
int tscGetMeterMeta(SSqlObj *pSql, char *meterId);
int tscGetMeterMetaEx(SSqlObj *pSql, char *meterId, bool createIfNotExists);

void tscAsyncInsertMultiVnodesProxy(void *param, TAOS_RES *tres, int numOfRows);

int tscRenewMeterMeta(SSqlObj *pSql, char *meterId);
void tscQueueAsyncRes(SSqlObj *pSql);
int tscGetMetricMeta(SSqlObj *pSql, char *meterId);
void tscQueueAsyncError(void(*fp), void *param);

int tscProcessLocalCmd(SSqlObj *pSql);
int tscCfgDynamicOptions(char *msg);
int taos_retrieve(TAOS_RES *res);

/*
 * transfer function for metric query in stream computing, the function need to be change
 * before send query message to vnode
 */
void tscTansformSQLFunctionForMetricQuery(SSqlCmd *pCmd);
void tscRestoreSQLFunctionForMetricQuery(SSqlCmd *pCmd);

/**
 * release both metric/meter meta information
 * @param pCmd  SSqlCmd object that contains the metric/meter meta info
 */
void tscClearSqlMetaInfo(SSqlCmd *pCmd);

void tscClearSqlMetaInfoForce(SSqlCmd *pCmd);

int32_t tscCreateResPointerInfo(SSqlCmd *pCmd, SSqlRes *pRes);
void tscDestroyResPointerInfo(SSqlRes *pRes);

void tscfreeSqlCmdData(SSqlCmd *pCmd);

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

//
// support functions for async metric query.
// we declare them as global visible functions, because we need them to check if a
// failed async query in tscMeterMetaCallBack is a metric query or not.

// expr: (fp == tscRetrieveDataRes or fp == tscRetrieveFromVnodeCallBack)
// If a query is async query, we simply abort current query process, instead of continuing
//
void tscRetrieveDataRes(void *param, TAOS_RES *tres, int numOfRows);
void tscRetrieveFromVnodeCallBack(void *param, TAOS_RES *tres, int numOfRows);

void tscProcessMultiVnodesInsert(SSqlObj *pSql);
void tscProcessMultiVnodesInsertForFile(SSqlObj *pSql);
void tscKillMetricQuery(SSqlObj *pSql);
void tscInitResObjForLocalQuery(SSqlObj *pObj, int32_t numOfRes, int32_t rowLen);
int32_t tscBuildResultsForEmptyRetrieval(SSqlObj *pSql);

// transfer SSqlInfo to SqlCmd struct
int32_t tscToSQLCmd(SSqlObj *pSql, struct SSqlInfo *pInfo);

void tscQueueAsyncFreeResult(SSqlObj *pSql);

extern void *   pVnodeConn;
extern void *   pTscMgmtConn;
extern void *   tscCacheHandle;
extern uint8_t  globalCode;
extern int      slaveIndex;
extern void *   tscTmr;
extern void *   tscConnCache;
extern void *   tscQhandle;
extern int      tscKeepConn[];
extern int      tsInsertHeadSize;
extern int      tscNumOfThreads;
extern uint32_t tsServerIp;

#ifdef __cplusplus
}
#endif

#endif

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

#ifndef TDENGINE_CLIENTINT_H
#define TDENGINE_CLIENTINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "parser.h"
#include "planner.h"
#include "query.h"
#include "taos.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "tmsgtype.h"
#include "trpc.h"

#include "tconfig.h"

#define CHECK_CODE_GOTO(expr, label) \
  do {                               \
    code = expr;                     \
    if (TSDB_CODE_SUCCESS != code) { \
      goto label;                    \
    }                                \
  } while (0)

#define ERROR_MSG_BUF_DEFAULT_SIZE 512
#define HEARTBEAT_INTERVAL         1500  // ms

enum {
  RES_TYPE__QUERY = 1,
  RES_TYPE__TMQ,
};

#define TD_RES_QUERY(res) (*(int8_t*)res == RES_TYPE__QUERY)
#define TD_RES_TMQ(res)   (*(int8_t*)res == RES_TYPE__TMQ)

typedef struct SAppInstInfo SAppInstInfo;

typedef struct {
  void*         param;
  SClientHbReq* req;
} SHbConnInfo;

typedef struct {
  char* key;
  // statistics
  int32_t reportCnt;
  int32_t connKeyCnt;
  int64_t reportBytes;  // not implemented
  int64_t startTime;
  // ctl
  SRWLatch lock;  // lock is used in serialization
  // connection
  SAppInstInfo* pAppInstInfo;
  // info
  SHashObj* activeInfo;  // hash<SClientHbKey, SClientHbReq>
  SHashObj* connInfo;    // hash<SClientHbKey, SHbConnInfo>
} SAppHbMgr;

typedef int32_t (*FHbRspHandle)(SAppHbMgr* pAppHbMgr, SClientHbRsp* pRsp);

typedef int32_t (*FHbReqHandle)(SClientHbKey* connKey, void* param, SClientHbReq* req);

typedef struct {
  int8_t inited;
  // ctl
  int8_t        threadStop;
  TdThread      thread;
  TdThreadMutex lock;       // used when app init and cleanup
  SArray*       appHbMgrs;  // SArray<SAppHbMgr*> one for each cluster
  FHbReqHandle  reqHandle[CONN_TYPE__MAX];
  FHbRspHandle  rspHandle[CONN_TYPE__MAX];
} SClientHbMgr;

typedef struct SQueryExecMetric {
  int64_t start;   // start timestamp, us
  int64_t parsed;  // start to parse, us
  int64_t send;    // start to send to server, us
  int64_t rsp;     // receive response from server, us
} SQueryExecMetric;

typedef struct SInstanceSummary {
  uint64_t numOfInsertsReq;
  uint64_t numOfInsertRows;
  uint64_t insertElapsedTime;
  uint64_t insertBytes;  // submit to tsdb since launched.

  uint64_t fetchBytes;
  uint64_t queryElapsedTime;
  uint64_t numOfSlowQueries;
  uint64_t totalRequests;
  uint64_t currentRequests;  // the number of SRequestObj
} SInstanceSummary;

typedef struct SHeartBeatInfo {
  void* pTimer;  // timer, used to send request msg to mnode
} SHeartBeatInfo;

struct SAppInstInfo {
  int64_t          numOfConns;
  SCorEpSet        mgmtEp;
  SInstanceSummary summary;
  SList*           pConnList;  // STscObj linked list
  int64_t          clusterId;
  void*            pTransporter;
  SAppHbMgr*       pAppHbMgr;
};

typedef struct SAppInfo {
  int64_t       startTime;
  char          appName[TSDB_APP_NAME_LEN];
  char*         ep;
  int32_t       pid;
  int32_t       numOfThreads;
  SHashObj*     pInstMap;
  TdThreadMutex mutex;
} SAppInfo;

typedef struct STscObj {
  char          user[TSDB_USER_LEN];
  char          pass[TSDB_PASSWORD_LEN];
  char          db[TSDB_DB_FNAME_LEN];
  char          ver[128];
  int8_t        connType;
  int32_t       acctId;
  uint32_t      connId;
  uint64_t      id;         // ref ID returned by taosAddRef
  TdThreadMutex mutex;      // used to protect the operation on db
  int32_t       numOfReqs;  // number of sqlObj bound to this connection
  SAppInstInfo* pAppInfo;
  SHashObj*     pRequests;
} STscObj;

typedef struct SResultColumn {
  union {
    char*    nullbitmap;  // bitmap, one bit for each item in the list
    int32_t* offset;
  };
  char* pData;
} SResultColumn;

typedef struct SReqResultInfo {
  const char*    pRspMsg;
  const char*    pData;
  TAOS_FIELD*    fields;      // todo, column names are not needed.
  TAOS_FIELD*    userFields;  // the fields info that return to user
  uint32_t       numOfCols;
  int32_t*       length;
  char**         convertBuf;
  TAOS_ROW       row;
  SResultColumn* pCol;
  uint32_t       numOfRows;
  uint64_t       totalRows;
  uint32_t       current;
  bool           completed;
  int32_t        precision;
  int32_t        payloadLen;
} SReqResultInfo;

typedef struct SRequestSendRecvBody {
  tsem_t             rspSem;  // not used now
  void*              fp;
  SDataBuf           requestMsg;
  int64_t            queryJob;  // query job, created according to sql query DAG.
  struct SQueryPlan* pDag;      // the query dag, generated according to the sql statement.
  SReqResultInfo     resInfo;
} SRequestSendRecvBody;

typedef struct {
  int8_t         resType;
  char           topic[TSDB_TOPIC_FNAME_LEN];
  int32_t        vgId;
  SSchemaWrapper schema;
  int32_t        resIter;
  SMqDataBlkRsp  rsp;
  SReqResultInfo resInfo;
} SMqRspObj;

typedef struct SRequestObj {
  int8_t               resType;  // query or tmq
  uint64_t             requestId;
  int32_t              type;  // request type
  STscObj*             pTscObj;
  char*                pDb;     // current database string
  char*                sqlstr;  // sql string
  int32_t              sqlLen;
  int64_t              self;
  char*                msgBuf;
  int32_t              msgBufLen;
  int32_t              code;
  SArray*              dbList;
  SArray*              tableList;
  SQueryExecMetric     metric;
  SRequestSendRecvBody body;
} SRequestObj;

void*   doFetchRows(SRequestObj* pRequest, bool setupOneRowPtr, bool convertUcs4);
void    doSetOneRowPtr(SReqResultInfo* pResultInfo);
void    setResPrecision(SReqResultInfo* pResInfo, int32_t precision);
int32_t setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp, bool convertUcs4);
void    setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols);
void    doFreeReqResultInfo(SReqResultInfo* pResInfo);

static FORCE_INLINE SReqResultInfo* tmqGetCurResInfo(TAOS_RES* res) {
  SMqRspObj* msg = (SMqRspObj*)res;
  return (SReqResultInfo*)&msg->resInfo;
}

static FORCE_INLINE SReqResultInfo* tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4) {
  SMqRspObj* msg = (SMqRspObj*)res;
  msg->resIter++;
  if (msg->resIter < msg->rsp.blockNum) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(msg->rsp.blockData, msg->resIter);
    if (msg->rsp.withSchema) {
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(msg->rsp.blockSchema, msg->resIter);
      setResSchemaInfo(&msg->resInfo, pSW->pSchema, pSW->nCols);
      taosMemoryFreeClear(msg->resInfo.row);
      taosMemoryFreeClear(msg->resInfo.pCol);
      taosMemoryFreeClear(msg->resInfo.length);
      taosMemoryFreeClear(msg->resInfo.convertBuf);
    }
    setQueryResultFromRsp(&msg->resInfo, pRetrieve, convertUcs4);
    return &msg->resInfo;
  }
  return NULL;
}

static FORCE_INLINE SReqResultInfo* tscGetCurResInfo(TAOS_RES* res) {
  if (TD_RES_QUERY(res)) return &(((SRequestObj*)res)->body.resInfo);
  return tmqGetCurResInfo(res);
}

extern SAppInfo appInfo;
extern int32_t  clientReqRefPool;
extern int32_t  clientConnRefPool;

extern int (*handleRequestRspFp[TDMT_MAX])(void*, const SDataBuf* pMsg, int32_t code);
int           genericRspCallback(void* param, const SDataBuf* pMsg, int32_t code);
SMsgSendInfo* buildMsgInfoImpl(SRequestObj* pReqObj);

void*    createTscObj(const char* user, const char* auth, const char* db, int32_t connType, SAppInstInfo* pAppInfo);
void     destroyTscObj(void* pObj);
STscObj* acquireTscObj(int64_t rid);
int32_t  releaseTscObj(int64_t rid);

uint64_t generateRequestId();

void*        createRequest(STscObj* pObj, __taos_async_fn_t fp, void* param, int32_t type);
void         destroyRequest(SRequestObj* pRequest);
SRequestObj* acquireRequest(int64_t rid);
int32_t      releaseRequest(int64_t rid);

char* getDbOfConnection(STscObj* pObj);
void  setConnectionDB(STscObj* pTscObj, const char* db);
void  resetConnectDB(STscObj* pTscObj);

int taos_options_imp(TSDB_OPTION option, const char* str);

void* openTransporter(const char* user, const char* auth, int32_t numOfThreads);

bool persistConnForSpecificMsg(void* parenct, tmsg_t msgType);
void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet);

void initMsgHandleFp();

TAOS* taos_connect_internal(const char* ip, const char* user, const char* pass, const char* auth, const char* db,
                            uint16_t port, int connType);

int32_t parseSql(SRequestObj* pRequest, bool topicQuery, SQuery** pQuery, SStmtCallback* pStmtCb);

int32_t getPlan(SRequestObj* pRequest, SQuery* pQuery, SQueryPlan** pPlan, SArray* pNodeList);

int32_t buildRequest(STscObj* pTscObj, const char* sql, int sqlLen, SRequestObj** pRequest);

// --- heartbeat
// global, called by mgmt
int  hbMgrInit();
void hbMgrCleanUp();
int  hbHandleRsp(SClientHbBatchRsp* hbRsp);

// cluster level
SAppHbMgr* appHbMgrInit(SAppInstInfo* pAppInstInfo, char* key);
void       appHbMgrCleanup(void);

// conn level
int  hbRegisterConn(SAppHbMgr* pAppHbMgr, int64_t tscRefId, int64_t clusterId, int8_t connType);
void hbDeregisterConn(SAppHbMgr* pAppHbMgr, SClientHbKey connKey);

int hbAddConnInfo(SAppHbMgr* pAppHbMgr, SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen);

// --- mq
void hbMgrInitMqHbRspHandle();

SRequestObj* launchQueryImpl(SRequestObj* pRequest, SQuery* pQuery, int32_t code, bool keepQuery, void** res);
int32_t      getQueryPlan(SRequestObj* pRequest, SQuery* pQuery, SArray** pNodeList);
int32_t      scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList, void** res);
int32_t      refreshMeta(STscObj* pTscObj, SRequestObj* pRequest);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTINT_H

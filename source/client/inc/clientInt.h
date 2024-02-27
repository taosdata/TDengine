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

#include "catalog.h"
#include "parser.h"
#include "planner.h"
#include "query.h"
#include "taos.h"
#include "tcommon.h"
#include "tmisce.h"
#include "tdef.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "tmsgtype.h"
#include "trpc.h"

#include "tconfig.h"

#define ERROR_MSG_BUF_DEFAULT_SIZE 512
#define HEARTBEAT_INTERVAL         1500  // ms

enum {
  RES_TYPE__QUERY = 1,
  RES_TYPE__TMQ,
  RES_TYPE__TMQ_META,
  RES_TYPE__TMQ_METADATA,
};

#define SHOW_VARIABLES_RESULT_COLS       3
#define SHOW_VARIABLES_RESULT_FIELD1_LEN (TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE)
#define SHOW_VARIABLES_RESULT_FIELD2_LEN (TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE)
#define SHOW_VARIABLES_RESULT_FIELD3_LEN (TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE)

#define TD_RES_QUERY(res)        (*(int8_t*)res == RES_TYPE__QUERY)
#define TD_RES_TMQ(res)          (*(int8_t*)res == RES_TYPE__TMQ)
#define TD_RES_TMQ_META(res)     (*(int8_t*)res == RES_TYPE__TMQ_META)
#define TD_RES_TMQ_METADATA(res) (*(int8_t*)res == RES_TYPE__TMQ_METADATA)

typedef struct SAppInstInfo SAppInstInfo;

typedef struct {
  char*   key;
  int32_t idx;
  // statistics
  int32_t reportCnt;
  int32_t connKeyCnt;
  int8_t  connHbFlag;   // 0 init, 1 send req, 2 get resp
  int64_t reportBytes;  // not implemented
  int64_t startTime;
  // ctl
  SRWLatch      lock;  // lock is used in serialization
  SAppInstInfo* pAppInstInfo;
  SHashObj*     activeInfo;  // hash<SClientHbKey, SClientHbReq>
} SAppHbMgr;

typedef int32_t (*FHbRspHandle)(SAppHbMgr* pAppHbMgr, SClientHbRsp* pRsp);

typedef int32_t (*FHbReqHandle)(SClientHbKey* connKey, void* param, SClientHbReq* req);

typedef struct {
  int8_t  inited;
  int64_t appId;
  // ctl
  int8_t        threadStop;
  int8_t        quitByKill;
  TdThread      thread;
  TdThreadMutex lock;       // used when app init and cleanup
  SHashObj*     appSummary;
  SHashObj*     appHbHash;  // key: clusterId
  SArray*       appHbMgrs;  // SArray<SAppHbMgr*> one for each cluster
  FHbReqHandle  reqHandle[CONN_TYPE__MAX];
  FHbRspHandle  rspHandle[CONN_TYPE__MAX];
} SClientHbMgr;

typedef struct SQueryExecMetric {
  int64_t start;        // start timestamp, us
  int64_t ctgStart;     // start to parse, us
  int64_t execStart;     // start to parse, us

  int64_t parseCostUs;   
  int64_t ctgCostUs;
  int64_t analyseCostUs;
  int64_t planCostUs;
  int64_t execCostUs;
} SQueryExecMetric;

struct SAppInstInfo {
  int64_t            numOfConns;
  SCorEpSet          mgmtEp;
  int32_t            totalDnodes;
  int32_t            onlineDnodes;
  TdThreadMutex      qnodeMutex;
  SArray*            pQnodeList;
  SAppClusterSummary summary;
  SList*             pConnList;  // STscObj linked list
  uint64_t           clusterId;
  void*              pTransporter;
  SAppHbMgr*         pAppHbMgr;
  char*              instKey;
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

typedef struct {
  int32_t            ver;
  void*              param;
  __taos_notify_fn_t fp;
} STscNotifyInfo;

typedef STscNotifyInfo SPassInfo;

typedef struct {
  int64_t            ver;
  void*              param;
  __taos_notify_fn_t fp;
} SWhiteListInfo;

typedef struct STscObj {
  char           user[TSDB_USER_LEN];
  char           pass[TSDB_PASSWORD_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  char           sVer[TSDB_VERSION_LEN];
  char           sDetailVer[128];
  int8_t         sysInfo;
  int8_t         connType;
  int8_t         dropped;
  int8_t         biMode;
  int32_t        acctId;
  uint32_t       connId;
  int32_t        appHbMgrIdx;
  int64_t        id;         // ref ID returned by taosAddRef
  TdThreadMutex  mutex;      // used to protect the operation on db
  int32_t        numOfReqs;  // number of sqlObj bound to this connection
  int32_t        authVer;
  SAppInstInfo*  pAppInfo;
  SHashObj*      pRequests;
  SPassInfo      passInfo;
  SWhiteListInfo whiteListInfo;
  STscNotifyInfo userDroppedInfo;
} STscObj;

typedef struct STscDbg {
  bool memEnable;
} STscDbg;

typedef struct SResultColumn {
  union {
    char*    nullbitmap;  // bitmap, one bit for each item in the list
    int32_t* offset;
  };
  char* pData;
} SResultColumn;

typedef struct SReqResultInfo {
  SExecResult    execRes;
  const char*    pRspMsg;
  const char*    pData;
  TAOS_FIELD*    fields;      // todo, column names are not needed.
  TAOS_FIELD*    userFields;  // the fields info that return to user
  uint32_t       numOfCols;
  int32_t*       length;
  char**         convertBuf;
  TAOS_ROW       row;
  SResultColumn* pCol;
  uint64_t       numOfRows; // from int32_t change to int64_t
  uint64_t       totalRows;
  uint64_t       current;
  bool           localResultFetched;
  bool           completed;
  int32_t        precision;
  bool           convertUcs4;
  int32_t        payloadLen;
  char*          convertJson;
} SReqResultInfo;

typedef struct SRequestSendRecvBody {
  tsem_t            rspSem;  // not used now
  __taos_async_fn_t queryFp;
  __taos_async_fn_t fetchFp;
  EQueryExecMode    execMode;
  void*             interParam;
  SDataBuf          requestMsg;
  int64_t           queryJob;  // query job, created according to sql query DAG.
  int32_t           subplanNum;
  SReqResultInfo    resInfo;
} SRequestSendRecvBody;

typedef struct {
  int8_t         resType;
  char           topic[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  int32_t        vgId;
  SSchemaWrapper schema;
  int32_t        resIter;
  SReqResultInfo resInfo;
  SMqDataRsp     rsp;
} SMqRspObj;

typedef struct {
  int8_t     resType;
  char       topic[TSDB_TOPIC_FNAME_LEN];
  char       db[TSDB_DB_FNAME_LEN];
  int32_t    vgId;
  SMqMetaRsp metaRsp;
} SMqMetaRspObj;

typedef struct {
  int8_t         resType;
  char           topic[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  int32_t        vgId;
  SSchemaWrapper schema;
  int32_t        resIter;
  SReqResultInfo resInfo;
  STaosxRsp      rsp;
} SMqTaosxRspObj;

typedef struct SReqRelInfo {
  uint64_t userRefId;
  uint64_t prevRefId;
  uint64_t nextRefId;
} SReqRelInfo;

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
  SArray*              targetTableList;
  SQueryExecMetric     metric;
  SRequestSendRecvBody body;
  int32_t              stmtType;
  bool                 syncQuery;     // todo refactor: async query object
  bool                 stableQuery;   // todo refactor
  bool                 validateOnly;  // todo refactor
  bool                 parseOnly;
  bool                 killed;
  bool                 inRetry;
  bool                 isSubReq;
  bool                 inCallback;
  uint32_t             prevCode;  // previous error code: todo refactor, add update flag for catalog
  uint32_t             retry;
  int64_t              allocatorRefId;
  SQuery*              pQuery;
  void*                pPostPlan;
  SReqRelInfo          relation;
  void*                pWrapper;
  SMetaData            parseMeta;
  char*                effectiveUser;
} SRequestObj;

typedef struct SSyncQueryParam {
  tsem_t       sem;
  SRequestObj* pRequest;
  void*        userParam;
} SSyncQueryParam;

void* doAsyncFetchRows(SRequestObj* pRequest, bool setupOneRowPtr, bool convertUcs4);
void* doFetchRows(SRequestObj* pRequest, bool setupOneRowPtr, bool convertUcs4);

void    doSetOneRowPtr(SReqResultInfo* pResultInfo);
void    setResPrecision(SReqResultInfo* pResInfo, int32_t precision);
int32_t setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp, bool convertUcs4,
                              bool freeAfterUse);
int32_t setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows,
                         bool convertUcs4);
void    setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols);
void    doFreeReqResultInfo(SReqResultInfo* pResInfo);
int32_t transferTableNameList(const char* tbList, int32_t acctId, char* dbName, SArray** pReq);
void    syncCatalogFn(SMetaData* pResult, void* param, int32_t code);

TAOS_RES* taosQueryImpl(TAOS* taos, const char* sql, bool validateOnly);
TAOS_RES* taosQueryImplWithReqid(TAOS* taos, const char* sql, bool validateOnly, int64_t reqid);

void taosAsyncQueryImpl(uint64_t connId, const char* sql, __taos_async_fn_t fp, void* param, bool validateOnly);
void taosAsyncQueryImplWithReqid(uint64_t connId, const char* sql, __taos_async_fn_t fp, void* param, bool validateOnly,
                                 int64_t reqid);
void taosAsyncFetchImpl(SRequestObj *pRequest, __taos_async_fn_t fp, void *param);
int32_t clientParseSql(void* param, const char* dbName, const char* sql, bool parseOnly, const char* effectiveUser, SParseSqlRes* pRes);
void syncQueryFn(void* param, void* res, int32_t code);

int32_t getVersion1BlockMetaSize(const char* p, int32_t numOfCols);

static FORCE_INLINE SReqResultInfo* tmqGetCurResInfo(TAOS_RES* res) {
  SMqRspObj* msg = (SMqRspObj*)res;
  return (SReqResultInfo*)&msg->resInfo;
}

SReqResultInfo* tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4);

static FORCE_INLINE SReqResultInfo* tscGetCurResInfo(TAOS_RES* res) {
  if (TD_RES_QUERY(res)) return &(((SRequestObj*)res)->body.resInfo);
  return tmqGetCurResInfo(res);
}

extern SAppInfo appInfo;
extern int32_t  clientReqRefPool;
extern int32_t  clientConnRefPool;
extern int32_t  timestampDeltaLimit;
extern int64_t  lastClusterId;

__async_send_cb_fn_t getMsgRspHandle(int32_t msgType);

SMsgSendInfo* buildMsgInfoImpl(SRequestObj* pReqObj);

void*    createTscObj(const char* user, const char* auth, const char* db, int32_t connType, SAppInstInfo* pAppInfo);
void     destroyTscObj(void* pObj);
STscObj* acquireTscObj(int64_t rid);
int32_t  releaseTscObj(int64_t rid);
void     destroyAppInst(SAppInstInfo* pAppInfo);

uint64_t generateRequestId();

void*        createRequest(uint64_t connId, int32_t type, int64_t reqid);
void         destroyRequest(SRequestObj* pRequest);
SRequestObj* acquireRequest(int64_t rid);
int32_t      releaseRequest(int64_t rid);
int32_t      removeRequest(int64_t rid);
void         doDestroyRequest(void* p);

char* getDbOfConnection(STscObj* pObj);
void  setConnectionDB(STscObj* pTscObj, const char* db);
void  resetConnectDB(STscObj* pTscObj);

int taos_options_imp(TSDB_OPTION option, const char* str);

void* openTransporter(const char* user, const char* auth, int32_t numOfThreads);
void tscStopCrashReport();

typedef struct AsyncArg {
  SRpcMsg msg;
  SEpSet* pEpset;
} AsyncArg;

bool persistConnForSpecificMsg(void* parenct, tmsg_t msgType);
void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet);

STscObj* taos_connect_internal(const char* ip, const char* user, const char* pass, const char* auth, const char* db,
                               uint16_t port, int connType);

int32_t parseSql(SRequestObj* pRequest, bool topicQuery, SQuery** pQuery, SStmtCallback* pStmtCb);

int32_t getPlan(SRequestObj* pRequest, SQuery* pQuery, SQueryPlan** pPlan, SArray* pNodeList);

int32_t buildRequest(uint64_t connId, const char* sql, int sqlLen, void* param, bool validateSql,
                     SRequestObj** pRequest, int64_t reqid);

void taos_close_internal(void* taos);

// --- heartbeat
// global, called by mgmt
int  hbMgrInit();
void hbMgrCleanUp();

// cluster level
SAppHbMgr* appHbMgrInit(SAppInstInfo* pAppInstInfo, char* key);
void       appHbMgrCleanup(void);
void       hbRemoveAppHbMrg(SAppHbMgr** pAppHbMgr);
void       destroyAllRequests(SHashObj* pRequests);
void       stopAllRequests(SHashObj* pRequests);

SAppInstInfo* getAppInstInfo(const char* clusterKey);

// conn level
int  hbRegisterConn(SAppHbMgr* pAppHbMgr, int64_t tscRefId, int64_t clusterId, int8_t connType);
void hbDeregisterConn(STscObj* pTscObj, SClientHbKey connKey);

typedef struct SSqlCallbackWrapper {
  SParseContext* pParseCtx;
  SCatalogReq*   pCatalogReq;
  SRequestObj*   pRequest;
  void*          pPlanInfo;
} SSqlCallbackWrapper;

SRequestObj* launchQueryImpl(SRequestObj* pRequest, SQuery* pQuery, bool keepQuery, void** res);
int32_t      scheduleQuery(SRequestObj* pRequest, SQueryPlan* pDag, SArray* pNodeList);
void    launchAsyncQuery(SRequestObj* pRequest, SQuery* pQuery, SMetaData* pResultMeta, SSqlCallbackWrapper* pWrapper);
int32_t refreshMeta(STscObj* pTscObj, SRequestObj* pRequest);
int32_t updateQnodeList(SAppInstInfo* pInfo, SArray* pNodeList);
void    doAsyncQuery(SRequestObj* pRequest, bool forceUpdateMeta);
int32_t removeMeta(STscObj* pTscObj, SArray* tbList, bool isView);
int32_t handleAlterTbExecRes(void* res, struct SCatalog* pCatalog);
int32_t handleCreateTbExecRes(void* res, SCatalog* pCatalog);
bool    qnodeRequired(SRequestObj* pRequest);
void    continueInsertFromCsv(SSqlCallbackWrapper* pWrapper, SRequestObj* pRequest);
void    destorySqlCallbackWrapper(SSqlCallbackWrapper* pWrapper);
void    handleQueryAnslyseRes(SSqlCallbackWrapper *pWrapper, SMetaData *pResultMeta, int32_t code);
void    restartAsyncQuery(SRequestObj *pRequest, int32_t code);
int32_t buildPreviousRequest(SRequestObj *pRequest, const char* sql, SRequestObj** pNewRequest);
int32_t prepareAndParseSqlSyntax(SSqlCallbackWrapper **ppWrapper, SRequestObj *pRequest, bool updateMetaForce);
void    returnToUser(SRequestObj* pRequest);
void    stopAllQueries(SRequestObj *pRequest);
void    doRequestCallback(SRequestObj* pRequest, int32_t code);
void    freeQueryParam(SSyncQueryParam* param);

#ifdef TD_ENTERPRISE
int32_t clientParseSqlImpl(void* param, const char* dbName, const char* sql, bool parseOnly, const char* effeciveUser, SParseSqlRes* pRes);
#endif

void clientSlowQueryMonitorInit(const char* clusterKey);
void SlowQueryLog(int64_t rid, bool killed, int32_t code, int32_t cost);

void clientSQLReqMonitorInit(const char* clusterKey);

enum {
  MONITORSQLTYPESELECT = 0,
  MONITORSQLTYPEINSERT = 1,
  MONITORSQLTYPEDELETE = 2
};

void sqlReqLog(int64_t rid,  bool killed, int32_t code, int8_t type);

void clientMonitorClose(const char* clusterKey);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTINT_H

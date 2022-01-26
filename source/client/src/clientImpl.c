
#include "../../libs/scheduler/inc/schedulerInt.h"
#include "clientInt.h"
#include "clientLog.h"
#include "parser.h"
#include "planner.h"
#include "scheduler.h"
#include "tdef.h"
#include "tep.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tnote.h"
#include "tpagedfile.h"
#include "tref.h"

#define CHECK_CODE_GOTO(expr, label) \
  do {                               \
    int32_t code = expr;             \
    if (TSDB_CODE_SUCCESS != code) { \
      terrno = code;                 \
      goto label;                    \
    }                                \
  } while (0)

static int32_t initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet);
static SMsgSendInfo* buildConnectMsg(SRequestObj *pRequest);
static void destroySendMsgInfo(SMsgSendInfo* pMsgBody);
static void setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp);

static bool stringLengthCheck(const char* str, size_t maxsize) {
  if (str == NULL) {
    return false;
  }

  size_t len = strlen(str);
  if (len <= 0 || len > maxsize) {
    return false;
  }

  return true;
}

static bool validateUserName(const char* user) {
  return stringLengthCheck(user, TSDB_USER_LEN - 1);
}

static bool validatePassword(const char* passwd) {
  return stringLengthCheck(passwd, TSDB_PASSWORD_LEN - 1);
}

static bool validateDbName(const char* db) {
  return stringLengthCheck(db, TSDB_DB_NAME_LEN - 1);
}

static char* getClusterKey(const char* user, const char* auth, const char* ip, int32_t port) {
  char key[512] = {0};
  snprintf(key, sizeof(key), "%s:%s:%s:%d", user, auth, ip, port);
  return strdup(key);
}

static STscObj* taosConnectImpl(const char *user, const char *auth, const char *db, __taos_async_fn_t fp, void *param, SAppInstInfo* pAppInfo);
static void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols);

TAOS *taos_connect_internal(const char *ip, const char *user, const char *pass, const char *auth, const char *db, uint16_t port) {
  if (taos_init() != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  if (!validateUserName(user)) {
    terrno = TSDB_CODE_TSC_INVALID_USER_LENGTH;
    return NULL;
  }

  char localDb[TSDB_DB_NAME_LEN] = {0};
  if (db != NULL) {
    if(!validateDbName(db)) {
      terrno = TSDB_CODE_TSC_INVALID_DB_LENGTH;
      return NULL;
    }

    tstrncpy(localDb, db, sizeof(localDb));
    strdequote(localDb);
  }

  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  if (auth == NULL) {
    if (!validatePassword(pass)) {
      terrno = TSDB_CODE_TSC_INVALID_PASS_LENGTH;
      return NULL;
    }

    taosEncryptPass_c((uint8_t *)pass, strlen(pass), secretEncrypt);
  } else {
    tstrncpy(secretEncrypt, auth, tListLen(secretEncrypt));
  }

  SCorEpSet epSet = {0};
  if (ip) {
    if (initEpSetFromCfg(ip, NULL, &epSet) < 0) {
      return NULL;
    }

    if (port) {
      epSet.epSet.eps[0].port = port;
    }
  } else {
    if (initEpSetFromCfg(tsFirst, tsSecond, &epSet) < 0) {
      return NULL;
    }
  }

  char* key = getClusterKey(user, secretEncrypt, ip, port);
  SAppInstInfo** pInst = NULL;

  pthread_mutex_lock(&appInfo.mutex);

  pInst = taosHashGet(appInfo.pInstMap, key, strlen(key));
  if (pInst == NULL) {
    SAppInstInfo* p = calloc(1, sizeof(struct SAppInstInfo));
    p->mgmtEp       = epSet;
    p->pTransporter = openTransporter(user, secretEncrypt, tsNumOfCores);
    p->pAppHbMgr = appHbMgrInit(p);
    taosHashPut(appInfo.pInstMap, key, strlen(key), &p, POINTER_BYTES);

    pInst = &p;
  }

  pthread_mutex_unlock(&appInfo.mutex);

  tfree(key);
  return taosConnectImpl(user, &secretEncrypt[0], localDb, NULL, NULL, *pInst);
}

int32_t buildRequest(STscObj *pTscObj, const char *sql, int sqlLen, SRequestObj** pRequest) {
  *pRequest = createRequest(pTscObj, NULL, NULL, TSDB_SQL_SELECT);
  if (*pRequest == NULL) {
    tscError("failed to malloc sqlObj");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  (*pRequest)->sqlstr = malloc(sqlLen + 1);
  if ((*pRequest)->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to prepare sql string buffer", (*pRequest)->self);
    (*pRequest)->msgBuf = strdup("failed to prepare sql string buffer");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  strntolower((*pRequest)->sqlstr, sql, (int32_t)sqlLen);
  (*pRequest)->sqlstr[sqlLen] = 0;
  (*pRequest)->sqlLen = sqlLen;

  tscDebugL("0x%"PRIx64" SQL: %s, reqId:0x%"PRIx64, (*pRequest)->self, (*pRequest)->sqlstr, (*pRequest)->requestId);
  return TSDB_CODE_SUCCESS;
}

int32_t parseSql(SRequestObj* pRequest, SQueryNode** pQuery) {
  STscObj* pTscObj = pRequest->pTscObj;

  SParseContext cxt = {
    .requestId = pRequest->requestId,
    .acctId    = pTscObj->acctId,
    .db        = getDbOfConnection(pTscObj),
    .pSql      = pRequest->sqlstr,
    .sqlLen    = pRequest->sqlLen,
    .pMsg      = pRequest->msgBuf,
    .msgLen    = ERROR_MSG_BUF_DEFAULT_SIZE,
    .pTransporter = pTscObj->pAppInfo->pTransporter,
  };

  cxt.mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  int32_t code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &cxt.pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(cxt.db);
    return code;
  }

  code = qParseQuerySql(&cxt, pQuery);

  tfree(cxt.db);
  return code;
}

int32_t execDdlQuery(SRequestObj* pRequest, SQueryNode* pQuery) {
  SDclStmtInfo* pDcl = (SDclStmtInfo*)pQuery;
  pRequest->type = pDcl->msgType;
  pRequest->body.requestMsg = (SDataBuf){.pData = pDcl->pMsg, .len = pDcl->msgLen};

  STscObj* pTscObj = pRequest->pTscObj;
  SMsgSendInfo* pSendMsg = buildMsgInfoImpl(pRequest);

  int64_t transporterId = 0;
  if (pDcl->msgType == TDMT_VND_CREATE_TABLE || pDcl->msgType == TDMT_VND_SHOW_TABLES) {
    if (pDcl->msgType == TDMT_VND_SHOW_TABLES) {
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      if (pShowReqInfo->pArray == NULL) {
        pShowReqInfo->currentIndex = 0;  // set the first vnode/ then iterate the next vnode
        pShowReqInfo->pArray = pDcl->pExtension;
      }
    }
    asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pDcl->epSet, &transporterId, pSendMsg);
  } else {
    asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pDcl->epSet, &transporterId, pSendMsg);
  }

  tsem_wait(&pRequest->body.rspSem);
  return TSDB_CODE_SUCCESS;
}

int32_t getPlan(SRequestObj* pRequest, SQueryNode* pQueryNode, SQueryDag** pDag, SArray* pNodeList) {
  pRequest->type = pQueryNode->type;

  SSchema* pSchema = NULL;
  int32_t  numOfCols = 0;
  int32_t code = qCreateQueryDag(pQueryNode, pDag, &pSchema, &numOfCols, pNodeList, pRequest->requestId);
  if (code != 0) {
    return code;
  }

  if (pQueryNode->type == TSDB_SQL_SELECT) {
    setResSchemaInfo(&pRequest->body.resInfo, pSchema, numOfCols);
    tfree(pSchema);
    pRequest->type = TDMT_VND_QUERY;
  } else {
    tfree(pSchema);
  }

  return code;
}

void setResSchemaInfo(SReqResultInfo* pResInfo, const SSchema* pSchema, int32_t numOfCols) {
  assert(pSchema != NULL && numOfCols > 0);

  pResInfo->numOfCols = numOfCols;
  pResInfo->fields = calloc(numOfCols, sizeof(pSchema[0]));

  for (int32_t i = 0; i < pResInfo->numOfCols; ++i) {
    pResInfo->fields[i].bytes = pSchema[i].bytes;
    pResInfo->fields[i].type  = pSchema[i].type;
    tstrncpy(pResInfo->fields[i].name, pSchema[i].name, tListLen(pResInfo->fields[i].name));
  }
}

int32_t scheduleQuery(SRequestObj* pRequest, SQueryDag* pDag, SArray* pNodeList) {
  if (TSDB_SQL_INSERT == pRequest->type || TSDB_SQL_CREATE_TABLE == pRequest->type) {
    SQueryResult res = {.code = 0, .numOfRows = 0, .msgSize = ERROR_MSG_BUF_DEFAULT_SIZE, .msg = pRequest->msgBuf};
    int32_t code = schedulerExecJob(pRequest->pTscObj->pAppInfo->pTransporter, NULL, pDag, &pRequest->body.pQueryJob, &res);
    if (code != TSDB_CODE_SUCCESS) {
      // handle error and retry
    } else {
      if (pRequest->body.pQueryJob != NULL) {
        schedulerFreeJob(pRequest->body.pQueryJob);
      }
    }

    pRequest->body.resInfo.numOfRows = res.numOfRows;
    pRequest->code = res.code;
    return pRequest->code;
  }

  return schedulerAsyncExecJob(pRequest->pTscObj->pAppInfo->pTransporter, pNodeList, pDag, &pRequest->body.pQueryJob);
}


typedef struct SMqClientVg {
  // statistics
  int64_t pollCnt;
  // offset
  int64_t committedOffset;
  int64_t currentOffset;
  //connection info
  int32_t vgId;
  SEpSet  epSet;
} SMqClientVg;

typedef struct SMqClientTopic {
  // subscribe info
  int32_t sqlLen;
  char*   sql;
  char*   topicName;
  int64_t topicId;
  int32_t nextVgIdx;
  SArray* vgs;    //SArray<SMqClientVg>
} SMqClientTopic;

typedef struct tmq_resp_err_t {
  int32_t code;
} tmq_resp_err_t;

typedef struct tmq_topic_vgroup_t {
  char*   topic;
  int32_t vgId;
  int64_t commitOffset;
} tmq_topic_vgroup_t;

typedef struct tmq_topic_vgroup_list_t {
  int32_t cnt;
  int32_t size;
  tmq_topic_vgroup_t* elems;
} tmq_topic_vgroup_list_t;

typedef void (tmq_commit_cb(tmq_t*, tmq_resp_err_t, tmq_topic_vgroup_list_t*, void* param));

struct tmq_conf_t {
  char           clientId[256];
  char           groupId[256];
  char*          ip;
  uint16_t       port;
  tmq_commit_cb* commit_cb;
};

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = calloc(1, sizeof(tmq_conf_t));
  return conf;
}

int32_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (strcmp(key, "group.id") == 0) {
    strcpy(conf->groupId, value);
  }
  if (strcmp(key, "client.id") == 0) {
    strcpy(conf->clientId, value);
  }
  return 0;
}

struct tmq_t {
  char           groupId[256];
  char           clientId[256];
  int64_t        consumerId;
  int64_t        status;
  tsem_t         rspSem;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  int32_t        nextTopicIdx;
  SArray*        clientTopics;  //SArray<SMqClientTopic>
};

tmq_t* taos_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = calloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->status = 0;
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->commit_cb = conf->commit_cb;
  tsem_init(&pTmq->rspSem, 0, 0);
  pTmq->consumerId = generateRequestId() & ((uint64_t)-1 >> 1);
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  return pTmq;
}

struct tmq_list_t {
  int32_t cnt;
  int32_t tot;
  char*   elems[];
};
tmq_list_t* tmq_list_new() {
  tmq_list_t *ptr = malloc(sizeof(tmq_list_t) + 8 * sizeof(char*));
  if (ptr == NULL) {
    return ptr;
  }
  ptr->cnt = 0;
  ptr->tot = 8;
  return ptr;
}

int32_t tmq_list_append(tmq_list_t* ptr, char* src) {
  if (ptr->cnt >= ptr->tot-1) return -1;
  ptr->elems[ptr->cnt] = strdup(src);
  ptr->cnt++;
  return 0;
}


int32_t tmq_null_cb(void* param, const SDataBuf* pMsg, int32_t code)  {
  if (code == 0) {
    //
  }
  //
  return 0;
}

TAOS_RES* tmq_subscribe(tmq_t* tmq, tmq_list_t* topic_list) {
  SRequestObj *pRequest = NULL;
  int32_t sz = topic_list->cnt;
  //destroy ex
  taosArrayDestroy(tmq->clientTopics);
  tmq->clientTopics = taosArrayInit(sz, sizeof(SMqClientTopic));

  SCMSubscribeReq req;
  req.topicNum = sz;
  req.consumerId = tmq->consumerId;
  req.consumerGroup = strdup(tmq->groupId);
  req.topicNames = taosArrayInit(sz, sizeof(void*));

  for (int i = 0; i < sz; i++) {
    char* topicName = topic_list->elems[i];

    SName name = {0};
    char* dbName = getDbOfConnection(tmq->pTscObj);
    tNameSetDbName(&name, tmq->pTscObj->acctId, dbName, strlen(dbName));     
    tNameFromString(&name, topicName, T_NAME_TABLE);

    char* topicFname = calloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFname == NULL) {

    }
    tNameExtractFullName(&name, topicFname);
    tscDebug("subscribe topic: %s", topicFname);
    SMqClientTopic topic = {
      .nextVgIdx = 0,
      .sql = NULL,
      .sqlLen = 0,
      .topicId = 0,
      .topicName = topicFname,
      .vgs = NULL
    };
    topic.vgs = taosArrayInit(0, sizeof(SMqClientVg));
    taosArrayPush(tmq->clientTopics, &topic); 
    /*SMqClientTopic topic = {*/
      /*.*/
    /*};*/
    taosArrayPush(req.topicNames, &topicFname);
  }

  int tlen = tSerializeSCMSubscribeReq(NULL, &req);
  void* buf = malloc(tlen);
  if(buf == NULL) {
    goto _return;
  }

  void* abuf = buf;
  tSerializeSCMSubscribeReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_SUBSCRIBE);
  if (pRequest == NULL) {
    tscError("failed to malloc sqlObj");
  }

  pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  /*sendInfo->fp*/
  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  tsem_wait(&pRequest->body.rspSem);

_return:
  /*if (sendInfo != NULL) {*/
    /*destroySendMsgInfo(sendInfo);*/
  /*}*/

  if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {
    pRequest->code = terrno;
  }

  return pRequest;
}

void tmq_conf_set_offset_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb) {
  conf->commit_cb = cb;
}

SArray* tmqGetConnInfo(SClientHbKey connKey, void* param) {
  tmq_t* pTmq = (void*)param;
  SArray* pArray = taosArrayInit(0, sizeof(SKv));
  if (pArray == NULL) {
    return NULL;
  }
  SKv kv = {0};
  kv.key = malloc(256);
  if (kv.key == NULL) {
    taosArrayDestroy(pArray);
    return NULL;
  }
  strcpy(kv.key, "mq-tmp");
  kv.keyLen = strlen("mq-tmp") + 1;
  SMqHbMsg* pMqHb = malloc(sizeof(SMqHbMsg));
  if (pMqHb == NULL) {
    return pArray;
  }
  pMqHb->consumerId = connKey.connId;
  SArray* clientTopics = pTmq->clientTopics;
  int sz = taosArrayGetSize(clientTopics);
  for (int i = 0; i < sz; i++) {
    SMqClientTopic* pCTopic = taosArrayGet(clientTopics, i);
    /*if (pCTopic->vgId == -1) {*/
      /*pMqHb->status = 1;*/
      /*break;*/
    /*}*/
  }
  kv.value = pMqHb;
  kv.valueLen = sizeof(SMqHbMsg);
  taosArrayPush(pArray, &kv);

  return pArray;
}

tmq_t* tmqCreateConsumerImpl(TAOS* conn, tmq_conf_t* conf) {
  tmq_t* pTmq = malloc(sizeof(tmq_t));
  if (pTmq == NULL) {
    return NULL;
  }
  strcpy(pTmq->groupId, conf->groupId);
  strcpy(pTmq->clientId, conf->clientId);
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->pTscObj->connType = HEARTBEAT_TYPE_MQ;

  return pTmq;
}

TAOS_RES *taos_create_topic(TAOS* taos, const char* topicName, const char* sql, int sqlLen) {
  STscObj     *pTscObj = (STscObj*)taos;
  SRequestObj *pRequest = NULL;
  SQueryNode  *pQueryNode = NULL;
  char        *pStr = NULL;

  terrno = TSDB_CODE_SUCCESS;
  if (taos == NULL || topicName == NULL || sql == NULL) {
    tscError("invalid parameters for creating topic, connObj:%p, topic name:%s, sql:%s", taos, topicName, sql);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    goto _return;
  }

  if (strlen(topicName) >= TSDB_TOPIC_NAME_LEN) {
    tscError("topic name too long, max length:%d", TSDB_TOPIC_NAME_LEN - 1);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    goto _return;
  }

  if (sqlLen > tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    goto _return;
  }

  tscDebug("start to create topic, %s", topicName);

  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, &pQueryNode), _return);

  SQueryStmtInfo* pQueryStmtInfo = (SQueryStmtInfo* ) pQueryNode;
  pQueryStmtInfo->info.continueQuery = true;

  // todo check for invalid sql statement and return with error code

  SSchema *schema = NULL;
  int32_t  numOfCols = 0;
  CHECK_CODE_GOTO(qCreateQueryDag(pQueryNode, &pRequest->body.pDag, &schema, &numOfCols, NULL, pRequest->requestId), _return);

  pStr = qDagToString(pRequest->body.pDag);
  if(pStr == NULL) {
    goto _return;
  }

  printf("%s\n", pStr);

  // The topic should be related to a database that the queried table is belonged to.
  SName name = {0};
  char dbName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&((SQueryStmtInfo*) pQueryNode)->pTableMetaInfo[0]->name, dbName);

  tNameFromString(&name, dbName, T_NAME_ACCT|T_NAME_DB);
  tNameFromString(&name, topicName, T_NAME_TABLE);

  char topicFname[TSDB_TOPIC_FNAME_LEN] = {0};
  tNameExtractFullName(&name, topicFname);

  SCMCreateTopicReq req = {
    .name         = (char*) topicFname,
    .igExists     = 1,
    .physicalPlan = (char*) pStr,
    .sql          = (char*) sql,
    .logicalPlan  = "no logic plan",
  };

  int tlen = tSerializeSCMCreateTopicReq(NULL, &req);
  void* buf = malloc(tlen);
  if(buf == NULL) {
    goto _return;
  }

  void* abuf = buf;
  tSerializeSCMCreateTopicReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };
  pRequest->type = TDMT_MND_CREATE_TOPIC;

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  SEpSet epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  tsem_wait(&pRequest->body.rspSem);

_return:
  qDestroyQuery(pQueryNode);
  /*if (sendInfo != NULL) {*/
    /*destroySendMsgInfo(sendInfo);*/
  /*}*/

  if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {
    pRequest->code = terrno;
  }

  return pRequest;
}

/*typedef SMqConsumeRsp tmq_message_t;*/

struct tmq_message_t {
  SMqConsumeRsp rsp;
};

int32_t tmq_poll_cb_inner(void* param, const SDataBuf* pMsg, int32_t code) {
  return 0;
}

int32_t tmq_ask_ep_cb(void* param, const SDataBuf* pMsg, int32_t code) {
  tmq_t* tmq = (tmq_t*)param;
  if (code != 0) {
    tsem_post(&tmq->rspSem);
    return 0;
  }
  tscDebug("tmq ask ep cb called");
  bool set = false;
  SMqCMGetSubEpRsp rsp;
  tDecodeSMqCMGetSubEpRsp(pMsg->pData, &rsp);
  int32_t sz = taosArrayGetSize(rsp.topics);
  // TODO: lock
  tmq->clientTopics = taosArrayInit(sz, sizeof(SMqClientTopic));
  for (int32_t i = 0; i < sz; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(rsp.topics, i);
    topic.topicName = strdup(pTopicEp->topic);
    int32_t vgSz = taosArrayGetSize(pTopicEp->vgs);
    topic.vgs = taosArrayInit(vgSz, sizeof(SMqClientVg));
    for (int32_t j = 0; j < vgSz; j++) {
      SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
      SMqClientVg clientVg = {
        .pollCnt = 0,
        .committedOffset = -1,
        .currentOffset = -1,
        .vgId = pVgEp->vgId,
        .epSet = pVgEp->epSet
      };
      taosArrayPush(topic.vgs, &clientVg);
      set = true;
    }
    taosArrayPush(tmq->clientTopics, &topic);
  }
  if(set) tmq->status = 1;
  // unlock
  tsem_post(&tmq->rspSem);
  return 0;
}

tmq_message_t* tmq_consume_poll(tmq_t* tmq, int64_t blocking_time) {

  if (taosArrayGetSize(tmq->clientTopics) == 0 || tmq->status == 0) {
    int32_t tlen = sizeof(SMqCMGetSubEpReq);
    SMqCMGetSubEpReq* buf = malloc(tlen);
    if (buf == NULL) {
      tscError("failed to malloc get subscribe ep buf");
    }
    buf->consumerId = htobe64(tmq->consumerId);
    strcpy(buf->cgroup, tmq->groupId);
    
    SRequestObj *pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_GET_SUB_EP);
    if (pRequest == NULL) {
      tscError("failed to malloc subscribe ep request");
    }

    pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };

    SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
    sendInfo->requestObjRefId = 0;
    sendInfo->param = tmq;
    sendInfo->fp = tmq_ask_ep_cb;

    SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

    int64_t transporterId = 0;
    asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

    tsem_wait(&tmq->rspSem);
  }

  if (taosArrayGetSize(tmq->clientTopics) == 0) {
    tscDebug("consumer:%ld poll but not assigned", tmq->consumerId);
    return NULL;
  }

  SMqConsumeReq* pReq = malloc(sizeof(SMqConsumeReq));
  pReq->reqType = 1;
  pReq->blockingTime = blocking_time;
  pReq->consumerId = tmq->consumerId;
  tmq_message_t* tmq_message = NULL;
  strcpy(pReq->cgroup, tmq->groupId);

  SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, tmq->nextTopicIdx);
  tmq->nextTopicIdx = (tmq->nextTopicIdx + 1) % taosArrayGetSize(tmq->clientTopics);
  strcpy(pReq->topic, pTopic->topicName);
  int32_t nextVgIdx = pTopic->nextVgIdx;
  pTopic->nextVgIdx = (nextVgIdx + 1) % taosArrayGetSize(pTopic->vgs);
  SMqClientVg* pVg = taosArrayGet(pTopic->vgs, nextVgIdx);
  pReq->offset = pVg->currentOffset;

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqConsumeReq));

  SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_VND_CONSUME);
  pRequest->body.requestMsg = (SDataBuf){ .pData = pReq, .len = sizeof(SMqConsumeReq) };

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  /*sendInfo->requestObjRefId = 0;*/
  /*sendInfo->param = &tmq_message;*/
  /*sendInfo->fp = tmq_poll_cb_inner;*/

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);

  tsem_wait(&pRequest->body.rspSem);

  return tmq_message;

  /*tsem_wait(&pRequest->body.rspSem);*/

  /*if (body != NULL) {*/
    /*destroySendMsgInfo(body);*/
  /*}*/

  /*if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {*/
    /*pRequest->code = terrno;*/
  /*}*/

  /*return pRequest;*/
}

tmq_resp_err_t* tmq_commit(tmq_t* tmq, tmq_topic_vgroup_list_t* tmq_topic_vgroup_list, int32_t async) {
  SMqConsumeReq req = {0};
  return NULL;
}

void tmq_message_destroy(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;
}


TAOS_RES *taos_query_l(TAOS *taos, const char *sql, int sqlLen) {
  STscObj *pTscObj = (STscObj *)taos;
  if (sqlLen > (size_t) tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    return NULL;
  }

  nPrintTsc("%s", sql)

  SRequestObj *pRequest = NULL;
  SQueryNode  *pQueryNode = NULL;
  SArray      *pNodeList = taosArrayInit(4, sizeof(struct SQueryNodeAddr));

  terrno = TSDB_CODE_SUCCESS;
  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, &pQueryNode), _return);

  if (qIsDdlQuery(pQueryNode)) {
    CHECK_CODE_GOTO(execDdlQuery(pRequest, pQueryNode), _return);
  } else {

    CHECK_CODE_GOTO(getPlan(pRequest, pQueryNode, &pRequest->body.pDag, pNodeList), _return);
    CHECK_CODE_GOTO(scheduleQuery(pRequest, pRequest->body.pDag, pNodeList), _return);
    pRequest->code = terrno;
  }

_return:
  taosArrayDestroy(pNodeList);
  qDestroyQuery(pQueryNode);
  if (NULL != pRequest && TSDB_CODE_SUCCESS != terrno) {
    pRequest->code = terrno;
  }

  return pRequest;
}

int initEpSetFromCfg(const char *firstEp, const char *secondEp, SCorEpSet *pEpSet) {
  pEpSet->version = 0;

  // init mnode ip set
  SEpSet *mgmtEpSet   = &(pEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse    = 0;

  if (firstEp && firstEp[0] != 0) {
    if (strlen(firstEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(firstEp, &mgmtEpSet->eps[0]);
    mgmtEpSet->numOfEps++;
  }

  if (secondEp && secondEp[0] != 0) {
    if (strlen(secondEp) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }

    taosGetFqdnPortFromEp(secondEp, &mgmtEpSet->eps[mgmtEpSet->numOfEps]);
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

STscObj* taosConnectImpl(const char *user, const char *auth, const char *db, __taos_async_fn_t fp, void *param, SAppInstInfo* pAppInfo) {
  STscObj *pTscObj = createTscObj(user, auth, db, pAppInfo);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pTscObj;
  }

  SRequestObj *pRequest = createRequest(pTscObj, fp, param, TDMT_MND_CONNECT);
  if (pRequest == NULL) {
    destroyTscObj(pTscObj);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SMsgSendInfo* body = buildConnectMsg(pRequest);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &transporterId, body);

  tsem_wait(&pRequest->body.rspSem);
  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char *errorMsg = (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(pRequest->code);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%"PRIx64" connection is opening, connId:%d, dnodeConn:%p, reqId:0x%"PRIx64, pTscObj->id, pTscObj->connId, pTscObj->pAppInfo->pTransporter, pRequest->requestId);
    destroyRequest(pRequest);
  }

  return pTscObj;
}

static SMsgSendInfo* buildConnectMsg(SRequestObj *pRequest) {
  SMsgSendInfo *pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pMsgSendInfo->msgType         = TDMT_MND_CONNECT;
  pMsgSendInfo->msgInfo.len     = sizeof(SConnectReq);
  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId       = pRequest->requestId;
  pMsgSendInfo->fp              = handleRequestRspFp[TMSG_INDEX(pMsgSendInfo->msgType)];
  pMsgSendInfo->param           = pRequest;

  SConnectReq *pConnect = calloc(1, sizeof(SConnectReq));
  if (pConnect == NULL) {
    tfree(pMsgSendInfo);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  STscObj *pObj = pRequest->pTscObj;

  char* db = getDbOfConnection(pObj);
  if (db != NULL) {
    tstrncpy(pConnect->db, db, sizeof(pConnect->db));
  }
  tfree(db);

  pConnect->pid = htonl(appInfo.pid);
  pConnect->startTime = htobe64(appInfo.startTime);
  tstrncpy(pConnect->app, appInfo.appName, tListLen(pConnect->app));

  pMsgSendInfo->msgInfo.pData = pConnect;
  return pMsgSendInfo;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}

void processMsgFromServer(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo *pSendInfo = (SMsgSendInfo *) pMsg->ahandle;
  assert(pMsg->ahandle != NULL);

  if (pSendInfo->requestObjRefId != 0) {
    SRequestObj *pRequest = (SRequestObj *)taosAcquireRef(clientReqRefPool, pSendInfo->requestObjRefId);
    assert(pRequest->self == pSendInfo->requestObjRefId);

    pRequest->metric.rsp = taosGetTimestampMs();
    pRequest->code = pMsg->code;

    STscObj *pTscObj = pRequest->pTscObj;
    if (pEpSet) {
      if (!isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, pEpSet)) {
        updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, pEpSet);
      }
    }

    /*
   * There is not response callback function for submit response.
   * The actual inserted number of points is the first number.
     */
    int32_t elapsed = pRequest->metric.rsp - pRequest->metric.start;
    if (pMsg->code == TSDB_CODE_SUCCESS) {
      tscDebug("0x%" PRIx64 " message:%s, code:%s rspLen:%d, elapsed:%d ms, reqId:0x%"PRIx64, pRequest->self,
          TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    } else {
      tscError("0x%" PRIx64 " SQL cmd:%s, code:%s rspLen:%d, elapsed time:%d ms, reqId:0x%"PRIx64, pRequest->self,
          TMSG_INFO(pMsg->msgType), tstrerror(pMsg->code), pMsg->contLen, elapsed, pRequest->requestId);
    }

    taosReleaseRef(clientReqRefPool, pSendInfo->requestObjRefId);
  }

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL};

  if (pMsg->contLen > 0) {
    buf.pData = calloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);
}

TAOS *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port) {
  tscDebug("try to connect to %s:%u by auth, user:%s db:%s", ip, port, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (auth == NULL) {
    tscError("No auth info is given, failed to connect to server");
    return NULL;
  }

  return taos_connect_internal(ip, user, NULL, auth, db, port);
}

TAOS *taos_connect_l(const char *ip, int ipLen, const char *user, int userLen, const char *pass, int passLen, const char *db, int dbLen, uint16_t port) {
  char ipStr[TSDB_EP_LEN]      = {0};
  char dbStr[TSDB_DB_NAME_LEN] = {0};
  char userStr[TSDB_USER_LEN]  = {0};
  char passStr[TSDB_PASSWORD_LEN]   = {0};

  strncpy(ipStr,   ip,   TMIN(TSDB_EP_LEN - 1, ipLen));
  strncpy(userStr, user, TMIN(TSDB_USER_LEN - 1, userLen));
  strncpy(passStr, pass, TMIN(TSDB_PASSWORD_LEN - 1, passLen));
  strncpy(dbStr,   db,   TMIN(TSDB_DB_NAME_LEN - 1, dbLen));
  return taos_connect(ipStr, userStr, passStr, dbStr, port);
}

void* doFetchRow(SRequestObj* pRequest) {
  assert(pRequest != NULL);
  SReqResultInfo* pResultInfo = &pRequest->body.resInfo;

  SEpSet epSet = {0};

  if (pResultInfo->pData == NULL || pResultInfo->current >= pResultInfo->numOfRows) {
    if (pRequest->type == TDMT_VND_QUERY) {
      // All data has returned to App already, no need to try again
      if (pResultInfo->completed) {
        return NULL;
      }

      SReqResultInfo* pResInfo = &pRequest->body.resInfo;
      int32_t code = schedulerFetchRows(pRequest->body.pQueryJob, (void **)&pResInfo->pData);
      if (code != TSDB_CODE_SUCCESS) {
        pRequest->code = code;
        return NULL;
      }

      setQueryResultFromRsp(&pRequest->body.resInfo, (SRetrieveTableRsp*)pResInfo->pData);
      tscDebug("0x%"PRIx64 " fetch results, numOfRows:%d total Rows:%"PRId64", complete:%d, reqId:0x%"PRIx64, pRequest->self, pResInfo->numOfRows,
               pResInfo->totalRows, pResInfo->completed, pRequest->requestId);

      if (pResultInfo->numOfRows == 0) {
        return NULL;
      }

      goto _return;
    } else if (pRequest->type == TDMT_MND_SHOW) {
      pRequest->type = TDMT_MND_SHOW_RETRIEVE;
      epSet = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES) {
      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      SVgroupInfo* pVgroupInfo = taosArrayGet(pShowReqInfo->pArray, pShowReqInfo->currentIndex);

      epSet = pVgroupInfo->epset;
    } else if (pRequest->type == TDMT_VND_SHOW_TABLES_FETCH) {
      pRequest->type = TDMT_VND_SHOW_TABLES;
      SShowReqInfo* pShowReqInfo = &pRequest->body.showInfo;
      pShowReqInfo->currentIndex += 1;
      if (pShowReqInfo->currentIndex >= taosArrayGetSize(pShowReqInfo->pArray)) {
        return NULL;
      }

      SVgroupInfo* pVgroupInfo = taosArrayGet(pShowReqInfo->pArray, pShowReqInfo->currentIndex);
      SVShowTablesReq* pShowReq = calloc(1, sizeof(SVShowTablesReq));
      pShowReq->head.vgId = htonl(pVgroupInfo->vgId);

      pRequest->body.requestMsg.len = sizeof(SVShowTablesReq);
      pRequest->body.requestMsg.pData = pShowReq;

      SMsgSendInfo* body = buildMsgInfoImpl(pRequest);
      epSet = pVgroupInfo->epset;

      int64_t  transporterId = 0;
      STscObj *pTscObj = pRequest->pTscObj;
      asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, body);
      tsem_wait(&pRequest->body.rspSem);

      pRequest->type = TDMT_VND_SHOW_TABLES_FETCH;
    } else if (pRequest->type == TDMT_MND_SHOW_RETRIEVE && pResultInfo->pData != NULL) {
      return NULL;
    }

    SMsgSendInfo* body = buildMsgInfoImpl(pRequest);

    int64_t  transporterId = 0;
    STscObj *pTscObj = pRequest->pTscObj;
    asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, body);

    tsem_wait(&pRequest->body.rspSem);

    pResultInfo->current = 0;
    if (pResultInfo->numOfRows <= pResultInfo->current) {
      return NULL;
    }
  }

_return:

  for(int32_t i = 0; i < pResultInfo->numOfCols; ++i) {
    pResultInfo->row[i] = pResultInfo->pCol[i] + pResultInfo->fields[i].bytes * pResultInfo->current;
    if (IS_VAR_DATA_TYPE(pResultInfo->fields[i].type)) {
      pResultInfo->length[i] = varDataLen(pResultInfo->row[i]);
      pResultInfo->row[i] = varDataVal(pResultInfo->row[i]);
    }
  }

  pResultInfo->current += 1;
  return pResultInfo->row;
}

static void doPrepareResPtr(SReqResultInfo* pResInfo) {
  if (pResInfo->row == NULL) {
    pResInfo->row    = calloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->pCol   = calloc(pResInfo->numOfCols, POINTER_BYTES);
    pResInfo->length = calloc(pResInfo->numOfCols, sizeof(int32_t));
  }
}

void setResultDataPtr(SReqResultInfo* pResultInfo, TAOS_FIELD* pFields, int32_t numOfCols, int32_t numOfRows) {
  assert(numOfCols > 0 && pFields != NULL && pResultInfo != NULL);
  if (numOfRows == 0) {
    return;
  }

  // todo check for the failure of malloc
  doPrepareResPtr(pResultInfo);

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pResultInfo->length[i] = pResultInfo->fields[i].bytes;
    pResultInfo->row[i]    = (char*) (pResultInfo->pData + offset * pResultInfo->numOfRows);
    pResultInfo->pCol[i]   = pResultInfo->row[i];
    offset += pResultInfo->fields[i].bytes;
  }
}

char* getDbOfConnection(STscObj* pObj) {
  char *p = NULL;
  pthread_mutex_lock(&pObj->mutex);
  size_t len = strlen(pObj->db);
  if (len > 0) {
    p = strndup(pObj->db, tListLen(pObj->db));
  }

  pthread_mutex_unlock(&pObj->mutex);
  return p;
}

void setConnectionDB(STscObj* pTscObj, const char* db) {
  assert(db != NULL && pTscObj != NULL);
  pthread_mutex_lock(&pTscObj->mutex);
  tstrncpy(pTscObj->db, db, tListLen(pTscObj->db));
  pthread_mutex_unlock(&pTscObj->mutex);
}

void setQueryResultFromRsp(SReqResultInfo* pResultInfo, const SRetrieveTableRsp* pRsp) {
  assert(pResultInfo != NULL && pRsp != NULL);

  pResultInfo->pRspMsg   = (const char*) pRsp;
  pResultInfo->pData     = (void*) pRsp->data;
  pResultInfo->numOfRows = htonl(pRsp->numOfRows);
  pResultInfo->current   = 0;
  pResultInfo->completed = (pRsp->completed == 1);

  pResultInfo->totalRows += pResultInfo->numOfRows;
  setResultDataPtr(pResultInfo, pResultInfo->fields, pResultInfo->numOfCols, pResultInfo->numOfRows);
}

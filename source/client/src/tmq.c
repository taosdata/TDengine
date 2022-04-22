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

#include "clientInt.h"
#include "clientLog.h"
#include "parser.h"
#include "planner.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tqueue.h"
#include "tref.h"

typedef struct {
  int8_t  tmqRspType;
  int32_t epoch;
} SMqRspWrapper;

typedef struct {
  int8_t           tmqRspType;
  int32_t          epoch;
  SMqCMGetSubEpRsp msg;
} SMqAskEpRspWrapper;

struct tmq_list_t {
  SArray container;
};

struct tmq_topic_vgroup_t {
  SMqOffset offset;
};

struct tmq_topic_vgroup_list_t {
  SArray container;  // SArray<tmq_topic_vgroup_t*>
};

struct tmq_conf_t {
  char           clientId[256];
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         autoCommit;
  int8_t         resetOffset;
  uint16_t       port;
  uint16_t       autoCommitInterval;
  char*          ip;
  char*          user;
  char*          pass;
  char*          db;
  tmq_commit_cb* commit_cb;
};

struct tmq_t {
  // conf
  char   groupId[TSDB_CGROUP_LEN];
  char   clientId[256];
  int8_t autoCommit;
  /*int8_t         inWaiting;*/
  int64_t        consumerId;
  int32_t        epoch;
  int32_t        resetOffsetCfg;
  int64_t        status;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  /*int32_t        nextTopicIdx;*/
  int8_t  epStatus;
  int32_t epSkipCnt;
  /*int32_t        waitingRequest;*/
  /*int32_t        readyRequest;*/
  SArray*     clientTopics;  // SArray<SMqClientTopic>
  STaosQueue* mqueue;        // queue of tmq_message_t
  STaosQall*  qall;
  tsem_t      rspSem;
  // stat
  int64_t pollCnt;
};

enum {
  TMQ_VG_STATUS__IDLE = 0,
  TMQ_VG_STATUS__WAIT,
};

enum {
  TMQ_CONSUMER_STATUS__INIT = 0,
  TMQ_CONSUMER_STATUS__READY,
};

typedef struct {
  // statistics
  int64_t pollCnt;
  // offset
  int64_t currentOffset;
  // connection info
  int32_t vgId;
  int32_t vgStatus;
  int32_t vgSkipCnt;
  SEpSet  epSet;
} SMqClientVg;

typedef struct {
  // subscribe info
  int32_t        sqlLen;
  char*          sql;
  char*          topicName;
  int64_t        topicId;
  SArray*        vgs;  // SArray<SMqClientVg>
  int8_t         isSchemaAdaptive;
  int32_t        numOfFields;
  SSchemaWrapper schema;
} SMqClientTopic;

typedef struct {
  int8_t          tmqRspType;
  int32_t         epoch;
  SMqClientVg*    vgHandle;
  SMqClientTopic* topicHandle;
  SMqDataBlkRsp   msg;
} SMqPollRspWrapper;

typedef struct {
  tmq_t*         tmq;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  tmq_t*  tmq;
  int32_t code;
  int32_t sync;
  tsem_t  rspSem;
} SMqAskEpCbParam;

typedef struct {
  tmq_t*          tmq;
  SMqClientVg*    pVg;
  SMqClientTopic* pTopic;
  int32_t         epoch;
  int32_t         vgId;
  tsem_t          rspSem;
  int32_t         sync;
} SMqPollCbParam;

typedef struct {
  tmq_t*         tmq;
  int32_t        async;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
  /*SMqClientVg* pVg;*/
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  conf->autoCommit = false;
  conf->resetOffset = TMQ_CONF__RESET_OFFSET__EARLIEAST;
  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if (conf) taosMemoryFree(conf);
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (strcmp(key, "group.id") == 0) {
    strcpy(conf->groupId, value);
    return TMQ_CONF_OK;
  }

  if (strcmp(key, "client.id") == 0) {
    strcpy(conf->clientId, value);
    return TMQ_CONF_OK;
  }

  if (strcmp(key, "enable.auto.commit") == 0) {
    if (strcmp(value, "true") == 0) {
      conf->autoCommit = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->autoCommit = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcmp(key, "auto.commit.interval.ms") == 0) {
    conf->autoCommitInterval = atoi(value);
    return TMQ_CONF_OK;
  }

  if (strcmp(key, "auto.offset.reset") == 0) {
    if (strcmp(value, "none") == 0) {
      conf->resetOffset = TMQ_CONF__RESET_OFFSET__NONE;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "earliest") == 0) {
      conf->resetOffset = TMQ_CONF__RESET_OFFSET__EARLIEAST;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "latest") == 0) {
      conf->resetOffset = TMQ_CONF__RESET_OFFSET__LATEST;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcmp(key, "td.connect.ip") == 0) {
    conf->ip = strdup(value);
    return TMQ_CONF_OK;
  }
  if (strcmp(key, "td.connect.user") == 0) {
    conf->user = strdup(value);
    return TMQ_CONF_OK;
  }
  if (strcmp(key, "td.connect.pass") == 0) {
    conf->pass = strdup(value);
    return TMQ_CONF_OK;
  }
  if (strcmp(key, "td.connect.port") == 0) {
    conf->port = atoi(value);
    return TMQ_CONF_OK;
  }
  if (strcmp(key, "td.connect.db") == 0) {
    conf->db = strdup(value);
    return TMQ_CONF_OK;
  }

  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() {
  //
  return (tmq_list_t*)taosArrayInit(0, sizeof(void*));
}

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  SArray* container = &list->container;
  char*   topic = strdup(src);
  if (taosArrayPush(container, &topic) == NULL) return -1;
  return 0;
}

void tmq_list_destroy(tmq_list_t* list) {
  SArray* container = &list->container;
  /*taosArrayDestroy(container);*/
  int32_t sz = taosArrayGetSize(container);
  for (int32_t i = 0; i < sz; i++) {
    char* str = taosArrayGetP(container, i);
    taosMemoryFree(str);
  }
  taosArrayDestroy(container);
}

int32_t tmq_list_get_size(const tmq_list_t* list) {
  const SArray* container = &list->container;
  return taosArrayGetSize(container);
}

char** tmq_list_to_c_array(const tmq_list_t* list) {
  const SArray* container = &list->container;
  return container->pData;
}

static int32_t tmqMakeTopicVgKey(char* dst, const char* topicName, int32_t vg) {
  return sprintf(dst, "%s:%d", topicName, vg);
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  SMqRspWrapper* msg = NULL;
  while (1) {
    taosGetQitem(tmq->qall, (void**)&msg);
    if (msg)
      taosFreeQitem(msg);
    else
      break;
  }

  msg = NULL;
  taosReadAllQitems(tmq->mqueue, tmq->qall);
  while (1) {
    taosGetQitem(tmq->qall, (void**)&msg);
    if (msg)
      taosFreeQitem(msg);
    else
      break;
  }
}

int32_t tmqSubscribeCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;
  tsem_post(&pParam->rspSem);
  return 0;
}

int32_t tmqCommitCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqCommitCbParam* pParam = (SMqCommitCbParam*)param;
  pParam->rspErr = code == 0 ? TMQ_RESP_ERR__SUCCESS : TMQ_RESP_ERR__FAIL;
  if (pParam->tmq->commit_cb) {
    pParam->tmq->commit_cb(pParam->tmq, pParam->rspErr, NULL);
  }
  if (!pParam->async) tsem_post(&pParam->rspSem);
  return 0;
}

tmq_resp_err_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (*topics == NULL) {
    *topics = tmq_list_new();
  }
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGetP(tmq->clientTopics, i);
    tmq_list_append(*topics, strdup(topic->topicName));
  }
  return TMQ_RESP_ERR__SUCCESS;
}

tmq_resp_err_t tmq_unsubscribe(tmq_t* tmq) {
  tmq_list_t* lst = tmq_list_new();
  return tmq_subscribe(tmq, lst);
}

#if 0
tmq_t* tmq_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = taosMemoryCalloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  /*pTmq->inWaiting = 0;*/
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  /*pTmq->waitingRequest = 0;*/
  /*pTmq->readyRequest = 0;*/
  pTmq->epStatus = 0;
  pTmq->epSkipCnt = 0;
  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->autoCommit;
  pTmq->commit_cb = conf->commit_cb;
  pTmq->resetOffsetCfg = conf->resetOffset;

  pTmq->consumerId = generateRequestId() & (((uint64_t)-1) >> 1);
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  if (pTmq->clientTopics == NULL) {
    taosMemoryFree(pTmq);
    return NULL;
  }

  pTmq->mqueue = taosOpenQueue();
  pTmq->qall = taosAllocateQall();

  tsem_init(&pTmq->rspSem, 0, 0);

  return pTmq;
}
#endif

tmq_t* tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    return NULL;
  }
  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  ASSERT(user);
  ASSERT(pass);
  ASSERT(conf->db);
  ASSERT(conf->groupId[0]);

  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, conf->db, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) return NULL;

  /*pTmq->inWaiting = 0;*/
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  /*pTmq->waitingRequest = 0;*/
  /*pTmq->readyRequest = 0;*/
  pTmq->epStatus = 0;
  pTmq->epSkipCnt = 0;
  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->autoCommit;
  pTmq->commit_cb = conf->commit_cb;
  pTmq->resetOffsetCfg = conf->resetOffset;

  pTmq->consumerId = tGenIdPI64();
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  if (pTmq->clientTopics == NULL) {
    taosMemoryFree(pTmq);
    return NULL;
  }

  pTmq->mqueue = taosOpenQueue();
  pTmq->qall = taosAllocateQall();

  tsem_init(&pTmq->rspSem, 0, 0);

  return pTmq;
}

tmq_resp_err_t tmq_commit(tmq_t* tmq, const tmq_topic_vgroup_list_t* offsets, int32_t async) {
  // TODO: add read write lock
  SRequestObj*   pRequest = NULL;
  tmq_resp_err_t resp = TMQ_RESP_ERR__SUCCESS;
  // build msg
  // send to mnode
  SMqCMCommitOffsetReq req;
  SArray*              pArray = NULL;

  if (offsets == NULL) {
    pArray = taosArrayInit(0, sizeof(SMqOffset));
    for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
      SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
      for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
        SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
        SMqOffset    offset;
        strcpy(offset.topicName, pTopic->topicName);
        strcpy(offset.cgroup, tmq->groupId);
        offset.vgId = pVg->vgId;
        offset.offset = pVg->currentOffset;
        taosArrayPush(pArray, &offset);
      }
    }
    req.num = pArray->size;
    req.offsets = pArray->pData;
  } else {
    req.num = taosArrayGetSize(&offsets->container);
    req.offsets = (SMqOffset*)offsets->container.pData;
  }

  SCoder encoder;

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, NULL, 0, TD_ENCODER);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  int32_t tlen = encoder.pos;
  void*   buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    tCoderClear(&encoder);
    return -1;
  }
  tCoderClear(&encoder);

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, tlen, TD_ENCODER);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  tCoderClear(&encoder);

  pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_MQ_COMMIT_OFFSET);
  if (pRequest == NULL) {
    tscError("failed to malloc request");
  }

  SMqCommitCbParam* pParam = taosMemoryMalloc(sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    return -1;
  }
  pParam->tmq = tmq;
  tsem_init(&pParam->rspSem, 0, 0);
  pParam->async = async;

  pRequest->body.requestMsg = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  sendInfo->param = pParam;
  sendInfo->fp = tmqCommitCb;
  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
    tsem_wait(&pParam->rspSem);
    resp = pParam->rspErr;
  }

  tsem_destroy(&pParam->rspSem);
  taosMemoryFree(pParam);

  if (pArray) {
    taosArrayDestroy(pArray);
  }

  return resp;
}

tmq_resp_err_t tmq_subscribe(tmq_t* tmq, tmq_list_t* topic_list) {
  SRequestObj* pRequest = NULL;
  SArray*      container = &topic_list->container;
  int32_t      sz = taosArrayGetSize(container);
  // destroy ex
  taosArrayDestroy(tmq->clientTopics);
  tmq->clientTopics = taosArrayInit(sz, sizeof(SMqClientTopic));

  SCMSubscribeReq req;
  req.topicNum = sz;
  req.consumerId = tmq->consumerId;
  strcpy(req.cgroup, tmq->groupId);
  req.topicNames = taosArrayInit(sz, sizeof(void*));

  for (int i = 0; i < sz; i++) {
    /*char* topicName = topic_list->elems[i];*/
    char* topicName = taosArrayGetP(container, i);

    SName name = {0};
    char* dbName = getDbOfConnection(tmq->pTscObj);
    if (dbName == NULL) {
      return TMQ_RESP_ERR__FAIL;
    }
    tNameSetDbName(&name, tmq->pTscObj->acctId, dbName, strlen(dbName));
    tNameFromString(&name, topicName, T_NAME_TABLE);

    char* topicFname = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFname == NULL) {
      goto _return;
    }
    tNameExtractFullName(&name, topicFname);
    tscDebug("subscribe topic: %s", topicFname);
    SMqClientTopic topic = {
        .sql = NULL,
        .sqlLen = 0,
        .topicId = 0,
        .topicName = topicFname,
        .vgs = NULL,
    };
    topic.vgs = taosArrayInit(0, sizeof(SMqClientVg));
    taosArrayPush(tmq->clientTopics, &topic);
    taosArrayPush(req.topicNames, &topicFname);
    taosMemoryFree(dbName);
  }

  int   tlen = tSerializeSCMSubscribeReq(NULL, &req);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  void* abuf = buf;
  tSerializeSCMSubscribeReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_SUBSCRIBE);
  if (pRequest == NULL) {
    tscError("failed to malloc request");
  }

  SMqSubscribeCbParam param = {
      .rspErr = TMQ_RESP_ERR__SUCCESS,
      .tmq = tmq,
  };
  tsem_init(&param.rspSem, 0, 0);

  pRequest->body.requestMsg = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  sendInfo->param = &param;
  sendInfo->fp = tmqSubscribeCb;
  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  tsem_wait(&param.rspSem);
  tsem_destroy(&param.rspSem);

_return:
  /*if (sendInfo != NULL) {*/
  /*destroySendMsgInfo(sendInfo);*/
  /*}*/

  return param.rspErr;
}

void tmq_conf_set_offset_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb) { conf->commit_cb = cb; }

TAOS_RES* tmq_create_stream(TAOS* taos, const char* streamName, const char* tbName, const char* sql) {
  STscObj*     pTscObj = (STscObj*)taos;
  SRequestObj* pRequest = NULL;
  SQuery*      pQueryNode = NULL;
  char*        astStr = NULL;
  int32_t      sqlLen;

  terrno = TSDB_CODE_SUCCESS;
  if (taos == NULL || streamName == NULL || sql == NULL) {
    tscError("invalid parameters for creating stream, connObj:%p, stream name:%s, sql:%s", taos, streamName, sql);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    goto _return;
  }
  sqlLen = strlen(sql);

  if (strlen(tbName) >= TSDB_TABLE_NAME_LEN) {
    tscError("output tb name too long, max length:%d", TSDB_TABLE_NAME_LEN - 1);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    goto _return;
  }

  if (sqlLen > TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    goto _return;
  }

  tscDebug("start to create stream: %s", streamName);

  int32_t code = 0;
  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, false, &pQueryNode, NULL), _return);

  // todo check for invalid sql statement and return with error code

  CHECK_CODE_GOTO(nodesNodeToString(pQueryNode->pRoot, false, &astStr, NULL), _return);

  /*printf("%s\n", pStr);*/

  SName name = {.acctId = pTscObj->acctId, .type = TSDB_TABLE_NAME_T};
  strcpy(name.dbname, pRequest->pDb);
  strcpy(name.tname, streamName);

  SCMCreateStreamReq req = {
      .igExists = 1,
      .ast = astStr,
      .sql = (char*)sql,
  };
  tNameExtractFullName(&name, req.name);
  strcpy(req.outputSTbName, tbName);

  int   tlen = tSerializeSCMCreateStreamReq(NULL, 0, &req);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  tSerializeSCMCreateStreamReq(buf, tlen, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };
  pRequest->type = TDMT_MND_CREATE_STREAM;

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  SEpSet        epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  tsem_wait(&pRequest->body.rspSem);

_return:
  taosMemoryFreeClear(astStr);
  qDestroyQuery(pQueryNode);
  /*if (sendInfo != NULL) {*/
  /*destroySendMsgInfo(sendInfo);*/
  /*}*/

  if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {
    pRequest->code = terrno;
  }

  return pRequest;
}

#if 0
TAOS_RES* tmq_create_topic(TAOS* taos, const char* topicName, const char* sql, int sqlLen) {
  STscObj*     pTscObj = (STscObj*)taos;
  SRequestObj* pRequest = NULL;
  SQuery*      pQueryNode = NULL;
  char*        astStr = NULL;

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

  if (sqlLen > TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    goto _return;
  }

  tscDebug("start to create topic: %s", topicName);

  int32_t code = TSDB_CODE_SUCCESS;
  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, true, &pQueryNode), _return);

  // todo check for invalid sql statement and return with error code

  CHECK_CODE_GOTO(nodesNodeToString(pQueryNode->pRoot, false, &astStr, NULL), _return);

  /*printf("%s\n", pStr);*/

  SName name = {.acctId = pTscObj->acctId, .type = TSDB_TABLE_NAME_T};
  strcpy(name.dbname, pRequest->pDb);
  strcpy(name.tname, topicName);

  SCMCreateTopicReq req = {
      .igExists = 1,
      .ast = astStr,
      .sql = (char*)sql,
  };
  tNameExtractFullName(&name, req.name);

  int   tlen = tSerializeSCMCreateTopicReq(NULL, 0, &req);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  tSerializeSCMCreateTopicReq(buf, tlen, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };
  pRequest->type = TDMT_MND_CREATE_TOPIC;

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  SEpSet        epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  tsem_wait(&pRequest->body.rspSem);

_return:
  taosMemoryFreeClear(astStr);
  qDestroyQuery(pQueryNode);
  /*if (sendInfo != NULL) {*/
  /*destroySendMsgInfo(sendInfo);*/
  /*}*/

  if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {
    pRequest->code = terrno;
  }

  return pRequest;
}
#endif

#if 0
int32_t tmqGetSkipLogNum(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return 0;
  SMqPollRsp* pRsp = &tmq_message->msg;
  return pRsp->skipLogNum;
}
#endif

int32_t tmqPollCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  SMqClientTopic* pTopic = pParam->pTopic;
  tmq_t*          tmq = pParam->tmq;
  if (code != 0) {
    tscWarn("msg discard from vg %d, epoch %d, code:%x", pParam->vgId, pParam->epoch, code);
    goto CREATE_MSG_FAIL;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t tmqEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < tmqEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("msg discard from vg %d since from earlier epoch, rsp epoch %d, current epoch %d", pParam->vgId, msgEpoch,
            tmqEpoch);
    /*tsem_post(&tmq->rspSem);*/
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("mismatch rsp from vg %d, epoch %d, current epoch %d", pParam->vgId, msgEpoch, tmqEpoch);
  }

#if 0
  if (pParam->sync == 1) {
    /**pParam->msg = taosMemoryMalloc(sizeof(tmq_message_t));*/
    *pParam->msg = taosAllocateQitem(sizeof(tmq_message_t));
    if (*pParam->msg) {
      memcpy(*pParam->msg, pMsg->pData, sizeof(SMqRspHead));
      tDecodeSMqConsumeRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &((*pParam->msg)->consumeRsp));
      if ((*pParam->msg)->consumeRsp.numOfTopics != 0) {
        pVg->currentOffset = (*pParam->msg)->consumeRsp.rspOffset;
      }
      taosWriteQitem(tmq->mqueue, *pParam->msg);
      tsem_post(&pParam->rspSem);
      return 0;
    }
    tsem_post(&pParam->rspSem);
    return -1;
  }
#endif

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper));
  if (pRspWrapper == NULL) {
    tscWarn("msg discard from vg %d, epoch %d since out of memory", pParam->vgId, pParam->epoch);
    goto CREATE_MSG_FAIL;
  }

  pRspWrapper->tmqRspType = TMQ_MSG_TYPE__POLL_RSP;
  pRspWrapper->vgHandle = pVg;
  pRspWrapper->topicHandle = pTopic;

  memcpy(&pRspWrapper->msg, pMsg->pData, sizeof(SMqRspHead));

  tDecodeSMqDataBlkRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRspWrapper->msg);

  tscDebug("consumer %ld recv poll: vg %d, req offset %ld, rsp offset %ld", tmq->consumerId, pVg->vgId,
           pRspWrapper->msg.reqOffset, pRspWrapper->msg.rspOffset);

  taosWriteQitem(tmq->mqueue, pRspWrapper);
  /*tsem_post(&tmq->rspSem);*/

  return 0;
CREATE_MSG_FAIL:
  if (pParam->epoch == tmq->epoch) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  /*tsem_post(&tmq->rspSem);*/
  return -1;
}

bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, SMqCMGetSubEpRsp* pRsp) {
  /*printf("call update ep %d\n", epoch);*/
  bool    set = false;
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  tscDebug("consumer %ld update ep epoch %d to epoch %d, topic num: %d", tmq->consumerId, tmq->epoch, epoch,
           topicNumGet);
  SArray* newTopics = taosArrayInit(topicNumGet, sizeof(SMqClientTopic));
  if (newTopics == NULL) {
    return false;
  }
  SHashObj* pHash = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pHash == NULL) {
    taosArrayDestroy(newTopics);
    return false;
  }

  // find topic, build hash
  for (int32_t i = 0; i < topicNumGet; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    topic.schema = pTopicEp->schema;
    taosHashClear(pHash);
    topic.topicName = strdup(pTopicEp->topic);

    tscDebug("consumer %ld update topic: %s", tmq->consumerId, topic.topicName);
    int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
    for (int32_t j = 0; j < topicNumCur; j++) {
      // find old topic
      SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
      if (pTopicCur->vgs && strcmp(pTopicCur->topicName, pTopicEp->topic) == 0) {
        int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
        tscDebug("consumer %ld new vg num: %d", tmq->consumerId, vgNumCur);
        if (vgNumCur == 0) break;
        for (int32_t k = 0; k < vgNumCur; k++) {
          SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, k);
          sprintf(vgKey, "%s:%d", topic.topicName, pVgCur->vgId);
          tscDebug("consumer %ld epoch %d vg %d build %s", tmq->consumerId, epoch, pVgCur->vgId, vgKey);
          taosHashPut(pHash, vgKey, strlen(vgKey), &pVgCur->currentOffset, sizeof(int64_t));
        }
        break;
      }
    }

    int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);
    topic.vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));
    for (int32_t j = 0; j < vgNumGet; j++) {
      SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
      sprintf(vgKey, "%s:%d", topic.topicName, pVgEp->vgId);
      int64_t* pOffset = taosHashGet(pHash, vgKey, strlen(vgKey));
      int64_t  offset = pVgEp->offset;
      tscDebug("consumer %ld epoch %d vg %d offset og to %ld", tmq->consumerId, epoch, pVgEp->vgId, offset);
      if (pOffset != NULL) {
        offset = *pOffset;
        tscDebug("consumer %ld epoch %d vg %d found %s", tmq->consumerId, epoch, pVgEp->vgId, vgKey);
      }
      tscDebug("consumer %ld epoch %d vg %d offset set to %ld", tmq->consumerId, epoch, pVgEp->vgId, offset);
      SMqClientVg clientVg = {
          .pollCnt = 0,
          .currentOffset = offset,
          .vgId = pVgEp->vgId,
          .epSet = pVgEp->epSet,
          .vgStatus = TMQ_VG_STATUS__IDLE,
          .vgSkipCnt = 0,
      };
      taosArrayPush(topic.vgs, &clientVg);
      set = true;
    }
    taosArrayPush(newTopics, &topic);
  }
  if (tmq->clientTopics) taosArrayDestroy(tmq->clientTopics);
  taosHashCleanup(pHash);
  tmq->clientTopics = newTopics;
  atomic_store_32(&tmq->epoch, epoch);
  return set;
}

int32_t tmqAskEpCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = pParam->tmq;
  pParam->code = code;
  if (code != 0) {
    tscError("consumer %ld get topic endpoint error, not ready, wait:%d", tmq->consumerId, pParam->sync);
    goto END;
  }

  // tmq's epoch is monotonically increase,
  // so it's safe to discard any old epoch msg.
  // Epoch will only increase when received newer epoch ep msg
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tscDebug("consumer %ld recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (head->epoch <= epoch) {
    goto END;
  }

  if (pParam->sync) {
    SMqCMGetSubEpRsp rsp;
    tDecodeSMqCMGetSubEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
    if (tmqUpdateEp(tmq, head->epoch, &rsp)) {
      atomic_store_64(&tmq->status, TMQ_CONSUMER_STATUS__READY);
    }
    tDeleteSMqCMGetSubEpRsp(&rsp);
  } else {
    SMqAskEpRspWrapper* pWrapper = taosAllocateQitem(sizeof(SMqAskEpRspWrapper));
    if (pWrapper == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      code = -1;
      goto END;
    }
    pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
    pWrapper->epoch = head->epoch;
    memcpy(&pWrapper->msg, pMsg->pData, sizeof(SMqRspHead));
    tDecodeSMqCMGetSubEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pWrapper->msg);

    taosWriteQitem(tmq->mqueue, pWrapper);
    /*tsem_post(&tmq->rspSem);*/
    taosMemoryFree(pParam);
  }

END:
  atomic_store_8(&tmq->epStatus, 0);
  if (pParam->sync) {
    tsem_post(&pParam->rspSem);
  }
  return code;
}

int32_t tmqAskEp(tmq_t* tmq, bool sync) {
  int32_t code = 0;
  int8_t  epStatus = atomic_val_compare_exchange_8(&tmq->epStatus, 0, 1);
  if (epStatus == 1) {
    int32_t epSkipCnt = atomic_add_fetch_32(&tmq->epSkipCnt, 1);
    tscTrace("consumer %ld skip ask ep cnt %d", tmq->consumerId, epSkipCnt);
    if (epSkipCnt < 5000) return 0;
  }
  atomic_store_32(&tmq->epSkipCnt, 0);
  int32_t           tlen = sizeof(SMqCMGetSubEpReq);
  SMqCMGetSubEpReq* req = taosMemoryMalloc(tlen);
  if (req == NULL) {
    tscError("failed to malloc get subscribe ep buf");
    atomic_store_8(&tmq->epStatus, 0);
    return -1;
  }
  req->consumerId = htobe64(tmq->consumerId);
  req->epoch = htonl(tmq->epoch);
  strcpy(req->cgroup, tmq->groupId);

  SMqAskEpCbParam* pParam = taosMemoryMalloc(sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    taosMemoryFree(req);
    atomic_store_8(&tmq->epStatus, 0);
    return -1;
  }
  pParam->tmq = tmq;
  pParam->sync = sync;
  tsem_init(&pParam->rspSem, 0, 0);

  SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
    taosMemoryFree(req);
    atomic_store_8(&tmq->epStatus, 0);
    return -1;
  }

  sendInfo->msgInfo = (SDataBuf){
      .pData = req,
      .len = tlen,
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqAskEpCb;
  sendInfo->msgType = TDMT_MND_GET_SUB_EP;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  tscDebug("consumer %ld ask ep", tmq->consumerId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (sync) {
    tsem_wait(&pParam->rspSem);
    code = pParam->code;
    taosMemoryFree(pParam);
  }
  return code;
}

tmq_resp_err_t tmq_seek(tmq_t* tmq, const tmq_topic_vgroup_t* offset) {
  const SMqOffset* pOffset = &offset->offset;
  if (strcmp(pOffset->cgroup, tmq->groupId) != 0) {
    return TMQ_RESP_ERR__FAIL;
  }
  int32_t sz = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < sz; i++) {
    SMqClientTopic* clientTopic = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(clientTopic->topicName, pOffset->topicName) == 0) {
      int32_t vgSz = taosArrayGetSize(clientTopic->vgs);
      for (int32_t j = 0; j < vgSz; j++) {
        SMqClientVg* pVg = taosArrayGet(clientTopic->vgs, j);
        if (pVg->vgId == pOffset->vgId) {
          pVg->currentOffset = pOffset->offset;
          tmqClearUnhandleMsg(tmq);
          return TMQ_RESP_ERR__SUCCESS;
        }
      }
    }
  }
  return TMQ_RESP_ERR__FAIL;
}

SMqPollReqV2* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t blockingTime, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  int64_t reqOffset;
  if (pVg->currentOffset >= 0) {
    reqOffset = pVg->currentOffset;
  } else {
    if (tmq->resetOffsetCfg == TMQ_CONF__RESET_OFFSET__NONE) {
      tscError("unable to poll since no committed offset but reset offset is set to none");
      return NULL;
    }
    reqOffset = tmq->resetOffsetCfg;
  }

  SMqPollReqV2* pReq = taosMemoryMalloc(sizeof(SMqPollReqV2));
  if (pReq == NULL) {
    return NULL;
  }

  /*strcpy(pReq->topic, pTopic->topicName);*/
  /*strcpy(pReq->cgroup, tmq->groupId);*/

  int32_t tlen = strlen(tmq->groupId);
  memcpy(pReq->subKey, tmq->groupId, tlen);
  pReq->subKey[tlen] = TMQ_SEPARATOR;
  strcpy(pReq->subKey + tlen + 1, pTopic->topicName);

  pReq->blockingTime = blockingTime;
  pReq->consumerId = tmq->consumerId;
  pReq->epoch = tmq->epoch;
  pReq->currentOffset = reqOffset;
  pReq->reqId = generateRequestId();

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqPollReqV2));
  return pReq;
}

SMqRspObj* tmqBuildRspFromWrapper(SMqPollRspWrapper* pWrapper) {
  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  pRspObj->resType = RES_TYPE__TMQ;
  strncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  pRspObj->vgId = pWrapper->vgHandle->vgId;
  pRspObj->resIter = -1;
  memcpy(&pRspObj->rsp, &pWrapper->msg, sizeof(SMqDataBlkRsp));

  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;
  setResSchemaInfo(&pRspObj->resInfo, pWrapper->topicHandle->schema.pSchema, pWrapper->topicHandle->schema.nCols);

  taosFreeQitem(pWrapper);
  return pRspObj;
}

int32_t tmqPollImpl(tmq_t* tmq, int64_t blockingTime) {
  /*printf("call poll\n");*/
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      int32_t      vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus != TMQ_VG_STATUS__IDLE) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tscTrace("consumer %ld epoch %d skip vg %d skip cnt %d", tmq->consumerId, tmq->epoch, pVg->vgId, vgSkipCnt);
        continue;
        /*if (vgSkipCnt < 10000) continue;*/
#if 0
        if (skipCnt < 30000) {
          continue;
        } else {
        tscDebug("consumer %ld skip vg %d skip too much reset", tmq->consumerId, pVg->vgId);
        }
#endif
      }
      atomic_store_32(&pVg->vgSkipCnt, 0);
      SMqPollReqV2* pReq = tmqBuildConsumeReqImpl(tmq, blockingTime, pTopic, pVg);
      if (pReq == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        /*tsem_post(&tmq->rspSem);*/
        return -1;
      }
      SMqPollCbParam* pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
      if (pParam == NULL) {
        taosMemoryFree(pReq);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        /*tsem_post(&tmq->rspSem);*/
        return -1;
      }
      pParam->tmq = tmq;
      pParam->pVg = pVg;
      pParam->pTopic = pTopic;
      pParam->vgId = pVg->vgId;
      pParam->epoch = tmq->epoch;
      pParam->sync = 0;

      SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        taosMemoryFree(pReq);
        taosMemoryFree(pParam);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        /*tsem_post(&tmq->rspSem);*/
        return -1;
      }

      sendInfo->msgInfo = (SDataBuf){
          .pData = pReq,
          .len = sizeof(SMqPollReqV2),
          .handle = NULL,
      };
      sendInfo->requestId = pReq->reqId;
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqPollCb;
      sendInfo->msgType = TDMT_VND_CONSUME;

      int64_t transporterId = 0;
      /*printf("send poll\n");*/
      /*atomic_add_fetch_32(&tmq->waitingRequest, 1);*/
      tscDebug("consumer %ld send poll to %s : vg %d, epoch %d, req offset %ld, reqId %lu", tmq->consumerId,
               pTopic->topicName, pVg->vgId, tmq->epoch, pVg->currentOffset, pReq->reqId);
      /*printf("send vg %d %ld\n", pVg->vgId, pVg->currentOffset);*/
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
      pVg->pollCnt++;
      tmq->pollCnt++;
    }
  }
  return 0;
}

int32_t tmqHandleNoPollRsp(tmq_t* tmq, SMqRspWrapper* rspWrapper, bool* pReset) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    /*printf("ep %d %d\n", rspMsg->head.epoch, tmq->epoch);*/
    if (rspWrapper->epoch > atomic_load_32(&tmq->epoch)) {
      SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)rspWrapper;
      SMqCMGetSubEpRsp*   rspMsg = &pEpRspWrapper->msg;
      tmqUpdateEp(tmq, rspWrapper->epoch, rspMsg);
      /*tmqClearUnhandleMsg(tmq);*/
      *pReset = true;
    } else {
      *pReset = false;
    }
  } else {
    return -1;
  }
  return 0;
}

SMqRspObj* tmqHandleAllRsp(tmq_t* tmq, int64_t blockingTime, bool pollIfReset) {
  while (1) {
    SMqRspWrapper* rspWrapper = NULL;
    taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&rspWrapper);
      if (rspWrapper == NULL) return NULL;
    }

    if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      /*atomic_sub_fetch_32(&tmq->readyRequest, 1);*/
      /*printf("handle poll rsp %d\n", rspMsg->head.mqMsgType);*/
      if (pollRspWrapper->msg.head.epoch == atomic_load_32(&tmq->epoch)) {
        /*printf("epoch match\n");*/
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vg %d offset %ld up to %ld\n", pVg->vgId, pVg->currentOffset, rspMsg->msg.rspOffset);*/
        pVg->currentOffset = pollRspWrapper->msg.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        if (pollRspWrapper->msg.blockNum == 0) {
          taosFreeQitem(pollRspWrapper);
          rspWrapper = NULL;
          continue;
        }
        // build rsp
        SMqRspObj* pRsp = tmqBuildRspFromWrapper(pollRspWrapper);
        return pRsp;
      } else {
        /*printf("epoch mismatch\n");*/
        taosFreeQitem(pollRspWrapper);
      }
    } else {
      /*printf("handle ep rsp %d\n", rspMsg->head.mqMsgType);*/
      bool reset = false;
      tmqHandleNoPollRsp(tmq, rspWrapper, &reset);
      taosFreeQitem(rspWrapper);
      if (pollIfReset && reset) {
        tscDebug("consumer %ld reset and repoll", tmq->consumerId);
        tmqPollImpl(tmq, blockingTime);
      }
    }
  }
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t blocking_time) {
  SMqRspObj* rspObj;
  int64_t    startTime = taosGetTimestampMs();

  // TODO: put into another thread or delayed queue
  int64_t status = atomic_load_64(&tmq->status);
  while (0 != tmqAskEp(tmq, status == TMQ_CONSUMER_STATUS__INIT)) {
    tscDebug("not ready, retry\n");
    taosSsleep(1);
  }

  rspObj = tmqHandleAllRsp(tmq, blocking_time, false);
  if (rspObj) {
    return (TAOS_RES*)rspObj;
  }

  while (1) {
    /*printf("cycle\n");*/
    tmqAskEp(tmq, false);
    tmqPollImpl(tmq, blocking_time);

    /*tsem_wait(&tmq->rspSem);*/

    rspObj = tmqHandleAllRsp(tmq, blocking_time, false);
    if (rspObj) {
      return (TAOS_RES*)rspObj;
    }
    if (blocking_time != 0) {
      int64_t endTime = taosGetTimestampMs();
      if (endTime - startTime > blocking_time) {
        tscDebug("consumer %ld (epoch %d) timeout, no rsp", tmq->consumerId, tmq->epoch);
        return NULL;
      }
    }
  }
}

tmq_resp_err_t tmq_consumer_close(tmq_t* tmq) {
  // TODO
  return TMQ_RESP_ERR__SUCCESS;
}

const char* tmq_err2str(tmq_resp_err_t err) {
  if (err == TMQ_RESP_ERR__SUCCESS) {
    return "success";
  }
  return "fail";
}

char* tmq_get_topic_name(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return pRspObj->topic;
  } else {
    return NULL;
  }
}

int32_t tmq_get_vgroup_id(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return pRspObj->vgId;
  } else {
    return -1;
  }
}

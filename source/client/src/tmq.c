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
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"

int32_t tmqAskEp(tmq_t* tmq, bool async);

typedef struct {
  int8_t inited;
  tmr_h  timer;
} SMqMgmt;

static SMqMgmt tmqMgmt = {0};

typedef struct {
  int8_t  tmqRspType;
  int32_t epoch;
} SMqRspWrapper;

typedef struct {
  int8_t      tmqRspType;
  int32_t     epoch;
  SMqAskEpRsp msg;
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
  int32_t        autoCommitInterval;
  char*          ip;
  char*          user;
  char*          pass;
  char*          db;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
};

struct tmq_t {
  // conf
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[256];
  int8_t         autoCommit;
  int32_t        autoCommitInterval;
  int32_t        resetOffsetCfg;
  int64_t        consumerId;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;

  // status
  int8_t  status;
  int32_t epoch;
#if 0
  int8_t  epStatus;
  int32_t epSkipCnt;
#endif
  int64_t pollCnt;

  // timer
  tmr_h hbTimer;
  tmr_h reportTimer;
  tmr_h commitTimer;

  // connection
  STscObj* pTscObj;

  // container
  SArray*     clientTopics;  // SArray<SMqClientTopic>
  STaosQueue* mqueue;        // queue of rsp
  STaosQall*  qall;
  STaosQueue* delayedTask;  // delayed task queue for heartbeat and auto commit

  // ctl
  tsem_t rspSem;
};

enum {
  TMQ_VG_STATUS__IDLE = 0,
  TMQ_VG_STATUS__WAIT,
};

enum {
  TMQ_CONSUMER_STATUS__INIT = 0,
  TMQ_CONSUMER_STATUS__READY,
  TMQ_CONSUMER_STATUS__NO_TOPIC,
};

enum {
  TMQ_DELAYED_TASK__HB = 1,
  TMQ_DELAYED_TASK__REPORT,
  TMQ_DELAYED_TASK__COMMIT,
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
  char* topicName;

  SArray* vgs;  // SArray<SMqClientVg>

  int8_t         isSchemaAdaptive;
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
  int32_t async;
  tsem_t  rspSem;
} SMqAskEpCbParam;

typedef struct {
  tmq_t*          tmq;
  SMqClientVg*    pVg;
  SMqClientTopic* pTopic;
  int32_t         epoch;
  int32_t         vgId;
  tsem_t          rspSem;
} SMqPollCbParam;

typedef struct {
  tmq_t*         tmq;
  int32_t        async;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  conf->autoCommit = true;
  conf->autoCommitInterval = 5000;
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
  taosArrayDestroyP(container, taosMemoryFree);
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

void tmqAssignDelayedHbTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t));
  *pTaskType = TMQ_DELAYED_TASK__HB;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t));
  *pTaskType = TMQ_DELAYED_TASK__COMMIT;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

void tmqAssignDelayedReportTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t));
  *pTaskType = TMQ_DELAYED_TASK__REPORT;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

int32_t tmqHandleAllDelayedTask(tmq_t* tmq) {
  STaosQall* qall = taosAllocateQall();
  taosReadAllQitems(tmq->delayedTask, qall);
  while (1) {
    int8_t* pTaskType = NULL;
    taosGetQitem(qall, (void**)&pTaskType);
    if (pTaskType == NULL) break;

    if (*pTaskType == TMQ_DELAYED_TASK__HB) {
      tmqAskEp(tmq, true);
      taosTmrReset(tmqAssignDelayedHbTask, 1000, tmq, tmqMgmt.timer, &tmq->hbTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmq_commit(tmq, NULL, true);
      taosTmrReset(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, tmq, tmqMgmt.timer, &tmq->commitTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__REPORT) {
    } else {
      ASSERT(0);
    }
  }
  taosFreeQall(qall);
  return 0;
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
  tmq_t* tmq = pParam->tmq;
  tsem_post(&pParam->rspSem);
  return 0;
}

int32_t tmqCommitCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqCommitCbParam* pParam = (SMqCommitCbParam*)param;
  pParam->rspErr = code == 0 ? TMQ_RESP_ERR__SUCCESS : TMQ_RESP_ERR__FAIL;
  if (pParam->tmq->commitCb) {
    pParam->tmq->commitCb(pParam->tmq, pParam->rspErr, NULL, pParam->tmq->commitCbUserParam);
  }
  if (!pParam->async)
    tsem_post(&pParam->rspSem);
  else {
    tsem_destroy(&pParam->rspSem);
    /*if (pParam->pArray) {*/
    /*taosArrayDestroy(pParam->pArray);*/
    /*}*/
    taosMemoryFree(pParam);
  }
  return 0;
}

tmq_resp_err_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (*topics == NULL) {
    *topics = tmq_list_new();
  }
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    tmq_list_append(*topics, strchr(topic->topicName, '.') + 1);
  }
  return TMQ_RESP_ERR__SUCCESS;
}

tmq_resp_err_t tmq_unsubscribe(tmq_t* tmq) {
  tmq_list_t*    lst = tmq_list_new();
  tmq_resp_err_t rsp = tmq_subscribe(tmq, lst);
  tmq_list_destroy(lst);
  return rsp;
}

#if 0
tmq_t* tmq_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = taosMemoryCalloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
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
  // init timer
  int8_t inited = atomic_val_compare_exchange_8(&tmqMgmt.inited, 0, 1);
  if (inited == 0) {
    tmqMgmt.timer = taosTmrInit(1000, 100, 360000, "TMQ");
    if (tmqMgmt.timer == NULL) {
      atomic_store_8(&tmqMgmt.inited, 0);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    return NULL;
  }

  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  ASSERT(user);
  ASSERT(pass);
  ASSERT(conf->groupId[0]);

  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  pTmq->mqueue = taosOpenQueue();
  pTmq->qall = taosAllocateQall();
  pTmq->delayedTask = taosOpenQueue();

  if (pTmq->clientTopics == NULL || pTmq->mqueue == NULL || pTmq->qall == NULL || pTmq->delayedTask == NULL) {
    goto FAIL;
  }

  // init status
  pTmq->status = TMQ_CONSUMER_STATUS__INIT;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  /*pTmq->epStatus = 0;*/
  /*pTmq->epSkipCnt = 0;*/

  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->autoCommit;
  pTmq->autoCommitInterval = conf->autoCommitInterval;
  pTmq->commitCb = conf->commitCb;
  pTmq->commitCbUserParam = conf->commitCbUserParam;
  pTmq->resetOffsetCfg = conf->resetOffset;

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem_init(&pTmq->rspSem, 0, 0) != 0) {
    goto FAIL;
  }

  // init connection
  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) {
    tsem_destroy(&pTmq->rspSem);
    goto FAIL;
  }

  return pTmq;

FAIL:
  if (pTmq->clientTopics) taosArrayDestroy(pTmq->clientTopics);
  if (pTmq->mqueue) taosCloseQueue(pTmq->mqueue);
  if (pTmq->delayedTask) taosCloseQueue(pTmq->delayedTask);
  if (pTmq->qall) taosFreeQall(pTmq->qall);
  taosMemoryFree(pTmq);
  return NULL;
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

  SEncoder encoder;

  tEncoderInit(&encoder, NULL, 0);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  int32_t tlen = encoder.pos;
  void*   buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    tEncoderClear(&encoder);
    return -1;
  }
  tEncoderClear(&encoder);

  tEncoderInit(&encoder, buf, tlen);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  tEncoderClear(&encoder);

  pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_MQ_COMMIT_OFFSET);
  if (pRequest == NULL) {
    tscError("failed to malloc request");
  }

  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
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
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqCommitCb;
  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
    tsem_wait(&pParam->rspSem);
    resp = pParam->rspErr;
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);

    if (pArray) {
      taosArrayDestroy(pArray);
    }
  }

  return resp;
}

tmq_resp_err_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = -1;

  req.consumerId = tmq->consumerId;
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  req.topicNames = taosArrayInit(sz, sizeof(void*));
  if (req.topicNames == NULL) goto FAIL;

  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(container, i);

    SName name = {0};
    tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));

    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      goto FAIL;
    }
    tNameExtractFullName(&name, topicFName);

    tscDebug("subscribe topic: %s", topicFName);

    taosArrayPush(req.topicNames, &topicFName);
  }

  int32_t tlen = tSerializeSCMSubscribeReq(NULL, &req);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto FAIL;

  void* abuf = buf;
  tSerializeSCMSubscribeReq(&abuf, &req);

  SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
  if (sendInfo == NULL) goto FAIL;

  SMqSubscribeCbParam param = {
      .rspErr = TMQ_RESP_ERR__SUCCESS,
      .tmq = tmq,
  };

  if (tsem_init(&param.rspSem, 0, 0) != 0) goto FAIL;

  sendInfo->msgInfo = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = &param;
  sendInfo->fp = tmqSubscribeCb;
  sendInfo->msgType = TDMT_MND_SUBSCRIBE;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  // avoid double free if msg is sent
  buf = NULL;

  tsem_wait(&param.rspSem);
  tsem_destroy(&param.rspSem);

  code = param.rspErr;
  if (code != 0) goto FAIL;

  while (TSDB_CODE_MND_CONSUMER_NOT_READY == tmqAskEp(tmq, false)) {
    tscDebug("consumer not ready, retry");
    taosMsleep(500);
  }

  // init hb timer
  tmq->hbTimer = taosTmrStart(tmqAssignDelayedHbTask, 1000, tmq, tmqMgmt.timer);

  // init auto commit timer
  if (tmq->autoCommit) {
    tmq->commitTimer = taosTmrStart(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, tmq, tmqMgmt.timer);
  }

  code = 0;
FAIL:
  if (req.topicNames != NULL) taosArrayDestroyP(req.topicNames, taosMemoryFree);
  if (code != 0 && buf) {
    taosMemoryFree(buf);
  }
  return code;
}

void tmq_conf_set_offset_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  //
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

#if 0
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
  strcpy(req.targetStbFullName, tbName);

  int   tlen = tSerializeSCMCreateStreamReq(NULL, 0, &req);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  tSerializeSCMCreateStreamReq(buf, tlen, &req);

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
    tsem_post(&tmq->rspSem);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("mismatch rsp from vg %d, epoch %d, current epoch %d", pParam->vgId, msgEpoch, tmqEpoch);
  }

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
  tsem_post(&tmq->rspSem);

  return 0;
CREATE_MSG_FAIL:
  if (pParam->epoch == tmq->epoch) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  tsem_post(&tmq->rspSem);
  return -1;
}

bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, SMqAskEpRsp* pRsp) {
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

  if (taosArrayGetSize(tmq->clientTopics) == 0)
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__NO_TOPIC);
  else
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__READY);

  atomic_store_32(&tmq->epoch, epoch);
  return set;
}

int32_t tmqAskEpCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = pParam->tmq;
  pParam->code = code;
  if (code != 0) {
    tscError("consumer %ld get topic endpoint error, not ready, wait:%d", tmq->consumerId, pParam->async);
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

  if (!pParam->async) {
    SMqAskEpRsp rsp;
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
    tmqUpdateEp(tmq, head->epoch, &rsp);
    tDeleteSMqAskEpRsp(&rsp);
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
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pWrapper->msg);

    taosWriteQitem(tmq->mqueue, pWrapper);
    tsem_post(&tmq->rspSem);
    taosMemoryFree(pParam);
  }

END:
  /*atomic_store_8(&tmq->epStatus, 0);*/
  if (!pParam->async) {
    tsem_post(&pParam->rspSem);
  }
  return code;
}

int32_t tmqAskEp(tmq_t* tmq, bool async) {
  int32_t code = 0;
#if 0
  int8_t  epStatus = atomic_val_compare_exchange_8(&tmq->epStatus, 0, 1);
  if (epStatus == 1) {
    int32_t epSkipCnt = atomic_add_fetch_32(&tmq->epSkipCnt, 1);
    tscTrace("consumer %ld skip ask ep cnt %d", tmq->consumerId, epSkipCnt);
    if (epSkipCnt < 5000) return 0;
  }
  atomic_store_32(&tmq->epSkipCnt, 0);
#endif
  int32_t      tlen = sizeof(SMqAskEpReq);
  SMqAskEpReq* req = taosMemoryMalloc(tlen);
  if (req == NULL) {
    tscError("failed to malloc get subscribe ep buf");
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }
  req->consumerId = htobe64(tmq->consumerId);
  req->epoch = htonl(tmq->epoch);
  strcpy(req->cgroup, tmq->groupId);

  SMqAskEpCbParam* pParam = taosMemoryMalloc(sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    taosMemoryFree(req);
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }
  pParam->tmq = tmq;
  pParam->async = async;
  tsem_init(&pParam->rspSem, 0, 0);

  SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
    taosMemoryFree(req);
    /*atomic_store_8(&tmq->epStatus, 0);*/
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
  sendInfo->msgType = TDMT_MND_MQ_ASK_EP;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  tscDebug("consumer %ld ask ep", tmq->consumerId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
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

SMqPollReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t waitTime, SMqClientTopic* pTopic, SMqClientVg* pVg) {
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

  SMqPollReq* pReq = taosMemoryMalloc(sizeof(SMqPollReq));
  if (pReq == NULL) {
    return NULL;
  }

  /*strcpy(pReq->topic, pTopic->topicName);*/
  /*strcpy(pReq->cgroup, tmq->groupId);*/

  int32_t tlen = strlen(tmq->groupId);
  memcpy(pReq->subKey, tmq->groupId, tlen);
  pReq->subKey[tlen] = TMQ_SEPARATOR;
  strcpy(pReq->subKey + tlen + 1, pTopic->topicName);

  pReq->waitTime = waitTime;
  pReq->consumerId = tmq->consumerId;
  pReq->epoch = tmq->epoch;
  pReq->currentOffset = reqOffset;
  pReq->reqId = generateRequestId();

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqPollReq));
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
  if (!pWrapper->msg.withSchema) {
    setResSchemaInfo(&pRspObj->resInfo, pWrapper->topicHandle->schema.pSchema, pWrapper->topicHandle->schema.nCols);
  }

  taosFreeQitem(pWrapper);
  return pRspObj;
}

int32_t tmqPollImpl(tmq_t* tmq, int64_t waitTime) {
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
      SMqPollReq* pReq = tmqBuildConsumeReqImpl(tmq, waitTime, pTopic, pVg);
      if (pReq == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }
      SMqPollCbParam* pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
      if (pParam == NULL) {
        taosMemoryFree(pReq);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }
      pParam->tmq = tmq;
      pParam->pVg = pVg;
      pParam->pTopic = pTopic;
      pParam->vgId = pVg->vgId;
      pParam->epoch = tmq->epoch;

      SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        taosMemoryFree(pReq);
        taosMemoryFree(pParam);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }

      sendInfo->msgInfo = (SDataBuf){
          .pData = pReq,
          .len = sizeof(SMqPollReq),
          .handle = NULL,
      };
      sendInfo->requestId = pReq->reqId;
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqPollCb;
      sendInfo->msgType = TDMT_VND_CONSUME;

      int64_t transporterId = 0;
      /*printf("send poll\n");*/
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
      SMqAskEpRsp*        rspMsg = &pEpRspWrapper->msg;
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

SMqRspObj* tmqHandleAllRsp(tmq_t* tmq, int64_t waitTime, bool pollIfReset) {
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
        tmqPollImpl(tmq, waitTime);
      }
    }
  }
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t wait_time) {
  SMqRspObj* rspObj;
  int64_t    startTime = taosGetTimestampMs();

  rspObj = tmqHandleAllRsp(tmq, wait_time, false);
  if (rspObj) {
    return (TAOS_RES*)rspObj;
  }

  // in no topic status also need process delayed task
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    return NULL;
  }

  while (1) {
    tmqHandleAllDelayedTask(tmq);
    tmqPollImpl(tmq, wait_time);

    rspObj = tmqHandleAllRsp(tmq, wait_time, false);
    if (rspObj) {
      return (TAOS_RES*)rspObj;
    }
    if (wait_time != 0) {
      int64_t endTime = taosGetTimestampMs();
      int64_t leftTime = endTime - startTime;
      if (leftTime > wait_time) {
        tscDebug("consumer %ld (epoch %d) timeout, no rsp", tmq->consumerId, tmq->epoch);
        return NULL;
      }
      tsem_timewait(&tmq->rspSem, leftTime * 1000);
    } else {
      // use tsem_timewait instead of tsem_wait to avoid unexpected stuck
      tsem_timewait(&tmq->rspSem, 500 * 1000);
    }
  }
}

tmq_resp_err_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq->status == TMQ_CONSUMER_STATUS__READY) {
    tmq_list_t*    lst = tmq_list_new();
    tmq_resp_err_t rsp = tmq_subscribe(tmq, lst);
    tmq_list_destroy(lst);
    if (rsp == TMQ_RESP_ERR__SUCCESS) {
      // TODO: free resources
      return TMQ_RESP_ERR__SUCCESS;
    } else {
      return TMQ_RESP_ERR__FAIL;
    }
  }
  // TODO: free resources
  return TMQ_RESP_ERR__SUCCESS;
}

const char* tmq_err2str(tmq_resp_err_t err) {
  if (err == TMQ_RESP_ERR__SUCCESS) {
    return "success";
  }
  return "fail";
}

const char* tmq_get_topic_name(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return strchr(pRspObj->topic, '.') + 1;
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

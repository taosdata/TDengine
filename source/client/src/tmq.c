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

#define _DEFAULT_SOURCE

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
#include "tpagedbuf.h"
#include "tref.h"

struct tmq_list_t {
  int32_t cnt;
  int32_t tot;
  char*   elems[];
};

struct tmq_topic_vgroup_t {
  char*   topic;
  int32_t vgId;
  int64_t offset;
};

struct tmq_topic_vgroup_list_t {
  int32_t             cnt;
  int32_t             size;
  tmq_topic_vgroup_t* elems;
};

struct tmq_conf_t {
  char clientId[256];
  char groupId[256];
  bool auto_commit;
  /*char*          ip;*/
  /*uint16_t       port;*/
  tmq_commit_cb* commit_cb;
};

struct tmq_t {
  // conf
  char           groupId[256];
  char           clientId[256];
  bool           autoCommit;
  SRWLatch       lock;
  int64_t        consumerId;
  int32_t        epoch;
  int64_t        status;
  tsem_t         rspSem;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  int32_t        nextTopicIdx;
  SArray*        clientTopics;  // SArray<SMqClientTopic>
  // stat
  int64_t pollCnt;
};

struct tmq_message_t {
  SMqConsumeRsp rsp;
};

typedef struct SMqClientVg {
  // statistics
  int64_t pollCnt;
  // offset
  int64_t committedOffset;
  int64_t currentOffset;
  // connection info
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
  SArray* vgs;  // SArray<SMqClientVg>
} SMqClientTopic;

typedef struct {
  tmq_t*         tmq;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  tmq_t*  tmq;
  int32_t wait;
} SMqAskEpCbParam;

typedef struct {
  tmq_t*          tmq;
  SMqClientVg*    pVg;
  tmq_message_t** retMsg;
  tsem_t          rspSem;
} SMqConsumeCbParam;

typedef struct {
  tmq_t*       tmq;
  SMqClientVg* pVg;
  int32_t      async;
  tsem_t       rspSem;
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = calloc(1, sizeof(tmq_conf_t));
  conf->auto_commit = false;
  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if (conf) free(conf);
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
      conf->auto_commit = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->auto_commit = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }
  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() {
  tmq_list_t* ptr = malloc(sizeof(tmq_list_t) + 8 * sizeof(char*));
  if (ptr == NULL) {
    return ptr;
  }
  ptr->cnt = 0;
  ptr->tot = 8;
  return ptr;
}

int32_t tmq_list_append(tmq_list_t* ptr, const char* src) {
  if (ptr->cnt >= ptr->tot - 1) return -1;
  ptr->elems[ptr->cnt] = strdup(src);
  ptr->cnt++;
  return 0;
}

int32_t tmqSubscribeCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;
  tsem_post(&pParam->rspSem);
  return 0;
}

int32_t tmqCommitCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqCommitCbParam* pParam = (SMqCommitCbParam*)param;
  tmq_resp_err_t    rspErr = code == 0 ? TMQ_RESP_ERR__SUCCESS : TMQ_RESP_ERR__FAIL;
  if (pParam->tmq->commit_cb) {
    pParam->tmq->commit_cb(pParam->tmq, rspErr, NULL, NULL);
  }
  if (!pParam->async) tsem_post(&pParam->rspSem);
  return 0;
}

tmq_t* tmq_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = calloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  taosInitRWLatch(&pTmq->lock);
  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->auto_commit;
  pTmq->commit_cb = conf->commit_cb;

  tsem_init(&pTmq->rspSem, 0, 0);
  pTmq->consumerId = generateRequestId() & (((uint64_t)-1) >> 1);
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  return pTmq;
}

tmq_resp_err_t tmq_subscribe(tmq_t* tmq, tmq_list_t* topic_list) {
  SRequestObj* pRequest = NULL;
  int32_t      sz = topic_list->cnt;
  // destroy ex
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
    if (dbName == NULL) {
      return TMQ_RESP_ERR__FAIL;
    }
    tNameSetDbName(&name, tmq->pTscObj->acctId, dbName, strlen(dbName));
    tNameFromString(&name, topicName, T_NAME_TABLE);

    char* topicFname = calloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFname == NULL) {
    }
    tNameExtractFullName(&name, topicFname);
    tscDebug("subscribe topic: %s", topicFname);
    SMqClientTopic topic = {
        .nextVgIdx = 0, .sql = NULL, .sqlLen = 0, .topicId = 0, .topicName = topicFname, .vgs = NULL};
    topic.vgs = taosArrayInit(0, sizeof(SMqClientVg));
    taosArrayPush(tmq->clientTopics, &topic);
    /*SMqClientTopic topic = {*/
    /*.*/
    /*};*/
    taosArrayPush(req.topicNames, &topicFname);
    free(dbName);
  }

  int   tlen = tSerializeSCMSubscribeReq(NULL, &req);
  void* buf = malloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  void* abuf = buf;
  tSerializeSCMSubscribeReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_SUBSCRIBE);
  if (pRequest == NULL) {
    tscError("failed to malloc sqlObj");
  }

  SMqSubscribeCbParam param = {.rspErr = TMQ_RESP_ERR__SUCCESS, .tmq = tmq};
  tsem_init(&param.rspSem, 0, 0);

  pRequest->body.requestMsg = (SDataBuf){.pData = buf, .len = tlen};

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

SArray* tmqGetConnInfo(SClientHbKey connKey, void* param) {
  tmq_t*  pTmq = (void*)param;
  SArray* pArray = taosArrayInit(0, sizeof(SKv));
  if (pArray == NULL) {
    return NULL;
  }
  SKv kv = {0};
  kv.key = HEARTBEAT_KEY_MQ_TMP;

  SMqHbMsg* pMqHb = malloc(sizeof(SMqHbMsg));
  if (pMqHb == NULL) {
    return pArray;
  }
  pMqHb->consumerId = connKey.connId;
  SArray* clientTopics = pTmq->clientTopics;
  int     sz = taosArrayGetSize(clientTopics);
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

TAOS_RES* tmq_create_topic(TAOS* taos, const char* topicName, const char* sql, int sqlLen) {
  STscObj*     pTscObj = (STscObj*)taos;
  SRequestObj* pRequest = NULL;
  SQueryNode*  pQueryNode = NULL;
  char*        pStr = NULL;

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

  tscDebug("start to create topic, %s", topicName);

  CHECK_CODE_GOTO(buildRequest(pTscObj, sql, sqlLen, &pRequest), _return);
  CHECK_CODE_GOTO(parseSql(pRequest, &pQueryNode), _return);

  SQueryStmtInfo* pQueryStmtInfo = (SQueryStmtInfo*)pQueryNode;
  pQueryStmtInfo->info.continueQuery = true;

  // todo check for invalid sql statement and return with error code

  SSchema* schema = NULL;
  int32_t  numOfCols = 0;
  CHECK_CODE_GOTO(qCreateQueryDag(pQueryNode, &pRequest->body.pDag, &schema, &numOfCols, NULL, pRequest->requestId),
                  _return);

  pStr = qDagToString(pRequest->body.pDag);
  if (pStr == NULL) {
    goto _return;
  }

  /*printf("%s\n", pStr);*/

  // The topic should be related to a database that the queried table is belonged to.
  SName name = {0};
  char  dbName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&((SQueryStmtInfo*)pQueryNode)->pTableMetaInfo[0]->name, dbName);

  tNameFromString(&name, dbName, T_NAME_ACCT | T_NAME_DB);
  tNameFromString(&name, topicName, T_NAME_TABLE);

  char topicFname[TSDB_TOPIC_FNAME_LEN] = {0};
  tNameExtractFullName(&name, topicFname);

  SMCreateTopicReq req = {
      .name = (char*)topicFname,
      .igExists = 1,
      .physicalPlan = (char*)pStr,
      .sql = (char*)sql,
      .logicalPlan = (char*)"no logic plan",
  };

  int   tlen = tSerializeMCreateTopicReq(NULL, &req);
  void* buf = malloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  void* abuf = buf;
  tSerializeMCreateTopicReq(&abuf, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){.pData = buf, .len = tlen};
  pRequest->type = TDMT_MND_CREATE_TOPIC;

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  SEpSet        epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

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

static char* formatTimestamp(char* buf, int64_t val, int precision) {
  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  /* comment out as it make testcases like select_with_tags.sim fail.
    but in windows, this may cause the call to localtime crash if tt < 0,
    need to find a better solution.
    if (tt < 0) {
      tt = 0;
    }
    */

#ifdef WINDOWS
  if (tt < 0) tt = 0;
#endif
  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }

  struct tm* ptm = localtime(&tt);
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

int32_t tmqGetSkipLogNum(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return 0;
  SMqConsumeRsp* pRsp = (SMqConsumeRsp*)tmq_message;
  return pRsp->skipLogNum;
}

void tmqShowMsg(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;

  static bool    noPrintSchema;
  char           pBuf[128];
  SMqConsumeRsp* pRsp = (SMqConsumeRsp*)tmq_message;
  int32_t        colNum = pRsp->schemas->nCols;
  if (!noPrintSchema) {
    printf("|");
    for (int32_t i = 0; i < colNum; i++) {
      if (i == 0)
        printf(" %25s |", pRsp->schemas->pSchema[i].name);
      else
        printf(" %15s |", pRsp->schemas->pSchema[i].name);
    }
    printf("\n");
    printf("===============================================\n");
    noPrintSchema = true;
  }
  int32_t sz = taosArrayGetSize(pRsp->pBlockData);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pRsp->pBlockData, i);
    int32_t      rows = pDataBlock->info.rows;
    for (int32_t j = 0; j < rows; j++) {
      printf("|");
      for (int32_t k = 0; k < colNum; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(pBuf, *(uint64_t*)var, TSDB_TIME_PRECISION_MILLI);
            printf(" %25s |", pBuf);
            break;
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_UINT:
            printf(" %15u |", *(uint32_t*)var);
            break;
        }
      }
      printf("\n");
    }
  }
}

int32_t tmqPollCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqConsumeCbParam* pParam = (SMqConsumeCbParam*)param;
  SMqClientVg*       pVg = pParam->pVg;
  if (code != 0) {
    printf("msg discard\n");
    tsem_post(&pParam->rspSem);
    return 0;
  }

  SMqConsumeRsp* pRsp = calloc(1, sizeof(SMqConsumeRsp));
  if (pRsp == NULL) {
    tsem_post(&pParam->rspSem);
    return -1;
  }
  tDecodeSMqConsumeRsp(pMsg->pData, pRsp);
  /*printf("rsp commit off:%ld rsp off:%ld has data:%d\n", pRsp->committedOffset, pRsp->rspOffset, pRsp->numOfTopics);*/
  if (pRsp->numOfTopics == 0) {
    /*printf("no data\n");*/
    free(pRsp);
    tsem_post(&pParam->rspSem);
    return 0;
  }
  *pParam->retMsg = (tmq_message_t*)pRsp;
  pVg->currentOffset = pRsp->rspOffset;
  /*printf("rsp offset: %ld\n", rsp.rspOffset);*/
  /*printf("-----msg begin----\n");*/
  tsem_post(&pParam->rspSem);
  /*printf("\n-----msg end------\n");*/
  return 0;
}

int32_t tmqAskEpCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = pParam->tmq;
  if (code != 0) {
    printf("get topic endpoint error, not ready, wait:%d\n", pParam->wait);
    if (pParam->wait) {
      tsem_post(&tmq->rspSem);
    }
    return 0;
  }
  tscDebug("tmq ask ep cb called");
  bool             set = false;
  SMqCMGetSubEpRsp rsp;
  tDecodeSMqCMGetSubEpRsp(pMsg->pData, &rsp);
  int32_t sz = taosArrayGetSize(rsp.topics);
  // TODO: lock
  /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
  /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
  if (rsp.epoch != tmq->epoch) {
    // TODO
    if (tmq->clientTopics) taosArrayDestroy(tmq->clientTopics);
    tmq->clientTopics = taosArrayInit(sz, sizeof(SMqClientTopic));
    for (int32_t i = 0; i < sz; i++) {
      SMqClientTopic topic = {0};
      SMqSubTopicEp* pTopicEp = taosArrayGet(rsp.topics, i);
      topic.topicName = strdup(pTopicEp->topic);
      int32_t vgSz = taosArrayGetSize(pTopicEp->vgs);
      topic.vgs = taosArrayInit(vgSz, sizeof(SMqClientVg));
      for (int32_t j = 0; j < vgSz; j++) {
        SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
        // clang-format off
        SMqClientVg clientVg = {
            .pollCnt = 0,
            .committedOffset = -1,
            .currentOffset = -1,
            .vgId = pVgEp->vgId,
            .epSet = pVgEp->epSet
        };
        // clang-format on
        taosArrayPush(topic.vgs, &clientVg);
        set = true;
      }
      taosArrayPush(tmq->clientTopics, &topic);
    }
    tmq->epoch = rsp.epoch;
  }
  if (set) {
    atomic_store_64(&tmq->status, 1);
  }
  // unlock
  /*tsem_post(&tmq->rspSem);*/
  if (pParam->wait) {
    tsem_post(&tmq->rspSem);
  }
  tDeleteSMqCMGetSubEpRsp(&rsp);
  return 0;
}

int32_t tmqAskEp(tmq_t* tmq, bool wait) {
  int32_t           tlen = sizeof(SMqCMGetSubEpReq);
  SMqCMGetSubEpReq* buf = malloc(tlen);
  if (buf == NULL) {
    tscError("failed to malloc get subscribe ep buf");
    goto END;
  }
  buf->consumerId = htobe64(tmq->consumerId);
  buf->epoch = htonl(tmq->epoch);
  strcpy(buf->cgroup, tmq->groupId);

  SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_GET_SUB_EP);
  if (pRequest == NULL) {
    tscError("failed to malloc subscribe ep request");
    goto END;
  }

  pRequest->body.requestMsg = (SDataBuf){.pData = buf, .len = tlen};

  SMqAskEpCbParam* pParam = malloc(sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    goto END;
  }
  pParam->tmq = tmq;
  pParam->wait = wait;

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqAskEpCb;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

END:
  if (wait) tsem_wait(&tmq->rspSem);
  return 0;
}

SMqConsumeReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t blocking_time, int32_t type, SMqClientTopic* pTopic,
                                      SMqClientVg* pVg) {
  SMqConsumeReq* pReq = malloc(sizeof(SMqConsumeReq));
  if (pReq == NULL) {
    return NULL;
  }
  pReq->reqType = type;
  strcpy(pReq->topic, pTopic->topicName);
  pReq->blockingTime = blocking_time;
  pReq->consumerId = tmq->consumerId;
  strcpy(pReq->cgroup, tmq->groupId);

  if (type == TMQ_REQ_TYPE_COMMIT_ONLY) {
    pReq->offset = pVg->currentOffset;
  } else {
    pReq->offset = pVg->currentOffset + 1;
  }

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqConsumeReq));
  return pReq;
}

tmq_message_t* tmq_consumer_poll(tmq_t* tmq, int64_t blocking_time) {
  tmq_message_t* tmq_message = NULL;

  int64_t status = atomic_load_64(&tmq->status);
  tmqAskEp(tmq, status == 0);

  if (blocking_time <= 0) blocking_time = 1;
  if (blocking_time > 1000) blocking_time = 1000;
  /*blocking_time = 1;*/

  if (taosArrayGetSize(tmq->clientTopics) == 0) {
    tscDebug("consumer:%ld poll but not assigned", tmq->consumerId);
    usleep(blocking_time * 1000);
    return NULL;
  }
  SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, tmq->nextTopicIdx);
  if (taosArrayGetSize(pTopic->vgs) == 0) {
    usleep(blocking_time * 1000);
    return NULL;
  }

  tmq->nextTopicIdx = (tmq->nextTopicIdx + 1) % taosArrayGetSize(tmq->clientTopics);
  int32_t beginVgIdx = pTopic->nextVgIdx;
  while (1) {
    pTopic->nextVgIdx = (pTopic->nextVgIdx + 1) % taosArrayGetSize(pTopic->vgs);
    SMqClientVg* pVg = taosArrayGet(pTopic->vgs, pTopic->nextVgIdx);
    /*printf("consume vg %d, offset %ld\n", pVg->vgId, pVg->currentOffset);*/
    int32_t        reqType = tmq->autoCommit ? TMQ_REQ_TYPE_CONSUME_AND_COMMIT : TMQ_REQ_TYPE_COMMIT_ONLY;
    SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, blocking_time, reqType, pTopic, pVg);
    if (pReq == NULL) {
      ASSERT(false);
      usleep(blocking_time * 1000);
      return NULL;
    }

    SMqConsumeCbParam* param = malloc(sizeof(SMqConsumeCbParam));
    if (param == NULL) {
      ASSERT(false);
      usleep(blocking_time * 1000);
      return NULL;
    }
    param->tmq = tmq;
    param->retMsg = &tmq_message;
    param->pVg = pVg;
    tsem_init(&param->rspSem, 0, 0);

    SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_VND_CONSUME);
    pRequest->body.requestMsg = (SDataBuf){.pData = pReq, .len = sizeof(SMqConsumeReq)};

    SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
    sendInfo->requestObjRefId = 0;
    sendInfo->param = param;
    sendInfo->fp = tmqPollCb;

    /*printf("req offset: %ld\n", pReq->offset);*/

    int64_t transporterId = 0;
    asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
    tmq->pollCnt++;

    tsem_wait(&param->rspSem);
    tsem_destroy(&param->rspSem);
    free(param);

    if (tmq_message == NULL) {
      if (beginVgIdx == pTopic->nextVgIdx) {
        usleep(blocking_time * 1000);
      } else {
        continue;
      }
    }

    return tmq_message;
  }

  /*tsem_wait(&pRequest->body.rspSem);*/

  /*if (body != NULL) {*/
  /*destroySendMsgInfo(body);*/
  /*}*/

  /*if (pRequest != NULL && terrno != TSDB_CODE_SUCCESS) {*/
  /*pRequest->code = terrno;*/
  /*}*/

  /*return pRequest;*/
}

tmq_resp_err_t tmq_commit(tmq_t* tmq, const tmq_topic_vgroup_list_t* tmq_topic_vgroup_list, int32_t async) {
  if (tmq_topic_vgroup_list != NULL) {
    // TODO
  }

  // TODO: change semaphore to gate
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg*   pVg = taosArrayGet(pTopic->vgs, j);
      SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, 0, TMQ_REQ_TYPE_COMMIT_ONLY, pTopic, pVg);

      SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_VND_CONSUME);
      pRequest->body.requestMsg = (SDataBuf){.pData = pReq, .len = sizeof(SMqConsumeReq)};
      SMqCommitCbParam* pParam = malloc(sizeof(SMqCommitCbParam));
      if (pParam == NULL) {
        continue;
      }
      pParam->tmq = tmq;
      pParam->pVg = pVg;
      pParam->async = async;
      if (!async) tsem_init(&pParam->rspSem, 0, 0);

      SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqCommitCb;

      int64_t transporterId = 0;
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);

      if (!async) tsem_wait(&pParam->rspSem);
    }
  }

  return 0;
}

void tmq_message_destroy(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;
  SMqConsumeRsp* pRsp = (SMqConsumeRsp*)tmq_message;
  tDeleteSMqConsumeRsp(pRsp);
  free(tmq_message);
}

tmq_resp_err_t tmq_consumer_close(tmq_t* tmq) { return TMQ_RESP_ERR__SUCCESS; }

const char* tmq_err2str(tmq_resp_err_t err) {
  if (err == TMQ_RESP_ERR__SUCCESS) {
    return "success";
  }
  return "fail";
}
#if 0
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


static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}
#endif

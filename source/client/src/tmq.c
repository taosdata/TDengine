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
#include "scheduler.h"
#include "tdef.h"
#include "tep.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tnote.h"
#include "tpagedfile.h"
#include "tref.h"

struct tmq_list_t {
  int32_t cnt;
  int32_t tot;
  char*   elems[];
};
struct tmq_topic_vgroup_t {
  char*   topic;
  int32_t vgId;
  int64_t commitOffset;
};

struct tmq_topic_vgroup_list_t {
  int32_t cnt;
  int32_t size;
  tmq_topic_vgroup_t* elems;
};

struct tmq_conf_t {
  char           clientId[256];
  char           groupId[256];
  /*char*          ip;*/
  /*uint16_t       port;*/
  tmq_commit_cb* commit_cb;
};

struct tmq_t {
  char           groupId[256];
  char           clientId[256];
  SRWLatch       lock;
  int64_t        consumerId;
  int64_t        epoch;
  int64_t        status;
  tsem_t         rspSem;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  int32_t        nextTopicIdx;
  SArray*        clientTopics;  //SArray<SMqClientTopic>
  //stat
  int64_t        pollCnt;
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


typedef struct SMqAskEpCbParam {
  tmq_t*  tmq;
  int32_t wait;
} SMqAskEpCbParam;

typedef struct SMqConsumeCbParam {
  tmq_t*          tmq;
  SMqClientVg*    pVg;
  tmq_message_t** retMsg;
} SMqConsumeCbParam;

typedef struct SMqSubscribeCbParam {
  tmq_t*           tmq;
  tsem_t           rspSem;
  tmq_resp_err_t   rspErr;
} SMqSubscribeCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = calloc(1, sizeof(tmq_conf_t));
  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if(conf) free(conf);
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (strcmp(key, "group.id") == 0) {
    strcpy(conf->groupId, value);
  }
  if (strcmp(key, "client.id") == 0) {
    strcpy(conf->clientId, value);
  }
  return TMQ_CONF_OK;
}

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

int32_t tmqSubscribeCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;
  tsem_post(&pParam->rspSem);
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
  pTmq->epoch   = 0;
  taosInitRWLatch(&pTmq->lock);
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->commit_cb = conf->commit_cb;
  tsem_init(&pTmq->rspSem, 0, 0);
  pTmq->consumerId = generateRequestId() & ((uint64_t)-1 >> 1);
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  return pTmq;
}

tmq_resp_err_t tmq_subscribe(tmq_t* tmq, tmq_list_t* topic_list) {
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

  SMqSubscribeCbParam param = {
    .rspErr = TMQ_RESP_ERR__SUCCESS,
    .tmq = tmq
  };
  tsem_init(&param.rspSem, 0, 0);

  pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };

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
  kv.key = HEARTBEAT_KEY_MQ_TMP;

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

  if (sqlLen > TSDB_MAX_ALLOWED_SQL_LEN) {
    tscError("sql string exceeds max length:%d", TSDB_MAX_ALLOWED_SQL_LEN);
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
  if (buf == NULL) {
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

static char *formatTimestamp(char *buf, int64_t val, int precision) {
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

  struct tm *ptm = localtime(&tt);
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

int32_t tmq_poll_cb_inner(void* param, const SDataBuf* pMsg, int32_t code) {
  if (code == -1) {
    printf("msg discard\n");
    return 0;
  }
  char pBuf[128];
  SMqConsumeCbParam* pParam = (SMqConsumeCbParam*)param;
  SMqClientVg* pVg = pParam->pVg;
  SMqConsumeRsp rsp;
  tDecodeSMqConsumeRsp(pMsg->pData, &rsp);
  if (rsp.numOfTopics == 0) {
    /*printf("no data\n");*/
    return 0;
  }
  int32_t colNum = rsp.schemas->nCols;
  pVg->currentOffset = rsp.rspOffset;
  /*printf("rsp offset: %ld\n", rsp.rspOffset);*/
  /*printf("-----msg begin----\n");*/
  printf("|");
  for (int32_t i = 0; i < colNum; i++) {
    if (i == 0) printf(" %25s |", rsp.schemas->pSchema[i].name);
    else printf(" %15s |", rsp.schemas->pSchema[i].name);
  }
  printf("\n");
  printf("===============================================\n");
  int32_t sz = taosArrayGetSize(rsp.pBlockData);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(rsp.pBlockData, i);
    int32_t rows = pDataBlock->info.rows;
    for (int32_t j = 0; j < rows; j++) {
      printf("|");
      for (int32_t k = 0; k < colNum; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch(pColInfoData->info.type) {
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
  /*printf("\n-----msg end------\n");*/
  return 0;
}

int32_t tmq_ask_ep_cb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t* tmq = pParam->tmq;
  if (code != 0) {
    printf("get topic endpoint error, not ready, wait:%d\n", pParam->wait);
    if (pParam->wait) {
      tsem_post(&tmq->rspSem);
    }
    return 0;
  }
  tscDebug("tmq ask ep cb called");
  bool set = false;
  SMqCMGetSubEpRsp rsp;
  tDecodeSMqCMGetSubEpRsp(pMsg->pData, &rsp);
  int32_t sz = taosArrayGetSize(rsp.topics);
  // TODO: lock
    /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
  if (rsp.epoch != tmq->epoch) {
    //TODO
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
  free(pParam);
  return 0;
}

int32_t tmqAsyncAskEp(tmq_t* tmq, bool wait) {
    int32_t tlen = sizeof(SMqCMGetSubEpReq);
    SMqCMGetSubEpReq* buf = malloc(tlen);
    if (buf == NULL) {
      goto END;
      tscError("failed to malloc get subscribe ep buf");
    }
    buf->consumerId = htobe64(tmq->consumerId);
    strcpy(buf->cgroup, tmq->groupId);
    
    SRequestObj *pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_MND_GET_SUB_EP);
    if (pRequest == NULL) {
      goto END;
      tscError("failed to malloc subscribe ep request");
    }

    pRequest->body.requestMsg = (SDataBuf){ .pData = buf, .len = tlen };

    SMqAskEpCbParam *pParam = malloc(sizeof(SMqAskEpCbParam));
    if (pParam == NULL) {
      goto END;
    }
    pParam->tmq = tmq;
    pParam->wait = wait;

    SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
    sendInfo->requestObjRefId = 0;
    sendInfo->param = pParam;
    sendInfo->fp = tmq_ask_ep_cb;

    SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

    int64_t transporterId = 0;
    asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

END:
    if (wait) tsem_wait(&tmq->rspSem);
    return 0;
}

SMqConsumeReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t blocking_time, int32_t type, SMqClientTopic* pTopic, SMqClientVg** ppVg) {
  SMqConsumeReq* pReq = malloc(sizeof(SMqConsumeReq));
  if (pReq == NULL) {
    return NULL;
  }
  pReq->reqType = type;
  pReq->blockingTime = blocking_time;
  pReq->consumerId = tmq->consumerId;
  strcpy(pReq->cgroup, tmq->groupId);

  tmq->nextTopicIdx = (tmq->nextTopicIdx + 1) % taosArrayGetSize(tmq->clientTopics);
  strcpy(pReq->topic, pTopic->topicName);
  pTopic->nextVgIdx = (pTopic->nextVgIdx + 1 % taosArrayGetSize(pTopic->vgs));
  SMqClientVg* pVg = taosArrayGet(pTopic->vgs, pTopic->nextVgIdx);
  pReq->offset = pVg->currentOffset+1;
  *ppVg = pVg;

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqConsumeReq));
  return pReq;
}

tmq_message_t* tmq_consumer_poll(tmq_t* tmq, int64_t blocking_time) {
  tmq_message_t* tmq_message = NULL;

  int64_t status = atomic_load_64(&tmq->status);
  tmqAsyncAskEp(tmq, taosArrayGetSize(tmq->clientTopics));

  /*if (blocking_time < 0) blocking_time = 500;*/
  blocking_time = 1;

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

  SMqClientVg* pVg = NULL;
  SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, blocking_time, TMQ_REQ_TYPE_CONSUME_ONLY, pTopic, &pVg);
  if (pReq == NULL) {
    usleep(blocking_time * 1000);
    return NULL;
  }

  SMqConsumeCbParam* param = malloc(sizeof(SMqConsumeCbParam));
  if (param == NULL) {
    usleep(blocking_time * 1000);
    return NULL;
  }
  param->tmq = tmq;
  param->retMsg = &tmq_message;
  param->pVg = pVg;

  SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_VND_CONSUME);
  pRequest->body.requestMsg = (SDataBuf){ .pData = pReq, .len = sizeof(SMqConsumeReq) };

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  sendInfo->requestObjRefId = 0;
  sendInfo->param = param;
  sendInfo->fp = tmq_poll_cb_inner;

  /*printf("req offset: %ld\n", pReq->offset);*/

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
  tmq->pollCnt++;

  usleep(blocking_time * 1000);

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

tmq_resp_err_t tmq_commit(tmq_t* tmq, const tmq_topic_vgroup_list_t* tmq_topic_vgroup_list, int32_t async) {
  SMqConsumeReq req = {0};
  return 0;
}

void tmq_message_destroy(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;
}

static void destroySendMsgInfo(SMsgSendInfo* pMsgBody) {
  assert(pMsgBody != NULL);
  tfree(pMsgBody->msgInfo.pData);
  tfree(pMsgBody);
}

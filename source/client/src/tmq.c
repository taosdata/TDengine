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
#include "tpagedbuf.h"
#include "tqueue.h"
#include "tref.h"

struct tmq_list_t {
  int32_t cnt;
  int32_t tot;
  char*   elems[];
};

struct tmq_topic_vgroup_t {
  SMqOffset offset;
};

struct tmq_topic_vgroup_list_t {
  int32_t             cnt;
  int32_t             size;
  tmq_topic_vgroup_t* elems;
};

struct tmq_conf_t {
  char           clientId[256];
  char           groupId[256];
  int8_t         auto_commit;
  int8_t         resetOffset;
  tmq_commit_cb* commit_cb;
  /*char*          ip;*/
  /*uint16_t       port;*/
};

struct tmq_t {
  // conf
  char           groupId[256];
  char           clientId[256];
  int8_t         autoCommit;
  int64_t        consumerId;
  int32_t        epoch;
  int32_t        resetOffsetCfg;
  int64_t        status;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  int32_t        nextTopicIdx;
  SArray*        clientTopics;  // SArray<SMqClientTopic>
  STaosQueue*    mqueue;        // queue of tmq_message_t
  STaosQall*     qall;
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
  SEpSet  epSet;
} SMqClientVg;

typedef struct {
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
  int32_t sync;
  tsem_t  rspSem;
} SMqAskEpCbParam;

typedef struct {
  tmq_t*       tmq;
  SMqClientVg* pVg;
  int32_t      epoch;
  tsem_t       rspSem;
} SMqPollCbParam;

typedef struct {
  tmq_t* tmq;
  /*SMqClientVg* pVg;*/
  int32_t        async;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = calloc(1, sizeof(tmq_conf_t));
  conf->auto_commit = false;
  conf->resetOffset = TMQ_CONF__RESET_OFFSET__EARLIEAST;
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
  if (!pParam->async)
    tsem_post(&pParam->rspSem);
  else {
    tsem_destroy(&pParam->rspSem);
    free(param);
  }
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

tmq_t* tmq_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = calloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->auto_commit;
  pTmq->commit_cb = conf->commit_cb;
  pTmq->resetOffsetCfg = conf->resetOffset;

  pTmq->consumerId = generateRequestId() & (((uint64_t)-1) >> 1);
  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));

  pTmq->mqueue = taosOpenQueue();
  pTmq->qall = taosAllocateQall();
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
    req.num = offsets->cnt;
    req.offsets = (SMqOffset*)offsets->elems;
  }

  SCoder encoder;

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, NULL, 0, TD_ENCODER);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  int32_t tlen = encoder.pos;
  void*   buf = malloc(tlen);
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

  SMqCommitCbParam* pParam = malloc(sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    return -1;
  }
  pParam->tmq = tmq;
  tsem_init(&pParam->rspSem, 0, 0);

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

  if (pArray) {
    taosArrayDestroy(pArray);
  }

  return resp;
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
      goto _return;
    }
    tNameExtractFullName(&name, topicFname);
    tscDebug("subscribe topic: %s", topicFname);
    SMqClientTopic topic = {
        .nextVgIdx = 0, .sql = NULL, .sqlLen = 0, .topicId = 0, .topicName = topicFname, .vgs = NULL};
    topic.vgs = taosArrayInit(0, sizeof(SMqClientVg));
    taosArrayPush(tmq->clientTopics, &topic);
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

  SMCreateTopicReq req = {
      .igExists = 1,
      .physicalPlan = (char*)pStr,
      .sql = (char*)sql,
      .logicalPlan = (char*)"no logic plan",
  };
  tNameExtractFullName(&name, req.name);

  int   tlen = tSerializeMCreateTopicReq(NULL, 0, &req);
  void* buf = malloc(tlen);
  if (buf == NULL) {
    goto _return;
  }

  tSerializeMCreateTopicReq(buf, tlen, &req);
  /*printf("formatted: %s\n", dagStr);*/

  pRequest->body.requestMsg = (SDataBuf){.pData = buf, .len = tlen, .handle = NULL};
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
  SMqConsumeRsp* pRsp = &tmq_message->consumeRsp;
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
  printf("recv poll\n");
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  tmq_t*          tmq = pParam->tmq;
  if (code != 0) {
    printf("msg discard\n");
    if (pParam->epoch == tmq->epoch) {
      atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
    }
    return 0;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t tmqEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < tmqEpoch) {
    printf("discard rsp epoch %d, current epoch %d\n", msgEpoch, tmqEpoch);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    printf("mismatch rsp epoch %d, current epoch %d\n", msgEpoch, tmqEpoch);
  }

  /*SMqConsumeRsp* pRsp = calloc(1, sizeof(SMqConsumeRsp));*/
  tmq_message_t* pRsp = taosAllocateQitem(sizeof(tmq_message_t));
  if (pRsp == NULL) {
    printf("fail\n");
    return -1;
  }
  memcpy(pRsp, pMsg->pData, sizeof(SMqRspHead));
  tDecodeSMqConsumeRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRsp->consumeRsp);
  /*printf("rsp commit off:%ld rsp off:%ld has data:%d\n", pRsp->committedOffset, pRsp->rspOffset, pRsp->numOfTopics);*/
  if (pRsp->consumeRsp.numOfTopics == 0) {
    printf("no data\n");
    if (pParam->epoch == tmq->epoch) {
      atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
    }
    taosFreeQitem(pRsp);
    return 0;
  }
  pRsp->extra = pParam->pVg;
  taosWriteQitem(tmq->mqueue, pRsp);
  printf("poll in queue\n");
  /*pParam->rspMsg = (tmq_message_t*)pRsp;*/
  /*pVg->currentOffset = pRsp->consumeRsp.rspOffset;*/

  /*printf("rsp offset: %ld\n", rsp.rspOffset);*/
  /*printf("-----msg begin----\n");*/
  /*printf("\n-----msg end------\n");*/
  return 0;
}

bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, SMqCMGetSubEpRsp* pRsp) {
  bool    set = false;
  int32_t sz = taosArrayGetSize(pRsp->topics);
  if (tmq->clientTopics) taosArrayDestroy(tmq->clientTopics);
  tmq->clientTopics = taosArrayInit(sz, sizeof(SMqClientTopic));
  for (int32_t i = 0; i < sz; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    topic.topicName = strdup(pTopicEp->topic);
    int32_t vgSz = taosArrayGetSize(pTopicEp->vgs);
    topic.vgs = taosArrayInit(vgSz, sizeof(SMqClientVg));
    for (int32_t j = 0; j < vgSz; j++) {
      SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
      SMqClientVg clientVg = {
          .pollCnt = 0,
          .currentOffset = pVgEp->offset,
          .vgId = pVgEp->vgId,
          .epSet = pVgEp->epSet,
          .vgStatus = TMQ_VG_STATUS__IDLE,
      };
      taosArrayPush(topic.vgs, &clientVg);
      set = true;
    }
    taosArrayPush(tmq->clientTopics, &topic);
  }
  atomic_store_32(&tmq->epoch, epoch);
  return set;
}

int32_t tmqAskEpCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = pParam->tmq;
  if (code != 0) {
    printf("get topic endpoint error, not ready, wait:%d\n", pParam->sync);
    if (pParam->sync) {
      tsem_post(&pParam->rspSem);
    }
    return 0;
  }
  tscDebug("tmq ask ep cb called");
  if (pParam->sync) {
    SMqRspHead*      head = pMsg->pData;
    SMqCMGetSubEpRsp rsp;
    tDecodeSMqCMGetSubEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
    int32_t epoch = atomic_load_32(&tmq->epoch);
    if (head->epoch > epoch && tmqUpdateEp(tmq, head->epoch, &rsp)) {
      atomic_store_64(&tmq->status, TMQ_CONSUMER_STATUS__READY);
    }
    tsem_post(&pParam->rspSem);
    tDeleteSMqCMGetSubEpRsp(&rsp);
  } else {
    tmq_message_t* pRsp = taosAllocateQitem(sizeof(tmq_message_t));
    if (pRsp == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    memcpy(pRsp, pMsg->pData, sizeof(SMqRspHead));
    tDecodeSMqCMGetSubEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRsp->getEpRsp);
    taosWriteQitem(tmq->mqueue, pRsp);
  }
  return 0;
}

int32_t tmqAskEp(tmq_t* tmq, bool sync) {
  printf("ask ep sync %d\n", sync);
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

  pRequest->body.requestMsg = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };

  SMqAskEpCbParam* pParam = malloc(sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    goto END;
  }
  pParam->tmq = tmq;
  pParam->sync = sync;
  tsem_init(&pParam->rspSem, 0, 0);

  SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqAskEpCb;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

END:
  if (sync) tsem_wait(&pParam->rspSem);
  return 0;
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
          return TMQ_RESP_ERR__SUCCESS;
        }
      }
    }
  }
  return TMQ_RESP_ERR__FAIL;
}

SMqConsumeReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t blocking_time, SMqClientTopic* pTopic, SMqClientVg* pVg) {
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

  SMqConsumeReq* pReq = malloc(sizeof(SMqConsumeReq));
  if (pReq == NULL) {
    return NULL;
  }

  strcpy(pReq->topic, pTopic->topicName);
  strcpy(pReq->cgroup, tmq->groupId);

  pReq->blockingTime = blocking_time;
  pReq->consumerId = tmq->consumerId;
  pReq->epoch = tmq->epoch;
  pReq->currentOffset = reqOffset;

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqConsumeReq));
  return pReq;
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  tmq_message_t* msg;
  while (1) {
    taosGetQitem(tmq->qall, (void**)&msg);
    if (msg)
      taosFreeQitem(msg);
    else
      break;
  }

  taosReadAllQitems(tmq->mqueue, tmq->qall);
  while (1) {
    taosGetQitem(tmq->qall, (void**)&msg);
    if (msg)
      taosFreeQitem(msg);
    else
      break;
  }
}

int32_t tmqPollImpl(tmq_t* tmq, int64_t blockingTime) {
  printf("call poll\n");
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      int32_t      vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus != TMQ_VG_STATUS__IDLE) {
        continue;
      }
      SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, blockingTime, pTopic, pVg);
      if (pReq == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // TODO: out of mem
        return -1;
      }
      SMqPollCbParam* param = malloc(sizeof(SMqPollCbParam));
      if (param == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // TODO: out of mem
        return -1;
      }
      param->tmq = tmq;
      param->pVg = pVg;
      param->epoch = tmq->epoch;
      SRequestObj* pRequest = createRequest(tmq->pTscObj, NULL, NULL, TDMT_VND_CONSUME);
      pRequest->body.requestMsg = (SDataBuf){
          .pData = pReq,
          .len = sizeof(SMqConsumeReq),
          .handle = NULL,
      };

      SMsgSendInfo* sendInfo = buildMsgInfoImpl(pRequest);
      sendInfo->requestObjRefId = 0;
      sendInfo->param = param;
      sendInfo->fp = tmqPollCb;

      int64_t transporterId = 0;
      printf("send poll\n");
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
      pVg->pollCnt++;
      tmq->pollCnt++;
    }
  }
  return 0;
}

// return
int32_t tmqHandleRes(tmq_t* tmq, tmq_message_t* rspMsg, bool* pReset) {
  if (rspMsg->head.mqMsgType == TMQ_MSG_TYPE__EP_RSP) {
    printf("ep %d %d\n", rspMsg->head.epoch, tmq->epoch);
    if (rspMsg->head.epoch > atomic_load_32(&tmq->epoch)) {
      tmqUpdateEp(tmq, rspMsg->head.epoch, &rspMsg->getEpRsp);
      tmqClearUnhandleMsg(tmq);
      *pReset = true;
    } else {
      *pReset = false;
    }
  } else {
    return -1;
  }
  return 0;
}

tmq_message_t* tmqHandleAllRsp(tmq_t* tmq, int64_t blockingTime, bool pollIfReset) {
  while (1) {
    tmq_message_t* rspMsg = NULL;
    taosGetQitem(tmq->qall, (void**)&rspMsg);
    if (rspMsg == NULL) {
      break;
    }

    if (rspMsg->head.mqMsgType == TMQ_MSG_TYPE__POLL_RSP) {
      printf("handle poll rsp %d\n", rspMsg->head.mqMsgType);
      if (rspMsg->head.epoch == atomic_load_32(&tmq->epoch)) {
        printf("epoch match\n");
        SMqClientVg* pVg = rspMsg->extra;
        pVg->currentOffset = rspMsg->consumeRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        return rspMsg;
      } else {
        printf("epoch mismatch\n");
        taosFreeQitem(rspMsg);
      }
    } else {
      printf("handle ep rsp %d\n", rspMsg->head.mqMsgType);
      bool reset = false;
      tmqHandleRes(tmq, rspMsg, &reset);
      taosFreeQitem(rspMsg);
      if (pollIfReset && reset) {
        printf("reset and repoll\n");
        tmqPollImpl(tmq, blockingTime);
      }
    }
  }
  return NULL;
}

tmq_message_t* tmq_consumer_poll(tmq_t* tmq, int64_t blocking_time) {
  tmq_message_t* rspMsg = NULL;
  int64_t        startTime = taosGetTimestampMs();

  // TODO: put into another thread or delayed queue
  int64_t status = atomic_load_64(&tmq->status);
  tmqAskEp(tmq, status == TMQ_CONSUMER_STATUS__INIT);

  taosGetQitem(tmq->qall, (void**)&rspMsg);
  if (rspMsg == NULL) {
    taosReadAllQitems(tmq->mqueue, tmq->qall);
  }
  tmqHandleAllRsp(tmq, blocking_time, false);

  tmqPollImpl(tmq, blocking_time);

  while (1) {
    /*printf("cycle\n");*/
    taosReadAllQitems(tmq->mqueue, tmq->qall);
    rspMsg = tmqHandleAllRsp(tmq, blocking_time, true);
    if (rspMsg) {
      return rspMsg;
    }
    if (blocking_time != 0) {
      int64_t endTime = taosGetTimestampMs();
      if (endTime - startTime > blocking_time) {
        printf("normal exit\n");
        return NULL;
      }
    }
  }
}

#if 0

  if (blocking_time <= 0) blocking_time = 1;
  if (blocking_time > 1000) blocking_time = 1000;
  /*blocking_time = 1;*/

  if (taosArrayGetSize(tmq->clientTopics) == 0) {
    tscDebug("consumer:%ld poll but not assigned", tmq->consumerId);
    /*printf("over1\n");*/
    usleep(blocking_time * 1000);
    return NULL;
  }
  SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, tmq->nextTopicIdx);
  if (taosArrayGetSize(pTopic->vgs) == 0) {
    /*printf("over2\n");*/
    usleep(blocking_time * 1000);
    return NULL;
  }

  tmq->nextTopicIdx = (tmq->nextTopicIdx + 1) % taosArrayGetSize(tmq->clientTopics);
  int32_t beginVgIdx = pTopic->nextVgIdx;
  while (1) {
    pTopic->nextVgIdx = (pTopic->nextVgIdx + 1) % taosArrayGetSize(pTopic->vgs);
    SMqClientVg* pVg = taosArrayGet(pTopic->vgs, pTopic->nextVgIdx);
    /*printf("consume vg %d, offset %ld\n", pVg->vgId, pVg->currentOffset);*/
    SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, blocking_time, pTopic, pVg);
    if (pReq == NULL) {
      ASSERT(false);
      usleep(blocking_time * 1000);
      return NULL;
    }

    SMqPollCbParam* param = malloc(sizeof(SMqPollCbParam));
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
    pRequest->body.requestMsg = (SDataBuf){
        .pData = pReq,
        .len = sizeof(SMqConsumeReq),
        .handle = NULL,
    };

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
#endif

#if 0
tmq_resp_err_t tmq_commit(tmq_t* tmq, const tmq_topic_vgroup_list_t* tmq_topic_vgroup_list, int32_t async) {
  if (tmq_topic_vgroup_list != NULL) {
    // TODO
  }

  // TODO: change semaphore to gate
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg*   pVg = taosArrayGet(pTopic->vgs, j);
      SMqConsumeReq* pReq = tmqBuildConsumeReqImpl(tmq, 0, pTopic, pVg);

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
#endif

void tmq_message_destroy(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;
  SMqConsumeRsp* pRsp = &tmq_message->consumeRsp;
  tDeleteSMqConsumeRsp(pRsp);
  taosFreeQitem(tmq_message);
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

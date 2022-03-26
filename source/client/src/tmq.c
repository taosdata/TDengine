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
#include "tdatablock.h"
#include "tdef.h"
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
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         auto_commit;
  int8_t         resetOffset;
  tmq_commit_cb* commit_cb;
  /*char*          ip;*/
  /*uint16_t       port;*/
};

struct tmq_t {
  // conf
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[256];
  int8_t         autoCommit;
  int8_t         inWaiting;
  int64_t        consumerId;
  int32_t        epoch;
  int32_t        resetOffsetCfg;
  int64_t        status;
  STscObj*       pTscObj;
  tmq_commit_cb* commit_cb;
  int32_t        nextTopicIdx;
  int32_t        waitingRequest;
  int32_t        readyRequest;
  SArray*        clientTopics;  // SArray<SMqClientTopic>
  STaosQueue*    mqueue;        // queue of tmq_message_t
  STaosQall*     qall;
  tsem_t         rspSem;
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
  tmq_t*          tmq;
  SMqClientVg*    pVg;
  int32_t         epoch;
  tsem_t          rspSem;
  tmq_message_t** msg;
  int32_t         sync;
} SMqPollCbParam;

typedef struct {
  tmq_t* tmq;
  /*SMqClientVg* pVg;*/
  int32_t        async;
  tsem_t         rspSem;
  tmq_resp_err_t rspErr;
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  conf->auto_commit = false;
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
  tmq_list_t* ptr = taosMemoryMalloc(sizeof(tmq_list_t) + 8 * sizeof(char*));
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
    pParam->tmq->commit_cb(pParam->tmq, pParam->rspErr, NULL, NULL);
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

tmq_t* tmq_consumer_new(void* conn, tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  tmq_t* pTmq = taosMemoryCalloc(sizeof(tmq_t), 1);
  if (pTmq == NULL) {
    return NULL;
  }
  pTmq->pTscObj = (STscObj*)conn;
  pTmq->inWaiting = 0;
  pTmq->status = 0;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  pTmq->waitingRequest = 0;
  pTmq->readyRequest = 0;
  // set conf
  strcpy(pTmq->clientId, conf->clientId);
  strcpy(pTmq->groupId, conf->groupId);
  pTmq->autoCommit = conf->auto_commit;
  pTmq->commit_cb = conf->commit_cb;
  pTmq->resetOffsetCfg = conf->resetOffset;

  tsem_init(&pTmq->rspSem, 0, 0);

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

    char* topicFname = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
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
  CHECK_CODE_GOTO(parseSql(pRequest, false, &pQueryNode), _return);

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
  SMqPollRsp* pRsp = &tmq_message->msg;
  return pRsp->skipLogNum;
}

void tmqShowMsg(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return;

  static bool noPrintSchema;
  char        pBuf[128];
  SMqPollRsp* pRsp = &tmq_message->msg;
  int32_t     colNum = pRsp->schema->nCols;
  if (!noPrintSchema) {
    printf("|");
    for (int32_t i = 0; i < colNum; i++) {
      if (i == 0)
        printf(" %25s |", pRsp->schema->pSchema[i].name);
      else
        printf(" %15s |", pRsp->schema->pSchema[i].name);
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
  /*printf("recv poll\n");*/
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  tmq_t*          tmq = pParam->tmq;
  if (code != 0) {
    printf("msg discard %x\n", code);
    goto WRITE_QUEUE_FAIL;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t tmqEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < tmqEpoch) {
    tsem_post(&tmq->rspSem);
    printf("discard rsp epoch %d, current epoch %d\n", msgEpoch, tmqEpoch);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    printf("mismatch rsp epoch %d, current epoch %d\n", msgEpoch, tmqEpoch);
  } else {
    atomic_sub_fetch_32(&tmq->waitingRequest, 1);
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

  /*SMqConsumeRsp* pRsp = taosMemoryCalloc(1, sizeof(SMqConsumeRsp));*/
  tmq_message_t* pRsp = taosAllocateQitem(sizeof(tmq_message_t));
  if (pRsp == NULL) {
    goto WRITE_QUEUE_FAIL;
  }
  memcpy(pRsp, pMsg->pData, sizeof(SMqRspHead));
  tDecodeSMqPollRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRsp->msg);
  pRsp->iter.curBlock = 0;
  pRsp->iter.curRow = 0;
  // TODO: alloc mem
  /*pRsp->*/
  /*printf("rsp commit off:%ld rsp off:%ld has data:%d\n", pRsp->committedOffset, pRsp->rspOffset, pRsp->numOfTopics);*/
  if (pRsp->msg.numOfTopics == 0) {
    /*printf("no data\n");*/
    taosFreeQitem(pRsp);
    goto WRITE_QUEUE_FAIL;
  }

  pRsp->vg = pParam->pVg;
  taosWriteQitem(tmq->mqueue, pRsp);
  atomic_add_fetch_32(&tmq->readyRequest, 1);
  tsem_post(&tmq->rspSem);
  return 0;

WRITE_QUEUE_FAIL:
  if (pParam->epoch == tmq->epoch) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  tsem_post(&tmq->rspSem);
  return code;
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
    goto END;
  }

  // tmq's epoch is monotonically increase,
  // so it's safe to discard any old epoch msg.
  // Epoch will only increase when received newer epoch ep msg
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
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
    SMqCMGetSubEpRsp* pRsp = taosAllocateQitem(sizeof(SMqCMGetSubEpRsp));
    if (pRsp == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      code = -1;
      goto END;
    }
    memcpy(pRsp, pMsg->pData, sizeof(SMqRspHead));
    tDecodeSMqCMGetSubEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pRsp);

    taosWriteQitem(tmq->mqueue, pRsp);
    tsem_post(&tmq->rspSem);
  }

END:
  if (pParam->sync) {
    tsem_post(&pParam->rspSem);
  }
  return code;
}

int32_t tmqAskEp(tmq_t* tmq, bool sync) {
  int32_t           tlen = sizeof(SMqCMGetSubEpReq);
  SMqCMGetSubEpReq* req = taosMemoryMalloc(tlen);
  if (req == NULL) {
    tscError("failed to malloc get subscribe ep buf");
    return -1;
  }
  req->consumerId = htobe64(tmq->consumerId);
  req->epoch = htonl(tmq->epoch);
  strcpy(req->cgroup, tmq->groupId);

  SMqAskEpCbParam* pParam = taosMemoryMalloc(sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    taosMemoryFree(req);
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

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

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
          tmqClearUnhandleMsg(tmq);
          return TMQ_RESP_ERR__SUCCESS;
        }
      }
    }
  }
  return TMQ_RESP_ERR__FAIL;
}

SMqPollReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t blockingTime, SMqClientTopic* pTopic, SMqClientVg* pVg) {
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

  strcpy(pReq->topic, pTopic->topicName);
  strcpy(pReq->cgroup, tmq->groupId);

  pReq->blockingTime = blockingTime;
  pReq->consumerId = tmq->consumerId;
  pReq->epoch = tmq->epoch;
  pReq->currentOffset = reqOffset;

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqPollReq));
  return pReq;
}

#if 0
tmq_message_t* tmqSyncPollImpl(tmq_t* tmq, int64_t blockingTime) {
  tmq_message_t* msg = NULL;
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      int32_t      vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      /*if (vgStatus != TMQ_VG_STATUS__IDLE) {*/
      /*continue;*/
      /*}*/
      SMqPollReq* pReq = tmqBuildConsumeReqImpl(tmq, blockingTime, pTopic, pVg);
      if (pReq == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // TODO: out of mem
        return NULL;
      }

      SMqPollCbParam* pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
      if (pParam == NULL) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // TODO: out of mem
        return NULL;
      }
      pParam->tmq = tmq;
      pParam->pVg = pVg;
      pParam->epoch = tmq->epoch;
      pParam->sync = 1;
      pParam->msg = &msg;
      tsem_init(&pParam->rspSem, 0, 0);

      SMsgSendInfo* sendInfo = taosMemoryMalloc(sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        return NULL;
      }

      sendInfo->msgInfo = (SDataBuf){
          .pData = pReq,
          .len = sizeof(SMqPollReq),
          .handle = NULL,
      };
      sendInfo->requestId = generateRequestId();
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqPollCb;
      sendInfo->msgType = TDMT_VND_CONSUME;

      int64_t transporterId = 0;
      /*printf("send poll\n");*/
      atomic_add_fetch_32(&tmq->waitingRequest, 1);
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
      pVg->pollCnt++;
      tmq->pollCnt++;

      tsem_wait(&pParam->rspSem);
      tmq_message_t* nmsg = NULL;
      while (1) {
        taosReadQitem(tmq->mqueue, (void**)&nmsg);
        if (nmsg == NULL) continue;
        while (nmsg->head.mqMsgType != TMQ_MSG_TYPE__POLL_RSP) {
          taosReadQitem(tmq->mqueue, (void**)&nmsg);
        }
        return nmsg;
      }
    }
  }
  return NULL;
}
#endif

int32_t tmqPollImpl(tmq_t* tmq, int64_t blockingTime) {
  /*printf("call poll\n");*/
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      int32_t      vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus != TMQ_VG_STATUS__IDLE) {
        continue;
      }
      SMqPollReq* pReq = tmqBuildConsumeReqImpl(tmq, blockingTime, pTopic, pVg);
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
      pParam->epoch = tmq->epoch;
      pParam->sync = 0;

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
      sendInfo->requestId = generateRequestId();
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqPollCb;
      sendInfo->msgType = TDMT_VND_CONSUME;

      int64_t transporterId = 0;
      /*printf("send poll\n");*/
      atomic_add_fetch_32(&tmq->waitingRequest, 1);
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
      pVg->pollCnt++;
      tmq->pollCnt++;
    }
  }
  return 0;
}

// return
int32_t tmqHandleRes(tmq_t* tmq, SMqRspHead* rspHead, bool* pReset) {
  if (rspHead->mqMsgType == TMQ_MSG_TYPE__EP_RSP) {
    /*printf("ep %d %d\n", rspMsg->head.epoch, tmq->epoch);*/
    if (rspHead->epoch > atomic_load_32(&tmq->epoch)) {
      SMqCMGetSubEpRsp* rspMsg = (SMqCMGetSubEpRsp*)rspHead;
      tmqUpdateEp(tmq, rspHead->epoch, rspMsg);
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
    SMqRspHead* rspHead = NULL;
    taosGetQitem(tmq->qall, (void**)&rspHead);
    if (rspHead == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&rspHead);
      if (rspHead == NULL) return NULL;
    }

    if (rspHead->mqMsgType == TMQ_MSG_TYPE__POLL_RSP) {
      tmq_message_t* rspMsg = (tmq_message_t*)rspHead;
      atomic_sub_fetch_32(&tmq->readyRequest, 1);
      /*printf("handle poll rsp %d\n", rspMsg->head.mqMsgType);*/
      if (rspMsg->msg.head.epoch == atomic_load_32(&tmq->epoch)) {
        /*printf("epoch match\n");*/
        SMqClientVg* pVg = rspMsg->vg;
        pVg->currentOffset = rspMsg->msg.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        return rspMsg;
      } else {
        /*printf("epoch mismatch\n");*/
        taosFreeQitem(rspMsg);
      }
    } else {
      /*printf("handle ep rsp %d\n", rspMsg->head.mqMsgType);*/
      bool reset = false;
      tmqHandleRes(tmq, rspHead, &reset);
      taosFreeQitem(rspHead);
      if (pollIfReset && reset) {
        printf("reset and repoll\n");
        tmqPollImpl(tmq, blockingTime);
      }
    }
  }
}

#if 0
tmq_message_t* tmq_consumer_poll_v1(tmq_t* tmq, int64_t blocking_time) {
  tmq_message_t* rspMsg = NULL;
  int64_t        startTime = taosGetTimestampMs();

  int64_t status = atomic_load_64(&tmq->status);
  tmqAskEp(tmq, status == TMQ_CONSUMER_STATUS__INIT);

  while (1) {
    rspMsg = tmqSyncPollImpl(tmq, blocking_time);
    if (rspMsg && rspMsg->consumeRsp.numOfTopics) {
      return rspMsg;
    }

    if (blocking_time != 0) {
      int64_t endTime = taosGetTimestampMs();
      if (endTime - startTime > blocking_time) {
        return NULL;
      }
    } else
      return NULL;
  }
}
#endif

tmq_message_t* tmq_consumer_poll(tmq_t* tmq, int64_t blocking_time) {
  tmq_message_t* rspMsg;
  int64_t        startTime = taosGetTimestampMs();

  // TODO: put into another thread or delayed queue
  int64_t status = atomic_load_64(&tmq->status);
  tmqAskEp(tmq, status == TMQ_CONSUMER_STATUS__INIT);

  rspMsg = tmqHandleAllRsp(tmq, blocking_time, false);
  if (rspMsg) {
    return rspMsg;
  }

  while (1) {
    /*printf("cycle\n");*/
    tmqPollImpl(tmq, blocking_time);

    tsem_wait(&tmq->rspSem);

    rspMsg = tmqHandleAllRsp(tmq, blocking_time, false);
    if (rspMsg) {
      return rspMsg;
    }
    if (blocking_time != 0) {
      int64_t endTime = taosGetTimestampMs();
      if (endTime - startTime > blocking_time) {
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
    taosMsleep(blocking_time);
    return NULL;
  }
  SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, tmq->nextTopicIdx);
  if (taosArrayGetSize(pTopic->vgs) == 0) {
    /*printf("over2\n");*/
    taosMsleep(blocking_time);
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
      taosMsleep(blocking_time);
      return NULL;
    }

    SMqPollCbParam* param = taosMemoryMalloc(sizeof(SMqPollCbParam));
    if (param == NULL) {
      ASSERT(false);
      taosMsleep(blocking_time);
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
    taosMemoryFree(param);

    if (tmq_message == NULL) {
      if (beginVgIdx == pTopic->nextVgIdx) {
        taosMsleep(blocking_time);
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
      SMqCommitCbParam* pParam = taosMemoryMalloc(sizeof(SMqCommitCbParam));
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
  SMqPollRsp* pRsp = &tmq_message->msg;
  tDeleteSMqConsumeRsp(pRsp);
  /*taosMemoryFree(tmq_message);*/
  taosFreeQitem(tmq_message);
}

tmq_resp_err_t tmq_consumer_close(tmq_t* tmq) { return TMQ_RESP_ERR__SUCCESS; }

const char* tmq_err2str(tmq_resp_err_t err) {
  if (err == TMQ_RESP_ERR__SUCCESS) {
    return "success";
  }
  return "fail";
}

TAOS_ROW tmq_get_row(tmq_message_t* message) {
  SMqPollRsp* rsp = &message->msg;
  while (1) {
    if (message->iter.curBlock < taosArrayGetSize(rsp->pBlockData)) {
      SSDataBlock* pBlock = taosArrayGet(rsp->pBlockData, message->iter.curBlock);
      if (message->iter.curRow < pBlock->info.rows) {
        for (int i = 0; i < pBlock->info.numOfCols; i++) {
          SColumnInfoData* pData = taosArrayGet(pBlock->pDataBlock, i);
          if (colDataIsNull_s(pData, message->iter.curRow))
            message->iter.uData[i] = NULL;
          else {
            message->iter.uData[i] = colDataGetData(pData, message->iter.curRow);
          }
        }
        message->iter.curRow++;
        return message->iter.uData;
      } else {
        message->iter.curBlock++;
        message->iter.curRow = 0;
        continue;
      }
    }
    return NULL;
  }
}

char* tmq_get_topic_name(tmq_message_t* message) { return "not implemented yet"; }

#if 0
tmq_t* tmqCreateConsumerImpl(TAOS* conn, tmq_conf_t* conf) {
  tmq_t* pTmq = taosMemoryMalloc(sizeof(tmq_t));
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
  taosMemoryFreeClear(pMsgBody->msgInfo.pData);
  taosMemoryFreeClear(pMsgBody);
}
#endif

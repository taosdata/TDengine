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

#include "cJSON.h"
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

#if 0
#undef tsem_post
#define tsem_post(x)                                         \
  tscInfo("call sem post at %s %d", __FUNCTION__, __LINE__); \
  sem_post(x)
#endif

int32_t tmqAskEp(tmq_t* tmq, bool async);

typedef struct {
  int8_t  inited;
  tmr_h   timer;
  int32_t rsetId;
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

struct tmq_conf_t {
  char    clientId[256];
  char    groupId[TSDB_CGROUP_LEN];
  int8_t  autoCommit;
  int8_t  resetOffset;
  int8_t  withTbName;
  int8_t  snapEnable;
  int32_t snapBatchSize;

  bool hbBgEnable;

  uint16_t       port;
  int32_t        autoCommitInterval;
  char*          ip;
  char*          user;
  char*          pass;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
};

struct tmq_t {
  int64_t refId;
  // conf
  char    groupId[TSDB_CGROUP_LEN];
  char    clientId[256];
  int8_t  withTbName;
  int8_t  useSnapshot;
  int8_t  autoCommit;
  int32_t autoCommitInterval;
  int32_t resetOffsetCfg;
  int64_t consumerId;

  bool hbBgEnable;

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
  tmr_h hbLiveTimer;
  tmr_h epTimer;
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
  TMQ_CONSUMER_STATUS__RECOVER,
};

enum {
  TMQ_DELAYED_TASK__ASK_EP = 1,
  TMQ_DELAYED_TASK__REPORT,
  TMQ_DELAYED_TASK__COMMIT,
};

typedef struct {
  // statistics
  int64_t pollCnt;
  // offset
  STqOffsetVal committedOffset;
  STqOffsetVal currentOffset;
  // connection info
  int32_t vgId;
  int32_t vgStatus;
  int32_t vgSkipCnt;
  SEpSet  epSet;
} SMqClientVg;

typedef struct {
  // subscribe info
  char topicName[TSDB_TOPIC_FNAME_LEN];
  char db[TSDB_DB_FNAME_LEN];

  SArray* vgs;  // SArray<SMqClientVg>

  SSchemaWrapper schema;
} SMqClientTopic;

typedef struct {
  int8_t          tmqRspType;
  int32_t         epoch;
  SMqClientVg*    vgHandle;
  SMqClientTopic* topicHandle;
  union {
    SMqDataRsp dataRsp;
    SMqMetaRsp metaRsp;
    STaosxRsp  taosxRsp;
  };
} SMqPollRspWrapper;

typedef struct {
  int64_t refId;
  int32_t epoch;
  tsem_t  rspSem;
  int32_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  int64_t refId;
  int32_t epoch;
  int32_t code;
  int32_t async;
  tsem_t  rspSem;
} SMqAskEpCbParam;

typedef struct {
  int64_t         refId;
  int32_t         epoch;
  SMqClientVg*    pVg;
  SMqClientTopic* pTopic;
  int32_t         vgId;
  tsem_t          rspSem;
} SMqPollCbParam;

typedef struct {
  int64_t        refId;
  int32_t        epoch;
  int8_t         automatic;
  int8_t         async;
  int32_t        waitingRspNum;
  int32_t        totalRspNum;
  int32_t        rspErr;
  tmq_commit_cb* userCb;
  /*SArray*        successfulOffsets;*/
  /*SArray*        failedOffsets;*/
  void*  userParam;
  tsem_t rspSem;
} SMqCommitCbParamSet;

typedef struct {
  SMqCommitCbParamSet* params;
  STqOffset*           pOffset;
  /*char                 topicName[TSDB_TOPIC_FNAME_LEN];*/
  /*int32_t              vgId;*/
} SMqCommitCbParam;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  conf->withTbName = false;
  conf->autoCommit = true;
  conf->autoCommitInterval = 5000;
  conf->resetOffset = TMQ_CONF__RESET_OFFSET__EARLIEAST;
  conf->hbBgEnable = true;
  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if (conf) {
    if (conf->ip) taosMemoryFree(conf->ip);
    if (conf->user) taosMemoryFree(conf->user);
    if (conf->pass) taosMemoryFree(conf->pass);
    taosMemoryFree(conf);
  }
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (strcmp(key, "group.id") == 0) {
    tstrncpy(conf->groupId, value, TSDB_CGROUP_LEN);
    return TMQ_CONF_OK;
  }

  if (strcmp(key, "client.id") == 0) {
    tstrncpy(conf->clientId, value, 256);
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

  if (strcmp(key, "msg.with.table.name") == 0) {
    if (strcmp(value, "true") == 0) {
      conf->withTbName = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->withTbName = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcmp(key, "experimental.snapshot.enable") == 0) {
    if (strcmp(value, "true") == 0) {
      conf->snapEnable = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->snapEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcmp(key, "experimental.snapshot.batch.size") == 0) {
    conf->snapBatchSize = atoi(value);
    return TMQ_CONF_OK;
  }

  if (strcmp(key, "enable.heartbeat.background") == 0) {
    if (strcmp(value, "true") == 0) {
      conf->hbBgEnable = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->hbBgEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
    return TMQ_CONF_OK;
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
    /*conf->db = strdup(value);*/
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
  if (src == NULL || src[0] == 0) return -1;
  char* topic = strdup(src);
  if (topic[0] != '`') {
    strtolower(topic, src);
  }
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

int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, pParamSet->refId);
  if (tmq == NULL) {
    if (!pParamSet->async) {
      tsem_destroy(&pParamSet->rspSem);
    }
    taosMemoryFree(pParamSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  // if no more waiting rsp
  if (pParamSet->async) {
    // call async cb func
    if (pParamSet->automatic && tmq->commitCb) {
      tmq->commitCb(tmq, pParamSet->rspErr, tmq->commitCbUserParam);
    } else if (!pParamSet->automatic && pParamSet->userCb) {
      // sem post
      pParamSet->userCb(tmq, pParamSet->rspErr, pParamSet->userParam);
    }
    taosMemoryFree(pParamSet);
  } else {
    tsem_post(&pParamSet->rspSem);
  }

#if 0
    taosArrayDestroyP(pParamSet->successfulOffsets, taosMemoryFree);
    taosArrayDestroyP(pParamSet->failedOffsets, taosMemoryFree);
#endif
  return 0;
}

static void tmqCommitRspCountDown(SMqCommitCbParamSet* pParamSet) {
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  ASSERT(waitingRspNum >= 0);
  if (waitingRspNum == 0) {
    tmqCommitDone(pParamSet);
  }
}

int32_t tmqCommitCb(void* param, SDataBuf* pBuf, int32_t code) {
  SMqCommitCbParam*    pParam = (SMqCommitCbParam*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;
  // push into array
#if 0
  if (code == 0) {
    taosArrayPush(pParamSet->failedOffsets, &pParam->pOffset);
  } else {
    taosArrayPush(pParamSet->successfulOffsets, &pParam->pOffset);
  }
#endif

  taosMemoryFree(pParam->pOffset);
  taosMemoryFree(pBuf->pData);
  taosMemoryFree(pBuf->pEpSet);

  /*tscDebug("receive offset commit cb of %s on vgId:%d, offset is %" PRId64, pParam->pOffset->subKey, pParam->->vgId,
   * pOffset->version);*/

  tmqCommitRspCountDown(pParamSet);

  return 0;
}

static int32_t tmqSendCommitReq(tmq_t* tmq, SMqClientVg* pVg, SMqClientTopic* pTopic, SMqCommitCbParamSet* pParamSet) {
  STqOffset* pOffset = taosMemoryCalloc(1, sizeof(STqOffset));
  if (pOffset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pOffset->val = pVg->currentOffset;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset->subKey, tmq->groupId, groupLen);
  pOffset->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset->subKey + groupLen + 1, pTopic->topicName);

  int32_t len;
  int32_t code;
  tEncodeSize(tEncodeSTqOffset, pOffset, len, code);
  if (code < 0) {
    return -1;
  }
  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    taosMemoryFree(pOffset);
    return -1;
  }
  ((SMsgHead*)buf)->vgId = htonl(pVg->vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, len);
  tEncodeSTqOffset(&encoder, pOffset);
  tEncoderClear(&encoder);

  // build param
  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    taosMemoryFree(pOffset);
    taosMemoryFree(buf);
    return -1;
  }
  pParam->params = pParamSet;
  pParam->pOffset = pOffset;

  // build send info
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    taosMemoryFree(pOffset);
    taosMemoryFree(buf);
    taosMemoryFree(pParam);
    return -1;
  }
  pMsgSendInfo->msgInfo = (SDataBuf){
      .pData = buf,
      .len = sizeof(SMsgHead) + len,
      .handle = NULL,
  };

  tscDebug("consumer:%" PRId64 ", commit offset of %s on vgId:%d, offset is %" PRId64, tmq->consumerId, pOffset->subKey,
           pVg->vgId, pOffset->val.version);

  // TODO: put into cb
  pVg->committedOffset = pVg->currentOffset;

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = TDMT_VND_TMQ_COMMIT_OFFSET;
  // send msg

  atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  atomic_add_fetch_32(&pParamSet->totalRspNum, 1);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, pMsgSendInfo);
  return 0;
}

int32_t tmqCommitMsgImpl(tmq_t* tmq, const TAOS_RES* msg, int8_t async, tmq_commit_cb* userCb, void* userParam) {
  char*   topic;
  int32_t vgId;
  ASSERT(msg != NULL);
  if (TD_RES_TMQ(msg)) {
    SMqRspObj* pRspObj = (SMqRspObj*)msg;
    topic = pRspObj->topic;
    vgId = pRspObj->vgId;
  } else if (TD_RES_TMQ_META(msg)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)msg;
    topic = pMetaRspObj->topic;
    vgId = pMetaRspObj->vgId;
  } else if (TD_RES_TMQ_METADATA(msg)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)msg;
    topic = pRspObj->topic;
    vgId = pRspObj->vgId;
  } else {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }

  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;
  pParamSet->automatic = 0;
  pParamSet->async = async;
  pParamSet->userCb = userCb;
  pParamSet->userParam = userParam;
  tsem_init(&pParamSet->rspSem, 0, 0);

  int32_t code = -1;

  for (int32_t i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopic->topicName, topic) != 0) continue;
    for (int32_t j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg->vgId != vgId) continue;

      if (pVg->currentOffset.type > 0 && !tOffsetEqual(&pVg->currentOffset, &pVg->committedOffset)) {
        if (tmqSendCommitReq(tmq, pVg, pTopic, pParamSet) < 0) {
          tsem_destroy(&pParamSet->rspSem);
          taosMemoryFree(pParamSet);
          goto FAIL;
        }
        goto HANDLE_RSP;
      }
    }
  }

HANDLE_RSP:
  if (pParamSet->totalRspNum == 0) {
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return 0;
  }

  if (!async) {
    tsem_wait(&pParamSet->rspSem);
    code = pParamSet->rspErr;
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return code;
  } else {
    code = 0;
  }

FAIL:
  if (code != 0 && async) {
    userCb(tmq, code, userParam);
  }
  return 0;
}

static int32_t tmqCommitConsumerImpl(tmq_t* tmq, int8_t automatic, int8_t async, tmq_commit_cb* userCb,
                                     void* userParam) {
  int32_t code = -1;

  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    if (async) {
      if (automatic) {
        tmq->commitCb(tmq, code, tmq->commitCbUserParam);
      } else {
        userCb(tmq, code, userParam);
      }
    }
    return -1;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;

  pParamSet->automatic = automatic;
  pParamSet->async = async;
  pParamSet->userCb = userCb;
  pParamSet->userParam = userParam;
  tsem_init(&pParamSet->rspSem, 0, 0);

  // init as 1 to prevent concurrency issue
  pParamSet->waitingRspNum = 1;

  for (int32_t i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);

    tscDebug("consumer:%" PRId64 ", begin commit for topic %s, vgNum %d", tmq->consumerId, pTopic->topicName,
             (int32_t)taosArrayGetSize(pTopic->vgs));

    for (int32_t j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

      tscDebug("consumer:%" PRId64 ", begin commit for topic %s, vgId:%d", tmq->consumerId, pTopic->topicName,
               pVg->vgId);

      if (pVg->currentOffset.type > 0 && !tOffsetEqual(&pVg->currentOffset, &pVg->committedOffset)) {
        tscDebug("consumer: %" PRId64 ", vg:%d, current %" PRId64 ", committed %" PRId64 "", tmq->consumerId, pVg->vgId,
                 pVg->currentOffset.version, pVg->committedOffset.version);
        if (tmqSendCommitReq(tmq, pVg, pTopic, pParamSet) < 0) {
          continue;
        }
      }
    }
  }

  // no request is sent
  if (pParamSet->totalRspNum == 0) {
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return 0;
  }

  // count down since waiting rsp num init as 1
  tmqCommitRspCountDown(pParamSet);

  if (!async) {
    tsem_wait(&pParamSet->rspSem);
    code = pParamSet->rspErr;
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
#if 0
    taosArrayDestroyP(pParamSet->successfulOffsets, taosMemoryFree);
    taosArrayDestroyP(pParamSet->failedOffsets, taosMemoryFree);
#endif
  }

  return code;
}

int32_t tmqCommitInner(tmq_t* tmq, const TAOS_RES* msg, int8_t automatic, int8_t async, tmq_commit_cb* userCb,
                       void* userParam) {
  if (msg) {
    return tmqCommitMsgImpl(tmq, msg, async, userCb, userParam);
  } else {
    return tmqCommitConsumerImpl(tmq, automatic, async, userCb, userParam);
  }
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
    *pTaskType = TMQ_DELAYED_TASK__ASK_EP;
    taosWriteQitem(tmq->delayedTask, pTaskType);
    tsem_post(&tmq->rspSem);
  }
  taosMemoryFree(param);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
    *pTaskType = TMQ_DELAYED_TASK__COMMIT;
    taosWriteQitem(tmq->delayedTask, pTaskType);
    tsem_post(&tmq->rspSem);
  }
  taosMemoryFree(param);
}

void tmqAssignDelayedReportTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
    *pTaskType = TMQ_DELAYED_TASK__REPORT;
    taosWriteQitem(tmq->delayedTask, pTaskType);
    tsem_post(&tmq->rspSem);
  }
  taosMemoryFree(param);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  return 0;
}

void tmqSendHbReq(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    taosMemoryFree(param);
    return;
  }

  SMqHbReq req = {0};
  req.consumerId = tmq->consumerId;
  req.epoch = tmq->epoch;

  int32_t tlen = tSerializeSMqHbReq(NULL, 0, &req);
  if (tlen < 0) {
    tscError("tSerializeSMqHbReq failed");
    return;
  }
  void* pReq = taosMemoryCalloc(1, tlen);
  if (tlen < 0) {
    tscError("failed to malloc MqHbReq msg, size:%d", tlen);
    return;
  }
  if (tSerializeSMqHbReq(pReq, tlen, &req) < 0) {
    tscError("tSerializeSMqHbReq %d failed", tlen);
    taosMemoryFree(pReq);
    return;
  }

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pReq);
    goto OVER;
  }
  sendInfo->msgInfo = (SDataBuf){
      .pData = pReq,
      .len = tlen,
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = NULL;
  sendInfo->fp = tmqHbCb;
  sendInfo->msgType = TDMT_MND_TMQ_HB;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

OVER:
  taosTmrReset(tmqSendHbReq, 1000, param, tmqMgmt.timer, &tmq->hbLiveTimer);
}

int32_t tmqHandleAllDelayedTask(tmq_t* tmq) {
  STaosQall* qall = taosAllocateQall();
  taosReadAllQitems(tmq->delayedTask, qall);
  while (1) {
    int8_t* pTaskType = NULL;
    taosGetQitem(qall, (void**)&pTaskType);
    if (pTaskType == NULL) break;

    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      tmqAskEp(tmq, true);

      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = tmq->refId;

      taosTmrReset(tmqAssignAskEpTask, 1000, pRefId, tmqMgmt.timer, &tmq->epTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmqCommitInner(tmq, NULL, 1, 1, tmq->commitCb, tmq->commitCbUserParam);

      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = tmq->refId;

      taosTmrReset(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, pRefId, tmqMgmt.timer, &tmq->commitTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__REPORT) {
    } else {
      ASSERT(0);
    }
    taosFreeQitem(pTaskType);
  }
  taosFreeQall(qall);
  return 0;
}

static void tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
    // do nothing
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)rspWrapper;
    tDeleteSMqAskEpRsp(&pEpRspWrapper->msg);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosArrayDestroyP(pRsp->dataRsp.blockData, taosMemoryFree);
    taosArrayDestroy(pRsp->dataRsp.blockDataLen);
    taosArrayDestroyP(pRsp->dataRsp.blockTbName, taosMemoryFree);
    taosArrayDestroyP(pRsp->dataRsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFree(pRsp->metaRsp.metaRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__TAOSX_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosArrayDestroyP(pRsp->taosxRsp.blockData, taosMemoryFree);
    taosArrayDestroy(pRsp->taosxRsp.blockDataLen);
    taosArrayDestroyP(pRsp->taosxRsp.blockTbName, taosMemoryFree);
    taosArrayDestroyP(pRsp->taosxRsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
    // taosx
    taosArrayDestroy(pRsp->taosxRsp.createTableLen);
    taosArrayDestroyP(pRsp->taosxRsp.createTableReq, taosMemoryFree);
  }
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  SMqRspWrapper* rspWrapper = NULL;
  while (1) {
    taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper) {
      tmqFreeRspWrapper(rspWrapper);
      taosFreeQitem(rspWrapper);
    } else {
      break;
    }
  }

  rspWrapper = NULL;
  taosReadAllQitems(tmq->mqueue, tmq->qall);
  while (1) {
    taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper) {
      tmqFreeRspWrapper(rspWrapper);
      taosFreeQitem(rspWrapper);
    } else {
      break;
    }
  }
}

int32_t tmqSubscribeCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;

  taosMemoryFree(pMsg->pEpSet);
  tsem_post(&pParam->rspSem);
  return 0;
}

int32_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (*topics == NULL) {
    *topics = tmq_list_new();
  }
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    tmq_list_append(*topics, strchr(topic->topicName, '.') + 1);
  }
  return 0;
}

int32_t tmq_unsubscribe(tmq_t* tmq) {
  int32_t     rsp;
  int32_t     retryCnt = 0;
  tmq_list_t* lst = tmq_list_new();
  while (1) {
    rsp = tmq_subscribe(tmq, lst);
    if (rsp != TSDB_CODE_MND_CONSUMER_NOT_READY || retryCnt > 5) {
      break;
    } else {
      retryCnt++;
      taosMsleep(500);
    }
  }

  tmq_list_destroy(lst);
  return rsp;
}

void tmqFreeImpl(void* handle) {
  tmq_t* tmq = (tmq_t*)handle;

  // TODO stop timer
  if (tmq->mqueue) {
    tmqClearUnhandleMsg(tmq);
    taosCloseQueue(tmq->mqueue);
  }
  if (tmq->delayedTask) taosCloseQueue(tmq->delayedTask);
  taosFreeQall(tmq->qall);

  tsem_destroy(&tmq->rspSem);

  int32_t sz = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < sz; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    taosMemoryFreeClear(pTopic->schema.pSchema);
    taosArrayDestroy(pTopic->vgs);
  }
  taosArrayDestroy(tmq->clientTopics);
  taos_close_internal(tmq->pTscObj);
  taosMemoryFree(tmq);
}

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
    tmqMgmt.rsetId = taosOpenRef(10000, tmqFreeImpl);
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("setting up new consumer failed since %s, consumer group %s", terrstr(), conf->groupId);
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("consumer %" PRId64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
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
  pTmq->withTbName = conf->withTbName;
  pTmq->useSnapshot = conf->snapEnable;
  pTmq->autoCommit = conf->autoCommit;
  pTmq->autoCommitInterval = conf->autoCommitInterval;
  pTmq->commitCb = conf->commitCb;
  pTmq->commitCbUserParam = conf->commitCbUserParam;
  pTmq->resetOffsetCfg = conf->resetOffset;

  pTmq->hbBgEnable = conf->hbBgEnable;

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem_init(&pTmq->rspSem, 0, 0) != 0) {
    tscError("consumer %" PRId64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
    goto FAIL;
  }

  // init connection
  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) {
    tscError("consumer %" PRId64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
    tsem_destroy(&pTmq->rspSem);
    goto FAIL;
  }

  pTmq->refId = taosAddRef(tmqMgmt.rsetId, pTmq);
  if (pTmq->refId < 0) {
    tmqFreeImpl(pTmq);
    return NULL;
  }

  if (pTmq->hbBgEnable) {
    int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
    *pRefId = pTmq->refId;
    pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, 1000, pRefId, tmqMgmt.timer);
  }

  tscInfo("consumer %" PRId64 " is setup, consumer group %s", pTmq->consumerId, pTmq->groupId);

  return pTmq;

FAIL:
  if (pTmq->clientTopics) taosArrayDestroy(pTmq->clientTopics);
  if (pTmq->mqueue) taosCloseQueue(pTmq->mqueue);
  if (pTmq->delayedTask) taosCloseQueue(pTmq->delayedTask);
  if (pTmq->qall) taosFreeQall(pTmq->qall);
  taosMemoryFree(pTmq);
  return NULL;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = -1;

  tscDebug("tmq subscribe, consumer: %" PRId64 ", topic num %d", tmq->consumerId, sz);

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, 256);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  req.topicNames = taosArrayInit(sz, sizeof(void*));
  if (req.topicNames == NULL) goto FAIL;

  tscDebug("tmq subscribe, consumer: %" PRId64 ", topic num %d", tmq->consumerId, sz);

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

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) goto FAIL;

  SMqSubscribeCbParam param = {
      .rspErr = 0,
      .refId = tmq->refId,
      .epoch = tmq->epoch,
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
  sendInfo->msgType = TDMT_MND_TMQ_SUBSCRIBE;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  // avoid double free if msg is sent
  buf = NULL;
  sendInfo = NULL;

  tsem_wait(&param.rspSem);
  tsem_destroy(&param.rspSem);

  code = param.rspErr;
  if (code != 0) goto FAIL;

  int32_t retryCnt = 0;
  while (TSDB_CODE_MND_CONSUMER_NOT_READY == tmqAskEp(tmq, false)) {
    if (retryCnt++ > 10) {
      goto FAIL;
    }
    tscDebug("consumer not ready, retry");
    taosMsleep(500);
  }

  // init ep timer
  if (tmq->epTimer == NULL) {
    int64_t* pRefId1 = taosMemoryMalloc(sizeof(int64_t));
    *pRefId1 = tmq->refId;
    tmq->epTimer = taosTmrStart(tmqAssignAskEpTask, 1000, pRefId1, tmqMgmt.timer);
  }

  // init auto commit timer
  if (tmq->autoCommit && tmq->commitTimer == NULL) {
    int64_t* pRefId2 = taosMemoryMalloc(sizeof(int64_t));
    *pRefId2 = tmq->refId;
    tmq->commitTimer = taosTmrStart(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, pRefId2, tmqMgmt.timer);
  }

  code = 0;
FAIL:
  taosArrayDestroyP(req.topicNames, taosMemoryFree);
  taosMemoryFree(buf);
  taosMemoryFree(sendInfo);

  return code;
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  //
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  SMqClientTopic* pTopic = pParam->pTopic;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);
  if (tmq == NULL) {
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  int32_t epoch = pParam->epoch;
  int32_t vgId = pParam->vgId;
  taosMemoryFree(pParam);
  if (code != 0) {
    tscWarn("msg discard from vgId:%d, epoch %d, since %s", vgId, epoch, terrstr());
    if (pMsg->pData) taosMemoryFree(pMsg->pData);
    if (pMsg->pEpSet) taosMemoryFree(pMsg->pEpSet);

    if (code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__RECOVER);
      goto CREATE_MSG_FAIL;
    }
    if (code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
      if (pRspWrapper == NULL) {
        tscWarn("msg discard from vgId:%d, epoch %d since out of memory", vgId, epoch);
        goto CREATE_MSG_FAIL;
      }
      pRspWrapper->tmqRspType = TMQ_MSG_TYPE__END_RSP;
      /*pRspWrapper->vgHandle = pVg;*/
      /*pRspWrapper->topicHandle = pTopic;*/
      taosWriteQitem(tmq->mqueue, pRspWrapper);
      tsem_post(&tmq->rspSem);
    }
    goto CREATE_MSG_FAIL;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t tmqEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < tmqEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d", vgId, msgEpoch,
            tmqEpoch);
    tsem_post(&tmq->rspSem);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("mismatch rsp from vgId:%d, epoch %d, current epoch %d", vgId, msgEpoch, tmqEpoch);
  }

  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
  if (pRspWrapper == NULL) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tscWarn("msg discard from vgId:%d, epoch %d since out of memory", vgId, epoch);
    goto CREATE_MSG_FAIL;
  }

  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->vgHandle = pVg;
  pRspWrapper->topicHandle = pTopic;

  if (rspType == TMQ_MSG_TYPE__POLL_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSMqDataRsp(&decoder, &pRspWrapper->dataRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));

    tscDebug("consumer:%" PRId64 ", recv poll: vgId:%d, req offset %" PRId64 ", rsp offset %" PRId64 " type %d",
             tmq->consumerId, pVg->vgId, pRspWrapper->dataRsp.reqOffset.version, pRspWrapper->dataRsp.rspOffset.version,
             rspType);

  } else if (rspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSMqMetaRsp(&decoder, &pRspWrapper->metaRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
  } else if (rspType == TMQ_MSG_TYPE__TAOSX_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSTaosxRsp(&decoder, &pRspWrapper->taosxRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->taosxRsp, pMsg->pData, sizeof(SMqRspHead));
  } else {
    ASSERT(0);
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  tscDebug("consumer:%" PRId64 ", put poll res into mqueue %p", tmq->consumerId, pRspWrapper);

  taosWriteQitem(tmq->mqueue, pRspWrapper);
  tsem_post(&tmq->rspSem);

  return 0;
CREATE_MSG_FAIL:
  if (epoch == tmq->epoch) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  tsem_post(&tmq->rspSem);
  return -1;
}

bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  bool set = false;

  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  tscDebug("consumer:%" PRId64 ", update ep epoch %d to epoch %d, topic num:%d", tmq->consumerId, tmq->epoch, epoch,
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
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < topicNumCur; i++) {
    // find old topic
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tscDebug("consumer:%" PRId64 ", new vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        sprintf(vgKey, "%s:%d", pTopicCur->topicName, pVgCur->vgId);
        char buf[80];
        tFormatOffset(buf, 80, &pVgCur->currentOffset);
        tscDebug("consumer:%" PRId64 ", epoch %d vgId:%d vgKey is %s, offset is %s", tmq->consumerId, epoch,
                 pVgCur->vgId, vgKey, buf);
        taosHashPut(pHash, vgKey, strlen(vgKey), &pVgCur->currentOffset, sizeof(STqOffsetVal));
      }
    }
  }

  for (int32_t i = 0; i < topicNumGet; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    topic.schema = pTopicEp->schema;
    pTopicEp->schema.nCols = 0;
    pTopicEp->schema.pSchema = NULL;
    tstrncpy(topic.topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
    tstrncpy(topic.db, pTopicEp->db, TSDB_DB_FNAME_LEN);

    tscDebug("consumer:%" PRId64 ", update topic: %s", tmq->consumerId, topic.topicName);

    int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);
    topic.vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));
    for (int32_t j = 0; j < vgNumGet; j++) {
      SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
      sprintf(vgKey, "%s:%d", topic.topicName, pVgEp->vgId);
      STqOffsetVal* pOffset = taosHashGet(pHash, vgKey, strlen(vgKey));
      STqOffsetVal  offsetNew = {.type = tmq->resetOffsetCfg};
      if (pOffset != NULL) {
        offsetNew = *pOffset;
      }

      SMqClientVg clientVg = {
          .pollCnt = 0,
          .currentOffset = offsetNew,
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
  if (tmq->clientTopics) {
    int32_t sz = taosArrayGetSize(tmq->clientTopics);
    for (int32_t i = 0; i < sz; i++) {
      SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
      if (pTopic->schema.nCols) taosMemoryFreeClear(pTopic->schema.pSchema);
      taosArrayDestroy(pTopic->vgs);
    }
    taosArrayDestroy(tmq->clientTopics);
  }
  taosHashCleanup(pHash);
  tmq->clientTopics = newTopics;

  if (taosArrayGetSize(tmq->clientTopics) == 0)
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__NO_TOPIC);
  else
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__READY);

  atomic_store_32(&tmq->epoch, epoch);
  return set;
}

int32_t tmqAskEpCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  int8_t           async = pParam->async;
  tmq_t*           tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);

  if (tmq == NULL) {
    if (!async) {
      tsem_destroy(&pParam->rspSem);
    } else {
      taosMemoryFree(pParam);
    }
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  pParam->code = code;
  if (code != 0) {
    tscError("consumer:%" PRId64 ", get topic endpoint error, not ready, wait:%d, code %x", tmq->consumerId,
             pParam->async, code);
    goto END;
  }

  // tmq's epoch is monotonically increase,
  // so it's safe to discard any old epoch msg.
  // Epoch will only increase when received newer epoch ep msg
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tscDebug("consumer:%" PRId64 ", recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (head->epoch <= epoch) {
    goto END;
  }

  if (!async) {
    SMqAskEpRsp rsp;
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    /*printf("rsp epoch %" PRId64 " sz %" PRId64 "\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %" PRId64 " sz %" PRId64 "\n", tmq->epoch, tmq->clientTopics->size);*/
    tmqUpdateEp(tmq, head->epoch, &rsp);
    tDeleteSMqAskEpRsp(&rsp);
  } else {
    SMqAskEpRspWrapper* pWrapper = taosAllocateQitem(sizeof(SMqAskEpRspWrapper), DEF_QITEM, 0);
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
  }

END:
  /*atomic_store_8(&tmq->epStatus, 0);*/
  if (!async) {
    tsem_post(&pParam->rspSem);
  } else {
    taosMemoryFree(pParam);
  }

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  return code;
}

int32_t tmqAskEp(tmq_t* tmq, bool async) {
  int32_t code = 0;
#if 0
  int8_t  epStatus = atomic_val_compare_exchange_8(&tmq->epStatus, 0, 1);
  if (epStatus == 1) {
    int32_t epSkipCnt = atomic_add_fetch_32(&tmq->epSkipCnt, 1);
    tscTrace("consumer:%" PRId64 ", skip ask ep cnt %d", tmq->consumerId, epSkipCnt);
    if (epSkipCnt < 5000) return 0;
  }
  atomic_store_32(&tmq->epSkipCnt, 0);
#endif
  SMqAskEpReq req = {0};
  req.consumerId = tmq->consumerId;
  req.epoch = tmq->epoch;
  strcpy(req.cgroup, tmq->groupId);

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  if (tlen < 0) {
    tscError("tSerializeSMqAskEpReq failed");
    return -1;
  }
  void* pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("failed to malloc askEpReq msg, size:%d", tlen);
    return -1;
  }
  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tscError("tSerializeSMqAskEpReq %d failed", tlen);
    taosMemoryFree(pReq);
    return -1;
  }

  SMqAskEpCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    taosMemoryFree(pReq);
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }
  pParam->refId = tmq->refId;
  pParam->epoch = tmq->epoch;
  pParam->async = async;
  tsem_init(&pParam->rspSem, 0, 0);

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }

  sendInfo->msgInfo = (SDataBuf){
      .pData = pReq,
      .len = tlen,
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqAskEpCb;
  sendInfo->msgType = TDMT_MND_TMQ_ASK_EP;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  tscDebug("consumer:%" PRId64 ", ask ep", tmq->consumerId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
    tsem_wait(&pParam->rspSem);
    code = pParam->code;
    taosMemoryFree(pParam);
  }
  return code;
}

void tmqBuildConsumeReqImpl(SMqPollReq* pReq, tmq_t* tmq, int64_t timeout, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  /*strcpy(pReq->topic, pTopic->topicName);*/
  /*strcpy(pReq->cgroup, tmq->groupId);*/

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pReq->subKey, tmq->groupId, groupLen);
  pReq->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pReq->subKey + groupLen + 1, pTopic->topicName);

  pReq->withTbName = tmq->withTbName;
  pReq->timeout = timeout;
  pReq->consumerId = tmq->consumerId;
  pReq->epoch = tmq->epoch;
  /*pReq->currentOffset = reqOffset;*/
  pReq->reqOffset = pVg->currentOffset;
  pReq->reqId = generateRequestId();

  pReq->useSnapshot = tmq->useSnapshot;

  pReq->head.vgId = pVg->vgId;
}

SMqMetaRspObj* tmqBuildMetaRspFromWrapper(SMqPollRspWrapper* pWrapper) {
  SMqMetaRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqMetaRspObj));
  pRspObj->resType = RES_TYPE__TMQ_META;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pWrapper->vgHandle->vgId;

  memcpy(&pRspObj->metaRsp, &pWrapper->metaRsp, sizeof(SMqMetaRsp));
  return pRspObj;
}

SMqRspObj* tmqBuildRspFromWrapper(SMqPollRspWrapper* pWrapper) {
  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  pRspObj->resType = RES_TYPE__TMQ;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pWrapper->vgHandle->vgId;
  pRspObj->resIter = -1;
  memcpy(&pRspObj->rsp, &pWrapper->dataRsp, sizeof(SMqDataRsp));

  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;
  if (!pWrapper->dataRsp.withSchema) {
    setResSchemaInfo(&pRspObj->resInfo, pWrapper->topicHandle->schema.pSchema, pWrapper->topicHandle->schema.nCols);
  }

  return pRspObj;
}

SMqTaosxRspObj* tmqBuildTaosxRspFromWrapper(SMqPollRspWrapper* pWrapper) {
  SMqTaosxRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqTaosxRspObj));
  pRspObj->resType = RES_TYPE__TMQ_METADATA;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pWrapper->vgHandle->vgId;
  pRspObj->resIter = -1;
  memcpy(&pRspObj->rsp, &pWrapper->taosxRsp, sizeof(STaosxRsp));

  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;
  if (!pWrapper->taosxRsp.withSchema) {
    setResSchemaInfo(&pRspObj->resInfo, pWrapper->topicHandle->schema.pSchema, pWrapper->topicHandle->schema.nCols);
  }

  return pRspObj;
}

int32_t tmqPollImpl(tmq_t* tmq, int64_t timeout) {
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    for (int j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      int32_t      vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus != TMQ_VG_STATUS__IDLE) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tscTrace("consumer:%" PRId64 ", epoch %d skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch, pVg->vgId,
                 vgSkipCnt);
        continue;
        /*if (vgSkipCnt < 10000) continue;*/
#if 0
        if (skipCnt < 30000) {
          continue;
        } else {
        tscDebug("consumer:%" PRId64 ",skip vgId:%d skip too much reset", tmq->consumerId, pVg->vgId);
        }
#endif
      }
      atomic_store_32(&pVg->vgSkipCnt, 0);

      SMqPollReq req = {0};
      tmqBuildConsumeReqImpl(&req, tmq, timeout, pTopic, pVg);
      int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
      if (msgSize < 0) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }
      char* msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }

      if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
        taosMemoryFree(msg);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }

      SMqPollCbParam* pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
      if (pParam == NULL) {
        taosMemoryFree(msg);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }
      pParam->refId = tmq->refId;
      pParam->epoch = tmq->epoch;

      pParam->pVg = pVg;
      pParam->pTopic = pTopic;
      pParam->vgId = pVg->vgId;

      SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        taosMemoryFree(msg);
        taosMemoryFree(pParam);
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        tsem_post(&tmq->rspSem);
        return -1;
      }

      sendInfo->msgInfo = (SDataBuf){
          .pData = msg,
          .len = msgSize,
          .handle = NULL,
      };
      sendInfo->requestId = req.reqId;
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqPollCb;
      sendInfo->msgType = TDMT_VND_TMQ_CONSUME;

      int64_t transporterId = 0;
      /*printf("send poll\n");*/

      char offsetFormatBuf[80];
      tFormatOffset(offsetFormatBuf, 80, &pVg->currentOffset);
      tscDebug("consumer:%" PRId64 ", send poll to %s vgId:%d, epoch %d, req offset:%s, reqId:%" PRIu64,
               tmq->consumerId, pTopic->topicName, pVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
      /*printf("send vgId:%d %" PRId64 "\n", pVg->vgId, pVg->currentOffset);*/
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
      tDeleteSMqAskEpRsp(rspMsg);
      *pReset = true;
    } else {
      tmqFreeRspWrapper(rspWrapper);
      *pReset = false;
    }
  } else {
    return -1;
  }
  return 0;
}

void* tmqHandleAllRsp(tmq_t* tmq, int64_t timeout, bool pollIfReset) {
  while (1) {
    SMqRspWrapper* rspWrapper = NULL;
    taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&rspWrapper);

      if (rspWrapper == NULL) {
        /*tscDebug("consumer %" PRId64 " mqueue empty", tmq->consumerId);*/
        return NULL;
      }
    }

    tscDebug("consumer:%" PRId64 " handle rsp %p", tmq->consumerId, rspWrapper);

    if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
      taosFreeQitem(rspWrapper);
      terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
      return NULL;
    } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      tscDebug("consumer %" PRId64 " actual process poll rsp", tmq->consumerId);
      /*atomic_sub_fetch_32(&tmq->readyRequest, 1);*/
      int32_t consumerEpoch = atomic_load_32(&tmq->epoch);
      if (pollRspWrapper->dataRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vgId:%d, offset %" PRId64 " up to %" PRId64 "\n", pVg->vgId, pVg->currentOffset,
         * rspMsg->msg.rspOffset);*/
        pVg->currentOffset = pollRspWrapper->dataRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        if (pollRspWrapper->dataRsp.blockNum == 0) {
          taosFreeQitem(pollRspWrapper);
          rspWrapper = NULL;
          continue;
        }
        // build rsp
        SMqRspObj* pRsp = tmqBuildRspFromWrapper(pollRspWrapper);
        taosFreeQitem(pollRspWrapper);
        return pRsp;
      } else {
        tscDebug("msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 pollRspWrapper->dataRsp.head.epoch, consumerEpoch);
        tmqFreeRspWrapper(rspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);
      if (pollRspWrapper->metaRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vgId:%d, offset %" PRId64 " up to %" PRId64 "\n", pVg->vgId, pVg->currentOffset,
         * rspMsg->msg.rspOffset);*/
        pVg->currentOffset = pollRspWrapper->metaRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // build rsp
        SMqMetaRspObj* pRsp = tmqBuildMetaRspFromWrapper(pollRspWrapper);
        taosFreeQitem(pollRspWrapper);
        return pRsp;
      } else {
        tscDebug("msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 pollRspWrapper->metaRsp.head.epoch, consumerEpoch);
        tmqFreeRspWrapper(rspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__TAOSX_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      /*atomic_sub_fetch_32(&tmq->readyRequest, 1);*/
      int32_t consumerEpoch = atomic_load_32(&tmq->epoch);
      if (pollRspWrapper->taosxRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vgId:%d, offset %" PRId64 " up to %" PRId64 "\n", pVg->vgId, pVg->currentOffset,
         * rspMsg->msg.rspOffset);*/
        pVg->currentOffset = pollRspWrapper->taosxRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        if (pollRspWrapper->taosxRsp.blockNum == 0) {
          taosFreeQitem(pollRspWrapper);
          rspWrapper = NULL;
          continue;
        }

        // build rsp
        void* pRsp = NULL;
        if (pollRspWrapper->taosxRsp.createTableNum == 0) {
          pRsp = tmqBuildRspFromWrapper(pollRspWrapper);
        } else {
          pRsp = tmqBuildTaosxRspFromWrapper(pollRspWrapper);
        }
        taosFreeQitem(pollRspWrapper);
        return pRsp;
      } else {
        tscDebug("msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 pollRspWrapper->taosxRsp.head.epoch, consumerEpoch);
        tmqFreeRspWrapper(rspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else {
      /*printf("handle ep rsp %d\n", rspMsg->head.mqMsgType);*/
      bool reset = false;
      tmqHandleNoPollRsp(tmq, rspWrapper, &reset);
      taosFreeQitem(rspWrapper);
      if (pollIfReset && reset) {
        tscDebug("consumer:%" PRId64 ", reset and repoll", tmq->consumerId);
        tmqPollImpl(tmq, timeout);
      }
    }
  }
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t timeout) {
  void*   rspObj;
  int64_t startTime = taosGetTimestampMs();

  tscDebug("consumer:%" PRId64 ", start poll at %" PRId64, tmq->consumerId, startTime);

#if 0
  tmqHandleAllDelayedTask(tmq);
  tmqPollImpl(tmq, timeout);
  rspObj = tmqHandleAllRsp(tmq, timeout, false);
  if (rspObj) {
    return (TAOS_RES*)rspObj;
  }
#endif

  // in no topic status, delayed task also need to be processed
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    tscDebug("consumer:%" PRId64 ", poll return since consumer status is init", tmq->consumerId);
    return NULL;
  }

  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER) {
    int32_t retryCnt = 0;
    while (TSDB_CODE_MND_CONSUMER_NOT_READY == tmqAskEp(tmq, false)) {
      if (retryCnt++ > 10) {
        return NULL;
      }
      tscDebug("consumer not ready, retry");
      taosMsleep(500);
    }
  }

  while (1) {
    tmqHandleAllDelayedTask(tmq);
    if (tmqPollImpl(tmq, timeout) < 0) {
      tscDebug("consumer:%" PRId64 " return since poll err", tmq->consumerId);
      /*return NULL;*/
    }

    rspObj = tmqHandleAllRsp(tmq, timeout, false);
    if (rspObj) {
      tscDebug("consumer:%" PRId64 ", return rsp %p", tmq->consumerId, rspObj);
      return (TAOS_RES*)rspObj;
    } else if (terrno == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      tscDebug("consumer:%" PRId64 ", return null since no committed offset", tmq->consumerId);
      return NULL;
    }
    if (timeout != -1) {
      int64_t currentTime = taosGetTimestampMs();
      int64_t passedTime = currentTime - startTime;
      if (passedTime > timeout) {
        tscDebug("consumer:%" PRId64 ", (epoch %d) timeout, no rsp, start time %" PRId64 ", current time %" PRId64,
                 tmq->consumerId, tmq->epoch, startTime, currentTime);
        return NULL;
      }
      /*tscInfo("consumer:%" PRId64 ", (epoch %d) wait, start time %" PRId64 ", current time %" PRId64*/
      /*", left time %" PRId64,*/
      /*tmq->consumerId, tmq->epoch, startTime, currentTime, (timeout - passedTime));*/
      tsem_timewait(&tmq->rspSem, (timeout - passedTime));
    } else {
      // use tsem_timewait instead of tsem_wait to avoid unexpected stuck
      tsem_timewait(&tmq->rspSem, 1000);
    }
  }
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq->status == TMQ_CONSUMER_STATUS__READY) {
    int32_t rsp = tmq_commit_sync(tmq, NULL);
    if (rsp != 0) {
      return rsp;
    }

    int32_t     retryCnt = 0;
    tmq_list_t* lst = tmq_list_new();
    while (1) {
      rsp = tmq_subscribe(tmq, lst);
      if (rsp != TSDB_CODE_MND_CONSUMER_NOT_READY || retryCnt > 5) {
        break;
      } else {
        retryCnt++;
        taosMsleep(500);
      }
    }

    tmq_list_destroy(lst);
  }
  taosRemoveRef(tmqMgmt.rsetId, tmq->refId);
  return 0;
}

const char* tmq_err2str(int32_t err) {
  if (err == 0) {
    return "success";
  } else if (err == -1) {
    return "fail";
  } else {
    return tstrerror(err);
  }
}

tmq_res_t tmq_get_res_type(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    return TMQ_RES_DATA;
  } else if (TD_RES_TMQ_META(res)) {
    return TMQ_RES_TABLE_META;
  } else if (TD_RES_TMQ_METADATA(res)) {
    return TMQ_RES_METADATA;
  } else {
    return TMQ_RES_INVALID;
  }
}

const char* tmq_get_topic_name(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return strchr(pRspObj->topic, '.') + 1;
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    return strchr(pMetaRspObj->topic, '.') + 1;
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)res;
    return strchr(pRspObj->topic, '.') + 1;
  } else {
    return NULL;
  }
}

const char* tmq_get_db_name(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return strchr(pRspObj->db, '.') + 1;
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    return strchr(pMetaRspObj->db, '.') + 1;
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)res;
    return strchr(pRspObj->db, '.') + 1;
  } else {
    return NULL;
  }
}

int32_t tmq_get_vgroup_id(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    return pRspObj->vgId;
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    return pMetaRspObj->vgId;
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)res;
    return pRspObj->vgId;
  } else {
    return -1;
  }
}

const char* tmq_get_table_name(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    if (!pRspObj->rsp.withTbName || pRspObj->rsp.blockTbName == NULL || pRspObj->resIter < 0 ||
        pRspObj->resIter >= pRspObj->rsp.blockNum) {
      return NULL;
    }
    return (const char*)taosArrayGetP(pRspObj->rsp.blockTbName, pRspObj->resIter);
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)res;
    if (!pRspObj->rsp.withTbName || pRspObj->rsp.blockTbName == NULL || pRspObj->resIter < 0 ||
        pRspObj->resIter >= pRspObj->rsp.blockNum) {
      return NULL;
    }
    return (const char*)taosArrayGetP(pRspObj->rsp.blockTbName, pRspObj->resIter);
  }
  return NULL;
}

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb* cb, void* param) {
  //
  tmqCommitInner(tmq, msg, 0, 1, cb, param);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* msg) {
  //
  return tmqCommitInner(tmq, msg, 0, 0, NULL, NULL);
}

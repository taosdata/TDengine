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

struct tmq_conf_t {
  char    clientId[256];
  char    groupId[TSDB_CGROUP_LEN];
  int8_t  autoCommit;
  int8_t  resetOffset;
  int8_t  withTbName;
  int8_t  ssEnable;
  int32_t ssBatchSize;

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
  /*int64_t      committedOffset;*/
  /*int64_t      currentOffset;*/
  STqOffsetVal committedOffsetNew;
  STqOffsetVal currentOffsetNew;
  // connection info
  int32_t vgId;
  int32_t vgStatus;
  int32_t vgSkipCnt;
  SEpSet  epSet;
} SMqClientVg;

typedef struct {
  // subscribe info
  char* topicName;
  char  db[TSDB_DB_FNAME_LEN];

  SArray* vgs;  // SArray<SMqClientVg>

  int8_t         isSchemaAdaptive;
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
  };
} SMqPollRspWrapper;

typedef struct {
  tmq_t*  tmq;
  tsem_t  rspSem;
  int32_t rspErr;
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
  tmq_t* tmq;
  int8_t automatic;
  int8_t async;
  /*int8_t         freeOffsets;*/
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
} SMqCommitCbParam2;

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  conf->withTbName = false;
  conf->autoCommit = true;
  conf->autoCommitInterval = 5000;
  conf->resetOffset = TMQ_CONF__RESET_OFFSET__EARLIEAST;
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
      conf->ssEnable = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->ssEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
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

  if (strcmp(key, "experimental.snapshot.batch.size") == 0) {
    conf->ssBatchSize = atoi(value);
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

int32_t tmqCommitCb2(void* param, SDataBuf* pBuf, int32_t code) {
  SMqCommitCbParam2*   pParam = (SMqCommitCbParam2*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;
  // push into array
#if 0
  if (code == 0) {
    taosArrayPush(pParamSet->failedOffsets, &pParam->pOffset);
  } else {
    taosArrayPush(pParamSet->successfulOffsets, &pParam->pOffset);
  }
#endif

  /*tscDebug("receive offset commit cb of %s on vgId:%d, offset is %" PRId64, pParam->pOffset->subKey, pParam->->vgId,
   * pOffset->version);*/

  // count down waiting rsp
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  ASSERT(waitingRspNum >= 0);

  if (waitingRspNum == 0) {
    // if no more waiting rsp
    if (pParamSet->async) {
      // call async cb func
      if (pParamSet->automatic && pParamSet->tmq->commitCb) {
        pParamSet->tmq->commitCb(pParamSet->tmq, pParamSet->rspErr, pParamSet->tmq->commitCbUserParam);
      } else if (!pParamSet->automatic && pParamSet->userCb) {
        // sem post
        pParamSet->userCb(pParamSet->tmq, pParamSet->rspErr, pParamSet->userParam);
      }
    } else {
      tsem_post(&pParamSet->rspSem);
    }

#if 0
    taosArrayDestroyP(pParamSet->successfulOffsets, taosMemoryFree);
    taosArrayDestroyP(pParamSet->failedOffsets, taosMemoryFree);
#endif
  }
  return 0;
}

static int32_t tmqSendCommitReq(tmq_t* tmq, SMqClientVg* pVg, SMqClientTopic* pTopic, SMqCommitCbParamSet* pParamSet) {
  STqOffset* pOffset = taosMemoryCalloc(1, sizeof(STqOffset));
  if (pOffset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pOffset->val = pVg->currentOffsetNew;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset->subKey, tmq->groupId, groupLen);
  pOffset->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset->subKey + groupLen + 1, pTopic->topicName);

  int32_t len;
  int32_t code;
  tEncodeSize(tEncodeSTqOffset, pOffset, len, code);
  if (code < 0) {
    ASSERT(0);
    return -1;
  }
  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) return -1;
  ((SMsgHead*)buf)->vgId = htonl(pVg->vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, len);
  tEncodeSTqOffset(&encoder, pOffset);

  // build param
  SMqCommitCbParam2* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam2));
  pParam->params = pParamSet;
  pParam->pOffset = pOffset;

  // build send info
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
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
  pVg->committedOffsetNew = pVg->currentOffsetNew;

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb2;
  pMsgSendInfo->msgType = TDMT_VND_MQ_COMMIT_OFFSET;
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
  } else {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }

  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pParamSet->tmq = tmq;
  pParamSet->automatic = 0;
  pParamSet->async = async;
  /*pParamSet->freeOffsets = 1;*/
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

      if (pVg->currentOffsetNew.type > 0 && !tOffsetEqual(&pVg->currentOffsetNew, &pVg->committedOffsetNew)) {
        if (tmqSendCommitReq(tmq, pVg, pTopic, pParamSet) < 0) {
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

int32_t tmqCommitInner2(tmq_t* tmq, const TAOS_RES* msg, int8_t automatic, int8_t async, tmq_commit_cb* userCb,
                        void* userParam) {
  int32_t code = -1;

  if (msg != NULL) {
    return tmqCommitMsgImpl(tmq, msg, async, userCb, userParam);
  }

  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pParamSet->tmq = tmq;
  pParamSet->automatic = automatic;
  pParamSet->async = async;
  /*pParamSet->freeOffsets = 1;*/
  pParamSet->userCb = userCb;
  pParamSet->userParam = userParam;
  tsem_init(&pParamSet->rspSem, 0, 0);

  for (int32_t i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);

    tscDebug("consumer:%" PRId64 ", begin commit for topic %s, vgNum %d", tmq->consumerId, pTopic->topicName,
             (int32_t)taosArrayGetSize(pTopic->vgs));

    for (int32_t j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

      tscDebug("consumer:%" PRId64 ", begin commit for topic %s, vgId:%d", tmq->consumerId, pTopic->topicName,
               pVg->vgId);

      if (pVg->currentOffsetNew.type > 0 && !tOffsetEqual(&pVg->currentOffsetNew, &pVg->committedOffsetNew)) {
        if (tmqSendCommitReq(tmq, pVg, pTopic, pParamSet) < 0) {
          continue;
        }
      }
    }
  }

  if (pParamSet->totalRspNum == 0) {
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return 0;
  }

  if (!async) {
    tsem_wait(&pParamSet->rspSem);
    code = pParamSet->rspErr;
    tsem_destroy(&pParamSet->rspSem);
  } else {
    code = 0;
  }

  if (code != 0 && async) {
    if (automatic) {
      tmq->commitCb(tmq, code, tmq->commitCbUserParam);
    } else {
      userCb(tmq, code, userParam);
    }
  }

#if 0
  if (!async) {
    taosArrayDestroyP(pParamSet->successfulOffsets, taosMemoryFree);
    taosArrayDestroyP(pParamSet->failedOffsets, taosMemoryFree);
  }
#endif

  return 0;
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM);
  *pTaskType = TMQ_DELAYED_TASK__ASK_EP;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM);
  *pTaskType = TMQ_DELAYED_TASK__COMMIT;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

void tmqAssignDelayedReportTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM);
  *pTaskType = TMQ_DELAYED_TASK__REPORT;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg && pMsg->pData) taosMemoryFree(pMsg->pData);
  return 0;
}

void tmqSendHbReq(void* param, void* tmrId) {
  // TODO replace with ref
  tmq_t*    tmq = (tmq_t*)param;
  int64_t   consumerId = tmq->consumerId;
  int32_t   epoch = tmq->epoch;
  SMqHbReq* pReq = taosMemoryMalloc(sizeof(SMqHbReq));
  if (pReq == NULL) goto OVER;
  pReq->consumerId = consumerId;
  pReq->epoch = epoch;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pReq);
  }
  sendInfo->msgInfo = (SDataBuf){
      .pData = pReq,
      .len = sizeof(SMqHbReq),
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = NULL;
  sendInfo->fp = tmqHbCb;
  sendInfo->msgType = TDMT_MND_MQ_HB;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

OVER:
  taosTmrReset(tmqSendHbReq, 1000, tmq, tmqMgmt.timer, &tmq->hbLiveTimer);
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
      taosTmrReset(tmqAssignAskEpTask, 1000, tmq, tmqMgmt.timer, &tmq->epTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmqCommitInner2(tmq, NULL, 1, 1, tmq->commitCb, tmq->commitCbUserParam);
      taosTmrReset(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, tmq, tmqMgmt.timer, &tmq->commitTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__REPORT) {
    } else {
      ASSERT(0);
    }
    taosFreeQitem(pTaskType);
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

int32_t tmqSubscribeCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;
  /*tmq_t* tmq = pParam->tmq;*/
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
  tmq_list_t* lst = tmq_list_new();
  int32_t     rsp = tmq_subscribe(tmq, lst);
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("consumer %" PRId64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
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
  pTmq->useSnapshot = conf->ssEnable;
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

  if (pTmq->hbBgEnable) {
    pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, 1000, pTmq, tmqMgmt.timer);
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

#if 0
int32_t tmq_commit(tmq_t* tmq, const tmq_topic_vgroup_list_t* offsets, int32_t async) {
  return tmqCommitInner2(tmq, offsets, 0, async, tmq->commitCb, tmq->commitCbUserParam);
}
#endif

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = -1;

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, 256);
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

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) goto FAIL;

  SMqSubscribeCbParam param = {
      .rspErr = 0,
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

  // init ep timer
  if (tmq->epTimer == NULL) {
    tmq->epTimer = taosTmrStart(tmqAssignAskEpTask, 1000, tmq, tmqMgmt.timer);
  }

  // init auto commit timer
  if (tmq->autoCommit && tmq->commitTimer == NULL) {
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

void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  //
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  SMqClientTopic* pTopic = pParam->pTopic;
  tmq_t*          tmq = pParam->tmq;
  int32_t         vgId = pParam->vgId;
  int32_t         epoch = pParam->epoch;
  taosMemoryFree(pParam);
  if (code != 0) {
    tscWarn("msg discard from vgId:%d, epoch %d, code:%x", vgId, epoch, code);
    if (pMsg->pData) taosMemoryFree(pMsg->pData);
    if (code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM);
      if (pRspWrapper == NULL) {
        taosMemoryFree(pMsg->pData);
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
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("mismatch rsp from vgId:%d, epoch %d, current epoch %d", vgId, msgEpoch, tmqEpoch);
  }

  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM);
  if (pRspWrapper == NULL) {
    taosMemoryFree(pMsg->pData);
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
  } else {
    ASSERT(rspType == TMQ_MSG_TYPE__POLL_META_RSP);
    tDecodeSMqMetaRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRspWrapper->metaRsp);
    memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
  }

  taosMemoryFree(pMsg->pData);

  tscDebug("consumer:%" PRId64 ", recv poll: vgId:%d, req offset %" PRId64 ", rsp offset %" PRId64 " type %d",
           tmq->consumerId, pVg->vgId, pRspWrapper->dataRsp.reqOffset.version, pRspWrapper->dataRsp.rspOffset.version,
           rspType);

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

bool tmqUpdateEp2(tmq_t* tmq, int32_t epoch, SMqAskEpRsp* pRsp) {
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
        tFormatOffset(buf, 80, &pVgCur->currentOffsetNew);
        tscDebug("consumer:%" PRId64 ", epoch %d vgId:%d vgKey is %s, offset is %s", tmq->consumerId, epoch,
                 pVgCur->vgId, vgKey, buf);
        taosHashPut(pHash, vgKey, strlen(vgKey), &pVgCur->currentOffsetNew, sizeof(STqOffsetVal));
      }
    }
  }

  for (int32_t i = 0; i < topicNumGet; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    topic.schema = pTopicEp->schema;
    topic.topicName = strdup(pTopicEp->topic);
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
          .currentOffsetNew = offsetNew,
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

#if 0
bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, SMqAskEpRsp* pRsp) {
  /*printf("call update ep %d\n", epoch);*/
  bool    set = false;
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  tscDebug("consumer:%" PRId64 ", update ep epoch %d to epoch %d, topic num: %d", tmq->consumerId, tmq->epoch, epoch,
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
    tstrncpy(topic.db, pTopicEp->db, TSDB_DB_FNAME_LEN);

    tscDebug("consumer:%" PRId64 ", update topic: %s", tmq->consumerId, topic.topicName);
    int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
    for (int32_t j = 0; j < topicNumCur; j++) {
      // find old topic
      SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
      if (pTopicCur->vgs && strcmp(pTopicCur->topicName, pTopicEp->topic) == 0) {
        int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
        tscDebug("consumer:%" PRId64 ", new vg num: %d", tmq->consumerId, vgNumCur);
        if (vgNumCur == 0) break;
        for (int32_t k = 0; k < vgNumCur; k++) {
          SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, k);
          sprintf(vgKey, "%s:%d", topic.topicName, pVgCur->vgId);
          tscDebug("consumer:%" PRId64 ", epoch %d vgId:%d build %s", tmq->consumerId, epoch, pVgCur->vgId, vgKey);
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
      tscDebug("consumer:%" PRId64 ", (epoch %d) original offset of vgId:%d is %" PRId64, tmq->consumerId, epoch, pVgEp->vgId, offset);
      if (pOffset != NULL) {
        offset = *pOffset;
        tscDebug("consumer:%" PRId64 ", (epoch %d) receive offset of vgId:%d, full key is %s", tmq->consumerId, epoch, pVgEp->vgId,
                 vgKey);
      }
      tscDebug("consumer:%" PRId64 ", (epoch %d) offset of vgId:%d updated to %" PRId64, tmq->consumerId, epoch, pVgEp->vgId, offset);
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
#endif

int32_t tmqAskEpCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = pParam->tmq;
  int8_t           async = pParam->async;
  pParam->code = code;
  if (code != 0) {
    tscError("consumer:%" PRId64 ", get topic endpoint error, not ready, wait:%d", tmq->consumerId, pParam->async);
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
    tmqUpdateEp2(tmq, head->epoch, &rsp);
    tDeleteSMqAskEpRsp(&rsp);
  } else {
    SMqAskEpRspWrapper* pWrapper = taosAllocateQitem(sizeof(SMqAskEpRspWrapper), DEF_QITEM);
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
  int32_t      tlen = sizeof(SMqAskEpReq);
  SMqAskEpReq* req = taosMemoryCalloc(1, tlen);
  if (req == NULL) {
    tscError("failed to malloc get subscribe ep buf");
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }
  req->consumerId = htobe64(tmq->consumerId);
  req->epoch = htonl(tmq->epoch);
  strcpy(req->cgroup, tmq->groupId);

  SMqAskEpCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("failed to malloc subscribe param");
    taosMemoryFree(req);
    /*atomic_store_8(&tmq->epStatus, 0);*/
    return -1;
  }
  pParam->tmq = tmq;
  pParam->async = async;
  tsem_init(&pParam->rspSem, 0, 0);

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
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

#if 0
int32_t tmq_seek(tmq_t* tmq, const tmq_topic_vgroup_t* offset) {
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
#endif

SMqPollReq* tmqBuildConsumeReqImpl(tmq_t* tmq, int64_t timeout, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  /*int64_t reqOffset;*/
  /*if (pVg->currentOffset >= 0) {*/
  /*reqOffset = pVg->currentOffset;*/
  /*} else {*/
  /*if (tmq->resetOffsetCfg == TMQ_CONF__RESET_OFFSET__NONE) {*/
  /*tscError("unable to poll since no committed offset but reset offset is set to none");*/
  /*return NULL;*/
  /*}*/
  /*reqOffset = tmq->resetOffsetCfg;*/
  /*}*/

  SMqPollReq* pReq = taosMemoryCalloc(1, sizeof(SMqPollReq));
  if (pReq == NULL) {
    return NULL;
  }

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
  pReq->reqOffset = pVg->currentOffsetNew;
  pReq->reqId = generateRequestId();

  pReq->useSnapshot = tmq->useSnapshot;

  pReq->head.vgId = htonl(pVg->vgId);
  pReq->head.contLen = htonl(sizeof(SMqPollReq));
  return pReq;
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

int32_t tmqPollImpl(tmq_t* tmq, int64_t timeout) {
  /*tscDebug("call poll");*/
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
      SMqPollReq* pReq = tmqBuildConsumeReqImpl(tmq, timeout, pTopic, pVg);
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

      SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
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

      char offsetFormatBuf[80];
      tFormatOffset(offsetFormatBuf, 80, &pVg->currentOffsetNew);
      tscDebug("consumer:%" PRId64 ", send poll to %s vgId:%d, epoch %d, req offset:%s, reqId:%" PRIu64,
               tmq->consumerId, pTopic->topicName, pVg->vgId, tmq->epoch, offsetFormatBuf, pReq->reqId);
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
      tmqUpdateEp2(tmq, rspWrapper->epoch, rspMsg);
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

void* tmqHandleAllRsp(tmq_t* tmq, int64_t timeout, bool pollIfReset) {
  while (1) {
    SMqRspWrapper* rspWrapper = NULL;
    taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&rspWrapper);
      if (rspWrapper == NULL) return NULL;
    }

    if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
      taosFreeQitem(rspWrapper);
      terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
      return NULL;
    } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      /*atomic_sub_fetch_32(&tmq->readyRequest, 1);*/
      int32_t consumerEpoch = atomic_load_32(&tmq->epoch);
      if (pollRspWrapper->dataRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vgId:%d, offset %" PRId64 " up to %" PRId64 "\n", pVg->vgId, pVg->currentOffset,
         * rspMsg->msg.rspOffset);*/
        pVg->currentOffsetNew = pollRspWrapper->dataRsp.rspOffset;
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
        tscDebug("msg discard since epoch mismatch: msg epoch %d, consumer epoch %d\n",
                 pollRspWrapper->dataRsp.head.epoch, consumerEpoch);
        taosFreeQitem(pollRspWrapper);
      }
    } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)rspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);
      if (pollRspWrapper->metaRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        /*printf("vgId:%d, offset %" PRId64 " up to %" PRId64 "\n", pVg->vgId, pVg->currentOffset,
         * rspMsg->msg.rspOffset);*/
        pVg->currentOffsetNew.version = pollRspWrapper->metaRsp.rspOffset;
        pVg->currentOffsetNew.type = TMQ_OFFSET__LOG;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // build rsp
        SMqMetaRspObj* pRsp = tmqBuildMetaRspFromWrapper(pollRspWrapper);
        taosFreeQitem(pollRspWrapper);
        return pRsp;
      } else {
        tscDebug("msg discard since epoch mismatch: msg epoch %d, consumer epoch %d\n",
                 pollRspWrapper->metaRsp.head.epoch, consumerEpoch);
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
  /*tscDebug("call poll1");*/
  void*   rspObj;
  int64_t startTime = taosGetTimestampMs();

#if 0
  tmqHandleAllDelayedTask(tmq);
  tmqPollImpl(tmq, timeout);
  rspObj = tmqHandleAllRsp(tmq, timeout, false);
  if (rspObj) {
    return (TAOS_RES*)rspObj;
  }
#endif

  // in no topic status also need process delayed task
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    return NULL;
  }

  while (1) {
    tmqHandleAllDelayedTask(tmq);
    if (tmqPollImpl(tmq, timeout) < 0) return NULL;

    rspObj = tmqHandleAllRsp(tmq, timeout, false);
    if (rspObj) {
      return (TAOS_RES*)rspObj;
    } else if (terrno == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      return NULL;
    }
    if (timeout != -1) {
      int64_t endTime = taosGetTimestampMs();
      int64_t leftTime = endTime - startTime;
      if (leftTime > timeout) {
        tscDebug("consumer:%" PRId64 ", (epoch %d) timeout, no rsp", tmq->consumerId, tmq->epoch);
        return NULL;
      }
      tsem_timewait(&tmq->rspSem, leftTime * 1000);
    } else {
      // use tsem_timewait instead of tsem_wait to avoid unexpected stuck
      tsem_timewait(&tmq->rspSem, 500 * 1000);
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

    return rsp;
  }
  // TODO: free resources
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
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DELETE) {
      return TMQ_RES_DATA;
    }
    return TMQ_RES_TABLE_META;
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
  }
  return NULL;
}

static char* buildCreateTableJson(SSchemaWrapper* schemaRow, SSchemaWrapper* schemaTag, char* name, int64_t id,
                                  int8_t t) {
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return string;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);

  //  char uid[32] = {0};
  //  sprintf(uid, "%"PRIi64, id);
  //  cJSON* id_ = cJSON_CreateString(uid);
  //  cJSON_AddItemToObject(json, "id", id_);
  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString(t == TSDB_NORMAL_TABLE ? "normal" : "super");
  cJSON_AddItemToObject(json, "tableType", tableType);
  //  cJSON* version = cJSON_CreateNumber(1);
  //  cJSON_AddItemToObject(json, "version", version);

  cJSON* columns = cJSON_CreateArray();
  for (int i = 0; i < schemaRow->nCols; i++) {
    cJSON*   column = cJSON_CreateObject();
    SSchema* s = schemaRow->pSchema + i;
    cJSON*   cname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(column, "name", cname);
    cJSON* ctype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(column, "type", ctype);
    if (s->type == TSDB_DATA_TYPE_BINARY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    }
    cJSON_AddItemToArray(columns, column);
  }
  cJSON_AddItemToObject(json, "columns", columns);

  cJSON* tags = cJSON_CreateArray();
  for (int i = 0; schemaTag && i < schemaTag->nCols; i++) {
    cJSON*   tag = cJSON_CreateObject();
    SSchema* s = schemaTag->pSchema + i;
    cJSON*   tname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(tag, "name", tname);
    cJSON* ttype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(tag, "type", ttype);
    if (s->type == TSDB_DATA_TYPE_BINARY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    }
    cJSON_AddItemToArray(tags, tag);
  }
  cJSON_AddItemToObject(json, "tags", tags);

  string = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);
  return string;
}

static char* buildAlterSTableJson(void* alterData, int32_t alterDataLen) {
  SMAlterStbReq req = {0};
  cJSON*        json = NULL;
  char*         string = NULL;

  if (tDeserializeSMAlterStbReq(alterData, alterDataLen, &req) != 0) {
    goto end;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto end;
  }
  cJSON* type = cJSON_CreateString("alter");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  SName name = {0};
  tNameFromString(&name, req.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  cJSON* tableName = cJSON_CreateString(name.tname);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString("super");
  cJSON_AddItemToObject(json, "tableType", tableType);

  cJSON* alterType = cJSON_CreateNumber(req.alterType);
  cJSON_AddItemToObject(json, "alterType", alterType);
  switch (req.alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(field->type);
      cJSON_AddItemToObject(json, "colType", colType);

      if (field->type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(field->type);
      cJSON_AddItemToObject(json, "colType", colType);
      if (field->type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD* oldField = taosArrayGet(req.pFields, 0);
      TAOS_FIELD* newField = taosArrayGet(req.pFields, 1);
      cJSON*      colName = cJSON_CreateString(oldField->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colNewName = cJSON_CreateString(newField->name);
      cJSON_AddItemToObject(json, "colNewName", colNewName);
      break;
    }
    default:
      break;
  }
  string = cJSON_PrintUnformatted(json);

end:
  cJSON_Delete(json);
  tFreeSMAltertbReq(&req);
  return string;
}

static char* processCreateStb(SMqMetaRsp* metaRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*          string = NULL;

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildCreateTableJson(&req.schemaRow, &req.schemaTag, req.name, req.suid, TSDB_SUPER_TABLE);
  tDecoderClear(&coder);
  return string;

_err:
  tDecoderClear(&coder);
  return string;
}

static char* processAlterStb(SMqMetaRsp* metaRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*          string = NULL;

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildAlterSTableJson(req.alterOriData, req.alterOriDataLen);
  tDecoderClear(&coder);
  return string;

_err:
  tDecoderClear(&coder);
  return string;
}

static char* buildCreateCTableJson(STag* pTag, char* sname, char* name, SArray* tagName, int64_t id, uint8_t tagNum) {
  char*   string = NULL;
  SArray* pTagVals = NULL;
  cJSON*  json = cJSON_CreateObject();
  if (json == NULL) {
    return string;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);
  //  char cid[32] = {0};
  //  sprintf(cid, "%"PRIi64, id);
  //  cJSON* cid_ = cJSON_CreateString(cid);
  //  cJSON_AddItemToObject(json, "id", cid_);

  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString("child");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* using = cJSON_CreateString(sname);
  cJSON_AddItemToObject(json, "using", using);
  cJSON* tagNumJson = cJSON_CreateNumber(tagNum);
  cJSON_AddItemToObject(json, "tagNum", tagNumJson);
  //  cJSON* version = cJSON_CreateNumber(1);
  //  cJSON_AddItemToObject(json, "version", version);

  cJSON*  tags = cJSON_CreateArray();
  int32_t code = tTagToValArray(pTag, &pTagVals);
  if (code) {
    goto end;
  }

  if (tTagIsJson(pTag)) {
    STag* p = (STag*)pTag;
    if (p->nTag == 0) {
      goto end;
    }
    char*    pJson = parseTagDatatoJson(pTag);
    cJSON*   tag = cJSON_CreateObject();
    STagVal* pTagVal = taosArrayGet(pTagVals, 0);

    char*  ptname = taosArrayGet(tagName, 0);
    cJSON* tname = cJSON_CreateString(ptname);
    cJSON_AddItemToObject(tag, "name", tname);
    //    cJSON* cid_ = cJSON_CreateString("");
    //    cJSON_AddItemToObject(tag, "cid", cid_);
    cJSON* ttype = cJSON_CreateNumber(TSDB_DATA_TYPE_JSON);
    cJSON_AddItemToObject(tag, "type", ttype);
    cJSON* tvalue = cJSON_CreateString(pJson);
    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
    taosMemoryFree(pJson);
    goto end;
  }

  for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);

    cJSON* tag = cJSON_CreateObject();

    char*  ptname = taosArrayGet(tagName, i);
    cJSON* tname = cJSON_CreateString(ptname);
    cJSON_AddItemToObject(tag, "name", tname);
    //    cJSON* cid = cJSON_CreateNumber(pTagVal->cid);
    //    cJSON_AddItemToObject(tag, "cid", cid);
    cJSON* ttype = cJSON_CreateNumber(pTagVal->type);
    cJSON_AddItemToObject(tag, "type", ttype);

    cJSON* tvalue = NULL;
    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      char* buf = taosMemoryCalloc(pTagVal->nData + 3, 1);
      if (!buf) goto end;
      dataConverToStr(buf, pTagVal->type, pTagVal->pData, pTagVal->nData, NULL);
      tvalue = cJSON_CreateString(buf);
      taosMemoryFree(buf);
    } else {
      double val = 0;
      GET_TYPED_DATA(val, double, pTagVal->type, &pTagVal->i64);
      tvalue = cJSON_CreateNumber(val);
    }

    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
  }

end:
  cJSON_AddItemToObject(json, "tags", tags);
  string = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);
  taosArrayDestroy(pTagVals);
  return string;
}

static char* processCreateTable(SMqMetaRsp* metaRsp) {
  SDecoder           decoder = {0};
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq*     pCreateReq;
  char*              string = NULL;
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    goto _exit;
  }

  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      string = buildCreateCTableJson((STag*)pCreateReq->ctb.pTag, pCreateReq->ctb.name, pCreateReq->name,
                                     pCreateReq->ctb.tagName, pCreateReq->uid, pCreateReq->ctb.tagNum);
    } else if (pCreateReq->type == TSDB_NORMAL_TABLE) {
      string =
          buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->name, pCreateReq->uid, TSDB_NORMAL_TABLE);
    }
  }

  tDecoderClear(&decoder);

_exit:
  tDecoderClear(&decoder);
  return string;
}

static char* processAlterTable(SMqMetaRsp* metaRsp) {
  SDecoder     decoder = {0};
  SVAlterTbReq vAlterTbReq = {0};
  char*        string = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVAlterTbReq(&decoder, &vAlterTbReq) < 0) {
    goto _exit;
  }

  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("alter");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  cJSON* tableName = cJSON_CreateString(vAlterTbReq.tbName);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString(vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL ? "child" : "normal");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* alterType = cJSON_CreateNumber(vAlterTbReq.action);
  cJSON_AddItemToObject(json, "alterType", alterType);

  switch (vAlterTbReq.action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      cJSON_AddItemToObject(json, "colType", colType);

      if (vAlterTbReq.type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.colModType);
      cJSON_AddItemToObject(json, "colType", colType);
      if (vAlterTbReq.colModType == TSDB_DATA_TYPE_BINARY) {
        int32_t length = vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (vAlterTbReq.colModType == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colNewName = cJSON_CreateString(vAlterTbReq.colNewName);
      cJSON_AddItemToObject(json, "colNewName", colNewName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL: {
      cJSON* tagName = cJSON_CreateString(vAlterTbReq.tagName);
      cJSON_AddItemToObject(json, "colName", tagName);

      bool isNull = vAlterTbReq.isNull;
      if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
        STag* jsonTag = (STag*)vAlterTbReq.pTagVal;
        if (jsonTag->nTag == 0) isNull = true;
      }
      if (!isNull) {
        char* buf = NULL;

        if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
          ASSERT(tTagIsJson(vAlterTbReq.pTagVal) == true);
          buf = parseTagDatatoJson(vAlterTbReq.pTagVal);
        } else {
          buf = taosMemoryCalloc(vAlterTbReq.nTagVal + 1, 1);
          dataConverToStr(buf, vAlterTbReq.tagType, vAlterTbReq.pTagVal, vAlterTbReq.nTagVal, NULL);
        }

        cJSON* colValue = cJSON_CreateString(buf);
        cJSON_AddItemToObject(json, "colValue", colValue);
        taosMemoryFree(buf);
      }

      cJSON* isNullCJson = cJSON_CreateBool(isNull);
      cJSON_AddItemToObject(json, "colValueNull", isNullCJson);
      break;
    }
    default:
      break;
  }
  string = cJSON_PrintUnformatted(json);

_exit:
  tDecoderClear(&decoder);
  return string;
}

static char* processDropSTable(SMqMetaRsp* metaRsp) {
  SDecoder     decoder = {0};
  SVDropStbReq req = {0};
  char*        string = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    goto _exit;
  }

  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("drop");
  cJSON_AddItemToObject(json, "type", type);
  cJSON* tableName = cJSON_CreateString(req.name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString("super");
  cJSON_AddItemToObject(json, "tableType", tableType);

  string = cJSON_PrintUnformatted(json);

_exit:
  tDecoderClear(&decoder);
  return string;
}

static char* processDropTable(SMqMetaRsp* metaRsp) {
  SDecoder         decoder = {0};
  SVDropTbBatchReq req = {0};
  char*            string = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropTbBatchReq(&decoder, &req) < 0) {
    goto _exit;
  }

  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("drop");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  //  cJSON* tableType = cJSON_CreateString("normal");
  //  cJSON_AddItemToObject(json, "tableType", tableType);

  cJSON* tableNameList = cJSON_CreateArray();
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;

    cJSON* tableName = cJSON_CreateString(pDropTbReq->name);
    cJSON_AddItemToArray(tableNameList, tableName);
  }
  cJSON_AddItemToObject(json, "tableNameList", tableNameList);

  string = cJSON_PrintUnformatted(json);

_exit:
  tDecoderClear(&decoder);
  return string;
}

static int32_t taosCreateStb(TAOS* taos, void* meta, int32_t metaLen) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  // build create stable
  pReq.pColumns = taosArrayInit(req.schemaRow.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaRow.nCols; i++) {
    SSchema* pSchema = req.schemaRow.pSchema + i;
    SField   field = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pColumns, &field);
  }
  pReq.pTags = taosArrayInit(req.schemaTag.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaTag.nCols; i++) {
    SSchema* pSchema = req.schemaTag.pSchema + i;
    SField   field = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pTags, &field);
  }

  pReq.colVer = req.schemaRow.version;
  pReq.tagVer = req.schemaTag.version;
  pReq.numOfColumns = req.schemaRow.nCols;
  pReq.numOfTags = req.schemaTag.nCols;
  pReq.commentLen = -1;
  pReq.suid = req.suid;
  pReq.source = TD_REQ_FROM_TAOX;
  pReq.igExists = true;

  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName;
  tNameExtractFullName(toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName), pReq.name);

  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveTableMeta(pCatalog, &tableName);
  }

  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

end:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  tDecoderClear(&coder);
  return code;
}

static int32_t taosDropStb(TAOS* taos, void* meta, int32_t metaLen) {
  SVDropStbReq req = {0};
  SDecoder     coder;
  SMDropStbReq pReq = {0};
  int32_t      code = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // build drop stable
  pReq.igNotExists = true;
  pReq.source = TD_REQ_FROM_TAOX;
  pReq.suid = req.suid;

  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName = {0};
  tNameExtractFullName(toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName), pReq.name);

  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_DROP_STB;
  pCmdMsg.msgLen = tSerializeSMDropStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMDropStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveTableMeta(pCatalog, &tableName);
  }

  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

end:
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  return code;
}

typedef struct SVgroupCreateTableBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
  char               dbName[TSDB_DB_NAME_LEN];
} SVgroupCreateTableBatch;

static void destroyCreateTbReqBatch(void* data) {
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosCreateTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVCreateTbBatchReq req = {0};
  SDecoder           coder = {0};
  int32_t            code = TSDB_CODE_SUCCESS;
  SRequestObj*       pRequest = NULL;
  SQuery*            pQuery = NULL;
  SHashObj*          pVgroupHashmap = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  STscObj* pTscObj = pRequest->pTscObj;

  SVCreateTbReq* pCreateReq = NULL;
  SCatalog*      pCatalog = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  taosHashSetFreeFp(pVgroupHashmap, destroyCreateTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pCreateReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
    taosArrayPush(pRequest->tableList, &pName);

    SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupCreateTableBatch tBatch = {0};
      tBatch.info = pInfo;
      strcpy(tBatch.dbName, pRequest->pDb);

      tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
      taosArrayPush(tBatch.req.pArray, pCreateReq);

      taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch));
    } else {  // add to the correct vgroup
      taosArrayPush(pTableBatch->req.pArray, pCreateReq);
    }
  }

  SArray* pBufArray = serializeVgroupsCreateTableBatch(pVgroupHashmap);
  if (NULL == pBufArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_CREATE_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    removeMeta(pTscObj, pRequest->tableList);
  }

  code = pRequest->code;

end:
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static void destroyDropTbReqBatch(void* data) {
  SVgroupDropTableBatch* pTbBatch = (SVgroupDropTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosDropTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVDropTbBatchReq req = {0};
  SDecoder         coder = {0};
  int32_t          code = TSDB_CODE_SUCCESS;
  SRequestObj*     pRequest = NULL;
  SQuery*          pQuery = NULL;
  SHashObj*        pVgroupHashmap = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  STscObj* pTscObj = pRequest->pTscObj;

  SVDropTbReq* pDropReq = NULL;
  SCatalog*    pCatalog = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};
  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;
    pDropReq->igNotExists = true;

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pDropReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    taosArrayPush(pRequest->tableList, &pName);
    SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupDropTableBatch tBatch = {0};
      tBatch.info = pInfo;
      tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
      taosArrayPush(tBatch.req.pArray, pDropReq);

      taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch));
    } else {  // add to the correct vgroup
      taosArrayPush(pTableBatch->req.pArray, pDropReq);
    }
  }

  SArray* pBufArray = serializeVgroupsDropTableBatch(pVgroupHashmap);
  if (NULL == pBufArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_DROP_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    removeMeta(pTscObj, pRequest->tableList);
  }
  code = pRequest->code;

end:
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

// delete from db.tabl where ..       -> delete from tabl where ..
// delete from db    .tabl where ..   -> delete from tabl where ..
// static void getTbName(char *sql){
//  char *ch = sql;
//
//  bool inBackQuote = false;
//  int8_t dotIndex = 0;
//  while(*ch != '\0'){
//    if(!inBackQuote && *ch == '`'){
//      inBackQuote = true;
//      ch++;
//      continue;
//    }
//
//    if(inBackQuote && *ch == '`'){
//      inBackQuote = false;
//      ch++;
//
//      continue;
//    }
//
//    if(!inBackQuote && *ch == '.'){
//      dotIndex ++;
//      if(dotIndex == 2){
//        memmove(sql, ch + 1, strlen(ch + 1) + 1);
//        break;
//      }
//    }
//    ch++;
//  }
//}

static int32_t taosDeleteData(TAOS* taos, void* meta, int32_t metaLen) {
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  int32_t    code = TSDB_CODE_SUCCESS;

  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  //  getTbName(req.tableFName);
  char sql[256] = {0};
  sprintf(sql, "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName, req.tsColName,
          req.skey, req.tsColName, req.ekey);
  printf("delete sql:%s\n", sql);

  TAOS_RES*    res = taos_query(taos, sql);
  SRequestObj* pRequest = (SRequestObj*)res;
  code = pRequest->code;
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    code = TSDB_CODE_SUCCESS;
  }
  taos_free_result(res);

end:
  tDecoderClear(&coder);
  return code;
}

static int32_t taosAlterTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVAlterTbReq   req = {0};
  SDecoder       coder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;
  SQuery*        pQuery = NULL;
  SArray*        pArray = NULL;
  SVgDataBlocks* pVgData = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest);

  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVAlterTbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // do not deal TSDB_ALTER_TABLE_UPDATE_OPTIONS
  if (req.action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    goto end;
  }

  STscObj*  pTscObj = pRequest->pTscObj;
  SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  SVgroupInfo pInfo = {0};
  SName       pName = {0};
  toName(pTscObj->acctId, pRequest->pDb, req.tbName, &pName);
  code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pArray = taosArrayInit(1, sizeof(void*));
  if (NULL == pArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgData->vg = pInfo;
  pVgData->pData = taosMemoryMalloc(metaLen);
  if (NULL == pVgData->pData) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  memcpy(pVgData->pData, meta, metaLen);
  ((SMsgHead*)pVgData->pData)->vgId = htonl(pInfo.vgId);
  pVgData->size = metaLen;
  pVgData->numOfTables = 1;
  taosArrayPush(pArray, &pVgData);

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_ALTER_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);

  pVgData = NULL;
  pArray = NULL;
  code = pRequest->code;
  if (code == TSDB_CODE_VND_TABLE_NOT_EXIST) {
    code = TSDB_CODE_SUCCESS;
  }

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SExecResult* pRes = &pRequest->body.resInfo.execRes;
    if (pRes->res != NULL) {
      code = handleAlterTbExecRes(pRes->res, pCatalog);
    }
  }
end:
  taosArrayDestroy(pArray);
  if (pVgData) taosMemoryFreeClear(pVgData->pData);
  taosMemoryFreeClear(pVgData);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

typedef struct {
  SVgroupInfo vg;
  void*       data;
} VgData;

static void destroyVgHash(void* data) {
  VgData* vgData = (VgData*)data;
  taosMemoryFreeClear(vgData->data);
}

int taos_write_raw_block(TAOS* taos, int rows, char* pData, const char* tbname) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;

  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    code = terrno;
    goto end;
  }

  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
  strcpy(pName.dbname, pRequest->pDb);
  strcpy(pName.tname, tbname);

  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  SVgroupInfo vgData = {0};
  code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgData);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbname);
    goto end;
  }

  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbname);
    goto end;
  }
  uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
  uint64_t uid = pTableMeta->uid;
  int32_t  numOfCols = pTableMeta->tableInfo.numOfColumns;

  uint16_t fLen = 0;
  int32_t  rowSize = 0;
  int16_t  nVar = 0;
  for (int i = 0; i < numOfCols; i++) {
    SSchema* schema = pTableMeta->schema + i;
    fLen += TYPE_BYTES[schema->type];
    rowSize += schema->bytes;
    if (IS_VAR_DATA_TYPE(schema->type)) {
      nVar++;
    }
  }

  int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                            (int32_t)TD_BITMAP_BYTES(numOfCols - 1);
  int32_t schemaLen = 0;
  int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

  int32_t     totalLen = sizeof(SSubmitReq) + submitLen;
  SSubmitReq* subReq = taosMemoryCalloc(1, totalLen);
  SSubmitBlk* blk = POINTER_SHIFT(subReq, sizeof(SSubmitReq));
  void*       blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
  STSRow*     rowData = POINTER_SHIFT(blkSchema, schemaLen);

  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTableMeta->sversion);
  tdSRowSetTpInfo(&rb, numOfCols, fLen);
  int32_t dataLen = 0;

  char*    pStart = pData + sizeof(int32_t) + sizeof(uint64_t) + numOfCols * (sizeof(int16_t) + sizeof(int32_t));
  int32_t* colLength = (int32_t*)pStart;
  pStart += sizeof(int32_t) * numOfCols;

  SResultColumn* pCol = taosMemoryCalloc(numOfCols, sizeof(SResultColumn));

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(pTableMeta->schema[i].type)) {
      pCol[i].offset = (int32_t*)pStart;
      pStart += rows * sizeof(int32_t);
    } else {
      pCol[i].nullbitmap = pStart;
      pStart += BitmapLen(rows);
    }

    pCol[i].pData = pStart;
    pStart += colLength[i];
  }

  for (int32_t j = 0; j < rows; j++) {
    tdSRowResetBuf(&rb, rowData);
    int32_t offset = 0;
    for (int32_t k = 0; k < numOfCols; k++) {
      const SSchema* pColumn = &pTableMeta->schema[k];

      if (IS_VAR_DATA_TYPE(pColumn->type)) {
        if (pCol[k].offset[j] != -1) {
          char* data = pCol[k].pData + pCol[k].offset[j];
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
        } else {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
        }
      } else {
        if (!colDataIsNull_f(pCol[k].nullbitmap, j)) {
          char* data = pCol[k].pData + pColumn->bytes * j;
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
        } else {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
        }
      }

      offset += TYPE_BYTES[pColumn->type];
    }
    int32_t rowLen = TD_ROW_LEN(rowData);
    rowData = POINTER_SHIFT(rowData, rowLen);
    dataLen += rowLen;
  }

  taosMemoryFree(pCol);

  blk->uid = htobe64(uid);
  blk->suid = htobe64(suid);
  blk->sversion = htonl(pTableMeta->sversion);
  blk->schemaLen = htonl(schemaLen);
  blk->numOfRows = htonl(rows);
  blk->dataLen = htonl(dataLen);
  subReq->length = sizeof(SSubmitReq) + sizeof(SSubmitBlk) + schemaLen + dataLen;
  subReq->numOfBlocks = 1;

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);
  nodeStmt->payloadType = PAYLOAD_TYPE_KV;
  nodeStmt->pDataBlocks = taosArrayInit(1, POINTER_BYTES);

  SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == dst) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto end;
  }
  dst->vg = vgData;
  dst->numOfTables = subReq->numOfBlocks;
  dst->size = subReq->length;
  dst->pData = (char*)subReq;
  subReq->header.vgId = htonl(dst->vg.vgId);
  subReq->version = htonl(1);
  subReq->header.contLen = htonl(subReq->length);
  subReq->length = htonl(subReq->length);
  subReq->numOfBlocks = htonl(subReq->numOfBlocks);
  subReq = NULL;  // no need free
  taosArrayPush(nodeStmt->pDataBlocks, &dst);

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  return code;
}

static int32_t tmqWriteRaw(TAOS* taos, void* data, int32_t dataLen) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* pVgHash = NULL;
  SQuery*   pQuery = NULL;
  SMqRspObj rspObj = {0};
  SDecoder  decoder = {0};

  terrno = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    return terrno;
  }

  rspObj.resIter = -1;
  rspObj.resType = RES_TYPE__TMQ;

  tDecoderInit(&decoder, data, dataLen);
  code = tDecodeSMqDataRsp(&decoder, &rspObj.rsp);
  if (code != 0) {
    uError("WriteRaw:decode smqDataRsp error");
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  taosHashSetFreeFp(pVgHash, destroyVgHash);
  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  printf("raw data block num:%d\n", rspObj.rsp.blockNum);
  while (++rspObj.resIter < rspObj.rsp.blockNum) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(rspObj.rsp.blockData, rspObj.resIter);
    if (!rspObj.rsp.withSchema) {
      uError("WriteRaw:no schema, iter:%d", rspObj.resIter);
      goto end;
    }
    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.rsp.blockSchema, rspObj.resIter);
    setResSchemaInfo(&rspObj.resInfo, pSW->pSchema, pSW->nCols);

    code = setQueryResultFromRsp(&rspObj.resInfo, pRetrieve, false, false);
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw: setQueryResultFromRsp error");
      goto end;
    }

    uint16_t fLen = 0;
    int32_t  rowSize = 0;
    int16_t  nVar = 0;
    for (int i = 0; i < pSW->nCols; i++) {
      SSchema* schema = pSW->pSchema + i;
      fLen += TYPE_BYTES[schema->type];
      rowSize += schema->bytes;
      if (IS_VAR_DATA_TYPE(schema->type)) {
        nVar++;
      }
    }

    int32_t rows = rspObj.resInfo.numOfRows;
    int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                              (int32_t)TD_BITMAP_BYTES(pSW->nCols - 1);
    int32_t schemaLen = 0;
    int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

    const char* tbName = (const char*)taosArrayGetP(rspObj.rsp.blockTbName, rspObj.resIter);
    if (!tbName) {
      uError("WriteRaw: tbname is null");
      code = TSDB_CODE_TMQ_INVALID_MSG;
      goto end;
    }

    printf("raw data tbname:%s\n", tbName);
    SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
    strcpy(pName.dbname, pRequest->pDb);
    strcpy(pName.tname, tbName);

    VgData vgData = {0};
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &(vgData.vg));
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbName);
      goto end;
    }

    SSubmitReq* subReq = NULL;
    SSubmitBlk* blk = NULL;
    void*       hData = taosHashGet(pVgHash, &vgData.vg.vgId, sizeof(vgData.vg.vgId));
    if (hData) {
      vgData = *(VgData*)hData;

      int32_t totalLen = ((SSubmitReq*)(vgData.data))->length + submitLen;
      void*   tmp = taosMemoryRealloc(vgData.data, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_TSC_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      ((VgData*)hData)->data = tmp;
      subReq = (SSubmitReq*)(vgData.data);
      blk = POINTER_SHIFT(vgData.data, subReq->length);
    } else {
      int32_t totalLen = sizeof(SSubmitReq) + submitLen;
      void*   tmp = taosMemoryCalloc(1, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_TSC_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      taosHashPut(pVgHash, (const char*)&vgData.vg.vgId, sizeof(vgData.vg.vgId), (char*)&vgData, sizeof(vgData));
      subReq = (SSubmitReq*)(vgData.data);
      subReq->length = sizeof(SSubmitReq);
      subReq->numOfBlocks = 0;

      blk = POINTER_SHIFT(vgData.data, sizeof(SSubmitReq));
    }

    STableMeta* pTableMeta = NULL;
    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbName);
      goto end;
    }
    uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
    uint64_t uid = pTableMeta->uid;
    taosMemoryFreeClear(pTableMeta);

    void*   blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
    STSRow* rowData = POINTER_SHIFT(blkSchema, schemaLen);

    SRowBuilder rb = {0};
    tdSRowInit(&rb, pSW->version);
    tdSRowSetTpInfo(&rb, pSW->nCols, fLen);
    int32_t dataLen = 0;

    for (int32_t j = 0; j < rows; j++) {
      tdSRowResetBuf(&rb, rowData);

      doSetOneRowPtr(&rspObj.resInfo);
      rspObj.resInfo.current += 1;

      int32_t offset = 0;
      for (int32_t k = 0; k < pSW->nCols; k++) {
        const SSchema* pColumn = &pSW->pSchema[k];
        char*          data = rspObj.resInfo.row[k];
        if (!data) {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
        } else {
          if (IS_VAR_DATA_TYPE(pColumn->type)) {
            data -= VARSTR_HEADER_SIZE;
          }
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
        }
        offset += TYPE_BYTES[pColumn->type];
      }
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      dataLen += rowLen;
    }

    blk->uid = htobe64(uid);
    blk->suid = htobe64(suid);
    blk->sversion = htonl(pSW->version);
    blk->schemaLen = htonl(schemaLen);
    blk->numOfRows = htonl(rows);
    blk->dataLen = htonl(dataLen);
    subReq->length += sizeof(SSubmitBlk) + schemaLen + dataLen;
    subReq->numOfBlocks++;
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);
  nodeStmt->payloadType = PAYLOAD_TYPE_KV;

  int32_t numOfVg = taosHashGetSize(pVgHash);
  nodeStmt->pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);

  VgData* vData = (VgData*)taosHashIterate(pVgHash, NULL);
  while (vData) {
    SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto end;
    }
    dst->vg = vData->vg;
    SSubmitReq* subReq = (SSubmitReq*)(vData->data);
    dst->numOfTables = subReq->numOfBlocks;
    dst->size = subReq->length;
    dst->pData = (char*)subReq;
    vData->data = NULL;  // no need free
    subReq->header.vgId = htonl(dst->vg.vgId);
    subReq->version = htonl(1);
    subReq->header.contLen = htonl(subReq->length);
    subReq->length = htonl(subReq->length);
    subReq->numOfBlocks = htonl(subReq->numOfBlocks);
    taosArrayPush(nodeStmt->pDataBlocks, &dst);
    vData = (VgData*)taosHashIterate(pVgHash, vData);
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  tDecoderClear(&decoder);
  taos_free_result(&rspObj);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  return code;
}

char* tmq_get_json_meta(TAOS_RES* res) {
  if (!TD_RES_TMQ_META(res)) {
    return NULL;
  }

  SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
  if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_STB) {
    return processCreateStb(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_STB) {
    return processAlterStb(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_STB) {
    return processDropSTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_TABLE) {
    return processCreateTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_TABLE) {
    return processAlterTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_TABLE) {
    return processDropTable(&pMetaRspObj->metaRsp);
  }
  return NULL;
}

void tmq_free_json_meta(char* jsonMeta) { taosMemoryFreeClear(jsonMeta); }

int32_t tmq_get_raw(TAOS_RES* res, tmq_raw_data* raw) {
  if (!raw || !res) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    raw->raw = pMetaRspObj->metaRsp.metaRsp;
    raw->raw_len = pMetaRspObj->metaRsp.metaRspLen;
    raw->raw_type = pMetaRspObj->metaRsp.resMsgType;
  } else if (TD_RES_TMQ(res)) {
    SMqRspObj* rspObj = ((SMqRspObj*)res);

    int32_t len = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeSMqDataRsp, &rspObj->rsp, len, code);
    if (code < 0) {
      return -1;
    }

    void*    buf = taosMemoryCalloc(1, len);
    SEncoder encoder = {0};
    tEncoderInit(&encoder, buf, len);
    tEncodeSMqDataRsp(&encoder, &rspObj->rsp);
    tEncoderClear(&encoder);

    raw->raw = buf;
    raw->raw_len = len;
    raw->raw_type = RES_TYPE__TMQ;
  } else {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

void tmq_free_raw(tmq_raw_data raw) {
  if (raw.raw_type == RES_TYPE__TMQ) {
    taosMemoryFree(raw.raw);
  }
}

int32_t tmq_write_raw(TAOS* taos, tmq_raw_data raw) {
  if (!taos) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (raw.raw_type == TDMT_VND_CREATE_STB) {
    return taosCreateStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_ALTER_STB) {
    return taosCreateStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DROP_STB) {
    return taosDropStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_CREATE_TABLE) {
    return taosCreateTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_ALTER_TABLE) {
    return taosAlterTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DROP_TABLE) {
    return taosDropTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DELETE) {
    return taosDeleteData(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == RES_TYPE__TMQ) {
    return tmqWriteRaw(taos, raw.raw, raw.raw_len);
  }
  return TSDB_CODE_INVALID_PARA;
}

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb* cb, void* param) {
  //
  tmqCommitInner2(tmq, msg, 0, 1, cb, param);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* msg) {
  //
  return tmqCommitInner2(tmq, msg, 0, 0, NULL, NULL);
}

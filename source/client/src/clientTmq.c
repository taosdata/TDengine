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
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"

#define EMPTY_BLOCK_POLL_IDLE_DURATION  10
#define DEFAULT_AUTO_COMMIT_INTERVAL    5000

struct SMqMgmt {
  int8_t  inited;
  tmr_h   timer;
  int32_t rsetId;
};

static TdThreadOnce   tmqInit = PTHREAD_ONCE_INIT;  // initialize only once
volatile int32_t      tmqInitRes = 0;               // initialize rsp code
static struct SMqMgmt tmqMgmt = {0};

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
  char           clientId[256];
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         autoCommit;
  int8_t         resetOffset;
  int8_t         withTbName;
  int8_t         snapEnable;
  int32_t        snapBatchSize;
  bool           hbBgEnable;
  uint16_t       port;
  int32_t        autoCommitInterval;
  char*          ip;
  char*          user;
  char*          pass;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
};

struct tmq_t {
  int64_t        refId;
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[256];
  int8_t         withTbName;
  int8_t         useSnapshot;
  int8_t         autoCommit;
  int32_t        autoCommitInterval;
  int32_t        resetOffsetCfg;
  uint64_t       consumerId;
  bool           hbBgEnable;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;

  // status
  int8_t  status;
  int32_t epoch;
#if 0
  int8_t  epStatus;
  int32_t epSkipCnt;
#endif
  // poll info
  int64_t       pollCnt;
  int64_t       totalRows;

  // timer
  tmr_h         hbLiveTimer;
  tmr_h         epTimer;
  tmr_h         reportTimer;
  tmr_h         commitTimer;
  STscObj*      pTscObj;       // connection
  SArray*       clientTopics;  // SArray<SMqClientTopic>
  STaosQueue*   mqueue;        // queue of rsp
  STaosQall*    qall;
  STaosQueue*   delayedTask;   // delayed task queue for heartbeat and auto commit
  TdThreadMutex lock;          // used to protect the operation on each topic, when updating the epsets.
  tsem_t        rspSem;
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
  int64_t      pollCnt;
  int64_t      numOfRows;
  STqOffsetVal committedOffset;
  STqOffsetVal currentOffset;
  int32_t      vgId;
  int32_t      vgStatus;
  int32_t      vgSkipCnt;
  int64_t      emptyBlockReceiveTs; // once empty block is received, idle for ignoreCnt then start to poll data
  SEpSet       epSet;
} SMqClientVg;

typedef struct {
  char           topicName[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqClientVg>
  SSchemaWrapper schema;
} SMqClientTopic;

typedef struct {
  int8_t          tmqRspType;
  int32_t         epoch;         // epoch can be used to guard the vgHandle
  int32_t         vgId;
  SMqClientVg*    vgHandle;
  SMqClientTopic* topicHandle;
  uint64_t        reqId;
  SEpSet*         pEpset;
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
  uint64_t        requestId; // request id for debug purpose
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
  char                 topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t              vgId;
  tmq_t*               pTmq;
} SMqCommitCbParam;

static int32_t tmqAskEp(tmq_t* tmq, bool async);
static int32_t makeTopicVgroupKey(char* dst, const char* topicName, int32_t vg);
static int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet);
static int32_t doSendCommitMsg(tmq_t* tmq, SMqClientVg* pVg, const char* pTopicName, SMqCommitCbParamSet* pParamSet,
                               int32_t index, int32_t totalVgroups);
static void tmqCommitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId);

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  if (conf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return conf;
  }

  conf->withTbName = false;
  conf->autoCommit = true;
  conf->autoCommitInterval = DEFAULT_AUTO_COMMIT_INTERVAL;
  conf->resetOffset = TMQ_OFFSET__RESET_EARLIEAST;
  conf->hbBgEnable = true;

  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if (conf) {
    if (conf->ip) {
      taosMemoryFree(conf->ip);
    }
    if (conf->user) {
      taosMemoryFree(conf->user);
    }
    if (conf->pass) {
      taosMemoryFree(conf->pass);
    }
    taosMemoryFree(conf);
  }
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (strcasecmp(key, "group.id") == 0) {
    tstrncpy(conf->groupId, value, TSDB_CGROUP_LEN);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "client.id") == 0) {
    tstrncpy(conf->clientId, value, 256);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "enable.auto.commit") == 0) {
    if (strcasecmp(value, "true") == 0) {
      conf->autoCommit = true;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "false") == 0) {
      conf->autoCommit = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "auto.commit.interval.ms") == 0) {
    conf->autoCommitInterval = taosStr2int64(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "auto.offset.reset") == 0) {
    if (strcasecmp(value, "none") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_NONE;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "earliest") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_EARLIEAST;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "latest") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_LATEST;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "msg.with.table.name") == 0) {
    if (strcasecmp(value, "true") == 0) {
      conf->withTbName = true;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "false") == 0) {
      conf->withTbName = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "experimental.snapshot.enable") == 0) {
    if (strcasecmp(value, "true") == 0) {
      conf->snapEnable = true;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "false") == 0) {
      conf->snapEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "experimental.snapshot.batch.size") == 0) {
    conf->snapBatchSize = taosStr2int64(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "enable.heartbeat.background") == 0) {
    if (strcasecmp(value, "true") == 0) {
      conf->hbBgEnable = true;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "false") == 0) {
      conf->hbBgEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "td.connect.ip") == 0) {
    conf->ip = taosStrdup(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.user") == 0) {
    conf->user = taosStrdup(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.pass") == 0) {
    conf->pass = taosStrdup(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.port") == 0) {
    conf->port = taosStr2int64(value);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.db") == 0) {
    return TMQ_CONF_OK;
  }

  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() {
  return (tmq_list_t*)taosArrayInit(0, sizeof(void*));
}

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) return -1;
  char* topic = taosStrdup(src);
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

static SMqClientVg* foundClientVg(SArray* pTopicList, const char* pName, int32_t vgId, int32_t* index, int32_t* numOfVgroups) {
  int32_t numOfTopics = taosArrayGetSize(pTopicList);
  *index = -1;
  *numOfVgroups = 0;

  for(int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopic = taosArrayGet(pTopicList, i);
    if (strcmp(pTopic->topicName, pName) != 0) {
      continue;
    }

    *numOfVgroups = taosArrayGetSize(pTopic->vgs);
    for (int32_t j = 0; j < (*numOfVgroups); ++j) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
      if (pClientVg->vgId == vgId) {
        *index = j;
        return pClientVg;
      }
    }
  }

  return NULL;
}

// Two problems do not need to be addressed here
// 1. update to of epset. the response of poll request will automatically handle this problem
// 2. commit failure. This one needs to be resolved.
static int32_t tmqCommitCb(void* param, SDataBuf* pBuf, int32_t code) {
  SMqCommitCbParam*    pParam = (SMqCommitCbParam*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;

//  if (code != TSDB_CODE_SUCCESS) { // if commit offset failed, let's try again
//    taosThreadMutexLock(&pParam->pTmq->lock);
//    int32_t numOfVgroups, index;
//    SMqClientVg* pVg = foundClientVg(pParam->pTmq->clientTopics, pParam->topicName, pParam->vgId, &index, &numOfVgroups);
//    if (pVg == NULL) {
//      tscDebug("consumer:0x%" PRIx64
//               " subKey:%s vgId:%d commit failed, code:%s has been transferred to other consumer, no need retry ordinal:%d/%d",
//               pParam->pTmq->consumerId, pParam->pOffset->subKey, pParam->vgId, tstrerror(code), index + 1, numOfVgroups);
//    } else { // let's retry the commit
//      int32_t code1 = doSendCommitMsg(pParam->pTmq, pVg, pParam->topicName, pParamSet, index, numOfVgroups);
//      if (code1 != TSDB_CODE_SUCCESS) {  // retry failed.
//        tscError("consumer:0x%" PRIx64 " topic:%s vgId:%d offset:%" PRId64
//                 " retry failed, ignore this commit. code:%s ordinal:%d/%d",
//                 pParam->pTmq->consumerId, pParam->topicName, pVg->vgId, pVg->committedOffset.version,
//                 tstrerror(terrno), index + 1, numOfVgroups);
//      }
//    }
//
//    taosThreadMutexUnlock(&pParam->pTmq->lock);
//
//    taosMemoryFree(pParam->pOffset);
//    taosMemoryFree(pBuf->pData);
//    taosMemoryFree(pBuf->pEpSet);
//
//    tmqCommitRspCountDown(pParamSet, pParam->pTmq->consumerId, pParam->topicName, pParam->vgId);
//    return 0;
//  }
//
//  // todo replace the pTmq with refId

  taosMemoryFree(pParam->pOffset);
  taosMemoryFree(pBuf->pData);
  taosMemoryFree(pBuf->pEpSet);

  tmqCommitRspCountDown(pParamSet, pParam->pTmq->consumerId, pParam->topicName, pParam->vgId);
  return 0;
}

static int32_t doSendCommitMsg(tmq_t* tmq, SMqClientVg* pVg, const char* pTopicName, SMqCommitCbParamSet* pParamSet,
                               int32_t index, int32_t totalVgroups) {
  STqOffset* pOffset = taosMemoryCalloc(1, sizeof(STqOffset));
  if (pOffset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pOffset->val = pVg->currentOffset;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset->subKey, tmq->groupId, groupLen);
  pOffset->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset->subKey + groupLen + 1, pTopicName);

  int32_t len = 0;
  int32_t code = 0;
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
  pParam->vgId = pVg->vgId;
  pParam->pTmq = tmq;

  tstrncpy(pParam->topicName, pTopicName, tListLen(pParam->topicName));

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

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = TDMT_VND_TMQ_COMMIT_OFFSET;

  atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  atomic_add_fetch_32(&pParamSet->totalRspNum, 1);

  SEp* pEp = GET_ACTIVE_EP(&pVg->epSet);
  tscDebug("consumer:0x%" PRIx64 " topic:%s on vgId:%d send offset:%" PRId64 " prev:%" PRId64
           ", ep:%s:%d, ordinal:%d/%d, req:0x%" PRIx64,
           tmq->consumerId, pOffset->subKey, pVg->vgId, pOffset->val.version, pVg->committedOffset.version, pEp->fqdn,
           pEp->port, index + 1, totalVgroups, pMsgSendInfo->requestId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, pMsgSendInfo);
  return 0;
}

static int32_t tmqCommitMsgImpl(tmq_t* tmq, const TAOS_RES* msg, int8_t async, tmq_commit_cb* userCb, void* userParam) {
  char*   topic;
  int32_t vgId;
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

  taosThreadMutexLock(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);

  tscDebug("consumer:0x%" PRIx64 " user invoked commit offset for %d", tmq->consumerId, numOfTopics);
  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopic->topicName, topic) != 0) {
      continue;
    }

    int32_t numOfVgroups = taosArrayGetSize(pTopic->vgs);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg->vgId != vgId) {
        continue;
      }

      if (pVg->currentOffset.type > 0 && !tOffsetEqual(&pVg->currentOffset, &pVg->committedOffset)) {
        if (doSendCommitMsg(tmq, pVg, pTopic->topicName, pParamSet, j, numOfVgroups) < 0) {
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
    taosThreadMutexUnlock(&tmq->lock);
    return 0;
  }

  if (!async) {
    taosThreadMutexUnlock(&tmq->lock);
    tsem_wait(&pParamSet->rspSem);
    code = pParamSet->rspErr;
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return code;
  } else {
    code = 0;
  }

FAIL:
  taosThreadMutexUnlock(&tmq->lock);
  if (code != 0 && async) {
    userCb(tmq, code, userParam);
  }

  return 0;
}

static int32_t doAutoCommit(tmq_t* tmq, int8_t automatic, int8_t async, tmq_commit_cb* userCb, void* userParam) {
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

  taosThreadMutexLock(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t         numOfVgroups = taosArrayGetSize(pTopic->vgs);

    tscDebug("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName,
             numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

      if (pVg->currentOffset.type > 0 && !tOffsetEqual(&pVg->currentOffset, &pVg->committedOffset)) {
        code = doSendCommitMsg(tmq, pVg, pTopic->topicName, pParamSet, j, numOfVgroups);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("consumer:0x%" PRIx64 " topic:%s vgId:%d offset:%" PRId64 " failed, code:%s ordinal:%d/%d",
                   tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->committedOffset.version, tstrerror(terrno),
                   j + 1, numOfVgroups);
          continue;
        }

        // update the offset value.
        pVg->committedOffset = pVg->currentOffset;
      } else {
        tscDebug("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, current:%" PRId64 ", ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->currentOffset.version, j + 1, numOfVgroups);
      }
    }
  }

  tscDebug("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - 1,
           numOfTopics);
  taosThreadMutexUnlock(&tmq->lock);

  // no request is sent
  if (pParamSet->totalRspNum == 0) {
    tsem_destroy(&pParamSet->rspSem);
    taosMemoryFree(pParamSet);
    return 0;
  }

  // count down since waiting rsp num init as 1
  tmqCommitRspCountDown(pParamSet, tmq->consumerId, "", 0);

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

static int32_t tmqCommitInner(tmq_t* tmq, const TAOS_RES* msg, int8_t automatic, int8_t async, tmq_commit_cb* userCb,
                              void* userParam) {
  if (msg) { // user invoked commit
    return tmqCommitMsgImpl(tmq, msg, async, userCb, userParam);
  } else {  // this for auto commit
    return doAutoCommit(tmq, automatic, async, userCb, userParam);
  }
}

static void generateTimedTask(int64_t refId, int32_t type) {
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
    *pTaskType = type;
    taosWriteQitem(tmq->delayedTask, pTaskType);
    tsem_post(&tmq->rspSem);
  }
  taosReleaseRef(tmqMgmt.rsetId, refId);
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__ASK_EP);
  taosMemoryFree(param);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__COMMIT);
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

  taosReleaseRef(tmqMgmt.rsetId, refId);
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
    goto OVER;
  }

  void* pReq = taosMemoryCalloc(1, tlen);
  if (tlen < 0) {
    tscError("failed to malloc MqHbReq msg, size:%d", tlen);
    goto OVER;
  }

  if (tSerializeSMqHbReq(pReq, tlen, &req) < 0) {
    tscError("tSerializeSMqHbReq %d failed", tlen);
    taosMemoryFree(pReq);
    goto OVER;
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
  taosReleaseRef(tmqMgmt.rsetId, refId);
}

int32_t tmqHandleAllDelayedTask(tmq_t* pTmq) {
  STaosQall* qall = taosAllocateQall();
  taosReadAllQitems(pTmq->delayedTask, qall);

  if (qall->numOfItems == 0) {
    taosFreeQall(qall);
    return TSDB_CODE_SUCCESS;
  }

  tscDebug("consumer:0x%" PRIx64 " handle delayed %d tasks before poll data", pTmq->consumerId, qall->numOfItems);
  int8_t* pTaskType = NULL;
  taosGetQitem(qall, (void**)&pTaskType);

  while (pTaskType != NULL) {
    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      tmqAskEp(pTmq, true);

      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = pTmq->refId;

      tscDebug("consumer:0x%" PRIx64 " retrieve ep from mnode in 1s", pTmq->consumerId);
      taosTmrReset(tmqAssignAskEpTask, 1000, pRefId, tmqMgmt.timer, &pTmq->epTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmqCommitInner(pTmq, NULL, 1, 1, pTmq->commitCb, pTmq->commitCbUserParam);

      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = pTmq->refId;

      tscDebug("consumer:0x%" PRIx64 " commit to vnode(s) in %.2fs", pTmq->consumerId,
               pTmq->autoCommitInterval / 1000.0);
      taosTmrReset(tmqAssignDelayedCommitTask, pTmq->autoCommitInterval, pRefId, tmqMgmt.timer, &pTmq->commitTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__REPORT) {
    }

    taosFreeQitem(pTaskType);
    taosGetQitem(qall, (void**)&pTaskType);
  }

  taosFreeQall(qall);
  return 0;
}

static void* tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
    // do nothing
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)rspWrapper;
    tDeleteSMqAskEpRsp(&pEpRspWrapper->msg);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);

    taosArrayDestroyP(pRsp->dataRsp.blockData, taosMemoryFree);
    taosArrayDestroy(pRsp->dataRsp.blockDataLen);
    taosArrayDestroyP(pRsp->dataRsp.blockTbName, taosMemoryFree);
    taosArrayDestroyP(pRsp->dataRsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);

    taosMemoryFree(pRsp->metaRsp.metaRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__TAOSX_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);

    taosArrayDestroyP(pRsp->taosxRsp.blockData, taosMemoryFree);
    taosArrayDestroy(pRsp->taosxRsp.blockDataLen);
    taosArrayDestroyP(pRsp->taosxRsp.blockTbName, taosMemoryFree);
    taosArrayDestroyP(pRsp->taosxRsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
    // taosx
    taosArrayDestroy(pRsp->taosxRsp.createTableLen);
    taosArrayDestroyP(pRsp->taosxRsp.createTableReq, taosMemoryFree);
  }

  return NULL;
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

static void freeClientVgImpl(void* param) {
  SMqClientTopic* pTopic = param;
  taosMemoryFreeClear(pTopic->schema.pSchema);
  taosArrayDestroy(pTopic->vgs);
}

void tmqFreeImpl(void* handle) {
  tmq_t*  tmq = (tmq_t*)handle;
  int64_t id = tmq->consumerId;

  // TODO stop timer
  if (tmq->mqueue) {
    tmqClearUnhandleMsg(tmq);
    taosCloseQueue(tmq->mqueue);
  }

  if (tmq->delayedTask) {
    taosCloseQueue(tmq->delayedTask);
  }

  taosFreeQall(tmq->qall);
  tsem_destroy(&tmq->rspSem);
  taosThreadMutexDestroy(&tmq->lock);

  taosArrayDestroyEx(tmq->clientTopics, freeClientVgImpl);
  taos_close_internal(tmq->pTscObj);
  taosMemoryFree(tmq);

  tscDebug("consumer:0x%" PRIx64 " closed", id);
}

static void tmqMgmtInit(void) {
  tmqInitRes = 0;
  tmqMgmt.timer = taosTmrInit(1000, 100, 360000, "TMQ");

  if (tmqMgmt.timer == NULL) {
    tmqInitRes = TSDB_CODE_OUT_OF_MEMORY;
  }

  tmqMgmt.rsetId = taosOpenRef(10000, tmqFreeImpl);
  if (tmqMgmt.rsetId < 0) {
    tmqInitRes = terrno;
  }
}

tmq_t* tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  taosThreadOnce(&tmqInit, tmqMgmtInit);
  if (tmqInitRes != 0) {
    terrno = tmqInitRes;
    return NULL;
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("failed to create consumer, groupId:%s, code:%s", conf->groupId, terrstr());
    return NULL;
  }

  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  pTmq->mqueue = taosOpenQueue();
  pTmq->delayedTask = taosOpenQueue();
  pTmq->qall = taosAllocateQall();

  taosThreadMutexInit(&pTmq->lock, NULL);
  if (pTmq->clientTopics == NULL || pTmq->mqueue == NULL || pTmq->qall == NULL || pTmq->delayedTask == NULL ||
      conf->groupId[0] == 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
    goto _failed;
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
    tscError("consumer:0x %" PRIx64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
    goto _failed;
  }

  // init connection
  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
    tsem_destroy(&pTmq->rspSem);
    goto _failed;
  }

  pTmq->refId = taosAddRef(tmqMgmt.rsetId, pTmq);
  if (pTmq->refId < 0) {
    goto _failed;
  }

  if (pTmq->hbBgEnable) {
    int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
    *pRefId = pTmq->refId;
    pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, 1000, pRefId, tmqMgmt.timer);
  }

  char         buf[80] = {0};
  STqOffsetVal offset = {.type = pTmq->resetOffsetCfg};
  tFormatOffset(buf, tListLen(buf), &offset);
  tscInfo("consumer:0x%" PRIx64 " is setup, refId:%"PRId64", groupId:%s, snapshot:%d, autoCommit:%d, commitInterval:%dms, offset:%s, backgroudHB:%d",
          pTmq->consumerId, pTmq->refId, pTmq->groupId, pTmq->useSnapshot, pTmq->autoCommit, pTmq->autoCommitInterval, buf,
          pTmq->hbBgEnable);

  return pTmq;

_failed:
  tmqFreeImpl(pTmq);
  return NULL;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = 0;

  tscDebug("consumer:0x%" PRIx64 " cgroup:%s, subscribe %d topics", tmq->consumerId, tmq->groupId, sz);

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, 256);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  req.topicNames = taosArrayInit(sz, sizeof(void*));

  if (req.topicNames == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(container, i);

    SName name = {0};
    tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));
    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      goto FAIL;
    }

    tNameExtractFullName(&name, topicFName);
    tscDebug("consumer:0x%" PRIx64 " subscribe topic:%s", tmq->consumerId, topicFName);

    taosArrayPush(req.topicNames, &topicFName);
  }

  int32_t tlen = tSerializeSCMSubscribeReq(NULL, &req);

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  void* abuf = buf;
  tSerializeSCMSubscribeReq(&abuf, &req);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  SMqSubscribeCbParam param = {
      .rspErr = 0,
      .refId = tmq->refId,
      .epoch = tmq->epoch,
  };

  if (tsem_init(&param.rspSem, 0, 0) != 0) {
    goto FAIL;
  }

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

  if (param.rspErr != 0) {
    code = param.rspErr;
    goto FAIL;
  }

  int32_t retryCnt = 0;
  while (TSDB_CODE_MND_CONSUMER_NOT_READY == tmqAskEp(tmq, false)) {
    if (retryCnt++ > 40) {
      goto FAIL;
    }

    tscDebug("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
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

FAIL:
  taosArrayDestroyP(req.topicNames, taosMemoryFree);
  taosMemoryFree(buf);
  taosMemoryFree(sendInfo);

  return code;
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;

  int64_t         refId = pParam->refId;
  SMqClientVg*    pVg = pParam->pVg;
  SMqClientTopic* pTopic = pParam->pTopic;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  int32_t  epoch = pParam->epoch;
  int32_t  vgId = pParam->vgId;
  uint64_t requestId = pParam->requestId;

  taosMemoryFree(pParam);

  if (code != 0) {
    tscWarn("consumer:0x%" PRIx64 " msg from vgId:%d discarded, epoch %d, since %s, reqId:0x%" PRIx64, tmq->consumerId,
            vgId, epoch, tstrerror(code), requestId);

    if (pMsg->pData) taosMemoryFree(pMsg->pData);
    if (pMsg->pEpSet) taosMemoryFree(pMsg->pEpSet);

    // in case of consumer mismatch, wait for 500ms and retry
    if (code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
      taosMsleep(500);
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__RECOVER);
      tscDebug("consumer:0x%" PRIx64" wait for the re-balance, wait for 500ms and set status to be RECOVER", tmq->consumerId);
    } else if (code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
      if (pRspWrapper == NULL) {
        tscWarn("consumer:0x%" PRIx64 " msg from vgId:%d discarded, epoch %d since out of memory, reqId:0x%" PRIx64,
                tmq->consumerId, vgId, epoch, requestId);
        goto CREATE_MSG_FAIL;
      }

      pRspWrapper->tmqRspType = TMQ_MSG_TYPE__END_RSP;
      taosWriteQitem(tmq->mqueue, pRspWrapper);
    }

    goto CREATE_MSG_FAIL;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t tmqEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < tmqEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("consumer:0x%" PRIx64 " msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d, reqId:0x%"PRIx64,
            tmq->consumerId, vgId, msgEpoch, tmqEpoch, requestId);

    tsem_post(&tmq->rspSem);
    taosReleaseRef(tmqMgmt.rsetId, refId);

    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("consumer:0x%" PRIx64 " mismatch rsp from vgId:%d, epoch %d, current epoch %d, reqId:0x%" PRIx64,
            tmq->consumerId, vgId, msgEpoch, tmqEpoch, requestId);
  }

  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
  if (pRspWrapper == NULL) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tscWarn("consumer:0x%"PRIx64" msg discard from vgId:%d, epoch %d since out of memory", tmq->consumerId, vgId, epoch);
    goto CREATE_MSG_FAIL;
  }

  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->vgHandle = pVg;
  pRspWrapper->topicHandle = pTopic;
  pRspWrapper->reqId = requestId;
  pRspWrapper->pEpset = pMsg->pEpSet;
  pRspWrapper->vgId = pVg->vgId;

  pMsg->pEpSet = NULL;
  if (rspType == TMQ_MSG_TYPE__POLL_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSMqDataRsp(&decoder, &pRspWrapper->dataRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));

    char buf[80];
    tFormatOffset(buf, 80, &pRspWrapper->dataRsp.rspOffset);
    tscDebug("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, req:%" PRId64 ", rsp:%s type %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, pRspWrapper->dataRsp.reqOffset.version, buf, rspType, requestId);
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
  } else { // invalid rspType
    tscError("consumer:0x%"PRIx64" invalid rsp msg received, type:%d ignored", tmq->consumerId, rspType);
  }

  taosMemoryFree(pMsg->pData);
  taosWriteQitem(tmq->mqueue, pRspWrapper);

  tscDebug("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d, vgId:%d, total in queue:%d, reqId:0x%" PRIx64,
           tmq->consumerId, rspType, vgId, tmq->mqueue->numOfItems, requestId);

  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);

  return 0;

CREATE_MSG_FAIL:
  if (epoch == tmq->epoch) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }

  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);

  return -1;
}

typedef struct SVgroupSaveInfo {
  STqOffsetVal offset;
  int64_t      numOfRows;
} SVgroupSaveInfo;

static void initClientTopicFromRsp(SMqClientTopic* pTopic, SMqSubTopicEp* pTopicEp, SHashObj* pVgOffsetHashMap,
                                   tmq_t* tmq) {
  pTopic->schema = pTopicEp->schema;
  pTopicEp->schema.nCols = 0;
  pTopicEp->schema.pSchema = NULL;

  char vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);

  tstrncpy(pTopic->topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pTopic->db, pTopicEp->db, TSDB_DB_FNAME_LEN);

  tscDebug("consumer:0x%" PRIx64 ", update topic:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName, vgNumGet);
  pTopic->vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));

  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);

    makeTopicVgroupKey(vgKey, pTopic->topicName, pVgEp->vgId);
    SVgroupSaveInfo* pInfo = taosHashGet(pVgOffsetHashMap, vgKey, strlen(vgKey));

    int64_t numOfRows = 0;
    STqOffsetVal  offsetNew = {.type = tmq->resetOffsetCfg};
    if (pInfo != NULL) {
      offsetNew = pInfo->offset;
      numOfRows = pInfo->numOfRows;
    }

    SMqClientVg clientVg = {
        .pollCnt = 0,
        .currentOffset = offsetNew,
        .vgId = pVgEp->vgId,
        .epSet = pVgEp->epSet,
        .vgStatus = TMQ_VG_STATUS__IDLE,
        .vgSkipCnt = 0,
        .emptyBlockReceiveTs = 0,
        .numOfRows = numOfRows,
    };

    taosArrayPush(pTopic->vgs, &clientVg);
  }
}

static void freeClientVgInfo(void* param) {
  SMqClientTopic* pTopic = param;
  if (pTopic->schema.nCols) {
    taosMemoryFreeClear(pTopic->schema.pSchema);
  }

  taosArrayDestroy(pTopic->vgs);
}

static bool tmqUpdateEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  bool set = false;

  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);

  char vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  tscDebug("consumer:0x%" PRIx64 " update ep epoch from %d to epoch %d, incoming topics:%d, existed topics:%d",
           tmq->consumerId, tmq->epoch, epoch, topicNumGet, topicNumCur);

  SArray* newTopics = taosArrayInit(topicNumGet, sizeof(SMqClientTopic));
  if (newTopics == NULL) {
    return false;
  }

  SHashObj* pVgOffsetHashMap = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pVgOffsetHashMap == NULL) {
    taosArrayDestroy(newTopics);
    return false;
  }

  // todo extract method
  for (int32_t i = 0; i < topicNumCur; i++) {
    // find old topic
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tscDebug("consumer:0x%" PRIx64 ", new vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        makeTopicVgroupKey(vgKey, pTopicCur->topicName, pVgCur->vgId);

        char buf[80];
        tFormatOffset(buf, 80, &pVgCur->currentOffset);
        tscDebug("consumer:0x%" PRIx64 ", epoch:%d vgId:%d vgKey:%s, offset:%s", tmq->consumerId, epoch,
                 pVgCur->vgId, vgKey, buf);

        SVgroupSaveInfo info = {.offset = pVgCur->currentOffset, .numOfRows = pVgCur->numOfRows};
        taosHashPut(pVgOffsetHashMap, vgKey, strlen(vgKey), &info, sizeof(SVgroupSaveInfo));
      }
    }
  }

  for (int32_t i = 0; i < topicNumGet; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    initClientTopicFromRsp(&topic, pTopicEp, pVgOffsetHashMap, tmq);
    taosArrayPush(newTopics, &topic);
  }

  taosHashCleanup(pVgOffsetHashMap);

  taosThreadMutexLock(&tmq->lock);
  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientVgInfo);
  }

  tmq->clientTopics = newTopics;
  taosThreadMutexUnlock(&tmq->lock);

  int8_t flag = (topicNumGet == 0)? TMQ_CONSUMER_STATUS__NO_TOPIC:TMQ_CONSUMER_STATUS__READY;
  atomic_store_8(&tmq->status, flag);
  atomic_store_32(&tmq->epoch, epoch);

  tscDebug("consumer:0x%" PRIx64 " update topic info completed", tmq->consumerId);
  return set;
}

static int32_t tmqAskEpCb(void* param, SDataBuf* pMsg, int32_t code) {
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
  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 ", get topic endpoint error, async:%d, code:%s", tmq->consumerId, pParam->async,
             tstrerror(code));
    goto END;
  }

  // tmq's epoch is monotonically increase,
  // so it's safe to discard any old epoch msg.
  // Epoch will only increase when received newer epoch ep msg
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  if (head->epoch <= epoch) {
    tscDebug("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d, no need to update local ep",
             tmq->consumerId, head->epoch, epoch);
    if (tmq->status == TMQ_CONSUMER_STATUS__RECOVER) {
      SMqAskEpRsp rsp;
      tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
      int8_t flag = (taosArrayGetSize(rsp.topics) == 0) ? TMQ_CONSUMER_STATUS__NO_TOPIC : TMQ_CONSUMER_STATUS__READY;
      atomic_store_8(&tmq->status, flag);
      tDeleteSMqAskEpRsp(&rsp);
    }

    goto END;
  }

  tscDebug("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d, update local ep", tmq->consumerId,
           head->epoch, epoch);

  if (!async) {
    SMqAskEpRsp rsp;
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
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
  taosReleaseRef(tmqMgmt.rsetId, pParam->refId);

  if (!async) {
    tsem_post(&pParam->rspSem);
  } else {
    taosMemoryFree(pParam);
  }

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  return code;
}

void tmqBuildConsumeReqImpl(SMqPollReq* pReq, tmq_t* tmq, int64_t timeout, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pReq->subKey, tmq->groupId, groupLen);
  pReq->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pReq->subKey + groupLen + 1, pTopic->topicName);

  pReq->withTbName = tmq->withTbName;
  pReq->consumerId = tmq->consumerId;
  pReq->timeout = timeout;
  pReq->epoch = tmq->epoch;
  /*pReq->currentOffset = reqOffset;*/
  pReq->reqOffset = pVg->currentOffset;
  pReq->head.vgId = pVg->vgId;
  pReq->useSnapshot = tmq->useSnapshot;
  pReq->reqId = generateRequestId();
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

SMqRspObj* tmqBuildRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows) {
  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  pRspObj->resType = RES_TYPE__TMQ;

  (*numOfRows) = 0;
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

  // extract the rows in this data packet
  for(int32_t i = 0; i < pRspObj->rsp.blockNum; ++i) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(pRspObj->rsp.blockData, i);
    int64_t rows = htobe64(pRetrieve->numOfRows);
    pVg->numOfRows += rows;
    (*numOfRows) += rows;
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

static int32_t handleErrorBeforePoll(SMqClientVg* pVg, tmq_t* pTmq) {
  atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  tsem_post(&pTmq->rspSem);
  return -1;
}

static int32_t doTmqPollImpl(tmq_t* pTmq, SMqClientTopic* pTopic, SMqClientVg* pVg, int64_t timeout) {
  SMqPollReq req = {0};
  tmqBuildConsumeReqImpl(&req, pTmq, timeout, pTopic, pVg);

  int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
  if (msgSize < 0) {
    return handleErrorBeforePoll(pVg, pTmq);
  }

  char* msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return handleErrorBeforePoll(pVg, pTmq);
  }

  if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
    taosMemoryFree(msg);
    return handleErrorBeforePoll(pVg, pTmq);
  }

  SMqPollCbParam* pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
  if (pParam == NULL) {
    taosMemoryFree(msg);
    return handleErrorBeforePoll(pVg, pTmq);
  }

  pParam->refId = pTmq->refId;
  pParam->epoch = pTmq->epoch;
  pParam->pVg = pVg;  // pVg may be released,fix it
  pParam->pTopic = pTopic;
  pParam->vgId = pVg->vgId;
  pParam->requestId = req.reqId;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pParam);
    taosMemoryFree(msg);
    return handleErrorBeforePoll(pVg, pTmq);
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
  char    offsetFormatBuf[80];
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->currentOffset);

  tscDebug("consumer:0x%" PRIx64 " send poll to %s vgId:%d, epoch %d, req:%s, reqId:0x%" PRIx64,
           pTmq->consumerId, pTopic->topicName, pVg->vgId, pTmq->epoch, offsetFormatBuf, req.reqId);
  asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);

  pVg->pollCnt++;
  pTmq->pollCnt++;

  return TSDB_CODE_SUCCESS;
}

// broadcast the poll request to all related vnodes
static int32_t tmqPollImpl(tmq_t* tmq, int64_t timeout) {
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to poll data, numOfTopics:%d", tmq->consumerId, numOfTopics);

  for (int i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t         numOfVg = taosArrayGetSize(pTopic->vgs);

    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (taosGetTimestampMs() - pVg->emptyBlockReceiveTs < EMPTY_BLOCK_POLL_IDLE_DURATION) { // less than 100ms
        tscTrace("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for 10ms before start next poll", tmq->consumerId, tmq->epoch,
                 pVg->vgId);
        continue;
      }

      int32_t vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus == TMQ_VG_STATUS__WAIT) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tscTrace("consumer:0x%" PRIx64 " epoch %d wait poll-rsp, skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch,
                 pVg->vgId, vgSkipCnt);
        continue;
#if 0
        if (skipCnt < 30000) {
          continue;
        } else {
        tscDebug("consumer:0x%" PRIx64 ",skip vgId:%d skip too much reset", tmq->consumerId, pVg->vgId);
        }
#endif
      }

      atomic_store_32(&pVg->vgSkipCnt, 0);
      int32_t code = doTmqPollImpl(tmq, pTopic, pVg, timeout);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  tscDebug("consumer:0x%" PRIx64 " end to poll data", tmq->consumerId);
  return 0;
}

static int32_t tmqHandleNoPollRsp(tmq_t* tmq, SMqRspWrapper* rspWrapper, bool* pReset) {
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

static void* tmqHandleAllRsp(tmq_t* tmq, int64_t timeout, bool pollIfReset) {
  tscDebug("consumer:0x%" PRIx64 " start to handle the rsp, total:%d", tmq->consumerId, tmq->qall->numOfItems);

  while (1) {
    SMqRspWrapper* pRspWrapper = NULL;
    taosGetQitem(tmq->qall, (void**)&pRspWrapper);

    if (pRspWrapper == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&pRspWrapper);

      if (pRspWrapper == NULL) {
        return NULL;
      }
    }

    tscDebug("consumer:0x%"PRIx64" handle rsp, type:%d", tmq->consumerId, pRspWrapper->tmqRspType);

    if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
      taosFreeQitem(pRspWrapper);
      terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
      tscError("consumer:0x%" PRIx64 " unexpected rsp from poll, code:%s", tmq->consumerId, tstrerror(terrno));
      return NULL;
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;

      int32_t consumerEpoch = atomic_load_32(&tmq->epoch);
      SMqDataRsp* pDataRsp = &pollRspWrapper->dataRsp;

      if (pDataRsp->head.epoch == consumerEpoch) {
        // todo fix it: race condition
        SMqClientVg* pVg = pollRspWrapper->vgHandle;

        // update the epset
        if (pollRspWrapper->pEpset != NULL) {
          SEp* pEp = GET_ACTIVE_EP(pollRspWrapper->pEpset);
          SEp* pOld = GET_ACTIVE_EP(&(pVg->epSet));
          tscDebug("consumer:0x%" PRIx64 " update epset vgId:%d, ep:%s:%d, old ep:%s:%d", tmq->consumerId,
                   pVg->vgId, pEp->fqdn, pEp->port, pOld->fqdn, pOld->port);
          pVg->epSet = *pollRspWrapper->pEpset;
        }

        pVg->currentOffset = pDataRsp->rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

        char buf[80];
        tFormatOffset(buf, 80, &pDataRsp->rspOffset);
        if (pDataRsp->blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " empty block received, vgId:%d, offset:%s, vg total:%"PRId64" total:%"PRId64" reqId:0x%" PRIx64, tmq->consumerId,
                   pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
          pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pollRspWrapper);
        } else {  // build rsp
          int64_t numOfRows = 0;
          SMqRspObj* pRsp = tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
          tmq->totalRows += numOfRows;

          tscDebug("consumer:0x%" PRIx64 " process poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                   " vg total:%" PRId64 " total:%" PRId64 ", reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pDataRsp->blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
                   pollRspWrapper->reqId);
          taosFreeQitem(pollRspWrapper);
          return pRsp;
        }
      } else {
        tscDebug("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 tmq->consumerId, pollRspWrapper->vgId, pDataRsp->head.epoch, consumerEpoch);
        pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      tscDebug("consumer:0x%" PRIx64 " process meta rsp", tmq->consumerId);

      if (pollRspWrapper->metaRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        pVg->currentOffset = pollRspWrapper->metaRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
        // build rsp
        SMqMetaRspObj* pRsp = tmqBuildMetaRspFromWrapper(pollRspWrapper);
        taosFreeQitem(pollRspWrapper);
        return pRsp;
      } else {
        tscDebug("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 tmq->consumerId, pollRspWrapper->vgId, pollRspWrapper->metaRsp.head.epoch, consumerEpoch);
        pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__TAOSX_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t consumerEpoch = atomic_load_32(&tmq->epoch);

      if (pollRspWrapper->taosxRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = pollRspWrapper->vgHandle;
        pVg->currentOffset = pollRspWrapper->taosxRsp.rspOffset;
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

        if (pollRspWrapper->taosxRsp.blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " taosx empty block received, vgId:%d, vg total:%" PRId64 " reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, pVg->numOfRows, pollRspWrapper->reqId);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pollRspWrapper);
          continue;
        } else {
          pVg->emptyBlockReceiveTs = 0; // reset the ts
        }

        // build rsp
        void* pRsp = NULL;
        int64_t numOfRows = 0;
        if (pollRspWrapper->taosxRsp.createTableNum == 0) {
          pRsp = tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
        } else {
          pRsp = tmqBuildTaosxRspFromWrapper(pollRspWrapper);
        }

        tmq->totalRows += numOfRows;

        char buf[80];
        tFormatOffset(buf, 80, &pVg->currentOffset);
        tscDebug("consumer:0x%" PRIx64 " process taosx poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                 ", vg total:%" PRId64 " total:%"PRId64" reqId:0x%" PRIx64,
                 tmq->consumerId, pVg->vgId, buf, pollRspWrapper->dataRsp.blockNum, numOfRows, pVg->numOfRows,
                 tmq->totalRows, pollRspWrapper->reqId);

        taosFreeQitem(pollRspWrapper);
        return pRsp;

      } else {
        tscDebug("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                 tmq->consumerId, pollRspWrapper->vgId, pollRspWrapper->taosxRsp.head.epoch, consumerEpoch);
        pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pollRspWrapper);
      }
    } else {
      tscDebug("consumer:0x%" PRIx64 " not data msg received", tmq->consumerId);

      bool reset = false;
      tmqHandleNoPollRsp(tmq, pRspWrapper, &reset);
      taosFreeQitem(pRspWrapper);
      if (pollIfReset && reset) {
        tscDebug("consumer:0x%" PRIx64 ", reset and repoll", tmq->consumerId);
        tmqPollImpl(tmq, timeout);
      }
    }
  }
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t timeout) {
  void*   rspObj;
  int64_t startTime = taosGetTimestampMs();

  tscDebug("consumer:0x%" PRIx64 " start to poll at %"PRId64", timeout:%" PRId64, tmq->consumerId, startTime, timeout);

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
    tscDebug("consumer:0x%" PRIx64 " poll return since consumer is init", tmq->consumerId);
    taosMsleep(500);  //     sleep for a while
    return NULL;
  }

  while (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER) {
    int32_t retryCnt = 0;
    while (TSDB_CODE_MND_CONSUMER_NOT_READY == tmqAskEp(tmq, false)) {
      if (retryCnt++ > 40) {
        return NULL;
      }

      tscDebug("consumer:0x%" PRIx64 " not ready, retry:%d/40 in 500ms", tmq->consumerId, retryCnt);
      taosMsleep(500);
    }
  }

  while (1) {
    tmqHandleAllDelayedTask(tmq);

    if (tmqPollImpl(tmq, timeout) < 0) {
      tscDebug("consumer:0x%" PRIx64 " return due to poll error", tmq->consumerId);
    }

    rspObj = tmqHandleAllRsp(tmq, timeout, false);
    if (rspObj) {
      tscDebug("consumer:0x%" PRIx64 " return rsp %p", tmq->consumerId, rspObj);
      return (TAOS_RES*)rspObj;
    } else if (terrno == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      tscDebug("consumer:0x%" PRIx64 " return null since no committed offset", tmq->consumerId);
      return NULL;
    }

    if (timeout >= 0) {
      int64_t currentTime = taosGetTimestampMs();
      int64_t elapsedTime = currentTime - startTime;
      if (elapsedTime > timeout) {
        tscDebug("consumer:0x%" PRIx64 " (epoch %d) timeout, no rsp, start time %" PRId64 ", current time %" PRId64,
                 tmq->consumerId, tmq->epoch, startTime, currentTime);
        return NULL;
      }
      tsem_timewait(&tmq->rspSem, (timeout - elapsedTime));
    } else {
      // use tsem_timewait instead of tsem_wait to avoid unexpected stuck
      tsem_timewait(&tmq->rspSem, 1000);
    }
  }
}

static void displayConsumeStatistics(const tmq_t* pTmq) {
  int32_t numOfTopics = taosArrayGetSize(pTmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " closing poll:%" PRId64 " rows:%" PRId64 " topics:%d, final epoch:%d",
           pTmq->consumerId, pTmq->pollCnt, pTmq->totalRows, numOfTopics, pTmq->epoch);

  tscDebug("consumer:0x%" PRIx64 " rows dist begin: ", pTmq->consumerId);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopics = taosArrayGet(pTmq->clientTopics, i);

    tscDebug("consumer:0x%" PRIx64 " topic:%d", pTmq->consumerId, i);
    int32_t numOfVgs = taosArrayGetSize(pTopics->vgs);
    for (int32_t j = 0; j < numOfVgs; ++j) {
      SMqClientVg* pVg = taosArrayGet(pTopics->vgs, j);
      tscDebug("topic:%s, %d. vgId:%d rows:%" PRId64, pTopics->topicName, j, pVg->vgId, pVg->numOfRows);
    }
  }

  tscDebug("consumer:0x%" PRIx64 " rows dist end", pTmq->consumerId);
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  tscDebug("consumer:0x%" PRIx64" start to close consumer, status:%d", tmq->consumerId, tmq->status);
  displayConsumeStatistics(tmq);

  if (tmq->status == TMQ_CONSUMER_STATUS__READY) {
    // if auto commit is set, commit before close consumer. Otherwise, do nothing.
    if (tmq->autoCommit) {
      int32_t rsp = tmq_commit_sync(tmq, NULL);
      if (rsp != 0) {
        return rsp;
      }
    }

    int32_t     retryCnt = 0;
    tmq_list_t* lst = tmq_list_new();
    while (1) {
      int32_t rsp = tmq_subscribe(tmq, lst);
      if (rsp != TSDB_CODE_MND_CONSUMER_NOT_READY || retryCnt > 5) {
        break;
      } else {
        retryCnt++;
        taosMsleep(500);
      }
    }

    tmq_list_destroy(lst);
  } else {
    tscWarn("consumer:0x%" PRIx64" not in ready state, close it directly", tmq->consumerId);
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
  tmqCommitInner(tmq, msg, 0, 1, cb, param);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* msg) {
  return tmqCommitInner(tmq, msg, 0, 0, NULL, NULL);
}

int32_t tmqAskEp(tmq_t* tmq, bool async) {
  int32_t code = TSDB_CODE_SUCCESS;
#if 0
  int8_t  epStatus = atomic_val_compare_exchange_8(&tmq->epStatus, 0, 1);
  if (epStatus == 1) {
    int32_t epSkipCnt = atomic_add_fetch_32(&tmq->epSkipCnt, 1);
    tscTrace("consumer:0x%" PRIx64 ", skip ask ep cnt %d", tmq->consumerId, epSkipCnt);
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
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq failed", tmq->consumerId);
    return -1;
  }

  void* pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc askEpReq msg, size:%d", tmq->consumerId, tlen);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq %d failed", tmq->consumerId, tlen);
    taosMemoryFree(pReq);
    return -1;
  }

  SMqAskEpCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc subscribe param", tmq->consumerId);
    taosMemoryFree(pReq);
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
  tscDebug("consumer:0x%" PRIx64 " ask ep from mnode, async:%d, reqId:0x%" PRIx64, tmq->consumerId, async,
           sendInfo->requestId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
    tsem_wait(&pParam->rspSem);
    code = pParam->code;
    taosMemoryFree(pParam);
  }

  return code;
}

int32_t makeTopicVgroupKey(char* dst, const char* topicName, int32_t vg) {
  return sprintf(dst, "%s:%d", topicName, vg);
}

int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  int64_t refId = pParamSet->refId;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
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
    } else if (!pParamSet->automatic && pParamSet->userCb) { // sem post
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

  taosReleaseRef(tmqMgmt.rsetId, refId);
  return 0;
}

void tmqCommitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  if (waitingRspNum == 0) {
    tscDebug("consumer:0x%" PRIx64 " topic:%s vgId:%d all commit-rsp received, commit completed", consumerId, pTopic,
             vgId);
    tmqCommitDone(pParamSet);
  } else {
    tscDebug("consumer:0x%" PRIx64 " topic:%s vgId:%d commit-rsp received, remain:%d", consumerId, pTopic, vgId,
             waitingRspNum);
  }
}

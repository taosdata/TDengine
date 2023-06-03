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

#define EMPTY_BLOCK_POLL_IDLE_DURATION 10
#define DEFAULT_AUTO_COMMIT_INTERVAL   5000

#define OFFSET_IS_RESET_OFFSET(_of)  ((_of) < 0)

typedef void (*__tmq_askep_fn_t)(tmq_t* pTmq, int32_t code, SDataBuf* pBuf, void* pParam);

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
  SRWLatch        lock;
  int8_t  status;
  int32_t epoch;
#if 0
  int8_t  epStatus;
  int32_t epSkipCnt;
#endif
  // poll info
  int64_t pollCnt;
  int64_t totalRows;

  // timer
  tmr_h       hbLiveTimer;
  tmr_h       epTimer;
  tmr_h       reportTimer;
  tmr_h       commitTimer;
  STscObj*    pTscObj;       // connection
  SArray*     clientTopics;  // SArray<SMqClientTopic>
  STaosQueue* mqueue;        // queue of rsp
  STaosQall*  qall;
  STaosQueue* delayedTask;  // delayed task queue for heartbeat and auto commit
  tsem_t      rspSem;
};

typedef struct SAskEpInfo {
  int32_t code;
  tsem_t  sem;
} SAskEpInfo;

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

typedef struct SVgOffsetInfo {
  STqOffsetVal committedOffset;
  STqOffsetVal currentOffset;
  int64_t      walVerBegin;
  int64_t      walVerEnd;
} SVgOffsetInfo;

typedef struct {
  int64_t       pollCnt;
  int64_t       numOfRows;
  SVgOffsetInfo offsetInfo;
  int32_t       vgId;
  int32_t       vgStatus;
  int32_t       vgSkipCnt;              // here used to mark the slow vgroups
  bool          receivedInfoFromVnode;  // has already received info from vnode
  int64_t       emptyBlockReceiveTs;    // once empty block is received, idle for ignoreCnt then start to poll data
  bool          seekUpdated;            // offset is updated by seek operator, therefore, not update by vnode rsp.
  SEpSet        epSet;
} SMqClientVg;

typedef struct {
  char           topicName[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqClientVg>
  SSchemaWrapper schema;
} SMqClientTopic;

typedef struct {
  int8_t          tmqRspType;
  int32_t         epoch;  // epoch can be used to guard the vgHandle
  int32_t         vgId;
  char            topicName[TSDB_TOPIC_FNAME_LEN];
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
//  int64_t refId;
//  int32_t epoch;
  tsem_t  rspSem;
  int32_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  int64_t          refId;
  int32_t          epoch;
  void*            pParam;
  __tmq_askep_fn_t pUserFn;
} SMqAskEpCbParam;

typedef struct {
  int64_t         refId;
  int32_t         epoch;
  char            topicName[TSDB_TOPIC_FNAME_LEN];
//  SMqClientVg*    pVg;
//  SMqClientTopic* pTopic;
  int32_t         vgId;
  uint64_t        requestId;  // request id for debug purpose
} SMqPollCbParam;

typedef struct SMqVgCommon {
  tsem_t        rsp;
  int32_t       numOfRsp;
  SArray*       pList;
  TdThreadMutex mutex;
  int64_t       consumerId;
  char*         pTopicName;
  int32_t       code;
} SMqVgCommon;

typedef struct SMqVgWalInfoParam {
  int32_t      vgId;
  int32_t      epoch;
  int32_t      totalReq;
  SMqVgCommon* pCommon;
} SMqVgWalInfoParam;

typedef struct {
  int64_t        refId;
  int32_t        epoch;
  int32_t        waitingRspNum;
  int32_t        totalRspNum;
  int32_t        code;
  tmq_commit_cb* callbackFn;
  /*SArray*        successfulOffsets;*/
  /*SArray*        failedOffsets;*/
  void* userParam;
} SMqCommitCbParamSet;

typedef struct {
  SMqCommitCbParamSet* params;
  SMqVgOffset*         pOffset;
  char                 topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t              vgId;
  tmq_t*               pTmq;
} SMqCommitCbParam;

typedef struct SSyncCommitInfo {
  tsem_t  sem;
  int32_t code;
} SSyncCommitInfo;

static int32_t doAskEp(tmq_t* tmq);
static int32_t makeTopicVgroupKey(char* dst, const char* topicName, int32_t vg);
static int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet);
static int32_t doSendCommitMsg(tmq_t* tmq, SMqClientVg* pVg, const char* pTopicName, SMqCommitCbParamSet* pParamSet,
                               int32_t index, int32_t totalVgroups, int32_t type);
static void    commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId);
static void    asyncAskEp(tmq_t* pTmq, __tmq_askep_fn_t askEpFn, void* param);
static void    addToQueueCallbackFn(tmq_t* pTmq, int32_t code, SDataBuf* pDataBuf, void* param);

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
    //    if (strcasecmp(value, "true") == 0) {
    //      conf->hbBgEnable = true;
    //      return TMQ_CONF_OK;
    //    } else if (strcasecmp(value, "false") == 0) {
    //      conf->hbBgEnable = false;
    //      return TMQ_CONF_OK;
    //    } else {
    tscError("the default value of enable.heartbeat.background is true, can not be seted");
    return TMQ_CONF_INVALID;
    //    }
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

tmq_list_t* tmq_list_new() { return (tmq_list_t*)taosArrayInit(0, sizeof(void*)); }

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) return -1;
  char* topic = taosStrdup(src);
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

static SMqClientVg* foundClientVg(SArray* pTopicList, const char* pName, int32_t vgId, int32_t* index,
                                  int32_t* numOfVgroups) {
  int32_t numOfTopics = taosArrayGetSize(pTopicList);
  *index = -1;
  *numOfVgroups = 0;

  for (int32_t i = 0; i < numOfTopics; ++i) {
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
  //    SMqClientVg* pVg = foundClientVg(pParam->pTmq->clientTopics, pParam->topicName, pParam->vgId, &index,
  //    &numOfVgroups); if (pVg == NULL) {
  //      tscDebug("consumer:0x%" PRIx64
  //               " subKey:%s vgId:%d commit failed, code:%s has been transferred to other consumer, no need retry
  //               ordinal:%d/%d", pParam->pTmq->consumerId, pParam->pOffset->subKey, pParam->vgId, tstrerror(code),
  //               index + 1, numOfVgroups);
  //    } else { // let's retry the commit
  //      int32_t code1 = doSendCommitMsg(pParam->pTmq, pVg, pParam->topicName, pParamSet, index, numOfVgroups);
  //      if (code1 != TSDB_CODE_SUCCESS) {  // retry failed.
  //        tscError("consumer:0x%" PRIx64 " topic:%s vgId:%d offset:%" PRId64
  //                 " retry failed, ignore this commit. code:%s ordinal:%d/%d",
  //                 pParam->pTmq->consumerId, pParam->topicName, pVg->vgId, pVg->offsetInfo.committedOffset.version,
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
  //    commitRspCountDown(pParamSet, pParam->pTmq->consumerId, pParam->topicName, pParam->vgId);
  //    return 0;
  //  }
  //
  //  // todo replace the pTmq with refId

  taosMemoryFree(pParam->pOffset);
  taosMemoryFree(pBuf->pData);
  taosMemoryFree(pBuf->pEpSet);

  commitRspCountDown(pParamSet, pParam->pTmq->consumerId, pParam->topicName, pParam->vgId);
  return 0;
}

static int32_t doSendCommitMsg(tmq_t* tmq, SMqClientVg* pVg, const char* pTopicName, SMqCommitCbParamSet* pParamSet,
                               int32_t index, int32_t totalVgroups, int32_t type) {
  SMqVgOffset* pOffset = taosMemoryCalloc(1, sizeof(SMqVgOffset));
  if (pOffset == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pOffset->consumerId = tmq->consumerId;
  pOffset->offset.val = pVg->offsetInfo.currentOffset;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset->offset.subKey, tmq->groupId, groupLen);
  pOffset->offset.subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset->offset.subKey + groupLen + 1, pTopicName);

  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqVgOffset, pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    taosMemoryFree(pOffset);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)buf)->vgId = htonl(pVg->vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, len);
  tEncodeMqVgOffset(&encoder, pOffset);
  tEncoderClear(&encoder);

  // build param
  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    taosMemoryFree(pOffset);
    taosMemoryFree(buf);
    return TSDB_CODE_OUT_OF_MEMORY;
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMsgSendInfo->msgInfo = (SDataBuf) { .pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL };

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = type;

  atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  atomic_add_fetch_32(&pParamSet->totalRspNum, 1);

  SEp* pEp = GET_ACTIVE_EP(&pVg->epSet);
  char offsetBuf[80] = {0};
  tFormatOffset(offsetBuf, tListLen(offsetBuf), &pOffset->offset.val);

  char commitBuf[80] = {0};
  tFormatOffset(commitBuf, tListLen(commitBuf), &pVg->offsetInfo.committedOffset);
  tscDebug("consumer:0x%" PRIx64 " topic:%s on vgId:%d send offset:%s prev:%s, ep:%s:%d, ordinal:%d/%d, req:0x%" PRIx64,
           tmq->consumerId, pOffset->offset.subKey, pVg->vgId, offsetBuf, commitBuf, pEp->fqdn, pEp->port, index + 1,
           totalVgroups, pMsgSendInfo->requestId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, pMsgSendInfo);

  return TSDB_CODE_SUCCESS;
}

static SMqClientTopic* getTopicByName(tmq_t* tmq, const char* pTopicName) {
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopic->topicName, pTopicName) != 0) {
      continue;
    }

    return pTopic;
  }

  tscError("consumer:0x%" PRIx64 ", total:%d, failed to find topic:%s", tmq->consumerId, numOfTopics, pTopicName);
  return NULL;
}

static void asyncCommitOffset(tmq_t* tmq, const TAOS_RES* pRes, int32_t type, tmq_commit_cb* pCommitFp, void* userParam) {
  char*   pTopicName = NULL;
  int32_t vgId = 0;
  int32_t code = 0;

  if (pRes == NULL || tmq == NULL) {
    pCommitFp(tmq, TSDB_CODE_INVALID_PARA, userParam);
    return;
  }

  if (TD_RES_TMQ(pRes)) {
    SMqRspObj* pRspObj = (SMqRspObj*)pRes;
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
  } else if (TD_RES_TMQ_META(pRes)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)pRes;
    pTopicName = pMetaRspObj->topic;
    vgId = pMetaRspObj->vgId;
  } else if (TD_RES_TMQ_METADATA(pRes)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)pRes;
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
  } else {
    pCommitFp(tmq, TSDB_CODE_TMQ_INVALID_MSG, userParam);
    return;
  }

  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    pCommitFp(tmq, TSDB_CODE_OUT_OF_MEMORY, userParam);
    return;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;
  pParamSet->callbackFn = pCommitFp;
  pParamSet->userParam = userParam;

  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);

  tscDebug("consumer:0x%" PRIx64 " do manual commit offset for %s, vgId:%d", tmq->consumerId, pTopicName, vgId);

  SMqClientTopic* pTopic = getTopicByName(tmq, pTopicName);
  if (pTopic == NULL) {
    tscWarn("consumer:0x%" PRIx64 " failed to find the specified topic:%s, total topics:%d", tmq->consumerId,
            pTopicName, numOfTopics);
    taosMemoryFree(pParamSet);
    pCommitFp(tmq, TSDB_CODE_SUCCESS, userParam);
    return;
  }

  int32_t j = 0;
  int32_t numOfVgroups = taosArrayGetSize(pTopic->vgs);
  for (j = 0; j < numOfVgroups; j++) {
    SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
    if (pVg->vgId == vgId) {
      break;
    }
  }

  if (j == numOfVgroups) {
    tscWarn("consumer:0x%" PRIx64 " failed to find the specified vgId:%d, total Vgs:%d, topic:%s", tmq->consumerId,
            vgId, numOfVgroups, pTopicName);
    taosMemoryFree(pParamSet);
    pCommitFp(tmq, TSDB_CODE_SUCCESS, userParam);
    return;
  }

  SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
  if (pVg->offsetInfo.currentOffset.type > 0 && !tOffsetEqual(&pVg->offsetInfo.currentOffset, &pVg->offsetInfo.committedOffset)) {
    code = doSendCommitMsg(tmq, pVg, pTopic->topicName, pParamSet, j, numOfVgroups, type);

    // failed to commit, callback user function directly.
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pParamSet);
      pCommitFp(tmq, code, userParam);
    }
  } else {  // do not perform commit, callback user function directly.
    taosMemoryFree(pParamSet);
    pCommitFp(tmq, code, userParam);
  }
}

static void asyncCommitAllOffsets(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam) {
  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    pCommitFp(tmq, TSDB_CODE_OUT_OF_MEMORY, userParam);
    return;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;
  pParamSet->callbackFn = pCommitFp;
  pParamSet->userParam = userParam;

  // init as 1 to prevent concurrency issue
  pParamSet->waitingRspNum = 1;

  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t         numOfVgroups = taosArrayGetSize(pTopic->vgs);

    tscDebug("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName,
             numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

      if (pVg->offsetInfo.currentOffset.type > 0 && !tOffsetEqual(&pVg->offsetInfo.currentOffset, &pVg->offsetInfo.committedOffset)) {
        int32_t code = doSendCommitMsg(tmq, pVg, pTopic->topicName, pParamSet, j, numOfVgroups, TDMT_VND_TMQ_COMMIT_OFFSET);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("consumer:0x%" PRIx64 " topic:%s vgId:%d offset:%" PRId64 " failed, code:%s ordinal:%d/%d",
                   tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->offsetInfo.committedOffset.version, tstrerror(terrno),
                   j + 1, numOfVgroups);
          continue;
        }

        // update the offset value.
        pVg->offsetInfo.committedOffset = pVg->offsetInfo.currentOffset;
      } else {
        tscDebug("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, current:%" PRId64 ", ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->offsetInfo.currentOffset.version, j + 1, numOfVgroups);
      }
    }
  }

  tscDebug("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - 1,
           numOfTopics);

  // no request is sent
  if (pParamSet->totalRspNum == 0) {
    taosMemoryFree(pParamSet);
    pCommitFp(tmq, TSDB_CODE_SUCCESS, userParam);
    return;
  }

  // count down since waiting rsp num init as 1
  commitRspCountDown(pParamSet, tmq->consumerId, "", 0);
}

static void generateTimedTask(int64_t refId, int32_t type) {
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
    *pTaskType = type;
    taosWriteQitem(tmq->delayedTask, pTaskType);
    tsem_post(&tmq->rspSem);
    taosReleaseRef(tmqMgmt.rsetId, refId);
  }
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

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
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

  sendInfo->msgInfo = (SDataBuf){ .pData = pReq, .len = tlen, .handle = NULL };

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

static void defaultCommitCbFn(tmq_t* pTmq, int32_t code, void* param) {
  if (code != 0) {
    tscDebug("consumer:0x%" PRIx64 ", failed to commit offset, code:%s", pTmq->consumerId, tstrerror(code));
  }
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
      asyncAskEp(pTmq, addToQueueCallbackFn, NULL);

      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = pTmq->refId;

      tscDebug("consumer:0x%" PRIx64 " retrieve ep from mnode in 1s", pTmq->consumerId);
      taosTmrReset(tmqAssignAskEpTask, 1000, pRefId, tmqMgmt.timer, &pTmq->epTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmq_commit_cb* pCallbackFn = pTmq->commitCb ? pTmq->commitCb : defaultCommitCbFn;

      asyncCommitAllOffsets(pTmq, pCallbackFn, pTmq->commitCbUserParam);
      int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
      *pRefId = pTmq->refId;

      tscDebug("consumer:0x%" PRIx64 " next commit to vnode(s) in %.2fs", pTmq->consumerId,
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
    taosArrayDestroyP(pRsp->dataRsp.blockSchema, (FDelete)tDeleteSchemaWrapper);
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
    taosArrayDestroyP(pRsp->taosxRsp.blockSchema, (FDelete)tDeleteSchemaWrapper);
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

  if (pTmq->clientTopics == NULL || pTmq->mqueue == NULL || pTmq->qall == NULL || pTmq->delayedTask == NULL ||
      conf->groupId[0] == 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
    goto _failed;
  }

  // init status
  pTmq->status = TMQ_CONSUMER_STATUS__INIT;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;

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
  taosInitRWLatch(&pTmq->lock);

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
  tscInfo("consumer:0x%" PRIx64 " is setup, refId:%" PRId64
          ", groupId:%s, snapshot:%d, autoCommit:%d, commitInterval:%dms, offset:%s, backgroudHB:%d",
          pTmq->consumerId, pTmq->refId, pTmq->groupId, pTmq->useSnapshot, pTmq->autoCommit, pTmq->autoCommitInterval,
          buf, pTmq->hbBgEnable);

  return pTmq;

_failed:
  tmqFreeImpl(pTmq);
  return NULL;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  const int32_t   MAX_RETRY_COUNT = 120 * 60;  // let's wait for 2 mins at most
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

  SMqSubscribeCbParam param = { .rspErr = 0};
  if (tsem_init(&param.rspSem, 0, 0) != 0) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto FAIL;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = tlen, .handle = NULL};

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
  while (TSDB_CODE_MND_CONSUMER_NOT_READY == doAskEp(tmq)) {
    if (retryCnt++ > MAX_RETRY_COUNT) {
      tscError("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
      code = TSDB_CODE_MND_CONSUMER_NOT_READY;
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

static SMqClientVg* getVgInfo(tmq_t* tmq, char* topicName, int32_t  vgId){
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for(int i = 0; i < topicNumCur; i++){
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if(strcmp(pTopicCur->topicName, topicName) == 0){
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if(pVgCur->vgId == vgId){
          return pVgCur;
        }
      }
    }
  }
  return NULL;
}

static SMqClientTopic* getTopicInfo(tmq_t* tmq, char* topicName){
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for(int i = 0; i < topicNumCur; i++){
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if(strcmp(pTopicCur->topicName, topicName) == 0){
      return pTopicCur;
    }
  }
  return NULL;
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;

  int64_t         refId = pParam->refId;
//  SMqClientVg*    pVg = pParam->pVg;
//  SMqClientTopic* pTopic = pParam->pTopic;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    taosMemoryFree(pParam);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  int32_t  epoch = pParam->epoch;
  int32_t  vgId = pParam->vgId;
  uint64_t requestId = pParam->requestId;

  if (code != 0) {
    if (pMsg->pData) taosMemoryFree(pMsg->pData);
    if (pMsg->pEpSet) taosMemoryFree(pMsg->pEpSet);

    // in case of consumer mismatch, wait for 500ms and retry
    if (code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
//      taosMsleep(500);
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__RECOVER);
      tscDebug("consumer:0x%" PRIx64 " wait for the re-balance, wait for 500ms and set status to be RECOVER",
               tmq->consumerId);
    } else if (code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
      if (pRspWrapper == NULL) {
        tscWarn("consumer:0x%" PRIx64 " msg from vgId:%d discarded, epoch %d since out of memory, reqId:0x%" PRIx64,
                tmq->consumerId, vgId, epoch, requestId);
        goto CREATE_MSG_FAIL;
      }

      pRspWrapper->tmqRspType = TMQ_MSG_TYPE__END_RSP;
      taosWriteQitem(tmq->mqueue, pRspWrapper);
//    } else if (code == TSDB_CODE_WAL_LOG_NOT_EXIST) {  // poll data while insert
//      taosMsleep(5);
    } else{
      tscError("consumer:0x%" PRIx64 " msg from vgId:%d discarded, epoch %d, since %s, reqId:0x%" PRIx64, tmq->consumerId,
               vgId, epoch, tstrerror(code), requestId);
    }

    goto CREATE_MSG_FAIL;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t clientEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < clientEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("consumer:0x%" PRIx64
            " msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d, reqId:0x%" PRIx64,
            tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);

    tsem_post(&tmq->rspSem);
    taosReleaseRef(tmqMgmt.rsetId, refId);

    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pParam);

    return 0;
  }

  if (msgEpoch != clientEpoch) {
    tscWarn("consumer:0x%" PRIx64 " mismatch rsp from vgId:%d, epoch %d, current epoch %d, reqId:0x%" PRIx64,
            tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
  }

  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
  if (pRspWrapper == NULL) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tscWarn("consumer:0x%" PRIx64 " msg discard from vgId:%d, epoch %d since out of memory", tmq->consumerId, vgId,
            epoch);
    goto CREATE_MSG_FAIL;
  }

  pRspWrapper->tmqRspType = rspType;
//  pRspWrapper->vgHandle = pVg;
//  pRspWrapper->topicHandle = pTopic;
  pRspWrapper->reqId = requestId;
  pRspWrapper->pEpset = pMsg->pEpSet;
  pRspWrapper->vgId = vgId;
  strcpy(pRspWrapper->topicName, pParam->topicName);

  pMsg->pEpSet = NULL;
  if (rspType == TMQ_MSG_TYPE__POLL_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqDataRsp(&decoder, &pRspWrapper->dataRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));

    char buf[80];
    tFormatOffset(buf, 80, &pRspWrapper->dataRsp.rspOffset);
    tscDebug("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, req ver:%" PRId64 ", rsp:%s type %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, pRspWrapper->dataRsp.reqOffset.version, buf, rspType, requestId);
  } else if (rspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqMetaRsp(&decoder, &pRspWrapper->metaRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
  } else if (rspType == TMQ_MSG_TYPE__TAOSX_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSTaosxRsp(&decoder, &pRspWrapper->taosxRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->taosxRsp, pMsg->pData, sizeof(SMqRspHead));
  } else {  // invalid rspType
    tscError("consumer:0x%" PRIx64 " invalid rsp msg received, type:%d ignored", tmq->consumerId, rspType);
  }

  taosMemoryFree(pMsg->pData);
  taosWriteQitem(tmq->mqueue, pRspWrapper);

  int32_t total = taosQueueItemSize(tmq->mqueue);
  tscDebug("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d, vgId:%d, total in queue:%d, reqId:0x%" PRIx64,
           tmq->consumerId, rspType, vgId, total, requestId);

  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);
  taosMemoryFree(pParam);

  return 0;

CREATE_MSG_FAIL:
  if (epoch == tmq->epoch) {
    taosWLockLatch(&tmq->lock);
    SMqClientVg* pVg = getVgInfo(tmq, pParam->topicName, vgId);
    if(pVg){
      atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
    }
    taosWUnLockLatch(&tmq->lock);
  }

  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);
  taosMemoryFree(pParam);

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

  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);

  tstrncpy(pTopic->topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pTopic->db, pTopicEp->db, TSDB_DB_FNAME_LEN);

  tscDebug("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d", tmq->consumerId, pTopic->topicName, vgNumGet);
  pTopic->vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));

  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);

    makeTopicVgroupKey(vgKey, pTopic->topicName, pVgEp->vgId);
    SVgroupSaveInfo* pInfo = taosHashGet(pVgOffsetHashMap, vgKey, strlen(vgKey));

    int64_t      numOfRows = 0;
    STqOffsetVal offsetNew = {.type = tmq->resetOffsetCfg};
    if (pInfo != NULL) {
      offsetNew = pInfo->offset;
      numOfRows = pInfo->numOfRows;
    }

    SMqClientVg clientVg = {
        .pollCnt = 0,
        .vgId = pVgEp->vgId,
        .epSet = pVgEp->epSet,
        .vgStatus = TMQ_VG_STATUS__IDLE,
        .vgSkipCnt = 0,
        .emptyBlockReceiveTs = 0,
        .numOfRows = numOfRows,
    };

    clientVg.offsetInfo.currentOffset = offsetNew;
    clientVg.offsetInfo.committedOffset = offsetNew;
    clientVg.offsetInfo.walVerBegin = -1;
    clientVg.offsetInfo.walVerEnd = -1;
    clientVg.seekUpdated = false;
    clientVg.receivedInfoFromVnode = false;

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

static bool doUpdateLocalEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  bool set = false;

  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);

  char vgKey[TSDB_TOPIC_FNAME_LEN + 22];
  tscDebug("consumer:0x%" PRIx64 " update ep epoch from %d to epoch %d, incoming topics:%d, existed topics:%d",
           tmq->consumerId, tmq->epoch, epoch, topicNumGet, topicNumCur);
  if (epoch <= tmq->epoch) {
    return false;
  }

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
      tscDebug("consumer:0x%" PRIx64 ", current vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        makeTopicVgroupKey(vgKey, pTopicCur->topicName, pVgCur->vgId);

        char buf[80];
        tFormatOffset(buf, 80, &pVgCur->offsetInfo.currentOffset);
        tscDebug("consumer:0x%" PRIx64 ", epoch:%d vgId:%d vgKey:%s, offset:%s", tmq->consumerId, epoch, pVgCur->vgId,
                 vgKey, buf);

        SVgroupSaveInfo info = {.offset = pVgCur->offsetInfo.currentOffset, .numOfRows = pVgCur->numOfRows};
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

  taosWLockLatch(&tmq->lock);
  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientVgInfo);
  }
  tmq->clientTopics = newTopics;
  taosWUnLockLatch(&tmq->lock);

  int8_t flag = (topicNumGet == 0) ? TMQ_CONSUMER_STATUS__NO_TOPIC : TMQ_CONSUMER_STATUS__READY;
  atomic_store_8(&tmq->status, flag);
  atomic_store_32(&tmq->epoch, epoch);

  tscDebug("consumer:0x%" PRIx64 " update topic info completed", tmq->consumerId);
  return set;
}

int32_t askEpCallbackFn(void* param, SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);

  if (tmq == NULL) {
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
//    pParam->pUserFn(tmq, terrno, NULL, pParam->pParam);

    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pParam);
    return terrno;
  }

  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 ", get topic endpoint error, code:%s", tmq->consumerId, tstrerror(code));
    pParam->pUserFn(tmq, code, NULL, pParam->pParam);

    taosReleaseRef(tmqMgmt.rsetId, pParam->refId);

    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pParam);
    return code;
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

  } else {
    tscDebug("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d, update local ep", tmq->consumerId,
             head->epoch, epoch);
  }

  pParam->pUserFn(tmq, code, pMsg, pParam->pParam);
  taosReleaseRef(tmqMgmt.rsetId, pParam->refId);

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pParam);
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
  pReq->reqOffset = pVg->offsetInfo.currentOffset;
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
  for (int32_t i = 0; i < pRspObj->rsp.blockNum; ++i) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(pRspObj->rsp.blockData, i);
    int64_t            rows = htobe64(pRetrieve->numOfRows);
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
//  pParam->pVg = pVg;  // pVg may be released,fix it
//  pParam->pTopic = pTopic;
  strcpy(pParam->topicName, pTopic->topicName);
  pParam->vgId = pVg->vgId;
  pParam->requestId = req.reqId;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pParam);
    taosMemoryFree(msg);
    return handleErrorBeforePoll(pVg, pTmq);
  }

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = req.reqId;
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqPollCb;
  sendInfo->msgType = TDMT_VND_TMQ_CONSUME;

  int64_t transporterId = 0;
  char    offsetFormatBuf[80];
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->offsetInfo.currentOffset);

  tscDebug("consumer:0x%" PRIx64 " send poll to %s vgId:%d, epoch %d, req:%s, reqId:0x%" PRIx64, pTmq->consumerId,
           pTopic->topicName, pVg->vgId, pTmq->epoch, offsetFormatBuf, req.reqId);
  asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);

  pVg->pollCnt++;
  pVg->seekUpdated = false;   // reset this flag.
  pTmq->pollCnt++;

  return TSDB_CODE_SUCCESS;
}

// broadcast the poll request to all related vnodes
static int32_t tmqPollImpl(tmq_t* tmq, int64_t timeout) {
  if(atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER){
    return 0;
  }
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to poll data, numOfTopics:%d", tmq->consumerId, numOfTopics);

  for (int i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t         numOfVg = taosArrayGetSize(pTopic->vgs);

    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (taosGetTimestampMs() - pVg->emptyBlockReceiveTs < EMPTY_BLOCK_POLL_IDLE_DURATION) {  // less than 100ms
        tscTrace("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for 10ms before start next poll", tmq->consumerId,
                 tmq->epoch, pVg->vgId);
        continue;
      }

      int32_t vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus == TMQ_VG_STATUS__WAIT) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tscTrace("consumer:0x%" PRIx64 " epoch %d wait poll-rsp, skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch,
                 pVg->vgId, vgSkipCnt);
        continue;
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
      doUpdateLocalEp(tmq, rspWrapper->epoch, rspMsg);
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

    tscDebug("consumer:0x%" PRIx64 " handle rsp, type:%d", tmq->consumerId, pRspWrapper->tmqRspType);

    if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__END_RSP) {
      taosFreeQitem(pRspWrapper);
      terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
      tscError("consumer:0x%" PRIx64 " unexpected rsp from poll, code:%s", tmq->consumerId, tstrerror(terrno));
      return NULL;
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;

      int32_t     consumerEpoch = atomic_load_32(&tmq->epoch);
      SMqDataRsp* pDataRsp = &pollRspWrapper->dataRsp;

      if (pDataRsp->head.epoch == consumerEpoch) {
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if(pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL){
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          return NULL;
        }
        // update the epset
        if (pollRspWrapper->pEpset != NULL) {
          SEp* pEp = GET_ACTIVE_EP(pollRspWrapper->pEpset);
          SEp* pOld = GET_ACTIVE_EP(&(pVg->epSet));
          tscDebug("consumer:0x%" PRIx64 " update epset vgId:%d, ep:%s:%d, old ep:%s:%d", tmq->consumerId, pVg->vgId,
                   pEp->fqdn, pEp->port, pOld->fqdn, pOld->port);
          pVg->epSet = *pollRspWrapper->pEpset;
        }

        // update the local offset value only for the returned values, only when the local offset is NOT updated
        // by tmq_offset_seek function
        if (!pVg->seekUpdated) {
          tscDebug("consumer:0x%" PRIx64" local offset is update, since seekupdate not set", tmq->consumerId);
          pVg->offsetInfo.currentOffset = pDataRsp->rspOffset;
        } else {
          tscDebug("consumer:0x%" PRIx64" local offset is NOT update, since seekupdate is set", tmq->consumerId);
        }

        // update the status
        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

        // update the valid wal version range
        pVg->offsetInfo.walVerBegin = pDataRsp->head.walsver;
        pVg->offsetInfo.walVerEnd = pDataRsp->head.walever;
        pVg->receivedInfoFromVnode = true;

        char buf[80];
        tFormatOffset(buf, 80, &pDataRsp->rspOffset);
        if (pDataRsp->blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " empty block received, vgId:%d, offset:%s, vg total:%" PRId64
                   " total:%" PRId64 " reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
          pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          taosFreeQitem(pollRspWrapper);
        } else {  // build rsp
          int64_t    numOfRows = 0;
          SMqRspObj* pRsp = tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
          tmq->totalRows += numOfRows;
          pVg->emptyBlockReceiveTs = 0;
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
      // todo handle the wal range and epset for each vgroup
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      tscDebug("consumer:0x%" PRIx64 " process meta rsp", tmq->consumerId);

      if (pollRspWrapper->metaRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if(pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL){
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          return NULL;
        }

        if(pollRspWrapper->metaRsp.rspOffset.type != 0){    // if offset is validate
          pVg->offsetInfo.currentOffset = pollRspWrapper->metaRsp.rspOffset;
        }

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
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      if (pollRspWrapper->taosxRsp.head.epoch == consumerEpoch) {
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if(pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL){
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          return NULL;
        }

        // update the local offset value only for the returned values, only when the local offset is NOT updated
        // by tmq_offset_seek function
        if (!pVg->seekUpdated) {
          if(pollRspWrapper->taosxRsp.rspOffset.type != 0) {    // if offset is validate
            tscDebug("consumer:0x%" PRIx64" local offset is update, since seekupdate not set", tmq->consumerId);
            pVg->offsetInfo.currentOffset = pollRspWrapper->taosxRsp.rspOffset;
          }
        } else {
          tscDebug("consumer:0x%" PRIx64" local offset is NOT update, since seekupdate is set", tmq->consumerId);
        }

        atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

        if (pollRspWrapper->taosxRsp.blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " taosx empty block received, vgId:%d, vg total:%" PRId64 " reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, pVg->numOfRows, pollRspWrapper->reqId);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          pRspWrapper = tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pollRspWrapper);
          continue;
        } else {
          pVg->emptyBlockReceiveTs = 0;  // reset the ts
        }

        // build rsp
        void*   pRsp = NULL;
        int64_t numOfRows = 0;
        if (pollRspWrapper->taosxRsp.createTableNum == 0) {
          pRsp = tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
        } else {
          pRsp = tmqBuildTaosxRspFromWrapper(pollRspWrapper);
        }

        tmq->totalRows += numOfRows;

        char buf[80];
        tFormatOffset(buf, 80, &pVg->offsetInfo.currentOffset);
        tscDebug("consumer:0x%" PRIx64 " process taosx poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                 ", vg total:%" PRId64 " total:%" PRId64 " reqId:0x%" PRIx64,
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

  tscDebug("consumer:0x%" PRIx64 " start to poll at %" PRId64 ", timeout:%" PRId64, tmq->consumerId, startTime,
           timeout);

  // in no topic status, delayed task also need to be processed
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    tscDebug("consumer:0x%" PRIx64 " poll return since consumer is init", tmq->consumerId);
    taosMsleep(500);  //     sleep for a while
    return NULL;
  }

  while (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER) {
    int32_t retryCnt = 0;
    while (TSDB_CODE_MND_CONSUMER_NOT_READY == doAskEp(tmq)) {
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
  tscDebug("consumer:0x%" PRIx64 " start to close consumer, status:%d", tmq->consumerId, tmq->status);
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
    tscWarn("consumer:0x%" PRIx64 " not in ready state, close it directly", tmq->consumerId);
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

int64_t tmq_get_vgroup_offset(TAOS_RES* res) {
  if (TD_RES_TMQ(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*) res;
    STqOffsetVal* pOffset = &pRspObj->rsp.rspOffset;
    if (pOffset->type == TMQ_OFFSET__LOG) {
      return pRspObj->rsp.rspOffset.version;
    }
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pRspObj = (SMqMetaRspObj*)res;
    if (pRspObj->metaRsp.rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->metaRsp.rspOffset.version;
    }
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*) res;
    if (pRspObj->rsp.rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->rsp.rspOffset.version;
    }
  }

  // data from tsdb, no valid offset info
  return -1;
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

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* cb, void* param) {
  if (pRes == NULL) {  // here needs to commit all offsets.
    asyncCommitAllOffsets(tmq, cb, param);
  } else {  // only commit one offset
    asyncCommitOffset(tmq, pRes, TDMT_VND_TMQ_COMMIT_OFFSET, cb, param);
  }
}

static void commitCallBackFn(tmq_t *UNUSED_PARAM(tmq), int32_t code, void* param) {
  SSyncCommitInfo* pInfo = (SSyncCommitInfo*) param;
  pInfo->code = code;
  tsem_post(&pInfo->sem);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* pRes) {
  int32_t code = 0;

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  tsem_init(&pInfo->sem, 0, 0);
  pInfo->code = 0;

  if (pRes == NULL) {
    asyncCommitAllOffsets(tmq, commitCallBackFn, pInfo);
  } else {
    asyncCommitOffset(tmq, pRes, TDMT_VND_TMQ_COMMIT_OFFSET, commitCallBackFn, pInfo);
  }

  tsem_wait(&pInfo->sem);
  code = pInfo->code;

  tsem_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);

  tscDebug("consumer:0x%" PRIx64 " sync commit done, code:%s", tmq->consumerId, tstrerror(code));
  return code;
}

void updateEpCallbackFn(tmq_t* pTmq, int32_t code, SDataBuf* pDataBuf, void* param) {
  SAskEpInfo* pInfo = param;
  pInfo->code = code;

  if (code == TSDB_CODE_SUCCESS) {
    SMqRspHead* head = pDataBuf->pData;

    SMqAskEpRsp rsp;
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pDataBuf->pData, sizeof(SMqRspHead)), &rsp);
    doUpdateLocalEp(pTmq, head->epoch, &rsp);
    tDeleteSMqAskEpRsp(&rsp);
  }

  tsem_post(&pInfo->sem);
}

void addToQueueCallbackFn(tmq_t* pTmq, int32_t code, SDataBuf* pDataBuf, void* param) {
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return;
  }

  SMqAskEpRspWrapper* pWrapper = taosAllocateQitem(sizeof(SMqAskEpRspWrapper), DEF_QITEM, 0);
  if (pWrapper == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return;
  }

  SMqRspHead* head = pDataBuf->pData;

  pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
  pWrapper->epoch = head->epoch;
  memcpy(&pWrapper->msg, pDataBuf->pData, sizeof(SMqRspHead));
  tDecodeSMqAskEpRsp(POINTER_SHIFT(pDataBuf->pData, sizeof(SMqRspHead)), &pWrapper->msg);

  taosWriteQitem(pTmq->mqueue, pWrapper);
}

int32_t doAskEp(tmq_t* pTmq) {
  SAskEpInfo* pInfo = taosMemoryMalloc(sizeof(SAskEpInfo));
  tsem_init(&pInfo->sem, 0, 0);

  asyncAskEp(pTmq, updateEpCallbackFn, pInfo);
  tsem_wait(&pInfo->sem);

  int32_t code = pInfo->code;
  tsem_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);
  return code;
}

void asyncAskEp(tmq_t* pTmq, __tmq_askep_fn_t askEpFn, void* param) {
  SMqAskEpReq req = {0};
  req.consumerId = pTmq->consumerId;
  req.epoch = pTmq->epoch;
  strcpy(req.cgroup, pTmq->groupId);

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  if (tlen < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq failed", pTmq->consumerId);
    askEpFn(pTmq, TSDB_CODE_INVALID_PARA, NULL, param);
    return;
  }

  void* pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc askEpReq msg, size:%d", pTmq->consumerId, tlen);
    askEpFn(pTmq, TSDB_CODE_OUT_OF_MEMORY, NULL, param);
    return;
  }

  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq %d failed", pTmq->consumerId, tlen);
    taosMemoryFree(pReq);

    askEpFn(pTmq, TSDB_CODE_INVALID_PARA, NULL, param);
    return;
  }

  SMqAskEpCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc subscribe param", pTmq->consumerId);
    taosMemoryFree(pReq);

    askEpFn(pTmq, TSDB_CODE_OUT_OF_MEMORY, NULL, param);
    return;
  }

  pParam->refId = pTmq->refId;
  pParam->epoch = pTmq->epoch;
  pParam->pUserFn = askEpFn;
  pParam->pParam = param;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    askEpFn(pTmq, TSDB_CODE_OUT_OF_MEMORY, NULL, param);
    return;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = askEpCallbackFn;
  sendInfo->msgType = TDMT_MND_TMQ_ASK_EP;

  SEpSet epSet = getEpSet_s(&pTmq->pTscObj->pAppInfo->mgmtEp);
  tscDebug("consumer:0x%" PRIx64 " ask ep from mnode, reqId:0x%" PRIx64, pTmq->consumerId, sendInfo->requestId);

  int64_t transporterId = 0;
  asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);
}

int32_t makeTopicVgroupKey(char* dst, const char* topicName, int32_t vg) {
  return sprintf(dst, "%s:%d", topicName, vg);
}

int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  int64_t refId = pParamSet->refId;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    taosMemoryFree(pParamSet);
    terrno = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    return -1;
  }

  // if no more waiting rsp
  pParamSet->callbackFn(tmq, pParamSet->code, pParamSet->userParam);
  taosMemoryFree(pParamSet);

  taosReleaseRef(tmqMgmt.rsetId, refId);
  return 0;
}

void commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
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

SReqResultInfo* tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4) {
  SMqRspObj* pRspObj = (SMqRspObj*)res;
  pRspObj->resIter++;

  if (pRspObj->resIter < pRspObj->rsp.blockNum) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(pRspObj->rsp.blockData, pRspObj->resIter);
    if (pRspObj->rsp.withSchema) {
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(pRspObj->rsp.blockSchema, pRspObj->resIter);
      setResSchemaInfo(&pRspObj->resInfo, pSW->pSchema, pSW->nCols);
      taosMemoryFreeClear(pRspObj->resInfo.row);
      taosMemoryFreeClear(pRspObj->resInfo.pCol);
      taosMemoryFreeClear(pRspObj->resInfo.length);
      taosMemoryFreeClear(pRspObj->resInfo.convertBuf);
      taosMemoryFreeClear(pRspObj->resInfo.convertJson);
    }

    setQueryResultFromRsp(&pRspObj->resInfo, pRetrieve, convertUcs4, false);
    return &pRspObj->resInfo;
  }

  return NULL;
}

static int32_t tmqGetWalInfoCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqVgWalInfoParam* pParam = param;
  SMqVgCommon* pCommon = pParam->pCommon;

  int32_t total = atomic_add_fetch_32(&pCommon->numOfRsp, 1);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 " failed to get the wal info from vgId:%d for topic:%s", pCommon->consumerId,
             pParam->vgId, pCommon->pTopicName);
    pCommon->code = code;
  } else {
    SMqDataRsp rsp;
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqDataRsp(&decoder, &rsp);
    tDecoderClear(&decoder);

    SMqRspHead* pHead = pMsg->pData;

    tmq_topic_assignment assignment = {.begin = pHead->walsver,
                                       .end = pHead->walever,
                                       .currentOffset = rsp.rspOffset.version,
                                       .vgId = pParam->vgId};

    taosThreadMutexLock(&pCommon->mutex);
    taosArrayPush(pCommon->pList, &assignment);
    taosThreadMutexUnlock(&pCommon->mutex);
  }

  if (total == pParam->totalReq) {
    tsem_post(&pCommon->rsp);
  }

  taosMemoryFree(pParam);
  return 0;
}

static void destroyCommonInfo(SMqVgCommon* pCommon) {
  taosArrayDestroy(pCommon->pList);
  tsem_destroy(&pCommon->rsp);
  taosThreadMutexDestroy(&pCommon->mutex);
  taosMemoryFree(pCommon->pTopicName);
  taosMemoryFree(pCommon);
}

int32_t tmq_get_topic_assignment(tmq_t* tmq, const char* pTopicName, tmq_topic_assignment** assignment,
                                 int32_t* numOfAssignment) {
  *numOfAssignment = 0;
  *assignment = NULL;

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[128] = {0};
  sprintf(tname, "%d.%s", accId, pTopicName);

  SMqClientTopic* pTopic = getTopicByName(tmq, tname);
  if (pTopic == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // in case of snapshot is opened, no valid offset will return
  *numOfAssignment = taosArrayGetSize(pTopic->vgs);

  *assignment = taosMemoryCalloc(*numOfAssignment, sizeof(tmq_topic_assignment));
  if (*assignment == NULL) {
    tscError("consumer:0x%" PRIx64 " failed to malloc buffer, size:%" PRIzu, tmq->consumerId,
             (*numOfAssignment) * sizeof(tmq_topic_assignment));
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  bool needFetch = false;

  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    if (!pClientVg->receivedInfoFromVnode) {
      needFetch = true;
      break;
    }

    tmq_topic_assignment* pAssignment = &(*assignment)[j];
    if (pClientVg->offsetInfo.currentOffset.type == TMQ_OFFSET__LOG) {
      pAssignment->currentOffset = pClientVg->offsetInfo.currentOffset.version;
    } else {
      pAssignment->currentOffset = 0;
    }

    pAssignment->begin = pClientVg->offsetInfo.walVerBegin;
    pAssignment->end = pClientVg->offsetInfo.walVerEnd;
    pAssignment->vgId = pClientVg->vgId;
  }

  if (needFetch) {
    SMqVgCommon* pCommon = taosMemoryCalloc(1, sizeof(SMqVgCommon));
    if (pCommon == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }

    pCommon->pList= taosArrayInit(4, sizeof(tmq_topic_assignment));
    tsem_init(&pCommon->rsp, 0, 0);
    taosThreadMutexInit(&pCommon->mutex, 0);
    pCommon->pTopicName = taosStrdup(pTopic->topicName);
    pCommon->consumerId = tmq->consumerId;

    terrno = TSDB_CODE_OUT_OF_MEMORY;
    for (int32_t i = 0; i < (*numOfAssignment); ++i) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);

      SMqVgWalInfoParam* pParam = taosMemoryMalloc(sizeof(SMqVgWalInfoParam));
      if (pParam == NULL) {
        destroyCommonInfo(pCommon);
        return terrno;
      }

      pParam->epoch = tmq->epoch;
      pParam->vgId = pClientVg->vgId;
      pParam->totalReq = *numOfAssignment;
      pParam->pCommon = pCommon;

      SMqPollReq req = {0};
      tmqBuildConsumeReqImpl(&req, tmq, 10, pTopic, pClientVg);

      int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
      if (msgSize < 0) {
        taosMemoryFree(pParam);
        destroyCommonInfo(pCommon);
        return terrno;
      }

      char* msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        taosMemoryFree(pParam);
        destroyCommonInfo(pCommon);
        return terrno;
      }

      if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
        taosMemoryFree(msg);
        taosMemoryFree(pParam);
        destroyCommonInfo(pCommon);
        return terrno;
      }

      SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        taosMemoryFree(pParam);
        taosMemoryFree(msg);
        destroyCommonInfo(pCommon);
        return terrno;
      }

      sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
      sendInfo->requestId = req.reqId;
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->fp = tmqGetWalInfoCb;
      sendInfo->msgType = TDMT_VND_TMQ_VG_WALINFO;

      int64_t transporterId = 0;
      char    offsetFormatBuf[80];
      tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pClientVg->offsetInfo.currentOffset);

      tscDebug("consumer:0x%" PRIx64 " %s retrieve wal info vgId:%d, epoch %d, req:%s, reqId:0x%" PRIx64,
               tmq->consumerId, pTopic->topicName, pClientVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
      asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pClientVg->epSet, &transporterId, sendInfo);
    }

    tsem_wait(&pCommon->rsp);
    int32_t code = pCommon->code;

    terrno = code;
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(*assignment);
      *assignment = NULL;
      *numOfAssignment = 0;
    } else {
      int32_t num = taosArrayGetSize(pCommon->pList);
      for(int32_t i = 0; i < num; ++i) {
        (*assignment)[i] = *(tmq_topic_assignment*)taosArrayGet(pCommon->pList, i);
      }
      *numOfAssignment = num;
    }

    for (int32_t j = 0; j < (*numOfAssignment); ++j) {
      tmq_topic_assignment* p = &(*assignment)[j];

      for(int32_t i = 0; i < taosArrayGetSize(pTopic->vgs); ++i) {
        SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
        if (pClientVg->vgId != p->vgId) {
          continue;
        }

        SVgOffsetInfo* pOffsetInfo = &pClientVg->offsetInfo;

        pOffsetInfo->currentOffset.type = TMQ_OFFSET__LOG;

        char offsetBuf[80] = {0};
        tFormatOffset(offsetBuf, tListLen(offsetBuf), &pOffsetInfo->currentOffset);

        tscDebug("vgId:%d offset is update to:%s", p->vgId, offsetBuf);

        pOffsetInfo->walVerBegin = p->begin;
        pOffsetInfo->walVerEnd = p->end;
        pOffsetInfo->currentOffset.version = p->currentOffset;
        pOffsetInfo->committedOffset.version = p->currentOffset;
      }
    }

    destroyCommonInfo(pCommon);
    return code;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

void tmq_free_assignment(tmq_topic_assignment* pAssignment) {
    if (pAssignment == NULL) {
        return;
    }

    taosMemoryFree(pAssignment);
}

int32_t tmq_offset_seek(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char tname[128] = {0};
  sprintf(tname, "%d.%s", accId, pTopicName);

  SMqClientTopic* pTopic = getTopicByName(tmq, tname);
  if (pTopic == NULL) {
    tscError("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    return TSDB_CODE_INVALID_PARA;
  }

  SMqClientVg* pVg = NULL;
  int32_t      numOfVgs = taosArrayGetSize(pTopic->vgs);
  for (int32_t i = 0; i < numOfVgs; ++i) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
    if (pClientVg->vgId == vgId) {
      pVg = pClientVg;
      break;
    }
  }

  if (pVg == NULL) {
    tscError("consumer:0x%" PRIx64 " invalid vgroup id:%d", tmq->consumerId, vgId);
    return TSDB_CODE_INVALID_PARA;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;

  int32_t type = pOffsetInfo->currentOffset.type;
  if (type != TMQ_OFFSET__LOG && !OFFSET_IS_RESET_OFFSET(type)) {
    tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, seek not allowed", tmq->consumerId, type);
    return TSDB_CODE_INVALID_PARA;
  }

  if (type == TMQ_OFFSET__LOG && (offset < pOffsetInfo->walVerBegin || offset > pOffsetInfo->walVerEnd)) {
    tscError("consumer:0x%" PRIx64 " invalid seek params, offset:%" PRId64 ", valid range:[%" PRId64 ", %" PRId64 "]",
             tmq->consumerId, offset, pOffsetInfo->walVerBegin, pOffsetInfo->walVerEnd);
    return TSDB_CODE_INVALID_PARA;
  }

  // update the offset, and then commit to vnode
  if (pOffsetInfo->currentOffset.type == TMQ_OFFSET__LOG) {
    pOffsetInfo->currentOffset.version = offset;
    pOffsetInfo->committedOffset.version = INT64_MIN;
    pVg->seekUpdated = true;
  }

  SMqRspObj rspObj = {.resType = RES_TYPE__TMQ, .vgId = pVg->vgId};
  tstrncpy(rspObj.topic, tname, tListLen(rspObj.topic));

  tscDebug("consumer:0x%" PRIx64 " seek to %" PRId64 " on vgId:%d", tmq->consumerId, offset, pVg->vgId);

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tscError("consumer:0x%"PRIx64" failed to prepare seek operation", tmq->consumerId);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tsem_init(&pInfo->sem, 0, 0);
  pInfo->code = 0;

  asyncCommitOffset(tmq, &rspObj, TDMT_VND_TMQ_SEEK_TO_OFFSET, commitCallBackFn, pInfo);

  tsem_wait(&pInfo->sem);
  int32_t code = pInfo->code;

  tsem_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);

  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 " failed to send seek to vgId:%d, code:%s", tmq->consumerId, pVg->vgId,
             tstrerror(code));
  }

  return code;
}
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
#define DEFAULT_HEARTBEAT_INTERVAL     3000
#define DEFAULT_ASKEP_INTERVAL         1000

struct SMqMgmt {
  tmr_h   timer;
  int32_t rsetId;
};

static TdThreadOnce   tmqInit = PTHREAD_ONCE_INIT;  // initialize only once
volatile int32_t      tmqInitRes = 0;               // initialize rsp code
static struct SMqMgmt tmqMgmt = {0};
static int8_t         pollFlag = 0;

typedef struct {
  int32_t code;
  int8_t  tmqRspType;
  int32_t epoch;
} SMqRspWrapper;

typedef struct {
  int32_t     code;
  int8_t      tmqRspType;
  int32_t     epoch;
  SMqAskEpRsp msg;
} SMqAskEpRspWrapper;

struct tmq_list_t {
  SArray container;
};

struct tmq_conf_t {
  char           clientId[TSDB_CLIENT_ID_LEN];
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         autoCommit;
  int8_t         resetOffset;
  int8_t         withTbName;
  int8_t         snapEnable;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
  uint16_t       port;
  int32_t        autoCommitInterval;
  int32_t        sessionTimeoutMs;
  int32_t        heartBeatIntervalMs;
  int32_t        maxPollIntervalMs;
  char*          ip;
  char*          user;
  char*          pass;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
  int8_t         enableBatchMeta;
};

struct tmq_t {
  int64_t        refId;
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[TSDB_CLIENT_ID_LEN];
  char           user[TSDB_USER_LEN];
  char           fqdn[TSDB_FQDN_LEN];
  int8_t         withTbName;
  int8_t         useSnapshot;
  int8_t         autoCommit;
  int32_t        autoCommitInterval;
  int32_t        sessionTimeoutMs;
  int32_t        heartBeatIntervalMs;
  int32_t        maxPollIntervalMs;
  int8_t         resetOffsetCfg;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
  int64_t        consumerId;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
  int8_t         enableBatchMeta;

  // status
  SRWLatch lock;
  int8_t   status;
  int32_t  epoch;
  // poll info
  int64_t pollCnt;
  int64_t totalRows;

  // timer
  tmr_h       hbLiveTimer;
  tmr_h       epTimer;
  tmr_h       commitTimer;
  STscObj*    pTscObj;       // connection
  SArray*     clientTopics;  // SArray<SMqClientTopic>
  STaosQueue* mqueue;        // queue of rsp
  STaosQall*  qall;
  STaosQueue* delayedTask;  // delayed task queue for heartbeat and auto commit
  tsem2_t     rspSem;
};

typedef struct SAskEpInfo {
  int32_t code;
  tsem2_t sem;
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
  TMQ_CONSUMER_STATUS__CLOSED,
};

enum {
  TMQ_DELAYED_TASK__ASK_EP = 1,
  TMQ_DELAYED_TASK__COMMIT,
};

typedef struct SVgOffsetInfo {
  STqOffsetVal committedOffset;
  STqOffsetVal endOffset;    // the last version in TAOS_RES + 1
  STqOffsetVal beginOffset;  // the first version in TAOS_RES
  int64_t      walVerBegin;
  int64_t      walVerEnd;
} SVgOffsetInfo;

typedef struct {
  int64_t       pollCnt;
  int64_t       numOfRows;
  SVgOffsetInfo offsetInfo;
  int32_t       vgId;
  int32_t       vgStatus;
  int32_t       vgSkipCnt;            // here used to mark the slow vgroups
  int64_t       emptyBlockReceiveTs;  // once empty block is received, idle for ignoreCnt then start to poll data
  int64_t       blockReceiveTs;       // once empty block is received, idle for ignoreCnt then start to poll data
  int64_t       blockSleepForReplay;  // once empty block is received, idle for ignoreCnt then start to poll data
  bool          seekUpdated;          // offset is updated by seek operator, therefore, not update by vnode rsp.
  SEpSet        epSet;
} SMqClientVg;

typedef struct {
  char           topicName[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqClientVg>
  SSchemaWrapper schema;
  int8_t         noPrivilege;
} SMqClientTopic;

typedef struct {
  int32_t         code;
  int8_t          tmqRspType;
  int32_t         epoch;  // epoch can be used to guard the vgHandle
  int32_t         vgId;
  char            topicName[TSDB_TOPIC_FNAME_LEN];
  SMqClientVg*    vgHandle;
  SMqClientTopic* topicHandle;
  uint64_t        reqId;
  SEpSet*         pEpset;
  union {
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    STaosxRsp       taosxRsp;
    SMqBatchMetaRsp batchMetaRsp;
  };
} SMqPollRspWrapper;

typedef struct {
  tsem2_t rspSem;
  int32_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  int64_t refId;
  bool    sync;
  void*   pParam;
} SMqAskEpCbParam;

typedef struct {
  int64_t  refId;
  char     topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t  vgId;
  uint64_t requestId;  // request id for debug purpose
} SMqPollCbParam;

typedef struct SMqVgCommon {
  tsem2_t       rsp;
  int32_t       numOfRsp;
  SArray*       pList;
  TdThreadMutex mutex;
  int64_t       consumerId;
  char*         pTopicName;
  int32_t       code;
} SMqVgCommon;

typedef struct SMqSeekParam {
  tsem2_t sem;
  int32_t code;
} SMqSeekParam;

typedef struct SMqCommittedParam {
  tsem2_t     sem;
  int32_t     code;
  SMqVgOffset vgOffset;
} SMqCommittedParam;

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
  int32_t        code;
  tmq_commit_cb* callbackFn;
  void*          userParam;
} SMqCommitCbParamSet;

typedef struct {
  SMqCommitCbParamSet* params;
  char                 topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t              vgId;
  int64_t              consumerId;
} SMqCommitCbParam;

typedef struct SSyncCommitInfo {
  tsem2_t sem;
  int32_t code;
} SSyncCommitInfo;

static int32_t syncAskEp(tmq_t* tmq);
static int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet);
static int32_t doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                               SMqCommitCbParamSet* pParamSet);
static int32_t commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId);
static int32_t askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpset);

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  if (conf == NULL) {
    return conf;
  }

  conf->withTbName = false;
  conf->autoCommit = true;
  conf->autoCommitInterval = DEFAULT_AUTO_COMMIT_INTERVAL;
  conf->resetOffset = TMQ_OFFSET__RESET_LATEST;
  conf->enableBatchMeta = false;
  conf->heartBeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL;
  conf->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
  conf->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;

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
  if (conf == NULL || key == NULL || value == NULL) {
    return TMQ_CONF_INVALID;
  }
  if (strcasecmp(key, "group.id") == 0) {
    tstrncpy(conf->groupId, value, TSDB_CGROUP_LEN);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "client.id") == 0) {
    tstrncpy(conf->clientId, value, TSDB_CLIENT_ID_LEN);
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
    int64_t tmp = taosStr2int64(value);
    if (tmp < 0 || EINVAL == errno || ERANGE == errno) {
      return TMQ_CONF_INVALID;
    }
    conf->autoCommitInterval = (tmp > INT32_MAX ? INT32_MAX : tmp);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "session.timeout.ms") == 0) {
    int64_t tmp = taosStr2int64(value);
    if (tmp < 6000 || tmp > 1800000) {
      return TMQ_CONF_INVALID;
    }
    conf->sessionTimeoutMs = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "heartbeat.interval.ms") == 0) {
    int64_t tmp = taosStr2int64(value);
    if (tmp < 1000 || tmp >= conf->sessionTimeoutMs) {
      return TMQ_CONF_INVALID;
    }
    conf->heartBeatIntervalMs = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "max.poll.interval.ms") == 0) {
    int64_t tmp = taosStr2int64(value);
    if (tmp < 1000 || tmp > INT32_MAX) {
      return TMQ_CONF_INVALID;
    }
    conf->maxPollIntervalMs = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "auto.offset.reset") == 0) {
    if (strcasecmp(value, "none") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_NONE;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "earliest") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_EARLIEST;
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
    int64_t tmp = taosStr2int64(value);
    if (tmp <= 0 || tmp > 65535) {
      return TMQ_CONF_INVALID;
    }

    conf->port = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "enable.replay") == 0) {
    if (strcasecmp(value, "true") == 0) {
      conf->replayEnable = true;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "false") == 0) {
      conf->replayEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }
  if (strcasecmp(key, "msg.consume.excluded") == 0) {
    conf->sourceExcluded = (taosStr2int64(value) != 0) ? TD_REQ_FROM_TAOX : 0;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.db") == 0) {
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "msg.enable.batchmeta") == 0) {
    conf->enableBatchMeta = (taosStr2int64(value) != 0) ? true : false;
    return TMQ_CONF_OK;
  }

  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() { return (tmq_list_t*)taosArrayInit(0, sizeof(void*)); }

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  if (list == NULL) return TSDB_CODE_INVALID_PARA;
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) return TSDB_CODE_INVALID_PARA;
  char* topic = taosStrdup(src);
  if (taosArrayPush(container, &topic) == NULL) return TSDB_CODE_INVALID_PARA;
  return 0;
}

void tmq_list_destroy(tmq_list_t* list) {
  if (list == NULL) return;
  SArray* container = &list->container;
  taosArrayDestroyP(container, taosMemoryFree);
}

int32_t tmq_list_get_size(const tmq_list_t* list) {
  if (list == NULL) return -1;
  const SArray* container = &list->container;
  return taosArrayGetSize(container);
}

char** tmq_list_to_c_array(const tmq_list_t* list) {
  if (list == NULL) return NULL;
  const SArray* container = &list->container;
  return container->pData;
}

static int32_t tmqCommitCb(void* param, SDataBuf* pBuf, int32_t code) {
  SMqCommitCbParam*    pParam = (SMqCommitCbParam*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;

  taosMemoryFree(pBuf->pData);
  taosMemoryFree(pBuf->pEpSet);

  return commitRspCountDown(pParamSet, pParam->consumerId, pParam->topicName, pParam->vgId);
}

static int32_t doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                               SMqCommitCbParamSet* pParamSet) {
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;
  pOffset.offset.val = *offset;
  (void)snprintf(pOffset.offset.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, pTopicName);
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqVgOffset, &pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  if (tEncodeMqVgOffset(&encoder, &pOffset) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return TSDB_CODE_INVALID_PARA;
  }
  tEncoderClear(&encoder);

  // build param
  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  pParam->params = pParamSet;
  pParam->vgId = vgId;
  pParam->consumerId = tmq->consumerId;

  tstrncpy(pParam->topicName, pTopicName, tListLen(pParam->topicName));

  // build send info
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(pParam);
    return terrno;
  }

  pMsgSendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = TDMT_VND_TMQ_COMMIT_OFFSET;

  // int64_t transporterId = 0;
  (void)atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, NULL, pMsgSendInfo);
  if (code != 0) {
    (void)atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
    return code;
  }
  return code;
}

static int32_t getTopicByName(tmq_t* tmq, const char* pTopicName, SMqClientTopic** topic) {
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL || strcmp(pTopic->topicName, pTopicName) != 0) {
      continue;
    }
    *topic = pTopic;
    return 0;
  }

  tscError("consumer:0x%" PRIx64 ", total:%d, failed to find topic:%s", tmq->consumerId, numOfTopics, pTopicName);
  return TSDB_CODE_TMQ_INVALID_TOPIC;
}

static int32_t prepareCommitCbParamSet(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam, int32_t rspNum,
                                       SMqCommitCbParamSet** ppParamSet) {
  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    return terrno;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;
  pParamSet->callbackFn = pCommitFp;
  pParamSet->userParam = userParam;
  pParamSet->waitingRspNum = rspNum;
  *ppParamSet = pParamSet;
  return 0;
}

static int32_t getClientVg(tmq_t* tmq, char* pTopicName, int32_t vgId, SMqClientVg** pVg) {
  SMqClientTopic* pTopic = NULL;
  int32_t         code = getTopicByName(tmq, pTopicName, &pTopic);
  if (code != 0) {
    tscError("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    return code;
  }

  int32_t numOfVgs = taosArrayGetSize(pTopic->vgs);
  for (int32_t i = 0; i < numOfVgs; ++i) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
    if (pClientVg && pClientVg->vgId == vgId) {
      *pVg = pClientVg;
      break;
    }
  }

  return *pVg == NULL ? TSDB_CODE_TMQ_INVALID_VGID : TSDB_CODE_SUCCESS;
}

static int32_t asyncCommitOffset(tmq_t* tmq, char* pTopicName, int32_t vgId, STqOffsetVal* offsetVal,
                                 tmq_commit_cb* pCommitFp, void* userParam) {
  tscInfo("consumer:0x%" PRIx64 " do manual commit offset for %s, vgId:%d", tmq->consumerId, pTopicName, vgId);
  taosRLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, pTopicName, vgId, &pVg);
  if (code != 0) {
    goto end;
  }
  if (offsetVal->type <= 0) {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto end;
  }
  if (tOffsetEqual(offsetVal, &pVg->offsetInfo.committedOffset)) {
    code = TSDB_CODE_TMQ_SAME_COMMITTED_VALUE;
    goto end;
  }
  char offsetBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetBuf, tListLen(offsetBuf), offsetVal);

  char commitBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(commitBuf, tListLen(commitBuf), &pVg->offsetInfo.committedOffset);

  SMqCommitCbParamSet* pParamSet = NULL;
  code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 0, &pParamSet);
  if (code != 0) {
    goto end;
  }

  code = doSendCommitMsg(tmq, pVg->vgId, &pVg->epSet, offsetVal, pTopicName, pParamSet);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 " topic:%s on vgId:%d end commit msg failed, send offset:%s committed:%s, code:%s",
             tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf, tstrerror(terrno));
    taosMemoryFree(pParamSet);
    goto end;
  }

  tscInfo("consumer:0x%" PRIx64 " topic:%s on vgId:%d send commit msg success, send offset:%s committed:%s",
          tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf);
  tOffsetCopy(&pVg->offsetInfo.committedOffset, offsetVal);

end:
  taosRUnLockLatch(&tmq->lock);
  return code;
}

static void asyncCommitFromResult(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* pCommitFp, void* userParam) {
  char*        pTopicName = NULL;
  int32_t      vgId = 0;
  STqOffsetVal offsetVal = {0};
  int32_t      code = 0;

  if (pRes == NULL || tmq == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  if (TD_RES_TMQ(pRes)) {
    SMqRspObj* pRspObj = (SMqRspObj*)pRes;
    pTopicName = pRspObj->common.topic;
    vgId = pRspObj->common.vgId;
    offsetVal = pRspObj->rsp.common.rspOffset;
  } else if (TD_RES_TMQ_META(pRes)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)pRes;
    pTopicName = pMetaRspObj->topic;
    vgId = pMetaRspObj->vgId;
    offsetVal = pMetaRspObj->metaRsp.rspOffset;
  } else if (TD_RES_TMQ_METADATA(pRes)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)pRes;
    pTopicName = pRspObj->common.topic;
    vgId = pRspObj->common.vgId;
    offsetVal = pRspObj->rsp.common.rspOffset;
  } else if (TD_RES_TMQ_BATCH_META(pRes)) {
    SMqBatchMetaRspObj* pBtRspObj = (SMqBatchMetaRspObj*)pRes;
    pTopicName = pBtRspObj->common.topic;
    vgId = pBtRspObj->common.vgId;
    offsetVal = pBtRspObj->rsp.rspOffset;
  } else {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto end;
  }

  code = asyncCommitOffset(tmq, pTopicName, vgId, &offsetVal, pCommitFp, userParam);

end:
  if (code != TSDB_CODE_SUCCESS && pCommitFp != NULL) {
    if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
    pCommitFp(tmq, code, userParam);
  }
}

static void asyncCommitAllOffsets(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam) {
  int32_t code = 0;
  // init as 1 to prevent concurrency issue
  SMqCommitCbParamSet* pParamSet = NULL;
  code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 1, &pParamSet);
  if (code != 0) {
    goto end;
  }
  taosRLockLatch(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      code = TSDB_CODE_TMQ_INVALID_TOPIC;
      taosRUnLockLatch(&tmq->lock);
      goto end;
    }
    int32_t numOfVgroups = taosArrayGetSize(pTopic->vgs);
    tscDebug("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName,
             numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        code = TSDB_CODE_INVALID_PARA;
        taosRUnLockLatch(&tmq->lock);
        goto end;
      }
      if (pVg->offsetInfo.endOffset.type > 0 &&
          !tOffsetEqual(&pVg->offsetInfo.endOffset, &pVg->offsetInfo.committedOffset)) {
        char offsetBuf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(offsetBuf, tListLen(offsetBuf), &pVg->offsetInfo.endOffset);

        char commitBuf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(commitBuf, tListLen(commitBuf), &pVg->offsetInfo.committedOffset);

        code = doSendCommitMsg(tmq, pVg->vgId, &pVg->epSet, &pVg->offsetInfo.endOffset, pTopic->topicName, pParamSet);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("consumer:0x%" PRIx64
                   " topic:%s on vgId:%d end commit msg failed, send offset:%s committed:%s, code:%s ordinal:%d/%d",
                   tmq->consumerId, pTopic->topicName, pVg->vgId, offsetBuf, commitBuf, tstrerror(terrno), j + 1,
                   numOfVgroups);
          continue;
        }

        tscDebug("consumer:0x%" PRIx64
                 " topic:%s on vgId:%d send commit msg success, send offset:%s committed:%s, ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, offsetBuf, commitBuf, j + 1, numOfVgroups);
        tOffsetCopy(&pVg->offsetInfo.committedOffset, &pVg->offsetInfo.endOffset);
      } else {
        tscDebug("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, current:%" PRId64 ", ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->offsetInfo.endOffset.version, j + 1, numOfVgroups);
      }
    }
  }
  taosRUnLockLatch(&tmq->lock);

  tscDebug("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - 1,
           numOfTopics);

  // request is sent
  if (pParamSet->waitingRspNum != 1) {
    // count down since waiting rsp num init as 1
    code = commitRspCountDown(pParamSet, tmq->consumerId, "", 0);
    if (code != 0) {
      tscError("consumer:0x%" PRIx64 " commit rsp count down failed, code:%s", tmq->consumerId, tstrerror(code));
      pParamSet = NULL;
      goto end;
    }
    return;
  }

end:
  taosMemoryFree(pParamSet);
  if (pCommitFp != NULL) {
    pCommitFp(tmq, code, userParam);
  }
  return;
}

static void generateTimedTask(int64_t refId, int32_t type) {
  tmq_t*  tmq = NULL;
  int8_t* pTaskType = NULL;
  int32_t code = 0;

  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  code = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0, (void**)&pTaskType);
  if (code == TSDB_CODE_SUCCESS) {
    *pTaskType = type;
    if (taosWriteQitem(tmq->delayedTask, pTaskType) == 0) {
      (void)tsem2_post(&tmq->rspSem);
    }
  }

  (void)taosReleaseRef(tmqMgmt.rsetId, refId);
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__ASK_EP);
}

void tmqReplayTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  (void)tsem2_post(&tmq->rspSem);
  (void)taosReleaseRef(tmqMgmt.rsetId, refId);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__COMMIT);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (code != 0) {
    goto _return;
  }
  if (pMsg == NULL || param == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    goto _return;
  }

  SMqHbRsp rsp = {0};
  code = tDeserializeSMqHbRsp(pMsg->pData, pMsg->len, &rsp);
  if (code != 0) {
    goto _return;
  }

  int64_t refId = (int64_t)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq != NULL) {
    taosWLockLatch(&tmq->lock);
    for (int32_t i = 0; i < taosArrayGetSize(rsp.topicPrivileges); i++) {
      STopicPrivilege* privilege = taosArrayGet(rsp.topicPrivileges, i);
      if (privilege && privilege->noPrivilege == 1) {
        int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
        for (int32_t j = 0; j < topicNumCur; j++) {
          SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
          if (pTopicCur && strcmp(pTopicCur->topicName, privilege->topic) == 0) {
            tscInfo("consumer:0x%" PRIx64 ", has no privilege, topic:%s", tmq->consumerId, privilege->topic);
            pTopicCur->noPrivilege = 1;
          }
        }
      }
    }
    taosWUnLockLatch(&tmq->lock);
    (void)taosReleaseRef(tmqMgmt.rsetId, refId);
  }

  tDestroySMqHbRsp(&rsp);

_return:

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  return code;
}

void tmqSendHbReq(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    return;
  }

  SMqHbReq req = {0};
  req.consumerId = tmq->consumerId;
  req.epoch = tmq->epoch;
  req.pollFlag = atomic_load_8(&pollFlag);
  req.topics = taosArrayInit(taosArrayGetSize(tmq->clientTopics), sizeof(TopicOffsetRows));
  if (req.topics == NULL) {
    return;
  }
  taosRLockLatch(&tmq->lock);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      continue;
    }
    int32_t          numOfVgroups = taosArrayGetSize(pTopic->vgs);
    TopicOffsetRows* data = taosArrayReserve(req.topics, 1);
    if (data == NULL) {
      continue;
    }
    (void)strcpy(data->topicName, pTopic->topicName);
    data->offsetRows = taosArrayInit(numOfVgroups, sizeof(OffsetRows));
    if (data->offsetRows == NULL) {
      continue;
    }
    for (int j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        continue;
      }
      OffsetRows* offRows = taosArrayReserve(data->offsetRows, 1);
      if (offRows == NULL) {
        continue;
      }
      offRows->vgId = pVg->vgId;
      offRows->rows = pVg->numOfRows;
      offRows->offset = pVg->offsetInfo.endOffset;
      offRows->ever = pVg->offsetInfo.walVerEnd == -1 ? 0 : pVg->offsetInfo.walVerEnd;
      char buf[TSDB_OFFSET_LEN] = {0};
      tFormatOffset(buf, TSDB_OFFSET_LEN, &offRows->offset);
      tscDebug("consumer:0x%" PRIx64 ",report offset, group:%s vgId:%d, offset:%s/%" PRId64 ", rows:%" PRId64,
               tmq->consumerId, tmq->groupId, offRows->vgId, buf, offRows->ever, offRows->rows);
    }
  }
  taosRUnLockLatch(&tmq->lock);

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

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = (void*)refId;
  sendInfo->fp = tmqHbCb;
  sendInfo->msgType = TDMT_MND_TMQ_HB;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int32_t code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    tscError("tmqSendHbReq asyncSendMsgToServer failed");
  }

  (void)atomic_val_compare_exchange_8(&pollFlag, 1, 0);
OVER:
  tDestroySMqHbReq(&req);
  if (tmrId != NULL) {
    (void)taosTmrReset(tmqSendHbReq, tmq->heartBeatIntervalMs, param, tmqMgmt.timer, &tmq->hbLiveTimer);
  }
  (void)taosReleaseRef(tmqMgmt.rsetId, refId);
}

static void defaultCommitCbFn(tmq_t* pTmq, int32_t code, void* param) {
  if (code != 0) {
    tscError("consumer:0x%" PRIx64 ", failed to commit offset, code:%s", pTmq->consumerId, tstrerror(code));
  }
}

void tmqHandleAllDelayedTask(tmq_t* pTmq) {
  STaosQall* qall = NULL;
  int32_t    code = 0;

  code = taosAllocateQall(&qall);
  if (code) {
    tscError("consumer:0x%" PRIx64 ", failed to allocate qall, code:%s", pTmq->consumerId, tstrerror(code));
    return;
  }

  (void)taosReadAllQitems(pTmq->delayedTask, qall);

  int32_t numOfItems = taosQallItemSize(qall);
  if (numOfItems == 0) {
    taosFreeQall(qall);
    return;
  }

  tscDebug("consumer:0x%" PRIx64 " handle delayed %d tasks before poll data", pTmq->consumerId, numOfItems);
  int8_t* pTaskType = NULL;
  (void)taosGetQitem(qall, (void**)&pTaskType);

  while (pTaskType != NULL) {
    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      code = askEp(pTmq, NULL, false, false);
      if (code != 0) {
        tscError("consumer:0x%" PRIx64 " failed to ask ep, code:%s", pTmq->consumerId, tstrerror(code));
        continue;
      }
      tscDebug("consumer:0x%" PRIx64 " retrieve ep from mnode in 1s", pTmq->consumerId);
      (void)taosTmrReset(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(pTmq->refId), tmqMgmt.timer,
                         &pTmq->epTimer);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmq_commit_cb* pCallbackFn = pTmq->commitCb ? pTmq->commitCb : defaultCommitCbFn;
      asyncCommitAllOffsets(pTmq, pCallbackFn, pTmq->commitCbUserParam);
      tscDebug("consumer:0x%" PRIx64 " next commit to vnode(s) in %.2fs", pTmq->consumerId,
               pTmq->autoCommitInterval / 1000.0);
      (void)taosTmrReset(tmqAssignDelayedCommitTask, pTmq->autoCommitInterval, (void*)(pTmq->refId), tmqMgmt.timer,
                         &pTmq->commitTimer);
    } else {
      tscError("consumer:0x%" PRIx64 " invalid task type:%d", pTmq->consumerId, *pTaskType);
    }

    taosFreeQitem(pTaskType);
    (void)taosGetQitem(qall, (void**)&pTaskType);
  }

  taosFreeQall(qall);
}

static void tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)rspWrapper;
    tDeleteSMqAskEpRsp(&pEpRspWrapper->msg);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);

    tDeleteMqDataRsp(&pRsp->dataRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);
    tDeleteMqMetaRsp(&pRsp->metaRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);
    tDeleteSTaosxRsp(&pRsp->taosxRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    SMqPollRspWrapper* pRsp = (SMqPollRspWrapper*)rspWrapper;
    taosMemoryFreeClear(pRsp->pEpset);
    tDeleteMqBatchMetaRsp(&pRsp->batchMetaRsp);
  }
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  SMqRspWrapper* rspWrapper = NULL;
  while (1) {
    (void)taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper) {
      tmqFreeRspWrapper(rspWrapper);
      taosFreeQitem(rspWrapper);
    } else {
      break;
    }
  }

  rspWrapper = NULL;
  (void)taosReadAllQitems(tmq->mqueue, tmq->qall);
  while (1) {
    (void)taosGetQitem(tmq->qall, (void**)&rspWrapper);
    if (rspWrapper) {
      tmqFreeRspWrapper(rspWrapper);
      taosFreeQitem(rspWrapper);
    } else {
      break;
    }
  }
}

int32_t tmqSubscribeCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (param == NULL) {
    return code;
  }

  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;

  if (pMsg) {
    taosMemoryFree(pMsg->pEpSet);
  }
  (void)tsem2_post(&pParam->rspSem);
  return 0;
}

int32_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  if (*topics == NULL) {
    *topics = tmq_list_new();
    if (*topics == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  taosRLockLatch(&tmq->lock);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    if (topic == NULL) {
      tscError("topic is null");
      continue;
    }
    char* tmp = strchr(topic->topicName, '.');
    if (tmp == NULL) {
      tscError("topic name is invalid:%s", topic->topicName);
      continue;
    }
    if (tmq_list_append(*topics, tmp + 1) != 0) {
      tscError("failed to append topic:%s", tmp + 1);
      continue;
    }
  }
  taosRUnLockLatch(&tmq->lock);
  return 0;
}

static void freeClientVg(void* param) {
  SMqClientVg* pVg = param;
  tOffsetDestroy(&pVg->offsetInfo.endOffset);
  tOffsetDestroy(&pVg->offsetInfo.beginOffset);
  tOffsetDestroy(&pVg->offsetInfo.committedOffset);
}
static void freeClientTopic(void* param) {
  SMqClientTopic* pTopic = param;
  taosMemoryFreeClear(pTopic->schema.pSchema);
  taosArrayDestroyEx(pTopic->vgs, freeClientVg);
}

void tmqFreeImpl(void* handle) {
  tmq_t*  tmq = (tmq_t*)handle;
  int64_t id = tmq->consumerId;

  if (tmq->mqueue) {
    tmqClearUnhandleMsg(tmq);
    taosCloseQueue(tmq->mqueue);
  }

  if (tmq->delayedTask) {
    taosCloseQueue(tmq->delayedTask);
  }

  taosFreeQall(tmq->qall);
  (void)tsem2_destroy(&tmq->rspSem);

  taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  taos_close_internal(tmq->pTscObj);

  if (tmq->commitTimer) {
    (void)taosTmrStopA(&tmq->commitTimer);
  }
  if (tmq->epTimer) {
    (void)taosTmrStopA(&tmq->epTimer);
  }
  if (tmq->hbLiveTimer) {
    (void)taosTmrStopA(&tmq->hbLiveTimer);
  }
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

void tmqMgmtClose(void) {
  if (tmqMgmt.timer) {
    taosTmrCleanUp(tmqMgmt.timer);
    tmqMgmt.timer = NULL;
  }

  if (tmqMgmt.rsetId >= 0) {
    taosCloseRef(tmqMgmt.rsetId);
    tmqMgmt.rsetId = -1;
  }
}

#define SET_ERROR_MSG_TMQ(MSG) \
  if (errstr != NULL) (void)snprintf(errstr, errstrLen, MSG);

tmq_t* tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  int32_t code = 0;

  if (conf == NULL) {
    SET_ERROR_MSG_TMQ("configure is null")
    return NULL;
  }
  code = taosThreadOnce(&tmqInit, tmqMgmtInit);
  if (code != 0) {
    SET_ERROR_MSG_TMQ("tmq init error")
    return NULL;
  }
  if (tmqInitRes != 0) {
    SET_ERROR_MSG_TMQ("tmq timer init error")
    return NULL;
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    tscError("failed to create consumer, groupId:%s", conf->groupId);
    SET_ERROR_MSG_TMQ("malloc tmq failed")
    return NULL;
  }

  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  if (pTmq->clientTopics == NULL) {
    tscError("failed to create consumer, groupId:%s", conf->groupId);
    SET_ERROR_MSG_TMQ("malloc client topics failed")
    goto _failed;
  }
  code = taosOpenQueue(&pTmq->mqueue);
  if (code) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("open queue failed")
    goto _failed;
  }

  code = taosOpenQueue(&pTmq->delayedTask);
  if (code) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("open delayed task queue failed")
    goto _failed;
  }

  code = taosAllocateQall(&pTmq->qall);
  if (code) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("allocate qall failed")
    goto _failed;
  }

  if (conf->groupId[0] == 0) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("malloc tmq element failed or group is empty")
    goto _failed;
  }

  // init status
  pTmq->status = TMQ_CONSUMER_STATUS__INIT;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;

  // set conf
  (void)strcpy(pTmq->clientId, conf->clientId);
  (void)strcpy(pTmq->groupId, conf->groupId);
  pTmq->withTbName = conf->withTbName;
  pTmq->useSnapshot = conf->snapEnable;
  pTmq->autoCommit = conf->autoCommit;
  pTmq->autoCommitInterval = conf->autoCommitInterval;
  pTmq->sessionTimeoutMs = conf->sessionTimeoutMs;
  pTmq->heartBeatIntervalMs = conf->heartBeatIntervalMs;
  pTmq->maxPollIntervalMs = conf->maxPollIntervalMs;
  pTmq->commitCb = conf->commitCb;
  pTmq->commitCbUserParam = conf->commitCbUserParam;
  pTmq->resetOffsetCfg = conf->resetOffset;
  pTmq->replayEnable = conf->replayEnable;
  pTmq->sourceExcluded = conf->sourceExcluded;
  pTmq->enableBatchMeta = conf->enableBatchMeta;
  tstrncpy(pTmq->user, user, TSDB_USER_LEN);
  if (taosGetFqdn(pTmq->fqdn) != 0) {
    (void)strcpy(pTmq->fqdn, "localhost");
  }
  if (conf->replayEnable) {
    pTmq->autoCommit = false;
  }
  taosInitRWLatch(&pTmq->lock);

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem2_init(&pTmq->rspSem, 0, 0) != 0) {
    tscError("consumer:0x %" PRIx64 " setup failed since %s, consumer group %s", pTmq->consumerId,
             tstrerror(TAOS_SYSTEM_ERROR(errno)), pTmq->groupId);
    SET_ERROR_MSG_TMQ("init t_sem failed")
    goto _failed;
  }

  // init connection
  code = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ, &pTmq->pTscObj);
  if (code) {
    terrno = code;
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
    (void)tsem2_destroy(&pTmq->rspSem);
    SET_ERROR_MSG_TMQ("init tscObj failed")
    goto _failed;
  }

  pTmq->refId = taosAddRef(tmqMgmt.rsetId, pTmq);
  if (pTmq->refId < 0) {
    SET_ERROR_MSG_TMQ("add tscObj ref failed")
    goto _failed;
  }

  pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, pTmq->heartBeatIntervalMs, (void*)pTmq->refId, tmqMgmt.timer);
  if (pTmq->hbLiveTimer == NULL) {
    SET_ERROR_MSG_TMQ("start heartbeat timer failed")
    goto _failed;
  }
  char         buf[TSDB_OFFSET_LEN] = {0};
  STqOffsetVal offset = {.type = pTmq->resetOffsetCfg};
  tFormatOffset(buf, tListLen(buf), &offset);
  tscInfo("consumer:0x%" PRIx64 " is setup, refId:%" PRId64
          ", groupId:%s, snapshot:%d, autoCommit:%d, commitInterval:%dms, offset:%s",
          pTmq->consumerId, pTmq->refId, pTmq->groupId, pTmq->useSnapshot, pTmq->autoCommit, pTmq->autoCommitInterval,
          buf);

  return pTmq;

_failed:
  tmqFreeImpl(pTmq);
  return NULL;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  if (tmq == NULL || topic_list == NULL) return TSDB_CODE_INVALID_PARA;
  const int32_t   MAX_RETRY_COUNT = 120 * 2;  // let's wait for 2 mins at most
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = 0;

  tscInfo("consumer:0x%" PRIx64 " cgroup:%s, subscribe %d topics", tmq->consumerId, tmq->groupId, sz);

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, TSDB_CLIENT_ID_LEN);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  tstrncpy(req.user, tmq->user, TSDB_USER_LEN);
  tstrncpy(req.fqdn, tmq->fqdn, TSDB_FQDN_LEN);

  req.topicNames = taosArrayInit(sz, sizeof(void*));
  if (req.topicNames == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  req.withTbName = tmq->withTbName;
  req.autoCommit = tmq->autoCommit;
  req.autoCommitInterval = tmq->autoCommitInterval;
  req.sessionTimeoutMs = tmq->sessionTimeoutMs;
  req.maxPollIntervalMs = tmq->maxPollIntervalMs;
  req.resetOffsetCfg = tmq->resetOffsetCfg;
  req.enableReplay = tmq->replayEnable;
  req.enableBatchMeta = tmq->enableBatchMeta;

  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(container, i);
    if (topic == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto FAIL;
    }
    SName name = {0};
    code = tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));
    if (code) {
      tscError("consumer:0x%" PRIx64 " cgroup:%s, failed to set topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      goto FAIL;
    }
    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      code = terrno;
      goto FAIL;
    }

    code = tNameExtractFullName(&name, topicFName);
    if (code) {
      tscError("consumer:0x%" PRIx64 " cgroup:%s, failed to extract topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      taosMemoryFree(topicFName);
      goto FAIL;
    }

    if (taosArrayPush(req.topicNames, &topicFName) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(topicFName);
      goto FAIL;
    }
    tscInfo("consumer:0x%" PRIx64 " subscribe topic:%s", tmq->consumerId, topicFName);
  }

  int32_t tlen = tSerializeSCMSubscribeReq(NULL, &req);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  void* abuf = buf;
  (void)tSerializeSCMSubscribeReq(&abuf, &req);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = terrno;
    taosMemoryFree(buf);
    goto FAIL;
  }

  SMqSubscribeCbParam param = {.rspErr = 0};
  if (tsem2_init(&param.rspSem, 0, 0) != 0) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    goto FAIL;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = tlen, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = &param;
  sendInfo->fp = tmqSubscribeCb;
  sendInfo->msgType = TDMT_MND_TMQ_SUBSCRIBE;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    goto FAIL;
  }

  (void)tsem2_wait(&param.rspSem);
  (void)tsem2_destroy(&param.rspSem);

  if (param.rspErr != 0) {
    code = param.rspErr;
    goto FAIL;
  }

  int32_t retryCnt = 0;
  while ((code = syncAskEp(tmq)) != 0) {
    if (retryCnt++ > MAX_RETRY_COUNT || code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
      tscError("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry more than 2 minutes, code:%s",
               tmq->consumerId, tstrerror(code));
      if (code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
        code = 0;
      }
      goto FAIL;
    }

    tscInfo("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
    taosMsleep(500);
  }

  tmq->epTimer = taosTmrStart(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(tmq->refId), tmqMgmt.timer);
  tmq->commitTimer =
      taosTmrStart(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, (void*)(tmq->refId), tmqMgmt.timer);
  if (tmq->epTimer == NULL || tmq->commitTimer == NULL) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto FAIL;
  }

FAIL:
  taosArrayDestroyP(req.topicNames, taosMemoryFree);

  return code;
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  if (conf == NULL) return;
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

static void getVgInfo(tmq_t* tmq, char* topicName, int32_t vgId, SMqClientVg** pVg) {
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int i = 0; i < topicNumCur; i++) {
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur && strcmp(pTopicCur->topicName, topicName) == 0) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur && pVgCur->vgId == vgId) {
          *pVg = pVgCur;
          return;
        }
      }
    }
  }
}

static SMqClientTopic* getTopicInfo(tmq_t* tmq, char* topicName) {
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int i = 0; i < topicNumCur; i++) {
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopicCur->topicName, topicName) == 0) {
      return pTopicCur;
    }
  }
  return NULL;
}

static void setVgIdle(tmq_t* tmq, char* topicName, int32_t vgId) {
  taosWLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  getVgInfo(tmq, topicName, vgId, &pVg);
  if (pVg) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  taosWUnLockLatch(&tmq->lock);
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  tmq_t*             tmq = NULL;
  SMqPollRspWrapper* pRspWrapper = NULL;
  int8_t             rspType = 0;
  int32_t            vgId = 0;
  uint64_t           requestId = 0;
  SMqPollCbParam*    pParam = (SMqPollCbParam*)param;
  if (pMsg == NULL) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (pParam == NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  int64_t refId = pParam->refId;
  vgId = pParam->vgId;
  requestId = pParam->requestId;
  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
    return TSDB_CODE_TMQ_CONSUMER_CLOSED;
  }

  int32_t ret = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0, (void**)&pRspWrapper);
  if (ret) {
    code = ret;
    tscWarn("consumer:0x%" PRIx64 " msg discard from vgId:%d, since out of memory", tmq->consumerId, vgId);
    goto END;
  }

  if (code != 0) {
    goto END;
  }

  if (pMsg->pData == NULL) {
    tscError("consumer:0x%" PRIx64 " msg discard from vgId:%d, since msg is NULL", tmq->consumerId, vgId);
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t clientEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < clientEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("consumer:0x%" PRIx64
            " msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d,QID:0x%" PRIx64,
            tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto END;
  }

  if (msgEpoch != clientEpoch) {
    tscError("consumer:0x%" PRIx64
             " msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto END;
  }
  // handle meta rsp
  rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;
  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->reqId = requestId;
  pRspWrapper->pEpset = pMsg->pEpSet;
  pMsg->pEpSet = NULL;

  if (rspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    if (tDecodeMqDataRsp(&decoder, &pRspWrapper->dataRsp) < 0) {
      tDecoderClear(&decoder);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    tDecoderClear(&decoder);
    (void)memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));

    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &pRspWrapper->dataRsp.common.rspOffset);
    tscDebug("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, req ver:%" PRId64 ", rsp:%s type %d,QID:0x%" PRIx64,
             tmq->consumerId, vgId, pRspWrapper->dataRsp.common.reqOffset.version, buf, rspType, requestId);
  } else if (rspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    if (tDecodeMqMetaRsp(&decoder, &pRspWrapper->metaRsp) < 0) {
      tDecoderClear(&decoder);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    tDecoderClear(&decoder);
    (void)memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
  } else if (rspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    if (tDecodeSTaosxRsp(&decoder, &pRspWrapper->taosxRsp) < 0) {
      tDecoderClear(&decoder);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    tDecoderClear(&decoder);
    (void)memcpy(&pRspWrapper->taosxRsp, pMsg->pData, sizeof(SMqRspHead));
  } else if (rspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    if (tSemiDecodeMqBatchMetaRsp(&decoder, &pRspWrapper->batchMetaRsp) < 0) {
      tDecoderClear(&decoder);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    tDecoderClear(&decoder);
    (void)memcpy(&pRspWrapper->batchMetaRsp, pMsg->pData, sizeof(SMqRspHead));
    tscDebug("consumer:0x%" PRIx64 " recv poll batchmeta rsp, vgId:%d,QID:0x%" PRIx64, tmq->consumerId, vgId,
             requestId);
  } else {  // invalid rspType
    tscError("consumer:0x%" PRIx64 " invalid rsp msg received, type:%d ignored", tmq->consumerId, rspType);
  }

END:
  if (pRspWrapper) {
    pRspWrapper->code = code;
    pRspWrapper->vgId = vgId;
    (void)strcpy(pRspWrapper->topicName, pParam->topicName);
    code = taosWriteQitem(tmq->mqueue, pRspWrapper);
    if (code != 0) {
      tscError("consumer:0x%" PRIx64 " put poll res into mqueue failed, code:%d", tmq->consumerId, code);
    }
  }
  int32_t total = taosQueueItemSize(tmq->mqueue);
  tscDebug("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d, vgId:%d, total in queue:%d,QID:0x%" PRIx64,
           tmq ? tmq->consumerId : 0, rspType, vgId, total, requestId);

  if (tmq) (void)tsem2_post(&tmq->rspSem);
  if (pMsg) taosMemoryFreeClear(pMsg->pData);
  if (pMsg) taosMemoryFreeClear(pMsg->pEpSet);
  (void)taosReleaseRef(tmqMgmt.rsetId, refId);

  return code;
}

typedef struct SVgroupSaveInfo {
  STqOffsetVal currentOffset;
  STqOffsetVal commitOffset;
  STqOffsetVal seekOffset;
  int64_t      numOfRows;
  int32_t      vgStatus;
} SVgroupSaveInfo;

static void initClientTopicFromRsp(SMqClientTopic* pTopic, SMqSubTopicEp* pTopicEp, SHashObj* pVgOffsetHashMap,
                                   tmq_t* tmq) {
  pTopic->schema = pTopicEp->schema;
  pTopicEp->schema.nCols = 0;
  pTopicEp->schema.pSchema = NULL;

  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
  int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);

  tstrncpy(pTopic->topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pTopic->db, pTopicEp->db, TSDB_DB_FNAME_LEN);

  tscInfo("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d", tmq->consumerId, pTopic->topicName, vgNumGet);
  pTopic->vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));
  if (pTopic->vgs == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to init vgs for topic:%s", tmq->consumerId, pTopic->topicName);
    return;
  }
  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
    if (pVgEp == NULL) {
      continue;
    }
    (void)sprintf(vgKey, "%s:%d", pTopic->topicName, pVgEp->vgId);
    SVgroupSaveInfo* pInfo = taosHashGet(pVgOffsetHashMap, vgKey, strlen(vgKey));

    STqOffsetVal offsetNew = {0};
    offsetNew.type = tmq->resetOffsetCfg;

    tscInfo("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d, num:%d, port:%d", tmq->consumerId,
            pTopic->topicName, vgNumGet, pVgEp->epSet.numOfEps, pVgEp->epSet.eps[pVgEp->epSet.inUse].port);

    SMqClientVg clientVg = {
        .pollCnt = 0,
        .vgId = pVgEp->vgId,
        .epSet = pVgEp->epSet,
        .vgStatus = pInfo ? pInfo->vgStatus : TMQ_VG_STATUS__IDLE,
        .vgSkipCnt = 0,
        .emptyBlockReceiveTs = 0,
        .blockReceiveTs = 0,
        .blockSleepForReplay = 0,
        .numOfRows = pInfo ? pInfo->numOfRows : 0,
    };

    clientVg.offsetInfo.walVerBegin = -1;
    clientVg.offsetInfo.walVerEnd = -1;
    clientVg.seekUpdated = false;
    if (pInfo) {
      tOffsetCopy(&clientVg.offsetInfo.endOffset, &pInfo->currentOffset);
      tOffsetCopy(&clientVg.offsetInfo.committedOffset, &pInfo->commitOffset);
      tOffsetCopy(&clientVg.offsetInfo.beginOffset, &pInfo->seekOffset);
    } else {
      clientVg.offsetInfo.endOffset = offsetNew;
      clientVg.offsetInfo.committedOffset = offsetNew;
      clientVg.offsetInfo.beginOffset = offsetNew;
    }
    if (taosArrayPush(pTopic->vgs, &clientVg) == NULL) {
      tscError("consumer:0x%" PRIx64 ", failed to push vg:%d into topic:%s", tmq->consumerId, pVgEp->vgId,
               pTopic->topicName);
      freeClientVg(&clientVg);
    }
  }
}

static bool doUpdateLocalEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  bool set = false;

  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  if (epoch < tmq->epoch || (epoch == tmq->epoch && topicNumGet == 0)) {
    tscDebug("consumer:0x%" PRIx64 " no update ep epoch from %d to epoch %d, incoming topics:%d", tmq->consumerId,
             tmq->epoch, epoch, topicNumGet);
    if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER) {
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__READY);
    }
    return false;
  }

  SArray* newTopics = taosArrayInit(topicNumGet, sizeof(SMqClientTopic));
  if (newTopics == NULL) {
    return false;
  }

  SHashObj* pVgOffsetHashMap = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pVgOffsetHashMap == NULL) {
    (void)taosArrayDestroy(newTopics);
    return false;
  }

  taosWLockLatch(&tmq->lock);
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);

  char vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
  tscInfo("consumer:0x%" PRIx64 " update ep epoch from %d to epoch %d, incoming topics:%d, existed topics:%d",
          tmq->consumerId, tmq->epoch, epoch, topicNumGet, topicNumCur);
  for (int32_t i = 0; i < topicNumCur; i++) {
    // find old topic
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur && pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tscInfo("consumer:0x%" PRIx64 ", current vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur == NULL) {
          continue;
        }
        (void)sprintf(vgKey, "%s:%d", pTopicCur->topicName, pVgCur->vgId);

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVgCur->offsetInfo.endOffset);
        tscInfo("consumer:0x%" PRIx64 ", epoch:%d vgId:%d vgKey:%s, offset:%s", tmq->consumerId, epoch, pVgCur->vgId,
                vgKey, buf);

        SVgroupSaveInfo info = {.currentOffset = pVgCur->offsetInfo.endOffset,
                                .seekOffset = pVgCur->offsetInfo.beginOffset,
                                .commitOffset = pVgCur->offsetInfo.committedOffset,
                                .numOfRows = pVgCur->numOfRows,
                                .vgStatus = pVgCur->vgStatus};
        if (taosHashPut(pVgOffsetHashMap, vgKey, strlen(vgKey), &info, sizeof(SVgroupSaveInfo)) != 0) {
          tscError("consumer:0x%" PRIx64 ", failed to put vg:%d into hashmap", tmq->consumerId, pVgCur->vgId);
        }
      }
    }
  }

  for (int32_t i = 0; i < topicNumGet; i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    if (pTopicEp == NULL) {
      continue;
    }
    initClientTopicFromRsp(&topic, pTopicEp, pVgOffsetHashMap, tmq);
    if (taosArrayPush(newTopics, &topic) == NULL) {
      tscError("consumer:0x%" PRIx64 ", failed to push topic:%s into new topics", tmq->consumerId, topic.topicName);
      freeClientTopic(&topic);
    }
  }

  taosHashCleanup(pVgOffsetHashMap);

  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  }
  tmq->clientTopics = newTopics;
  taosWUnLockLatch(&tmq->lock);

  int8_t flag = (topicNumGet == 0) ? TMQ_CONSUMER_STATUS__NO_TOPIC : TMQ_CONSUMER_STATUS__READY;
  atomic_store_8(&tmq->status, flag);
  atomic_store_32(&tmq->epoch, epoch);

  tscInfo("consumer:0x%" PRIx64 " update topic info completed", tmq->consumerId);
  return set;
}

void tmqBuildConsumeReqImpl(SMqPollReq* pReq, tmq_t* tmq, int64_t timeout, SMqClientTopic* pTopic, SMqClientVg* pVg) {
  (void)snprintf(pReq->subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, pTopic->topicName);
  pReq->withTbName = tmq->withTbName;
  pReq->consumerId = tmq->consumerId;
  pReq->timeout = timeout;
  pReq->epoch = tmq->epoch;
  pReq->reqOffset = pVg->offsetInfo.endOffset;
  pReq->head.vgId = pVg->vgId;
  pReq->useSnapshot = tmq->useSnapshot;
  pReq->reqId = generateRequestId();
  pReq->enableReplay = tmq->replayEnable;
  pReq->sourceExcluded = tmq->sourceExcluded;
  pReq->enableBatchMeta = tmq->enableBatchMeta;
}

int32_t tmqBuildMetaRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqMetaRspObj** ppRspObj) {
  SMqMetaRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqMetaRspObj));
  if (pRspObj == NULL) {
    return terrno;
  }
  pRspObj->resType = RES_TYPE__TMQ_META;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pWrapper->vgHandle->vgId;

  (void)memcpy(&pRspObj->metaRsp, &pWrapper->metaRsp, sizeof(SMqMetaRsp));
  *ppRspObj = pRspObj;
  return 0;
}

int32_t tmqBuildBatchMetaRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqBatchMetaRspObj** ppRspObj) {
  SMqBatchMetaRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqBatchMetaRspObj));
  if (pRspObj == NULL) {
    return terrno;
  }
  pRspObj->common.resType = RES_TYPE__TMQ_BATCH_META;
  tstrncpy(pRspObj->common.topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->common.db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->common.vgId = pWrapper->vgHandle->vgId;

  (void)memcpy(&pRspObj->rsp, &pWrapper->batchMetaRsp, sizeof(SMqBatchMetaRsp));
  tscDebug("build batchmeta Rsp from wrapper");
  *ppRspObj = pRspObj;
  return 0;
}

void changeByteEndian(char* pData) {
  if (pData == NULL) {
    return;
  }
  char* p = pData;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column
  // length | version:
  int32_t blockVersion = *(int32_t*)p;
  if (blockVersion != BLOCK_VERSION_1) {
    tscError("invalid block version:%d", blockVersion);
    return;
  }
  *(int32_t*)p = BLOCK_VERSION_2;

  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  int32_t cols = *(int32_t*)p;
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(uint64_t);
  // check fields
  p += cols * (sizeof(int8_t) + sizeof(int32_t));

  int32_t* colLength = (int32_t*)p;
  for (int32_t i = 0; i < cols; ++i) {
    colLength[i] = htonl(colLength[i]);
  }
}

static void tmqGetRawDataRowsPrecisionFromRes(void* pRetrieve, void** rawData, int64_t* rows, int32_t* precision) {
  if (pRetrieve == NULL) {
    return;
  }
  if (*(int64_t*)pRetrieve == 0) {
    *rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
    *rows = htobe64(((SRetrieveTableRsp*)pRetrieve)->numOfRows);
    if (precision != NULL) {
      *precision = ((SRetrieveTableRsp*)pRetrieve)->precision;
    }
  } else if (*(int64_t*)pRetrieve == 1) {
    *rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
    *rows = htobe64(((SRetrieveTableRspForTmq*)pRetrieve)->numOfRows);
    if (precision != NULL) {
      *precision = ((SRetrieveTableRspForTmq*)pRetrieve)->precision;
    }
  }
}

static void tmqBuildRspFromWrapperInner(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows,
                                        SMqRspObjCommon* pRspObj, SMqDataRspCommon* pDataRsp) {
  (*numOfRows) = 0;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);

  pRspObj->vgId = pWrapper->vgHandle->vgId;
  pRspObj->resIter = -1;

  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;

  bool needTransformSchema = !pDataRsp->withSchema;
  if (!pDataRsp->withSchema) {  // withSchema is false if subscribe subquery, true if subscribe db or stable
    pDataRsp->withSchema = true;
    pDataRsp->blockSchema = taosArrayInit(pDataRsp->blockNum, sizeof(void*));
    if (pDataRsp->blockSchema == NULL) {
      tscError("failed to allocate memory for blockSchema");
      return;
    }
  }
  // extract the rows in this data packet
  for (int32_t i = 0; i < pDataRsp->blockNum; ++i) {
    void*   pRetrieve = taosArrayGetP(pDataRsp->blockData, i);
    void*   rawData = NULL;
    int64_t rows = 0;
    // deal with compatibility
    tmqGetRawDataRowsPrecisionFromRes(pRetrieve, &rawData, &rows, NULL);

    pVg->numOfRows += rows;
    (*numOfRows) += rows;
    changeByteEndian(rawData);
    if (needTransformSchema) {  // withSchema is false if subscribe subquery, true if subscribe db or stable
      SSchemaWrapper* schema = tCloneSSchemaWrapper(&pWrapper->topicHandle->schema);
      if (schema) {
        if (taosArrayPush(pDataRsp->blockSchema, &schema) == NULL) {
          tscError("failed to push schema into blockSchema");
          continue;
        }
      }
    }
  }
}

int32_t tmqBuildRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows,
                               SMqRspObj** ppRspObj) {
  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  if (pRspObj == NULL) {
    return terrno;
  }
  pRspObj->common.resType = RES_TYPE__TMQ;
  (void)memcpy(&pRspObj->rsp, &pWrapper->dataRsp, sizeof(SMqDataRsp));
  tmqBuildRspFromWrapperInner(pWrapper, pVg, numOfRows, &pRspObj->common, &pRspObj->rsp.common);
  *ppRspObj = pRspObj;
  return 0;
}

int32_t tmqBuildTaosxRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows,
                                    SMqTaosxRspObj** ppRspObj) {
  SMqTaosxRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqTaosxRspObj));
  if (pRspObj == NULL) {
    return terrno;
  }
  pRspObj->common.resType = RES_TYPE__TMQ_METADATA;
  (void)memcpy(&pRspObj->rsp, &pWrapper->taosxRsp, sizeof(STaosxRsp));

  tmqBuildRspFromWrapperInner(pWrapper, pVg, numOfRows, &pRspObj->common, &pRspObj->rsp.common);
  *ppRspObj = pRspObj;
  return 0;
}

static int32_t doTmqPollImpl(tmq_t* pTmq, SMqClientTopic* pTopic, SMqClientVg* pVg, int64_t timeout) {
  SMqPollReq      req = {0};
  char*           msg = NULL;
  SMqPollCbParam* pParam = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  int             code = 0;
  tmqBuildConsumeReqImpl(&req, pTmq, timeout, pTopic, pVg);

  int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
  if (msgSize < 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return terrno;
  }

  if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    taosMemoryFreeClear(msg);
    return code;
  }

  pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
  if (pParam == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFreeClear(msg);
    return code;
  }

  pParam->refId = pTmq->refId;
  (void)strcpy(pParam->topicName, pTopic->topicName);
  pParam->vgId = pVg->vgId;
  pParam->requestId = req.reqId;

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFreeClear(pParam);
    taosMemoryFreeClear(msg);
    return terrno;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = req.reqId;
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->paramFreeFp = taosMemoryFree;
  sendInfo->fp = tmqPollCb;
  sendInfo->msgType = TDMT_VND_TMQ_CONSUME;

  // int64_t transporterId = 0;
  char offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->offsetInfo.endOffset);
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, NULL, sendInfo);
  tscDebug("consumer:0x%" PRIx64 " send poll to %s vgId:%d, code:%d, epoch %d, req:%s,QID:0x%" PRIx64, pTmq->consumerId,
           pTopic->topicName, pVg->vgId, code, pTmq->epoch, offsetFormatBuf, req.reqId);
  if (code != 0) {
    return code;
  }

  pVg->pollCnt++;
  pVg->seekUpdated = false;  // reset this flag.
  pTmq->pollCnt++;

  return 0;
}

// broadcast the poll request to all related vnodes
static int32_t tmqPollImpl(tmq_t* tmq, int64_t timeout) {
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__RECOVER) {
    return 0;
  }
  int32_t code = 0;

  taosWLockLatch(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " start to poll data, numOfTopics:%d", tmq->consumerId, numOfTopics);

  for (int i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      continue;
    }
    int32_t numOfVg = taosArrayGetSize(pTopic->vgs);
    if (pTopic->noPrivilege) {
      tscDebug("consumer:0x%" PRIx64 " has no privilegr for topic:%s", tmq->consumerId, pTopic->topicName);
      continue;
    }
    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        continue;
      }
      int64_t elapsed = taosGetTimestampMs() - pVg->emptyBlockReceiveTs;
      if (elapsed < EMPTY_BLOCK_POLL_IDLE_DURATION && elapsed >= 0) {  // less than 10ms
        tscDebug("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for 10ms before start next poll", tmq->consumerId,
                 tmq->epoch, pVg->vgId);
        continue;
      }

      elapsed = taosGetTimestampMs() - pVg->blockReceiveTs;
      if (tmq->replayEnable && elapsed < pVg->blockSleepForReplay && elapsed >= 0) {
        tscDebug("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for %" PRId64 "ms before start next poll when replay",
                 tmq->consumerId, tmq->epoch, pVg->vgId, pVg->blockSleepForReplay);
        continue;
      }

      int32_t vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus == TMQ_VG_STATUS__WAIT) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tscDebug("consumer:0x%" PRIx64 " epoch %d wait poll-rsp, skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch,
                 pVg->vgId, vgSkipCnt);
        continue;
      }

      atomic_store_32(&pVg->vgSkipCnt, 0);
      code = doTmqPollImpl(tmq, pTopic, pVg, timeout);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    }
  }

end:
  taosWUnLockLatch(&tmq->lock);
  tscDebug("consumer:0x%" PRIx64 " end to poll data, code:%d", tmq->consumerId, code);
  return code;
}

static void updateVgInfo(SMqClientVg* pVg, STqOffsetVal* reqOffset, STqOffsetVal* rspOffset, int64_t sver, int64_t ever,
                         int64_t consumerId, bool hasData) {
  if (!pVg->seekUpdated) {
    tscDebug("consumer:0x%" PRIx64 " local offset is update, since seekupdate not set", consumerId);
    if (hasData) {
      tOffsetCopy(&pVg->offsetInfo.beginOffset, reqOffset);
    }
    tOffsetCopy(&pVg->offsetInfo.endOffset, rspOffset);
  } else {
    tscDebug("consumer:0x%" PRIx64 " local offset is NOT update, since seekupdate is set", consumerId);
  }

  // update the status
  atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

  // update the valid wal version range
  pVg->offsetInfo.walVerBegin = sver;
  pVg->offsetInfo.walVerEnd = ever + 1;
}

static void* tmqHandleAllRsp(tmq_t* tmq, int64_t timeout) {
  tscDebug("consumer:0x%" PRIx64 " start to handle the rsp, total:%d", tmq->consumerId, taosQallItemSize(tmq->qall));

  while (1) {
    SMqRspWrapper* pRspWrapper = NULL;
    (void)taosGetQitem(tmq->qall, (void**)&pRspWrapper);

    if (pRspWrapper == NULL) {
      (void)taosReadAllQitems(tmq->mqueue, tmq->qall);
      (void)taosGetQitem(tmq->qall, (void**)&pRspWrapper);
      if (pRspWrapper == NULL) {
        return NULL;
      }
    }

    tscDebug("consumer:0x%" PRIx64 " handle rsp, type:%d", tmq->consumerId, pRspWrapper->tmqRspType);

    if (pRspWrapper->code != 0) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      if (pRspWrapper->code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
        atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__RECOVER);
        tscDebug("consumer:0x%" PRIx64 " wait for the rebalance, set status to be RECOVER", tmq->consumerId);
      } else if (pRspWrapper->code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
        tscInfo("consumer:0x%" PRIx64 " return null since no committed offset", tmq->consumerId);
      } else {
        if (pRspWrapper->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {  // for vnode transform
          int32_t code = askEp(tmq, NULL, false, true);
          if (code != 0) {
            tscError("consumer:0x%" PRIx64 " failed to ask ep, code:%s", tmq->consumerId, tstrerror(code));
          }
        }
        tscError("consumer:0x%" PRIx64 " msg from vgId:%d discarded, since %s", tmq->consumerId, pollRspWrapper->vgId,
                 tstrerror(pRspWrapper->code));
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = NULL;
        getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
        if (pVg) pVg->emptyBlockReceiveTs = taosGetTimestampMs();
        taosWUnLockLatch(&tmq->lock);
      }
      setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
      taosMemoryFreeClear(pollRspWrapper->pEpset);
      tmqFreeRspWrapper(pRspWrapper);
      taosFreeQitem(pRspWrapper);
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;

      int32_t           consumerEpoch = atomic_load_32(&tmq->epoch);
      SMqDataRspCommon* pDataRsp = (SMqDataRspCommon*)&pollRspWrapper->dataRsp;

      if (pDataRsp->head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = NULL;
        getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
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

        updateVgInfo(pVg, &pDataRsp->reqOffset, &pDataRsp->rspOffset, pDataRsp->head.walsver, pDataRsp->head.walever,
                     tmq->consumerId, pDataRsp->blockNum != 0);

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pDataRsp->rspOffset);
        if (pDataRsp->blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " empty block received, vgId:%d, offset:%s, vg total:%" PRId64
                   ", total:%" PRId64 ",QID:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
        } else {  // build rsp
          int64_t    numOfRows = 0;
          SMqRspObj* pRsp = NULL;
          (void)tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows, &pRsp);
          tmq->totalRows += numOfRows;
          pVg->emptyBlockReceiveTs = 0;
          if (pRsp && tmq->replayEnable) {
            pVg->blockReceiveTs = taosGetTimestampMs();
            pVg->blockSleepForReplay = pRsp->rsp.sleepTime;
            if (pVg->blockSleepForReplay > 0) {
              if (taosTmrStart(tmqReplayTask, pVg->blockSleepForReplay, (void*)(tmq->refId), tmqMgmt.timer) == NULL) {
                tscError("consumer:0x%" PRIx64 " failed to start replay timer, vgId:%d, sleep:%" PRId64,
                         tmq->consumerId, pVg->vgId, pVg->blockSleepForReplay);
              }
            }
          }
          tscDebug("consumer:0x%" PRIx64 " process poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                   ", vg total:%" PRId64 ", total:%" PRId64 ",QID:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pDataRsp->blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
                   pollRspWrapper->reqId);
          taosMemoryFreeClear(pollRspWrapper->pEpset);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
          return pRsp;
        }
      } else {
        tscInfo("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                tmq->consumerId, pollRspWrapper->vgId, pDataRsp->head.epoch, consumerEpoch);
        setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      tscDebug("consumer:0x%" PRIx64 " process meta rsp", tmq->consumerId);

      if (pollRspWrapper->metaRsp.head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = NULL;
        getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
          return NULL;
        }

        updateVgInfo(pVg, &pollRspWrapper->metaRsp.rspOffset, &pollRspWrapper->metaRsp.rspOffset,
                     pollRspWrapper->metaRsp.head.walsver, pollRspWrapper->metaRsp.head.walever, tmq->consumerId, true);
        // build rsp
        SMqMetaRspObj* pRsp = NULL;
        (void)tmqBuildMetaRspFromWrapper(pollRspWrapper, &pRsp);
        taosMemoryFreeClear(pollRspWrapper->pEpset);
        taosFreeQitem(pRspWrapper);
        taosWUnLockLatch(&tmq->lock);
        return pRsp;
      } else {
        tscInfo("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                tmq->consumerId, pollRspWrapper->vgId, pollRspWrapper->metaRsp.head.epoch, consumerEpoch);
        setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      tscDebug("consumer:0x%" PRIx64 " process meta rsp", tmq->consumerId);

      if (pollRspWrapper->batchMetaRsp.head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = NULL;
        getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
          return NULL;
        }

        // build rsp
        updateVgInfo(pVg, &pollRspWrapper->batchMetaRsp.rspOffset, &pollRspWrapper->batchMetaRsp.rspOffset,
                     pollRspWrapper->batchMetaRsp.head.walsver, pollRspWrapper->batchMetaRsp.head.walever,
                     tmq->consumerId, true);
        SMqBatchMetaRspObj* pRsp = NULL;
        (void)tmqBuildBatchMetaRspFromWrapper(pollRspWrapper, &pRsp);
        taosMemoryFreeClear(pollRspWrapper->pEpset);
        taosFreeQitem(pRspWrapper);
        taosWUnLockLatch(&tmq->lock);
        return pRsp;
      } else {
        tscInfo("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                tmq->consumerId, pollRspWrapper->vgId, pollRspWrapper->batchMetaRsp.head.epoch, consumerEpoch);
        setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);
      SMqDataRspCommon*  pDataRsp = (SMqDataRspCommon*)&pollRspWrapper->taosxRsp;

      if (pDataRsp->head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = NULL;
        getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
          return NULL;
        }

        updateVgInfo(pVg, &pDataRsp->reqOffset, &pDataRsp->rspOffset, pDataRsp->head.walsver, pDataRsp->head.walever,
                     tmq->consumerId, pDataRsp->blockNum != 0);

        if (pDataRsp->blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " taosx empty block received, vgId:%d, vg total:%" PRId64 ",QID:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, pVg->numOfRows, pollRspWrapper->reqId);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
          continue;
        } else {
          pVg->emptyBlockReceiveTs = 0;  // reset the ts
        }

        // build rsp
        int64_t         numOfRows = 0;
        SMqTaosxRspObj* pRsp = NULL;
        if (tmqBuildTaosxRspFromWrapper(pollRspWrapper, pVg, &numOfRows, &pRsp) != 0) {
          tscError("consumer:0x%" PRIx64 " build taosx rsp failed, vgId:%d", tmq->consumerId, pVg->vgId);
        }
        tmq->totalRows += numOfRows;

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVg->offsetInfo.endOffset);
        tscDebug("consumer:0x%" PRIx64 " process taosx poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                 ", vg total:%" PRId64 ", total:%" PRId64 ",QID:0x%" PRIx64,
                 tmq->consumerId, pVg->vgId, buf, pDataRsp->blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
                 pollRspWrapper->reqId);

        taosMemoryFreeClear(pollRspWrapper->pEpset);
        taosFreeQitem(pRspWrapper);
        taosWUnLockLatch(&tmq->lock);
        return pRsp;
      } else {
        tscInfo("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                tmq->consumerId, pollRspWrapper->vgId, pDataRsp->head.epoch, consumerEpoch);
        setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
      tscDebug("consumer:0x%" PRIx64 " ep msg received", tmq->consumerId);
      SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)pRspWrapper;
      SMqAskEpRsp*        rspMsg = &pEpRspWrapper->msg;
      (void)doUpdateLocalEp(tmq, pEpRspWrapper->epoch, rspMsg);
      tmqFreeRspWrapper(pRspWrapper);
      taosFreeQitem(pRspWrapper);
    } else {
      tscError("consumer:0x%" PRIx64 " invalid msg received:%d", tmq->consumerId, pRspWrapper->tmqRspType);
    }
  }
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t timeout) {
  if (tmq == NULL) return NULL;

  void*   rspObj = NULL;
  int64_t startTime = taosGetTimestampMs();

  tscDebug("consumer:0x%" PRIx64 " start to poll at %" PRId64 ", timeout:%" PRId64, tmq->consumerId, startTime,
           timeout);

  // in no topic status, delayed task also need to be processed
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    tscInfo("consumer:0x%" PRIx64 " poll return since consumer is init", tmq->consumerId);
    taosMsleep(500);  //     sleep for a while
    return NULL;
  }

  while (1) {
    if (atomic_load_8(&tmq->status) != TMQ_CONSUMER_STATUS__RECOVER) {
      break;
    }
    tscInfo("consumer:0x%" PRIx64 " tmq status is recover", tmq->consumerId);

    int32_t retryCnt = 0;
    while (syncAskEp(tmq) != 0) {
      if (retryCnt++ > 40) {
        return NULL;
      }

      tscInfo("consumer:0x%" PRIx64 " not ready, retry:%d/40 in 500ms", tmq->consumerId, retryCnt);
      taosMsleep(500);
    }
  }

  (void)atomic_val_compare_exchange_8(&pollFlag, 0, 1);

  while (1) {
    tmqHandleAllDelayedTask(tmq);

    if (tmqPollImpl(tmq, timeout) < 0) {
      tscError("consumer:0x%" PRIx64 " return due to poll error", tmq->consumerId);
    }

    rspObj = tmqHandleAllRsp(tmq, timeout);
    if (rspObj) {
      tscDebug("consumer:0x%" PRIx64 " return rsp %p", tmq->consumerId, rspObj);
      return (TAOS_RES*)rspObj;
    }

    if (timeout >= 0) {
      int64_t currentTime = taosGetTimestampMs();
      int64_t elapsedTime = currentTime - startTime;
      if (elapsedTime > timeout || elapsedTime < 0) {
        tscDebug("consumer:0x%" PRIx64 " (epoch %d) timeout, no rsp, start time %" PRId64 ", current time %" PRId64,
                 tmq->consumerId, tmq->epoch, startTime, currentTime);
        return NULL;
      }
      (void)tsem2_timewait(&tmq->rspSem, (timeout - elapsedTime));
    } else {
      (void)tsem2_timewait(&tmq->rspSem, 1000);
    }
  }
}

static void displayConsumeStatistics(tmq_t* pTmq) {
  taosRLockLatch(&pTmq->lock);
  int32_t numOfTopics = taosArrayGetSize(pTmq->clientTopics);
  tscDebug("consumer:0x%" PRIx64 " closing poll:%" PRId64 " rows:%" PRId64 " topics:%d, final epoch:%d",
           pTmq->consumerId, pTmq->pollCnt, pTmq->totalRows, numOfTopics, pTmq->epoch);

  tscDebug("consumer:0x%" PRIx64 " rows dist begin: ", pTmq->consumerId);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopics = taosArrayGet(pTmq->clientTopics, i);
    if (pTopics == NULL) continue;
    tscDebug("consumer:0x%" PRIx64 " topic:%d", pTmq->consumerId, i);
    int32_t numOfVgs = taosArrayGetSize(pTopics->vgs);
    for (int32_t j = 0; j < numOfVgs; ++j) {
      SMqClientVg* pVg = taosArrayGet(pTopics->vgs, j);
      if (pVg == NULL) continue;
      tscDebug("topic:%s, %d. vgId:%d rows:%" PRId64, pTopics->topicName, j, pVg->vgId, pVg->numOfRows);
    }
  }
  taosRUnLockLatch(&pTmq->lock);
  tscDebug("consumer:0x%" PRIx64 " rows dist end", pTmq->consumerId);
}

static int32_t innerClose(tmq_t* tmq) {
  if (tmq->status != TMQ_CONSUMER_STATUS__READY) {
    tscInfo("consumer:0x%" PRIx64 " not in ready state, unsubscribe it directly", tmq->consumerId);
    return 0;
  }
  if (tmq->autoCommit) {
    int32_t code = tmq_commit_sync(tmq, NULL);
    if (code != 0) {
      return code;
    }
  }
  tmqSendHbReq((void*)(tmq->refId), NULL);

  tmq_list_t* lst = tmq_list_new();
  if (lst == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = tmq_subscribe(tmq, lst);
  tmq_list_destroy(lst);
  return code;
}
int32_t tmq_unsubscribe(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;

  tscInfo("consumer:0x%" PRIx64 " start to unsubscribe consumer, status:%d", tmq->consumerId, tmq->status);
  int32_t code = 0;
  if (atomic_load_8(&tmq->status) != TMQ_CONSUMER_STATUS__CLOSED) {
    code = innerClose(tmq);
    if (code == 0) {
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
    }
  }
  return code;
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;

  tscInfo("consumer:0x%" PRIx64 " start to close consumer, status:%d", tmq->consumerId, tmq->status);
  displayConsumeStatistics(tmq);
  int32_t code = 0;
  if (atomic_load_8(&tmq->status) != TMQ_CONSUMER_STATUS__CLOSED) {
    code = innerClose(tmq);
    if (code == 0) {
      atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
    }
  }

  if (code == 0) {
    (void)taosRemoveRef(tmqMgmt.rsetId, tmq->refId);
  }
  return code;
}

const char* tmq_err2str(int32_t err) {
  if (err == 0) {
    return "success";
  } else if (err == -1) {
    return "fail";
  } else {
    if (*(taosGetErrMsg()) == 0) {
      return tstrerror(err);
    } else {
      (void)snprintf(taosGetErrMsgReturn(), ERR_MSG_LEN, "%s,detail:%s", tstrerror(err), taosGetErrMsg());
      return (const char*)taosGetErrMsgReturn();
    }
  }
}

tmq_res_t tmq_get_res_type(TAOS_RES* res) {
  if (res == NULL) {
    return TMQ_RES_INVALID;
  }
  if (TD_RES_TMQ(res)) {
    return TMQ_RES_DATA;
  } else if (TD_RES_TMQ_META(res)) {
    return TMQ_RES_TABLE_META;
  } else if (TD_RES_TMQ_METADATA(res)) {
    return TMQ_RES_METADATA;
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    return TMQ_RES_TABLE_META;
  } else {
    return TMQ_RES_INVALID;
  }
}

const char* tmq_get_topic_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) || TD_RES_TMQ_BATCH_META(res)) {
    char* tmp = strchr(((SMqRspObjCommon*)res)->topic, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else if (TD_RES_TMQ_META(res)) {
    char* tmp = strchr(((SMqMetaRspObj*)res)->topic, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else {
    return NULL;
  }
}

const char* tmq_get_db_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) || TD_RES_TMQ_BATCH_META(res)) {
    char* tmp = strchr(((SMqRspObjCommon*)res)->db, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else if (TD_RES_TMQ_META(res)) {
    char* tmp = strchr(((SMqMetaRspObj*)res)->db, '.');
    if (tmp == NULL) {
      return NULL;
    }
    return tmp + 1;
  } else {
    return NULL;
  }
}

int32_t tmq_get_vgroup_id(TAOS_RES* res) {
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) || TD_RES_TMQ_BATCH_META(res)) {
    return ((SMqRspObjCommon*)res)->vgId;
  } else if (TD_RES_TMQ_META(res)) {
    return ((SMqMetaRspObj*)res)->vgId;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int64_t tmq_get_vgroup_offset(TAOS_RES* res) {
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqDataRspCommon* common = (SMqDataRspCommon*)POINTER_SHIFT(res, sizeof(SMqRspObjCommon));
    STqOffsetVal*     pOffset = &common->reqOffset;
    if (common->reqOffset.type == TMQ_OFFSET__LOG) {
      return common->reqOffset.version;
    } else {
      tscError("invalid offset type:%d", common->reqOffset.type);
    }
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pRspObj = (SMqMetaRspObj*)res;
    if (pRspObj->metaRsp.rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->metaRsp.rspOffset.version;
    }
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    SMqBatchMetaRspObj* pBtRspObj = (SMqBatchMetaRspObj*)res;
    if (pBtRspObj->rsp.rspOffset.type == TMQ_OFFSET__LOG) {
      return pBtRspObj->rsp.rspOffset.version;
    }
  } else {
    tscError("invalid tmq type:%d", *(int8_t*)res);
  }

  // data from tsdb, no valid offset info
  return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
}

const char* tmq_get_table_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqDataRspCommon* common = (SMqDataRspCommon*)POINTER_SHIFT(res, sizeof(SMqRspObjCommon));

    SMqRspObjCommon* pRspObj = (SMqRspObjCommon*)res;
    if (!common->withTbName || common->blockTbName == NULL || pRspObj->resIter < 0 ||
        pRspObj->resIter >= common->blockNum) {
      return NULL;
    }
    return (const char*)taosArrayGetP(common->blockTbName, pRspObj->resIter);
  }
  return NULL;
}

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* cb, void* param) {
  if (tmq == NULL) {
    tscError("invalid tmq handle, null");
    if (cb != NULL) {
      cb(tmq, TSDB_CODE_INVALID_PARA, param);
    }
    return;
  }
  if (pRes == NULL) {  // here needs to commit all offsets.
    asyncCommitAllOffsets(tmq, cb, param);
  } else {  // only commit one offset
    asyncCommitFromResult(tmq, pRes, cb, param);
  }
}

static void commitCallBackFn(tmq_t* UNUSED_PARAM(tmq), int32_t code, void* param) {
  SSyncCommitInfo* pInfo = (SSyncCommitInfo*)param;
  pInfo->code = code;
  (void)tsem2_post(&pInfo->sem);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* pRes) {
  if (tmq == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tscError("failed to allocate memory for sync commit");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    tscError("failed to init sem for sync commit");
    taosMemoryFree(pInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pInfo->code = 0;

  if (pRes == NULL) {
    asyncCommitAllOffsets(tmq, commitCallBackFn, pInfo);
  } else {
    asyncCommitFromResult(tmq, pRes, commitCallBackFn, pInfo);
  }

  (void)tsem2_wait(&pInfo->sem);
  code = pInfo->code;

  (void)tsem2_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);

  tscInfo("consumer:0x%" PRIx64 " sync res commit done, code:%s", tmq->consumerId, tstrerror(code));
  return code;
}

// wal range will be ok after calling tmq_get_topic_assignment or poll interface
static int32_t checkWalRange(SVgOffsetInfo* offset, int64_t value) {
  if (offset->walVerBegin == -1 || offset->walVerEnd == -1) {
    tscError("Assignment or poll interface need to be called first");
    return TSDB_CODE_TMQ_NEED_INITIALIZED;
  }

  if (value != -1 && (value < offset->walVerBegin || value > offset->walVerEnd)) {
    tscError("invalid seek params, offset:%" PRId64 ", valid range:[%" PRId64 ", %" PRId64 "]", value,
             offset->walVerBegin, offset->walVerEnd);
    return TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE;
  }

  return 0;
}

int32_t tmq_commit_offset_sync(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL || pTopicName == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }
  taosWUnLockLatch(&tmq->lock);

  STqOffsetVal offsetVal = {.type = TMQ_OFFSET__LOG, .version = offset};

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tscError("consumer:0x%" PRIx64 " failed to prepare seek operation", tmq->consumerId);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    taosMemoryFree(pInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pInfo->code = 0;

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, commitCallBackFn, pInfo);
  if (code == 0) {
    (void)tsem2_wait(&pInfo->sem);
    code = pInfo->code;
  }

  if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
  (void)tsem2_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);

  tscInfo("consumer:0x%" PRIx64 " sync send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

  return code;
}

void tmq_commit_offset_async(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb* cb,
                             void* param) {
  int32_t code = 0;
  if (tmq == NULL || pTopicName == NULL) {
    tscError("invalid tmq handle, null");
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    goto end;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    goto end;
  }
  taosWUnLockLatch(&tmq->lock);

  STqOffsetVal offsetVal = {.type = TMQ_OFFSET__LOG, .version = offset};

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, cb, param);

  tscInfo("consumer:0x%" PRIx64 " async send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

end:
  if (code != 0 && cb != NULL) {
    if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
    cb(tmq, code, param);
  }
}

int32_t askEpCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (param == NULL) {
    goto FAIL;
  }

  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  tmq_t*           tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto FAIL;
  }

  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 ", get topic endpoint error, code:%s", tmq->consumerId, tstrerror(code));
    goto END;
  }

  if (pMsg == NULL) {
    goto END;
  }
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tscDebug("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (pParam->sync) {
    SMqAskEpRsp rsp = {0};
    if (tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp) != NULL) {
      (void)doUpdateLocalEp(tmq, head->epoch, &rsp);
    }
    tDeleteSMqAskEpRsp(&rsp);
  } else {
    SMqAskEpRspWrapper* pWrapper = NULL;
    code = taosAllocateQitem(sizeof(SMqAskEpRspWrapper), DEF_QITEM, 0, (void**)&pWrapper);
    if (code) {
      goto END;
    }

    pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
    pWrapper->epoch = head->epoch;
    (void)memcpy(&pWrapper->msg, pMsg->pData, sizeof(SMqRspHead));
    if (tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pWrapper->msg) == NULL) {
      tmqFreeRspWrapper((SMqRspWrapper*)pWrapper);
      taosFreeQitem(pWrapper);
    } else {
      (void)taosWriteQitem(tmq->mqueue, pWrapper);
    }
  }

END:
  (void)taosReleaseRef(tmqMgmt.rsetId, pParam->refId);

FAIL:
  if (pParam->sync) {
    SAskEpInfo* pInfo = pParam->pParam;
    if (pInfo) {
      pInfo->code = code;
      (void)tsem2_post(&pInfo->sem);
    }
  }

  if (pMsg) {
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pMsg->pData);
  }

  return code;
}

int32_t syncAskEp(tmq_t* pTmq) {
  SAskEpInfo* pInfo = taosMemoryMalloc(sizeof(SAskEpInfo));
  if (pInfo == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    taosMemoryFree(pInfo);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int32_t code = askEp(pTmq, pInfo, true, false);
  if (code == 0) {
    (void)tsem2_wait(&pInfo->sem);
    code = pInfo->code;
  }

  (void)tsem2_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);
  return code;
}

int32_t askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpSet) {
  SMqAskEpReq req = {0};
  req.consumerId = pTmq->consumerId;
  req.epoch = updateEpSet ? -1 : pTmq->epoch;
  (void)strcpy(req.cgroup, pTmq->groupId);
  int              code = 0;
  SMqAskEpCbParam* pParam = NULL;
  void*            pReq = NULL;

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  if (tlen < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq failed", pTmq->consumerId);
    return TSDB_CODE_INVALID_PARA;
  }

  pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc askEpReq msg, size:%d", pTmq->consumerId, tlen);
    return terrno;
  }

  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq %d failed", pTmq->consumerId, tlen);
    taosMemoryFree(pReq);
    return TSDB_CODE_INVALID_PARA;
  }

  pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc subscribe param", pTmq->consumerId);
    taosMemoryFree(pReq);
    return terrno;
  }

  pParam->refId = pTmq->refId;
  pParam->sync = sync;
  pParam->pParam = param;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pReq);
    taosMemoryFree(pParam);
    return terrno;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->paramFreeFp = taosMemoryFree;
  sendInfo->fp = askEpCb;
  sendInfo->msgType = TDMT_MND_TMQ_ASK_EP;

  SEpSet epSet = getEpSet_s(&pTmq->pTscObj->pAppInfo->mgmtEp);
  tscDebug("consumer:0x%" PRIx64 " ask ep from mnode,QID:0x%" PRIx64, pTmq->consumerId, sendInfo->requestId);
  return asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
}

int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  int64_t refId = pParamSet->refId;

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    taosMemoryFree(pParamSet);
    return TSDB_CODE_TMQ_CONSUMER_CLOSED;
  }

  // if no more waiting rsp
  if (pParamSet->callbackFn != NULL) {
    pParamSet->callbackFn(tmq, pParamSet->code, pParamSet->userParam);
  }

  taosMemoryFree(pParamSet);
  return taosReleaseRef(tmqMgmt.rsetId, refId);
}

int32_t commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  if (waitingRspNum == 0) {
    tscInfo("consumer:0x%" PRIx64 " topic:%s vgId:%d all commit-rsp received, commit completed", consumerId, pTopic,
            vgId);
    return tmqCommitDone(pParamSet);
  } else {
    tscInfo("consumer:0x%" PRIx64 " topic:%s vgId:%d commit-rsp received, remain:%d", consumerId, pTopic, vgId,
            waitingRspNum);
  }
  return 0;
}

int32_t tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4, SReqResultInfo** pResInfo) {
  SMqDataRspCommon* common = (SMqDataRspCommon*)POINTER_SHIFT(res, sizeof(SMqRspObjCommon));
  SMqRspObjCommon*  pRspObj = (SMqRspObjCommon*)res;
  pRspObj->resIter++;
  if (pRspObj->resIter < common->blockNum) {
    if (common->withSchema) {
      doFreeReqResultInfo(&pRspObj->resInfo);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(common->blockSchema, pRspObj->resIter);
      if (pSW) {
        TAOS_CHECK_RETURN(setResSchemaInfo(&pRspObj->resInfo, pSW->pSchema, pSW->nCols));
      }
    }

    void*   pRetrieve = taosArrayGetP(common->blockData, pRspObj->resIter);
    void*   rawData = NULL;
    int64_t rows = 0;
    int32_t precision = 0;
    tmqGetRawDataRowsPrecisionFromRes(pRetrieve, &rawData, &rows, &precision);

    pRspObj->resInfo.pData = rawData;
    pRspObj->resInfo.numOfRows = rows;
    pRspObj->resInfo.current = 0;
    pRspObj->resInfo.precision = precision;

    pRspObj->resInfo.totalRows += pRspObj->resInfo.numOfRows;
    int32_t code = setResultDataPtr(&pRspObj->resInfo, pRspObj->resInfo.fields, pRspObj->resInfo.numOfCols,
                                    pRspObj->resInfo.numOfRows, convertUcs4);
    if (code != 0) {
      return code;
    }
    *pResInfo = &pRspObj->resInfo;
    return code;
  }

  return TSDB_CODE_TSC_INTERNAL_ERROR;
}

static int32_t tmqGetWalInfoCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (param == NULL) {
    return code;
  }
  SMqVgWalInfoParam* pParam = param;
  SMqVgCommon*       pCommon = pParam->pCommon;

  int32_t total = atomic_add_fetch_32(&pCommon->numOfRsp, 1);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 " failed to get the wal info from vgId:%d for topic:%s", pCommon->consumerId,
             pParam->vgId, pCommon->pTopicName);

  } else {
    SMqDataRsp rsp = {0};
    SDecoder   decoder = {0};
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    code = tDecodeMqDataRsp(&decoder, &rsp);
    tDecoderClear(&decoder);
    if (code != 0) {
      goto END;
    }

    SMqRspHead*          pHead = pMsg->pData;
    tmq_topic_assignment assignment = {.begin = pHead->walsver,
                                       .end = pHead->walever + 1,
                                       .currentOffset = rsp.common.rspOffset.version,
                                       .vgId = pParam->vgId};

    (void)taosThreadMutexLock(&pCommon->mutex);
    if (taosArrayPush(pCommon->pList, &assignment) == NULL) {
      tscError("consumer:0x%" PRIx64 " failed to push the wal info from vgId:%d for topic:%s", pCommon->consumerId,
               pParam->vgId, pCommon->pTopicName);
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
    }
    (void)taosThreadMutexUnlock(&pCommon->mutex);
  }

END:
  pCommon->code = code;
  if (total == pParam->totalReq) {
    (void)tsem2_post(&pCommon->rsp);
  }

  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }

  return code;
}

static void destroyCommonInfo(SMqVgCommon* pCommon) {
  if (pCommon == NULL) {
    return;
  }
  (void)taosArrayDestroy(pCommon->pList);
  (void)tsem2_destroy(&pCommon->rsp);
  (void)taosThreadMutexDestroy(&pCommon->mutex);
  taosMemoryFree(pCommon->pTopicName);
  taosMemoryFree(pCommon);
}

static bool isInSnapshotMode(int8_t type, bool useSnapshot) {
  if ((type < TMQ_OFFSET__LOG && useSnapshot) || type > TMQ_OFFSET__LOG) {
    return true;
  }
  return false;
}

static int32_t tmCommittedCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqCommittedParam* pParam = param;

  if (code != 0) {
    goto end;
  }
  if (pMsg) {
    SDecoder decoder = {0};
    tDecoderInit(&decoder, (uint8_t*)pMsg->pData, pMsg->len);
    if (tDecodeMqVgOffset(&decoder, &pParam->vgOffset) < 0) {
      tOffsetDestroy(&pParam->vgOffset.offset);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    tDecoderClear(&decoder);
  }

end:
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  pParam->code = code;
  (void)tsem2_post(&pParam->sem);
  return code;
}

int64_t getCommittedFromServer(tmq_t* tmq, char* tname, int32_t vgId, SEpSet* epSet) {
  int32_t     code = 0;
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;
  (void)snprintf(pOffset.offset.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", tmq->groupId, TMQ_SEPARATOR, tname);

  int32_t len = 0;
  tEncodeSize(tEncodeMqVgOffset, &pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqVgOffset(&encoder, &pOffset);
  if (code < 0) {
    taosMemoryFree(buf);
    tEncoderClear(&encoder);
    return code;
  }
  tEncoderClear(&encoder);

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  SMqCommittedParam* pParam = taosMemoryMalloc(sizeof(SMqCommittedParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    return terrno;
  }
  if (tsem2_init(&pParam->sem, 0, 0) != 0) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    taosMemoryFree(pParam);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmCommittedCb;
  sendInfo->msgType = TDMT_VND_TMQ_VG_COMMITTEDINFO;

  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, NULL, sendInfo);
  if (code != 0) {
    (void)tsem2_destroy(&pParam->sem);
    taosMemoryFree(pParam);
    return code;
  }

  (void)tsem2_wait(&pParam->sem);
  code = pParam->code;
  if (code == TSDB_CODE_SUCCESS) {
    if (pParam->vgOffset.offset.val.type == TMQ_OFFSET__LOG) {
      code = pParam->vgOffset.offset.val.version;
    } else {
      tOffsetDestroy(&pParam->vgOffset.offset);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
    }
  }
  (void)tsem2_destroy(&pParam->sem);
  taosMemoryFree(pParam);

  return code;
}

int64_t tmq_position(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  int32_t        type = pOffsetInfo->endOffset.type;
  if (isInSnapshotMode(type, tmq->useSnapshot)) {
    tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, position error", tmq->consumerId, type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  code = checkWalRange(pOffsetInfo, -1);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }
  SEpSet  epSet = pVg->epSet;
  int64_t begin = pVg->offsetInfo.walVerBegin;
  int64_t end = pVg->offsetInfo.walVerEnd;
  taosWUnLockLatch(&tmq->lock);

  int64_t position = 0;
  if (type == TMQ_OFFSET__LOG) {
    position = pOffsetInfo->endOffset.version;
  } else if (type == TMQ_OFFSET__RESET_EARLIEST || type == TMQ_OFFSET__RESET_LATEST) {
    code = getCommittedFromServer(tmq, tname, vgId, &epSet);
    if (code == TSDB_CODE_TMQ_NO_COMMITTED) {
      if (type == TMQ_OFFSET__RESET_EARLIEST) {
        position = begin;
      } else if (type == TMQ_OFFSET__RESET_LATEST) {
        position = end;
      }
    } else {
      position = code;
    }
  } else {
    tscError("consumer:0x%" PRIx64 " offset type:%d can not be reach here", tmq->consumerId, type);
  }

  tscInfo("consumer:0x%" PRIx64 " tmq_position vgId:%d position:%" PRId64, tmq->consumerId, vgId, position);
  return position;
}

int64_t tmq_committed(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;
  if (isInSnapshotMode(pOffsetInfo->endOffset.type, tmq->useSnapshot)) {
    tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
             pOffsetInfo->endOffset.type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  if (isInSnapshotMode(pOffsetInfo->committedOffset.type, tmq->useSnapshot)) {
    tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
             pOffsetInfo->committedOffset.type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  int64_t committed = 0;
  if (pOffsetInfo->committedOffset.type == TMQ_OFFSET__LOG) {
    committed = pOffsetInfo->committedOffset.version;
    taosWUnLockLatch(&tmq->lock);
    goto end;
  }
  SEpSet epSet = pVg->epSet;
  taosWUnLockLatch(&tmq->lock);

  committed = getCommittedFromServer(tmq, tname, vgId, &epSet);

end:
  tscInfo("consumer:0x%" PRIx64 " tmq_committed vgId:%d committed:%" PRId64, tmq->consumerId, vgId, committed);
  return committed;
}

int32_t tmq_get_topic_assignment(tmq_t* tmq, const char* pTopicName, tmq_topic_assignment** assignment,
                                 int32_t* numOfAssignment) {
  if (tmq == NULL || pTopicName == NULL || assignment == NULL || numOfAssignment == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }
  *numOfAssignment = 0;
  *assignment = NULL;
  SMqVgCommon* pCommon = NULL;

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);

  SMqClientTopic* pTopic = NULL;
  int32_t         code = getTopicByName(tmq, tname, &pTopic);
  if (code != 0) {
    tscError("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    goto end;
  }

  // in case of snapshot is opened, no valid offset will return
  *numOfAssignment = taosArrayGetSize(pTopic->vgs);
  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    if (pClientVg == NULL) {
      continue;
    }
    int32_t type = pClientVg->offsetInfo.beginOffset.type;
    if (isInSnapshotMode(type, tmq->useSnapshot)) {
      tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, assignment not allowed", tmq->consumerId, type);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
      goto end;
    }
  }

  *assignment = taosMemoryCalloc(*numOfAssignment, sizeof(tmq_topic_assignment));
  if (*assignment == NULL) {
    tscError("consumer:0x%" PRIx64 " failed to malloc buffer, size:%" PRIzu, tmq->consumerId,
             (*numOfAssignment) * sizeof(tmq_topic_assignment));
    code = terrno;
    goto end;
  }

  bool needFetch = false;

  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    if (pClientVg == NULL) {
      continue;
    }
    if (pClientVg->offsetInfo.beginOffset.type != TMQ_OFFSET__LOG) {
      needFetch = true;
      break;
    }

    tmq_topic_assignment* pAssignment = &(*assignment)[j];
    pAssignment->currentOffset = pClientVg->offsetInfo.beginOffset.version;
    pAssignment->begin = pClientVg->offsetInfo.walVerBegin;
    pAssignment->end = pClientVg->offsetInfo.walVerEnd;
    pAssignment->vgId = pClientVg->vgId;
    tscInfo("consumer:0x%" PRIx64 " get assignment from local:%d->%" PRId64, tmq->consumerId, pAssignment->vgId,
            pAssignment->currentOffset);
  }

  if (needFetch) {
    pCommon = taosMemoryCalloc(1, sizeof(SMqVgCommon));
    if (pCommon == NULL) {
      code = terrno;
      goto end;
    }

    pCommon->pList = taosArrayInit(4, sizeof(tmq_topic_assignment));
    if (pCommon->pList == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    if (tsem2_init(&pCommon->rsp, 0, 0) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    (void)taosThreadMutexInit(&pCommon->mutex, 0);
    pCommon->pTopicName = taosStrdup(pTopic->topicName);
    pCommon->consumerId = tmq->consumerId;

    for (int32_t i = 0; i < (*numOfAssignment); ++i) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
      if (pClientVg == NULL) {
        continue;
      }
      SMqVgWalInfoParam* pParam = taosMemoryMalloc(sizeof(SMqVgWalInfoParam));
      if (pParam == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }

      pParam->epoch = tmq->epoch;
      pParam->vgId = pClientVg->vgId;
      pParam->totalReq = *numOfAssignment;
      pParam->pCommon = pCommon;

      SMqPollReq req = {0};
      tmqBuildConsumeReqImpl(&req, tmq, 10, pTopic, pClientVg);
      req.reqOffset = pClientVg->offsetInfo.beginOffset;

      int32_t msgSize = tSerializeSMqPollReq(NULL, 0, &req);
      if (msgSize < 0) {
        taosMemoryFree(pParam);
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }

      char* msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        taosMemoryFree(pParam);
        code = terrno;
        goto end;
      }

      if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
        taosMemoryFree(msg);
        taosMemoryFree(pParam);
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }

      SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
      if (sendInfo == NULL) {
        taosMemoryFree(pParam);
        taosMemoryFree(msg);
        code = terrno;
        goto end;
      }

      sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
      sendInfo->requestId = req.reqId;
      sendInfo->requestObjRefId = 0;
      sendInfo->param = pParam;
      sendInfo->paramFreeFp = taosMemoryFree;
      sendInfo->fp = tmqGetWalInfoCb;
      sendInfo->msgType = TDMT_VND_TMQ_VG_WALINFO;

      // int64_t transporterId = 0;
      char offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
      tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pClientVg->offsetInfo.beginOffset);

      tscInfo("consumer:0x%" PRIx64 " %s retrieve wal info vgId:%d, epoch %d, req:%s,QID:0x%" PRIx64, tmq->consumerId,
              pTopic->topicName, pClientVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
      code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pClientVg->epSet, NULL, sendInfo);
      if (code != 0) {
        goto end;
      }
    }

    (void)tsem2_wait(&pCommon->rsp);
    code = pCommon->code;

    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
    int32_t num = taosArrayGetSize(pCommon->pList);
    for (int32_t i = 0; i < num; ++i) {
      (*assignment)[i] = *(tmq_topic_assignment*)taosArrayGet(pCommon->pList, i);
    }
    *numOfAssignment = num;

    for (int32_t j = 0; j < (*numOfAssignment); ++j) {
      tmq_topic_assignment* p = &(*assignment)[j];

      for (int32_t i = 0; i < taosArrayGetSize(pTopic->vgs); ++i) {
        SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
        if (pClientVg == NULL) {
          continue;
        }
        if (pClientVg->vgId != p->vgId) {
          continue;
        }

        SVgOffsetInfo* pOffsetInfo = &pClientVg->offsetInfo;
        tscInfo("consumer:0x%" PRIx64 " %s vgId:%d offset is update to:%" PRId64, tmq->consumerId, pTopic->topicName,
                p->vgId, p->currentOffset);

        pOffsetInfo->walVerBegin = p->begin;
        pOffsetInfo->walVerEnd = p->end;
      }
    }
  }

end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(*assignment);
    *assignment = NULL;
    *numOfAssignment = 0;
  }
  destroyCommonInfo(pCommon);
  taosWUnLockLatch(&tmq->lock);
  return code;
}

void tmq_free_assignment(tmq_topic_assignment* pAssignment) {
  if (pAssignment == NULL) {
    return;
  }

  taosMemoryFree(pAssignment);
}

static int32_t tmqSeekCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
  }
  if (param == NULL) {
    return code;
  }
  SMqSeekParam* pParam = param;
  pParam->code = code;
  (void)tsem2_post(&pParam->sem);
  return 0;
}

// seek interface have to send msg to server to cancel push handle if needed, because consumer may be in wait status if
// there is no data to poll
int32_t tmq_offset_seek(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL || pTopicName == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t accId = tmq->pTscObj->acctId;
  char    tname[TSDB_TOPIC_FNAME_LEN] = {0};
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", accId, pTopicName);

  taosWLockLatch(&tmq->lock);

  SMqClientVg* pVg = NULL;
  int32_t      code = getClientVg(tmq, tname, vgId, &pVg);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  SVgOffsetInfo* pOffsetInfo = &pVg->offsetInfo;

  int32_t type = pOffsetInfo->endOffset.type;
  if (isInSnapshotMode(type, tmq->useSnapshot)) {
    tscError("consumer:0x%" PRIx64 " offset type:%d not wal version, seek not allowed", tmq->consumerId, type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  tscInfo("consumer:0x%" PRIx64 " seek to %" PRId64 " on vgId:%d", tmq->consumerId, offset, vgId);
  // update the offset, and then commit to vnode
  pOffsetInfo->endOffset.type = TMQ_OFFSET__LOG;
  pOffsetInfo->endOffset.version = offset;
  pOffsetInfo->beginOffset = pOffsetInfo->endOffset;
  pVg->seekUpdated = true;
  SEpSet epSet = pVg->epSet;
  taosWUnLockLatch(&tmq->lock);

  SMqSeekReq req = {0};
  (void)snprintf(req.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s:%s", tmq->groupId, tname);
  req.head.vgId = vgId;
  req.consumerId = tmq->consumerId;

  int32_t msgSize = tSerializeSMqSeekReq(NULL, 0, &req);
  if (msgSize < 0) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }

  char* msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return terrno;
  }

  if (tSerializeSMqSeekReq(msg, msgSize, &req) < 0) {
    taosMemoryFree(msg);
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(msg);
    return terrno;
  }

  SMqSeekParam* pParam = taosMemoryMalloc(sizeof(SMqSeekParam));
  if (pParam == NULL) {
    taosMemoryFree(msg);
    taosMemoryFree(sendInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (tsem2_init(&pParam->sem, 0, 0) != 0) {
    taosMemoryFree(msg);
    taosMemoryFree(sendInfo);
    taosMemoryFree(pParam);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqSeekCb;
  sendInfo->msgType = TDMT_VND_TMQ_SEEK;

  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
  if (code != 0) {
    (void)tsem2_destroy(&pParam->sem);
    taosMemoryFree(pParam);
    return code;
  }

  (void)tsem2_wait(&pParam->sem);
  code = pParam->code;
  (void)tsem2_destroy(&pParam->sem);
  taosMemoryFree(pParam);

  tscInfo("consumer:0x%" PRIx64 "send seek to vgId:%d, return code:%s", tmq->consumerId, vgId, tstrerror(code));

  return code;
}

TAOS* tmq_get_connect(tmq_t* tmq) {
  if (tmq && tmq->pTscObj) {
    return (TAOS*)(&(tmq->pTscObj->id));
  }
  return NULL;
}

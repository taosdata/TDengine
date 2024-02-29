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

struct SMqMgmt {
  int8_t  inited;
  tmr_h   timer;
  int32_t rsetId;
};

static TdThreadOnce   tmqInit = PTHREAD_ONCE_INIT;  // initialize only once
volatile int32_t      tmqInitRes = 0;               // initialize rsp code
static struct SMqMgmt tmqMgmt = {0};

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
  char           clientId[256];
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         autoCommit;
  int8_t         resetOffset;
  int8_t         withTbName;
  int8_t         snapEnable;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
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
  int8_t         resetOffsetCfg;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
  uint64_t       consumerId;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;

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
  tsem_t        rsp;
  int32_t       numOfRsp;
  SArray*       pList;
  TdThreadMutex mutex;
  int64_t       consumerId;
  char*         pTopicName;
  int32_t       code;
} SMqVgCommon;

typedef struct SMqSeekParam {
  tsem_t  sem;
  int32_t code;
} SMqSeekParam;

typedef struct SMqCommittedParam {
  tsem_t      sem;
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
  /*SArray*        successfulOffsets;*/
  /*SArray*        failedOffsets;*/
  void* userParam;
} SMqCommitCbParamSet;

typedef struct {
  SMqCommitCbParamSet* params;
  //  SMqVgOffset*         pOffset;
  char    topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t vgId;
  tmq_t*  pTmq;
} SMqCommitCbParam;

typedef struct SSyncCommitInfo {
  tsem_t  sem;
  int32_t code;
} SSyncCommitInfo;

static int32_t syncAskEp(tmq_t* tmq);
static int32_t makeTopicVgroupKey(char* dst, const char* topicName, int32_t vg);
static int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet);
static int32_t doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                               SMqCommitCbParamSet* pParamSet);
static void    commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId);
static void    askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpset);

tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  if (conf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return conf;
  }

  conf->withTbName = false;
  conf->autoCommit = true;
  conf->autoCommitInterval = DEFAULT_AUTO_COMMIT_INTERVAL;
  conf->resetOffset = TMQ_OFFSET__RESET_LATEST;

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
    conf->port = taosStr2int64(value);
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

  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() { return (tmq_list_t*)taosArrayInit(0, sizeof(void*)); }

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  if (list == NULL) return -1;
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) return -1;
  char* topic = taosStrdup(src);
  if (taosArrayPush(container, &topic) == NULL) return -1;
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

  commitRspCountDown(pParamSet, pParam->pTmq->consumerId, pParam->topicName, pParam->vgId);
  return 0;
}

static int32_t doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                               SMqCommitCbParamSet* pParamSet) {
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;
  pOffset.offset.val = *offset;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset.offset.subKey, tmq->groupId, groupLen);
  pOffset.offset.subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset.offset.subKey + groupLen + 1, pTopicName);

  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqVgOffset, &pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, len);
  tEncodeMqVgOffset(&encoder, &pOffset);
  tEncoderClear(&encoder);

  // build param
  SMqCommitCbParam* pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pParam->params = pParamSet;
  //  pParam->pOffset = pOffset;
  pParam->vgId = vgId;
  pParam->pTmq = tmq;

  tstrncpy(pParam->topicName, pTopicName, tListLen(pParam->topicName));

  // build send info
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pMsgSendInfo == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(pParam);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMsgSendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->fp = tmqCommitCb;
  pMsgSendInfo->msgType = TDMT_VND_TMQ_COMMIT_OFFSET;

  int64_t transporterId = 0;
  atomic_add_fetch_32(&pParamSet->waitingRspNum, 1);
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, &transporterId, pMsgSendInfo);
  if (code != 0) {
    atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
    return code;
  }
  return code;
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

static SMqCommitCbParamSet* prepareCommitCbParamSet(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam,
                                                    int32_t rspNum) {
  SMqCommitCbParamSet* pParamSet = taosMemoryCalloc(1, sizeof(SMqCommitCbParamSet));
  if (pParamSet == NULL) {
    return NULL;
  }

  pParamSet->refId = tmq->refId;
  pParamSet->epoch = tmq->epoch;
  pParamSet->callbackFn = pCommitFp;
  pParamSet->userParam = userParam;
  pParamSet->waitingRspNum = rspNum;

  return pParamSet;
}

static int32_t getClientVg(tmq_t* tmq, char* pTopicName, int32_t vgId, SMqClientVg** pVg) {
  SMqClientTopic* pTopic = getTopicByName(tmq, pTopicName);
  if (pTopic == NULL) {
    tscError("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
    return TSDB_CODE_TMQ_INVALID_TOPIC;
  }

  int32_t numOfVgs = taosArrayGetSize(pTopic->vgs);
  for (int32_t i = 0; i < numOfVgs; ++i) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
    if (pClientVg->vgId == vgId) {
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

  SMqCommitCbParamSet* pParamSet = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 0);
  if (pParamSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
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
  pVg->offsetInfo.committedOffset = *offsetVal;

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
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
    offsetVal = pRspObj->rsp.rspOffset;
  } else if (TD_RES_TMQ_META(pRes)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)pRes;
    pTopicName = pMetaRspObj->topic;
    vgId = pMetaRspObj->vgId;
    offsetVal = pMetaRspObj->metaRsp.rspOffset;
  } else if (TD_RES_TMQ_METADATA(pRes)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)pRes;
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
    offsetVal = pRspObj->rsp.rspOffset;
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
  SMqCommitCbParamSet* pParamSet = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 1);
  if (pParamSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  taosRLockLatch(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tscInfo("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t         numOfVgroups = taosArrayGetSize(pTopic->vgs);

    tscInfo("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName,
            numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

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

        tscInfo("consumer:0x%" PRIx64
                " topic:%s on vgId:%d send commit msg success, send offset:%s committed:%s, ordinal:%d/%d",
                tmq->consumerId, pTopic->topicName, pVg->vgId, offsetBuf, commitBuf, j + 1, numOfVgroups);
        pVg->offsetInfo.committedOffset = pVg->offsetInfo.endOffset;
      } else {
        tscInfo("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, current:%" PRId64 ", ordinal:%d/%d",
                tmq->consumerId, pTopic->topicName, pVg->vgId, pVg->offsetInfo.endOffset.version, j + 1, numOfVgroups);
      }
    }
  }
  taosRUnLockLatch(&tmq->lock);

  tscInfo("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - 1,
          numOfTopics);

  // request is sent
  if (pParamSet->waitingRspNum != 1) {
    // count down since waiting rsp num init as 1
    commitRspCountDown(pParamSet, tmq->consumerId, "", 0);
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
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM, 0);
  if (pTaskType == NULL) return;

  *pTaskType = type;
  taosWriteQitem(tmq->delayedTask, pTaskType);
  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__ASK_EP);
  taosMemoryFree(param);
}

void tmqReplayTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) goto END;

  tsem_post(&tmq->rspSem);
  taosReleaseRef(tmqMgmt.rsetId, refId);
END:
  taosMemoryFree(param);
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = *(int64_t*)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__COMMIT);
  taosMemoryFree(param);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    SMqHbRsp rsp = {0};
    tDeserializeSMqHbRsp(pMsg->pData, pMsg->len, &rsp);

    int64_t refId = *(int64_t*)param;
    tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
    if (tmq != NULL) {
      taosWLockLatch(&tmq->lock);
      for (int32_t i = 0; i < taosArrayGetSize(rsp.topicPrivileges); i++) {
        STopicPrivilege* privilege = taosArrayGet(rsp.topicPrivileges, i);
        if (privilege->noPrivilege == 1) {
          int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
          for (int32_t j = 0; j < topicNumCur; j++) {
            SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
            if (strcmp(pTopicCur->topicName, privilege->topic) == 0) {
              tscInfo("consumer:0x%" PRIx64 ", has no privilege, topic:%s", tmq->consumerId, privilege->topic);
              pTopicCur->noPrivilege = 1;
            }
          }
        }
      }
      taosWUnLockLatch(&tmq->lock);
      taosReleaseRef(tmqMgmt.rsetId, refId);
    }
    tDeatroySMqHbRsp(&rsp);
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
  taosRLockLatch(&tmq->lock);
  req.topics = taosArrayInit(taosArrayGetSize(tmq->clientTopics), sizeof(TopicOffsetRows));
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic*  pTopic = taosArrayGet(tmq->clientTopics, i);
    int32_t          numOfVgroups = taosArrayGetSize(pTopic->vgs);
    TopicOffsetRows* data = taosArrayReserve(req.topics, 1);
    strcpy(data->topicName, pTopic->topicName);
    data->offsetRows = taosArrayInit(numOfVgroups, sizeof(OffsetRows));
    for (int j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      OffsetRows*  offRows = taosArrayReserve(data->offsetRows, 1);
      offRows->vgId = pVg->vgId;
      offRows->rows = pVg->numOfRows;
      offRows->offset = pVg->offsetInfo.endOffset;
      offRows->ever = pVg->offsetInfo.walVerEnd;
      char buf[TSDB_OFFSET_LEN] = {0};
      tFormatOffset(buf, TSDB_OFFSET_LEN, &offRows->offset);
      tscInfo("consumer:0x%" PRIx64 ",report offset, group:%s vgId:%d, offset:%s/%" PRId64 ", rows:%" PRId64,
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
  sendInfo->paramFreeFp = taosMemoryFree;
  sendInfo->param = taosMemoryMalloc(sizeof(int64_t));
  *(int64_t*)sendInfo->param = refId;
  sendInfo->fp = tmqHbCb;
  sendInfo->msgType = TDMT_MND_TMQ_HB;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

OVER:
  tDeatroySMqHbReq(&req);
  taosTmrReset(tmqSendHbReq, DEFAULT_HEARTBEAT_INTERVAL, param, tmqMgmt.timer, &tmq->hbLiveTimer);
  taosReleaseRef(tmqMgmt.rsetId, refId);
}

static void defaultCommitCbFn(tmq_t* pTmq, int32_t code, void* param) {
  if (code != 0) {
    tscError("consumer:0x%" PRIx64 ", failed to commit offset, code:%s", pTmq->consumerId, tstrerror(code));
  }
}

int32_t tmqHandleAllDelayedTask(tmq_t* pTmq) {
  STaosQall* qall = taosAllocateQall();
  taosReadAllQitems(pTmq->delayedTask, qall);

  int32_t numOfItems = taosQallItemSize(qall);
  if (numOfItems == 0) {
    taosFreeQall(qall);
    return TSDB_CODE_SUCCESS;
  }

  tscDebug("consumer:0x%" PRIx64 " handle delayed %d tasks before poll data", pTmq->consumerId, numOfItems);
  int8_t* pTaskType = NULL;
  taosGetQitem(qall, (void**)&pTaskType);

  while (pTaskType != NULL) {
    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      askEp(pTmq, NULL, false, false);

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
    } else {
      tscError("consumer:0x%" PRIx64 " invalid task type:%d", pTmq->consumerId, *pTaskType);
    }

    taosFreeQitem(pTaskType);
    taosGetQitem(qall, (void**)&pTaskType);
  }

  taosFreeQall(qall);
  return 0;
}

static void tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)rspWrapper;
    tDeleteSMqAskEpRsp(&pEpRspWrapper->msg);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
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
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
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
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  if (*topics == NULL) {
    *topics = tmq_list_new();
  }
  taosRLockLatch(&tmq->lock);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    tmq_list_append(*topics, strchr(topic->topicName, '.') + 1);
  }
  taosRUnLockLatch(&tmq->lock);
  return 0;
}

int32_t tmq_unsubscribe(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  if (tmq->status != TMQ_CONSUMER_STATUS__READY) {
    tscInfo("consumer:0x%" PRIx64 " not in ready state, unsubscribe it directly", tmq->consumerId);
    return 0;
  }
  if (tmq->autoCommit) {
    int32_t rsp = tmq_commit_sync(tmq, NULL);
    if (rsp != 0) {
      return rsp;
    }
  }
  taosSsleep(2);  // sleep 2s for hb to send offset and rows to server

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

#define SET_ERROR_MSG(MSG) \
  if (errstr != NULL) snprintf(errstr, errstrLen, MSG);
tmq_t* tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen) {
  if (conf == NULL) {
    SET_ERROR_MSG("configure is null")
    return NULL;
  }
  taosThreadOnce(&tmqInit, tmqMgmtInit);
  if (tmqInitRes != 0) {
    terrno = tmqInitRes;
    SET_ERROR_MSG("tmq timer init error")
    return NULL;
  }

  tmq_t* pTmq = taosMemoryCalloc(1, sizeof(tmq_t));
  if (pTmq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tscError("failed to create consumer, groupId:%s, code:%s", conf->groupId, terrstr());
    SET_ERROR_MSG("malloc tmq failed")
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
    SET_ERROR_MSG("malloc tmq element failed or group is empty")
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
  pTmq->replayEnable = conf->replayEnable;
  pTmq->sourceExcluded = conf->sourceExcluded;
  if (conf->replayEnable) {
    pTmq->autoCommit = false;
  }
  taosInitRWLatch(&pTmq->lock);

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem_init(&pTmq->rspSem, 0, 0) != 0) {
    tscError("consumer:0x %" PRIx64 " setup failed since %s, consumer group %s", pTmq->consumerId, terrstr(),
             pTmq->groupId);
    SET_ERROR_MSG("init t_sem failed")
    goto _failed;
  }

  // init connection
  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) {
    tscError("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
    tsem_destroy(&pTmq->rspSem);
    SET_ERROR_MSG("init tscObj failed")
    goto _failed;
  }

  pTmq->refId = taosAddRef(tmqMgmt.rsetId, pTmq);
  if (pTmq->refId < 0) {
    SET_ERROR_MSG("add tscObj ref failed")
    goto _failed;
  }

  int64_t* pRefId = taosMemoryMalloc(sizeof(int64_t));
  *pRefId = pTmq->refId;
  pTmq->hbLiveTimer = taosTmrStart(tmqSendHbReq, DEFAULT_HEARTBEAT_INTERVAL, pRefId, tmqMgmt.timer);

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
  tstrncpy(req.clientId, tmq->clientId, 256);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  req.topicNames = taosArrayInit(sz, sizeof(void*));

  if (req.topicNames == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  req.withTbName = tmq->withTbName;
  req.autoCommit = tmq->autoCommit;
  req.autoCommitInterval = tmq->autoCommitInterval;
  req.resetOffsetCfg = tmq->resetOffsetCfg;
  req.enableReplay = tmq->replayEnable;

  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(container, i);

    SName name = {0};
    tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));
    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      goto FAIL;
    }

    tNameExtractFullName(&name, topicFName);
    tscInfo("consumer:0x%" PRIx64 " subscribe topic:%s", tmq->consumerId, topicFName);

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

  SMqSubscribeCbParam param = {.rspErr = 0};
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
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);
  if (code != 0) {
    goto FAIL;
  }

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
  while (syncAskEp(tmq) != 0) {
    if (retryCnt++ > MAX_RETRY_COUNT) {
      tscError("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry more than 2 minutes", tmq->consumerId);
      code = TSDB_CODE_MND_CONSUMER_NOT_READY;
      goto FAIL;
    }

    tscInfo("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
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
  if (conf == NULL) return;
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

static SMqClientVg* getVgInfo(tmq_t* tmq, char* topicName, int32_t vgId) {
  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int i = 0; i < topicNumCur; i++) {
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (strcmp(pTopicCur->topicName, topicName) == 0) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur->vgId == vgId) {
          return pVgCur;
        }
      }
    }
  }
  return NULL;
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
  SMqClientVg* pVg = getVgInfo(tmq, topicName, vgId);
  if (pVg) {
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  taosWUnLockLatch(&tmq->lock);
}

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  int64_t         refId = pParam->refId;
  int32_t         vgId = pParam->vgId;
  uint64_t        requestId = pParam->requestId;
  tmq_t*          tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto FAIL;
  }

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM, 0);
  if (pRspWrapper == NULL) {
    tscWarn("consumer:0x%" PRIx64 " msg discard from vgId:%d, since out of memory", tmq->consumerId, vgId);
    taosReleaseRef(tmqMgmt.rsetId, refId);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  if (code != 0) {
    goto END;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t clientEpoch = atomic_load_32(&tmq->epoch);
  if (msgEpoch < clientEpoch) {
    // do not write into queue since updating epoch reset
    tscWarn("consumer:0x%" PRIx64
            " msg discard from vgId:%d since from earlier epoch, rsp epoch %d, current epoch %d, reqId:0x%" PRIx64,
            tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
    code = -1;
    goto END;
  }

  ASSERT(msgEpoch == clientEpoch);
  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;
  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->reqId = requestId;
  pRspWrapper->pEpset = pMsg->pEpSet;
  pMsg->pEpSet = NULL;

  if (rspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqDataRsp(&decoder, &pRspWrapper->dataRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));

    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &pRspWrapper->dataRsp.rspOffset);
    tscDebug("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, req ver:%" PRId64 ", rsp:%s type %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, pRspWrapper->dataRsp.reqOffset.version, buf, rspType, requestId);
  } else if (rspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqMetaRsp(&decoder, &pRspWrapper->metaRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
  } else if (rspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSTaosxRsp(&decoder, &pRspWrapper->taosxRsp);
    tDecoderClear(&decoder);
    memcpy(&pRspWrapper->taosxRsp, pMsg->pData, sizeof(SMqRspHead));
  } else {  // invalid rspType
    tscError("consumer:0x%" PRIx64 " invalid rsp msg received, type:%d ignored", tmq->consumerId, rspType);
  }

END:
  pRspWrapper->code = code;
  pRspWrapper->vgId = vgId;
  strcpy(pRspWrapper->topicName, pParam->topicName);
  taosWriteQitem(tmq->mqueue, pRspWrapper);

  int32_t total = taosQueueItemSize(tmq->mqueue);
  tscDebug("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d, vgId:%d, total in queue:%d, reqId:0x%" PRIx64,
           tmq->consumerId, rspType, vgId, total, requestId);
  taosReleaseRef(tmqMgmt.rsetId, refId);

FAIL:
  if (tmq) tsem_post(&tmq->rspSem);
  taosMemoryFree(pParam);
  if (pMsg) taosMemoryFreeClear(pMsg->pData);
  if (pMsg) taosMemoryFreeClear(pMsg->pEpSet);

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

  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);

    makeTopicVgroupKey(vgKey, pTopic->topicName, pVgEp->vgId);
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

    clientVg.offsetInfo.endOffset = pInfo ? pInfo->currentOffset : offsetNew;
    clientVg.offsetInfo.committedOffset = pInfo ? pInfo->commitOffset : offsetNew;
    clientVg.offsetInfo.beginOffset = pInfo ? pInfo->seekOffset : offsetNew;
    clientVg.offsetInfo.walVerBegin = -1;
    clientVg.offsetInfo.walVerEnd = -1;
    clientVg.seekUpdated = false;

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

  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  if (epoch < tmq->epoch || (epoch == tmq->epoch && topicNumGet == 0)) {
    tscInfo("consumer:0x%" PRIx64 " no update ep epoch from %d to epoch %d, incoming topics:%d", tmq->consumerId,
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
    taosArrayDestroy(newTopics);
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
    if (pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tscInfo("consumer:0x%" PRIx64 ", current vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        makeTopicVgroupKey(vgKey, pTopicCur->topicName, pVgCur->vgId);

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVgCur->offsetInfo.endOffset);
        tscInfo("consumer:0x%" PRIx64 ", epoch:%d vgId:%d vgKey:%s, offset:%s", tmq->consumerId, epoch, pVgCur->vgId,
                vgKey, buf);

        SVgroupSaveInfo info = {.currentOffset = pVgCur->offsetInfo.endOffset,
                                .seekOffset = pVgCur->offsetInfo.beginOffset,
                                .commitOffset = pVgCur->offsetInfo.committedOffset,
                                .numOfRows = pVgCur->numOfRows,
                                .vgStatus = pVgCur->vgStatus};
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

  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientVgInfo);
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
  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pReq->subKey, tmq->groupId, groupLen);
  pReq->subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pReq->subKey + groupLen + 1, pTopic->topicName);

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


void changeByteEndian(char* pData){
  char* p = pData;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column length |
  // version:
  int32_t blockVersion = *(int32_t*)p;
  ASSERT(blockVersion == BLOCK_VERSION_1);
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

static void tmqBuildRspFromWrapperInner(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows, SMqRspObj* pRspObj) {
  (*numOfRows) = 0;
  tstrncpy(pRspObj->topic, pWrapper->topicHandle->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);

  pRspObj->vgId = pWrapper->vgHandle->vgId;
  pRspObj->resIter = -1;

  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;

  bool needTransformSchema = !pRspObj->rsp.withSchema;
  if (!pRspObj->rsp.withSchema) {  // withSchema is false if subscribe subquery, true if subscribe db or stable
    pRspObj->rsp.withSchema = true;
    pRspObj->rsp.blockSchema = taosArrayInit(pRspObj->rsp.blockNum, sizeof(void*));
  }
  // extract the rows in this data packet
  for (int32_t i = 0; i < pRspObj->rsp.blockNum; ++i) {
    void* pRetrieve = taosArrayGetP(pRspObj->rsp.blockData, i);
    void* rawData = NULL;
    int64_t rows = 0;
    // deal with compatibility
    if(*(int64_t*)pRetrieve == 0){
      rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
      rows = htobe64(((SRetrieveTableRsp*)pRetrieve)->numOfRows);
    }else if(*(int64_t*)pRetrieve == 1){
      rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
      rows = htobe64(((SRetrieveTableRspForTmq*)pRetrieve)->numOfRows);
    }

    pVg->numOfRows += rows;
    (*numOfRows) += rows;
    changeByteEndian(rawData);
    if (needTransformSchema) { //withSchema is false if subscribe subquery, true if subscribe db or stable
      SSchemaWrapper *schema = tCloneSSchemaWrapper(&pWrapper->topicHandle->schema);
      if(schema){
        taosArrayPush(pRspObj->rsp.blockSchema, &schema);
      }
    }
  }
}

SMqRspObj* tmqBuildRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows) {
  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  pRspObj->resType = RES_TYPE__TMQ;
  memcpy(&pRspObj->rsp, &pWrapper->dataRsp, sizeof(SMqDataRsp));
  tmqBuildRspFromWrapperInner(pWrapper, pVg, numOfRows, pRspObj);
  return pRspObj;
}

SMqTaosxRspObj* tmqBuildTaosxRspFromWrapper(SMqPollRspWrapper* pWrapper, SMqClientVg* pVg, int64_t* numOfRows) {
  SMqTaosxRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqTaosxRspObj));
  pRspObj->resType = RES_TYPE__TMQ_METADATA;
  memcpy(&pRspObj->rsp, &pWrapper->taosxRsp, sizeof(STaosxRsp));

  tmqBuildRspFromWrapperInner(pWrapper, pVg, numOfRows, (SMqRspObj*)pRspObj);
  return pRspObj;
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
    goto FAIL;
  }

  msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  if (tSerializeSMqPollReq(msg, msgSize, &req) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto FAIL;
  }

  pParam = taosMemoryMalloc(sizeof(SMqPollCbParam));
  if (pParam == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  pParam->refId = pTmq->refId;
  strcpy(pParam->topicName, pTopic->topicName);
  pParam->vgId = pVg->vgId;
  pParam->requestId = req.reqId;

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = req.reqId;
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqPollCb;
  sendInfo->msgType = TDMT_VND_TMQ_CONSUME;

  int64_t transporterId = 0;
  char    offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->offsetInfo.endOffset);
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, sendInfo);
  tscDebug("consumer:0x%" PRIx64 " send poll to %s vgId:%d, code:%d, epoch %d, req:%s, reqId:0x%" PRIx64,
           pTmq->consumerId, pTopic->topicName, pVg->vgId, code, pTmq->epoch, offsetFormatBuf, req.reqId);
  if (code != 0) {
    goto FAIL;
  }

  pVg->pollCnt++;
  pVg->seekUpdated = false;  // reset this flag.
  pTmq->pollCnt++;

  return 0;
FAIL:
  taosMemoryFreeClear(msg);
  return tmqPollCb(pParam, NULL, code);
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
    int32_t         numOfVg = taosArrayGetSize(pTopic->vgs);
    if (pTopic->noPrivilege) {
      tscDebug("consumer:0x%" PRIx64 " has no privilegr for topic:%s", tmq->consumerId, pTopic->topicName);
      continue;
    }
    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (taosGetTimestampMs() - pVg->emptyBlockReceiveTs < EMPTY_BLOCK_POLL_IDLE_DURATION) {  // less than 10ms
        tscTrace("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for 10ms before start next poll", tmq->consumerId,
                 tmq->epoch, pVg->vgId);
        continue;
      }

      if (tmq->replayEnable &&
          taosGetTimestampMs() - pVg->blockReceiveTs < pVg->blockSleepForReplay) {  // less than 10ms
        tscTrace("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for %" PRId64 "ms before start next poll when replay",
                 tmq->consumerId, tmq->epoch, pVg->vgId, pVg->blockSleepForReplay);
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
    if (hasData) pVg->offsetInfo.beginOffset = *reqOffset;
    pVg->offsetInfo.endOffset = *rspOffset;
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
    taosGetQitem(tmq->qall, (void**)&pRspWrapper);

    if (pRspWrapper == NULL) {
      taosReadAllQitems(tmq->mqueue, tmq->qall);
      taosGetQitem(tmq->qall, (void**)&pRspWrapper);
      if (pRspWrapper == NULL) {
        return NULL;
      }
    }

    tscDebug("consumer:0x%" PRIx64 " handle rsp, type:%d", tmq->consumerId, pRspWrapper->tmqRspType);

    if (pRspWrapper->code != 0) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      if (pRspWrapper->code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
        atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__RECOVER);
        tscDebug("consumer:0x%" PRIx64 " wait for the re-balance, set status to be RECOVER", tmq->consumerId);
      } else if (pRspWrapper->code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
        terrno = pRspWrapper->code;
        tscError("consumer:0x%" PRIx64 " unexpected rsp from poll, code:%s", tmq->consumerId,
                 tstrerror(pRspWrapper->code));
        taosFreeQitem(pRspWrapper);
        return NULL;
      } else {
        if (pRspWrapper->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {  // for vnode transform
          askEp(tmq, NULL, false, true);
        }
        tscError("consumer:0x%" PRIx64 " msg from vgId:%d discarded, since %s", tmq->consumerId, pollRspWrapper->vgId,
                 tstrerror(pRspWrapper->code));
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        if (pVg) pVg->emptyBlockReceiveTs = taosGetTimestampMs();
        taosWUnLockLatch(&tmq->lock);
      }
      setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
      taosFreeQitem(pRspWrapper);
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;

      int32_t     consumerEpoch = atomic_load_32(&tmq->epoch);
      SMqDataRsp* pDataRsp = &pollRspWrapper->dataRsp;

      if (pDataRsp->head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
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
                   ", total:%" PRId64 ", reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
          pVg->emptyBlockReceiveTs = taosGetTimestampMs();
          tmqFreeRspWrapper(pRspWrapper);
          taosFreeQitem(pRspWrapper);
          taosWUnLockLatch(&tmq->lock);
        } else {  // build rsp
          int64_t    numOfRows = 0;
          SMqRspObj* pRsp = tmqBuildRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
          tmq->totalRows += numOfRows;
          pVg->emptyBlockReceiveTs = 0;
          if (tmq->replayEnable) {
            pVg->blockReceiveTs = taosGetTimestampMs();
            pVg->blockSleepForReplay = pRsp->rsp.sleepTime;
            if (pVg->blockSleepForReplay > 0) {
              int64_t* pRefId1 = taosMemoryMalloc(sizeof(int64_t));
              *pRefId1 = tmq->refId;
              taosTmrStart(tmqReplayTask, pVg->blockSleepForReplay, pRefId1, tmqMgmt.timer);
            }
          }
          tscDebug("consumer:0x%" PRIx64 " process poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                   ", vg total:%" PRId64 ", total:%" PRId64 ", reqId:0x%" PRIx64,
                   tmq->consumerId, pVg->vgId, buf, pDataRsp->blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
                   pollRspWrapper->reqId);
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
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          taosWUnLockLatch(&tmq->lock);
          return NULL;
        }

        updateVgInfo(pVg, &pollRspWrapper->metaRsp.rspOffset, &pollRspWrapper->metaRsp.rspOffset,
                     pollRspWrapper->metaRsp.head.walsver, pollRspWrapper->metaRsp.head.walever, tmq->consumerId, true);
        // build rsp
        SMqMetaRspObj* pRsp = tmqBuildMetaRspFromWrapper(pollRspWrapper);
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
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
      SMqPollRspWrapper* pollRspWrapper = (SMqPollRspWrapper*)pRspWrapper;
      int32_t            consumerEpoch = atomic_load_32(&tmq->epoch);

      if (pollRspWrapper->taosxRsp.head.epoch == consumerEpoch) {
        taosWLockLatch(&tmq->lock);
        SMqClientVg* pVg = getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        pollRspWrapper->vgHandle = pVg;
        pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
        if (pollRspWrapper->vgHandle == NULL || pollRspWrapper->topicHandle == NULL) {
          tscError("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
                   pollRspWrapper->topicName, pollRspWrapper->vgId);
          taosWUnLockLatch(&tmq->lock);
          return NULL;
        }

        updateVgInfo(pVg, &pollRspWrapper->taosxRsp.reqOffset, &pollRspWrapper->taosxRsp.rspOffset,
                     pollRspWrapper->taosxRsp.head.walsver, pollRspWrapper->taosxRsp.head.walever, tmq->consumerId,
                     pollRspWrapper->taosxRsp.blockNum != 0);

        if (pollRspWrapper->taosxRsp.blockNum == 0) {
          tscDebug("consumer:0x%" PRIx64 " taosx empty block received, vgId:%d, vg total:%" PRId64 ", reqId:0x%" PRIx64,
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
        void*   pRsp = NULL;
        int64_t numOfRows = 0;
        if (pollRspWrapper->taosxRsp.createTableNum == 0) {
          tscError("consumer:0x%" PRIx64 " createTableNum should > 0 if rsp type is data_meta", tmq->consumerId);
        } else {
          pRsp = tmqBuildTaosxRspFromWrapper(pollRspWrapper, pVg, &numOfRows);
        }

        tmq->totalRows += numOfRows;

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVg->offsetInfo.endOffset);
        tscDebug("consumer:0x%" PRIx64 " process taosx poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                 ", vg total:%" PRId64 ", total:%" PRId64 ", reqId:0x%" PRIx64,
                 tmq->consumerId, pVg->vgId, buf, pollRspWrapper->dataRsp.blockNum, numOfRows, pVg->numOfRows,
                 tmq->totalRows, pollRspWrapper->reqId);

        taosFreeQitem(pRspWrapper);
        taosWUnLockLatch(&tmq->lock);
        return pRsp;
      } else {
        tscInfo("consumer:0x%" PRIx64 " vgId:%d msg discard since epoch mismatch: msg epoch %d, consumer epoch %d",
                tmq->consumerId, pollRspWrapper->vgId, pollRspWrapper->taosxRsp.head.epoch, consumerEpoch);
        setVgIdle(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId);
        tmqFreeRspWrapper(pRspWrapper);
        taosFreeQitem(pRspWrapper);
      }
    } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
      tscDebug("consumer:0x%" PRIx64 " ep msg received", tmq->consumerId);
      SMqAskEpRspWrapper* pEpRspWrapper = (SMqAskEpRspWrapper*)pRspWrapper;
      SMqAskEpRsp*        rspMsg = &pEpRspWrapper->msg;
      doUpdateLocalEp(tmq, pEpRspWrapper->epoch, rspMsg);
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

  while (1) {
    tmqHandleAllDelayedTask(tmq);

    if (tmqPollImpl(tmq, timeout) < 0) {
      tscError("consumer:0x%" PRIx64 " return due to poll error", tmq->consumerId);
    }

    rspObj = tmqHandleAllRsp(tmq, timeout);
    if (rspObj) {
      tscDebug("consumer:0x%" PRIx64 " return rsp %p", tmq->consumerId, rspObj);
      return (TAOS_RES*)rspObj;
    } else if (terrno == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      tscInfo("consumer:0x%" PRIx64 " return null since no committed offset", tmq->consumerId);
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

static void displayConsumeStatistics(tmq_t* pTmq) {
  taosRLockLatch(&pTmq->lock);
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
  taosRUnLockLatch(&pTmq->lock);
  tscDebug("consumer:0x%" PRIx64 " rows dist end", pTmq->consumerId);
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;

  tscInfo("consumer:0x%" PRIx64 " start to close consumer, status:%d", tmq->consumerId, tmq->status);
  displayConsumeStatistics(tmq);

  if (tmq->status == TMQ_CONSUMER_STATUS__READY) {
    // if auto commit is set, commit before close consumer. Otherwise, do nothing.
    if (tmq->autoCommit) {
      int32_t rsp = tmq_commit_sync(tmq, NULL);
      if (rsp != 0) {
        return rsp;
      }
    }
    taosSsleep(2);  // sleep 2s for hb to send offset and rows to server

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
    tscInfo("consumer:0x%" PRIx64 " not in ready state, close it directly", tmq->consumerId);
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
  if (res == NULL) {
    return TMQ_RES_INVALID;
  }
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
  if (res == NULL) {
    return NULL;
  }
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
  if (res == NULL) {
    return NULL;
  }

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
  if (res == NULL) {
    return -1;
  }
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
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ(res)) {
    SMqRspObj*    pRspObj = (SMqRspObj*)res;
    STqOffsetVal* pOffset = &pRspObj->rsp.reqOffset;
    if (pOffset->type == TMQ_OFFSET__LOG) {
      return pRspObj->rsp.reqOffset.version;
    } else {
      tscError("invalid offset type:%d", pOffset->type);
    }
  } else if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pRspObj = (SMqMetaRspObj*)res;
    if (pRspObj->metaRsp.rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->metaRsp.rspOffset.version;
    }
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pRspObj = (SMqTaosxRspObj*)res;
    if (pRspObj->rsp.reqOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->rsp.reqOffset.version;
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
  tsem_post(&pInfo->sem);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* pRes) {
  if (tmq == NULL) {
    tscError("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  tsem_init(&pInfo->sem, 0, 0);
  pInfo->code = 0;

  if (pRes == NULL) {
    asyncCommitAllOffsets(tmq, commitCallBackFn, pInfo);
  } else {
    asyncCommitFromResult(tmq, pRes, commitCallBackFn, pInfo);
  }

  tsem_wait(&pInfo->sem);
  code = pInfo->code;

  tsem_destroy(&pInfo->sem);
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
  sprintf(tname, "%d.%s", accId, pTopicName);

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

  tsem_init(&pInfo->sem, 0, 0);
  pInfo->code = 0;

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, commitCallBackFn, pInfo);
  if (code == 0) {
    tsem_wait(&pInfo->sem);
    code = pInfo->code;
  }

  if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
  tsem_destroy(&pInfo->sem);
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
  sprintf(tname, "%d.%s", accId, pTopicName);

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

  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tscInfo("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (pParam->sync) {
    SMqAskEpRsp rsp = {0};
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    doUpdateLocalEp(tmq, head->epoch, &rsp);
    tDeleteSMqAskEpRsp(&rsp);
  } else {
    SMqAskEpRspWrapper* pWrapper = taosAllocateQitem(sizeof(SMqAskEpRspWrapper), DEF_QITEM, 0);
    if (pWrapper == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
    pWrapper->epoch = head->epoch;
    memcpy(&pWrapper->msg, pMsg->pData, sizeof(SMqRspHead));
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pWrapper->msg);

    taosWriteQitem(tmq->mqueue, pWrapper);
  }

END:
  taosReleaseRef(tmqMgmt.rsetId, pParam->refId);

FAIL:
  if (pParam->sync) {
    SAskEpInfo* pInfo = pParam->pParam;
    pInfo->code = code;
    tsem_post(&pInfo->sem);
  }

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pParam);
  return code;
}

int32_t syncAskEp(tmq_t* pTmq) {
  SAskEpInfo* pInfo = taosMemoryMalloc(sizeof(SAskEpInfo));
  tsem_init(&pInfo->sem, 0, 0);

  askEp(pTmq, pInfo, true, false);
  tsem_wait(&pInfo->sem);

  int32_t code = pInfo->code;
  tsem_destroy(&pInfo->sem);
  taosMemoryFree(pInfo);
  return code;
}

void askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpSet) {
  SMqAskEpReq req = {0};
  req.consumerId = pTmq->consumerId;
  req.epoch = updateEpSet ? -1 : pTmq->epoch;
  strcpy(req.cgroup, pTmq->groupId);
  int              code = 0;
  SMqAskEpCbParam* pParam = NULL;
  void*            pReq = NULL;

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  if (tlen < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq failed", pTmq->consumerId);
    code = TSDB_CODE_INVALID_PARA;
    goto FAIL;
  }

  pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc askEpReq msg, size:%d", pTmq->consumerId, tlen);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tscError("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq %d failed", pTmq->consumerId, tlen);
    code = TSDB_CODE_INVALID_PARA;
    goto FAIL;
  }

  pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tscError("consumer:0x%" PRIx64 ", failed to malloc subscribe param", pTmq->consumerId);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  pParam->refId = pTmq->refId;
  pParam->sync = sync;
  pParam->pParam = param;

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  sendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = tlen, .handle = NULL};

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = askEpCb;
  sendInfo->msgType = TDMT_MND_TMQ_ASK_EP;

  SEpSet epSet = getEpSet_s(&pTmq->pTscObj->pAppInfo->mgmtEp);
  tscInfo("consumer:0x%" PRIx64 " ask ep from mnode, reqId:0x%" PRIx64, pTmq->consumerId, sendInfo->requestId);

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);
  if (code == 0) {
    return;
  }

FAIL:
  taosMemoryFreeClear(pParam);
  taosMemoryFreeClear(pReq);
  askEpCb(pParam, NULL, code);
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
  if (pParamSet->callbackFn != NULL) {
    pParamSet->callbackFn(tmq, pParamSet->code, pParamSet->userParam);
  }

  taosMemoryFree(pParamSet);
  taosReleaseRef(tmqMgmt.rsetId, refId);
  return 0;
}

void commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  if (waitingRspNum == 0) {
    tscInfo("consumer:0x%" PRIx64 " topic:%s vgId:%d all commit-rsp received, commit completed", consumerId, pTopic,
            vgId);
    tmqCommitDone(pParamSet);
  } else {
    tscInfo("consumer:0x%" PRIx64 " topic:%s vgId:%d commit-rsp received, remain:%d", consumerId, pTopic, vgId,
            waitingRspNum);
  }
}

SReqResultInfo* tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4) {
  SMqRspObj* pRspObj = (SMqRspObj*)res;
  pRspObj->resIter++;

  if (pRspObj->resIter < pRspObj->rsp.blockNum) {
    SRetrieveTableRspForTmq* pRetrieveTmq =
        (SRetrieveTableRspForTmq*)taosArrayGetP(pRspObj->rsp.blockData, pRspObj->resIter);
    if (pRspObj->rsp.withSchema) {
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(pRspObj->rsp.blockSchema, pRspObj->resIter);
      setResSchemaInfo(&pRspObj->resInfo, pSW->pSchema, pSW->nCols);
      taosMemoryFreeClear(pRspObj->resInfo.row);
      taosMemoryFreeClear(pRspObj->resInfo.pCol);
      taosMemoryFreeClear(pRspObj->resInfo.length);
      taosMemoryFreeClear(pRspObj->resInfo.convertBuf);
      taosMemoryFreeClear(pRspObj->resInfo.convertJson);
    }

    pRspObj->resInfo.pData = (void*)pRetrieveTmq->data;
    pRspObj->resInfo.numOfRows = htobe64(pRetrieveTmq->numOfRows);
    pRspObj->resInfo.current = 0;
    pRspObj->resInfo.precision = pRetrieveTmq->precision;

    // TODO handle the compressed case
    pRspObj->resInfo.totalRows += pRspObj->resInfo.numOfRows;
    setResultDataPtr(&pRspObj->resInfo, pRspObj->resInfo.fields, pRspObj->resInfo.numOfCols, pRspObj->resInfo.numOfRows,
                     convertUcs4);

    return &pRspObj->resInfo;
  }

  return NULL;
}

static int32_t tmqGetWalInfoCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqVgWalInfoParam* pParam = param;
  SMqVgCommon*       pCommon = pParam->pCommon;

  int32_t total = atomic_add_fetch_32(&pCommon->numOfRsp, 1);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("consumer:0x%" PRIx64 " failed to get the wal info from vgId:%d for topic:%s", pCommon->consumerId,
             pParam->vgId, pCommon->pTopicName);
    pCommon->code = code;
  } else {
    SMqDataRsp rsp;
    SDecoder   decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeMqDataRsp(&decoder, &rsp);
    tDecoderClear(&decoder);

    SMqRspHead* pHead = pMsg->pData;

    tmq_topic_assignment assignment = {.begin = pHead->walsver,
                                       .end = pHead->walever + 1,
                                       .currentOffset = rsp.rspOffset.version,
                                       .vgId = pParam->vgId};

    taosThreadMutexLock(&pCommon->mutex);
    taosArrayPush(pCommon->pList, &assignment);
    taosThreadMutexUnlock(&pCommon->mutex);
  }

  if (total == pParam->totalReq) {
    tsem_post(&pCommon->rsp);
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pParam);
  return 0;
}

static void destroyCommonInfo(SMqVgCommon* pCommon) {
  if (pCommon == NULL) {
    return;
  }
  taosArrayDestroy(pCommon->pList);
  tsem_destroy(&pCommon->rsp);
  taosThreadMutexDestroy(&pCommon->mutex);
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
    SDecoder decoder;
    tDecoderInit(&decoder, (uint8_t*)pMsg->pData, pMsg->len);
    if (tDecodeMqVgOffset(&decoder, &pParam->vgOffset) < 0) {
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
  tsem_post(&pParam->sem);
  return 0;
}

int64_t getCommittedFromServer(tmq_t* tmq, char* tname, int32_t vgId, SEpSet* epSet) {
  int32_t     code = 0;
  SMqVgOffset pOffset = {0};

  pOffset.consumerId = tmq->consumerId;

  int32_t groupLen = strlen(tmq->groupId);
  memcpy(pOffset.offset.subKey, tmq->groupId, groupLen);
  pOffset.offset.subKey[groupLen] = TMQ_SEPARATOR;
  strcpy(pOffset.offset.subKey + groupLen + 1, tname);

  int32_t len = 0;
  tEncodeSize(tEncodeMqVgOffset, &pOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, len);
  tEncodeMqVgOffset(&encoder, &pOffset);
  tEncoderClear(&encoder);

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(buf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SMqCommittedParam* pParam = taosMemoryMalloc(sizeof(SMqCommittedParam));
  if (pParam == NULL) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tsem_init(&pParam->sem, 0, 0);

  sendInfo->msgInfo = (SDataBuf){.pData = buf, .len = sizeof(SMsgHead) + len, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmCommittedCb;
  sendInfo->msgType = TDMT_VND_TMQ_VG_COMMITTEDINFO;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, epSet, &transporterId, sendInfo);
  if (code != 0) {
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    tsem_destroy(&pParam->sem);
    taosMemoryFree(pParam);
    return code;
  }

  tsem_wait(&pParam->sem);
  code = pParam->code;
  if (code == TSDB_CODE_SUCCESS) {
    if (pParam->vgOffset.offset.val.type == TMQ_OFFSET__LOG) {
      code = pParam->vgOffset.offset.val.version;
    } else {
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
    }
  }
  tsem_destroy(&pParam->sem);
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
  sprintf(tname, "%d.%s", accId, pTopicName);

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
  sprintf(tname, "%d.%s", accId, pTopicName);

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
  sprintf(tname, "%d.%s", accId, pTopicName);
  int32_t code = TSDB_CODE_SUCCESS;

  taosWLockLatch(&tmq->lock);
  SMqClientTopic* pTopic = getTopicByName(tmq, tname);
  if (pTopic == NULL) {
    code = TSDB_CODE_TMQ_INVALID_TOPIC;
    goto end;
  }

  // in case of snapshot is opened, no valid offset will return
  *numOfAssignment = taosArrayGetSize(pTopic->vgs);
  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
    int32_t      type = pClientVg->offsetInfo.beginOffset.type;
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
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  bool needFetch = false;

  for (int32_t j = 0; j < (*numOfAssignment); ++j) {
    SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, j);
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
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      code = terrno;
      goto end;
    }

    pCommon->pList = taosArrayInit(4, sizeof(tmq_topic_assignment));
    tsem_init(&pCommon->rsp, 0, 0);
    taosThreadMutexInit(&pCommon->mutex, 0);
    pCommon->pTopicName = taosStrdup(pTopic->topicName);
    pCommon->consumerId = tmq->consumerId;

    terrno = TSDB_CODE_OUT_OF_MEMORY;
    for (int32_t i = 0; i < (*numOfAssignment); ++i) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);

      SMqVgWalInfoParam* pParam = taosMemoryMalloc(sizeof(SMqVgWalInfoParam));
      if (pParam == NULL) {
        code = terrno;
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
        code = terrno;
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
        code = terrno;
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
      sendInfo->fp = tmqGetWalInfoCb;
      sendInfo->msgType = TDMT_VND_TMQ_VG_WALINFO;

      int64_t transporterId = 0;
      char    offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
      tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pClientVg->offsetInfo.beginOffset);

      tscInfo("consumer:0x%" PRIx64 " %s retrieve wal info vgId:%d, epoch %d, req:%s, reqId:0x%" PRIx64,
              tmq->consumerId, pTopic->topicName, pClientVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
      code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pClientVg->epSet, &transporterId, sendInfo);
      if (code != 0) {
        taosMemoryFree(pParam);
        taosMemoryFree(msg);
        goto end;
      }
    }

    tsem_wait(&pCommon->rsp);
    code = pCommon->code;

    terrno = code;
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
  SMqSeekParam* pParam = param;
  pParam->code = code;
  tsem_post(&pParam->sem);
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
  sprintf(tname, "%d.%s", accId, pTopicName);

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
  snprintf(req.subKey, TSDB_SUBSCRIBE_KEY_LEN, "%s:%s", tmq->groupId, tname);
  req.head.vgId = vgId;
  req.consumerId = tmq->consumerId;

  int32_t msgSize = tSerializeSMqSeekReq(NULL, 0, &req);
  if (msgSize < 0) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }

  char* msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (tSerializeSMqSeekReq(msg, msgSize, &req) < 0) {
    taosMemoryFree(msg);
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(msg);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SMqSeekParam* pParam = taosMemoryMalloc(sizeof(SMqSeekParam));
  if (pParam == NULL) {
    taosMemoryFree(msg);
    taosMemoryFree(sendInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tsem_init(&pParam->sem, 0, 0);

  sendInfo->msgInfo = (SDataBuf){.pData = msg, .len = msgSize, .handle = NULL};
  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqSeekCb;
  sendInfo->msgType = TDMT_VND_TMQ_SEEK;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);
  if (code != 0) {
    taosMemoryFree(msg);
    taosMemoryFree(sendInfo);
    tsem_destroy(&pParam->sem);
    taosMemoryFree(pParam);
    return code;
  }

  tsem_wait(&pParam->sem);
  code = pParam->code;
  tsem_destroy(&pParam->sem);
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

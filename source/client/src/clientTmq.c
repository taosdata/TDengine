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

#define tqFatalC(...) do { if (cDebugFlag & DEBUG_FATAL || tqClientDebug) { taosPrintLog("TQ  FATAL ", DEBUG_FATAL, tqDebugFlag, __VA_ARGS__); }}     while(0)
#define tqErrorC(...) do { if (cDebugFlag & DEBUG_ERROR || tqClientDebug) { taosPrintLog("TQ  ERROR ", DEBUG_ERROR, tqDebugFlag, __VA_ARGS__); }}     while(0)
#define tqWarnC(...)  do { if (cDebugFlag & DEBUG_WARN || tqClientDebug)  { taosPrintLog("TQ  WARN ", DEBUG_WARN, tqDebugFlag, __VA_ARGS__); }}       while(0)
#define tqInfoC(...)  do { if (cDebugFlag & DEBUG_INFO || tqClientDebug)  { taosPrintLog("TQ  ", DEBUG_INFO, tqDebugFlag, __VA_ARGS__); }}            while(0)
#define tqDebugC(...) do { if (cDebugFlag & DEBUG_DEBUG || tqClientDebug) { taosPrintLog("TQ  ", DEBUG_DEBUG, tqDebugFlag, __VA_ARGS__); }} while(0)
#define tqTraceC(...) do { if (cDebugFlag & DEBUG_TRACE || tqClientDebug) { taosPrintLog("TQ  ", DEBUG_TRACE, tqDebugFlag, __VA_ARGS__); }} while(0)

#define EMPTY_BLOCK_POLL_IDLE_DURATION 10
#define DEFAULT_AUTO_COMMIT_INTERVAL   5000
#define DEFAULT_HEARTBEAT_INTERVAL     3000
#define DEFAULT_ASKEP_INTERVAL         1000
#define DEFAULT_COMMIT_CNT             1
#define SUBSCRIBE_RETRY_MAX_COUNT      240
#define SUBSCRIBE_RETRY_INTERVAL       500


#define SET_ERROR_MSG_TMQ(MSG) \
  if (errstr != NULL) (void)snprintf(errstr, errstrLen, MSG);

#define PROCESS_POLL_RSP(FUNC,DATA) \
  SDecoder decoder = {0}; \
  tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead)); \
  if (FUNC(&decoder, DATA) < 0) { \
    tDecoderClear(&decoder); \
    code = TSDB_CODE_OUT_OF_MEMORY; \
    goto END;\
  }\
  tDecoderClear(&decoder);\
  (void)memcpy(DATA, pMsg->pData, sizeof(SMqRspHead));

#define DELETE_POLL_RSP(FUNC,DATA) \
  SMqPollRspWrapper* pRsp = &rspWrapper->pollRsp;\
  taosMemoryFreeClear(pRsp->pEpset);\
  FUNC(DATA);

enum {
  TMQ_VG_STATUS__IDLE = 0,
  TMQ_VG_STATUS__WAIT,
};

enum {
  TMQ_CONSUMER_STATUS__INIT = 0,
  TMQ_CONSUMER_STATUS__READY,
  TMQ_CONSUMER_STATUS__CLOSED,
};

enum {
  TMQ_DELAYED_TASK__ASK_EP = 1,
  TMQ_DELAYED_TASK__COMMIT,
};

typedef struct {
  tmr_h   timer;
  int32_t rsetId;
} SMqMgmt;

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
  int8_t  pollFlag;

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

typedef struct {
  int32_t code;
  tsem2_t sem;
} SAskEpInfo;

typedef struct {
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
  int32_t         vgId;
  char            topicName[TSDB_TOPIC_FNAME_LEN];
  SMqClientTopic* topicHandle;
  uint64_t        reqId;
  SEpSet*         pEpset;
  union {
    struct{
      SMqRspHead   head;
      STqOffsetVal rspOffset;
    };
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    SMqBatchMetaRsp batchMetaRsp;
  };
} SMqPollRspWrapper;

typedef struct {
  int32_t code;
  int8_t  tmqRspType;
  int32_t epoch;
  union{
    SMqPollRspWrapper pollRsp;
    SMqAskEpRsp       epRsp;
  };
} SMqRspWrapper;

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

typedef struct {
  tsem2_t       rsp;
  int32_t       numOfRsp;
  SArray*       pList;
  TdThreadMutex mutex;
  int64_t       consumerId;
  char*         pTopicName;
  int32_t       code;
} SMqVgCommon;

typedef struct {
  tsem2_t sem;
  int32_t code;
} SMqSeekParam;

typedef struct {
  tsem2_t     sem;
  int32_t     code;
  SMqVgOffset vgOffset;
} SMqCommittedParam;

typedef struct {
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

typedef struct {
  tsem2_t sem;
  int32_t code;
} SSyncCommitInfo;

typedef struct {
  STqOffsetVal currentOffset;
  STqOffsetVal commitOffset;
  STqOffsetVal seekOffset;
  int64_t      numOfRows;
  int32_t      vgStatus;
} SVgroupSaveInfo;

static   TdThreadOnce   tmqInit = PTHREAD_ONCE_INIT;  // initialize only once
volatile int32_t        tmqInitRes = 0;               // initialize rsp code
static   SMqMgmt        tmqMgmt = {0};

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
  int32_t code = 0;
  if (conf == NULL || key == NULL || value == NULL) {
    tqErrorC("tmq_conf_set null, conf:%p key:%p value:%p", conf, key, value);
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
      tqErrorC("invalid value for enable.auto.commit: %s", value);
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "auto.commit.interval.ms") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    if (tmp < 0 || code != 0) {
      tqErrorC("invalid value for auto.commit.interval.ms: %s", value);
      return TMQ_CONF_INVALID;
    }
    conf->autoCommitInterval = (tmp > INT32_MAX ? INT32_MAX : tmp);
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "session.timeout.ms") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    if (tmp < 6000 || tmp > 1800000 || code != 0) {
      tqErrorC("invalid value for session.timeout.ms: %s", value);
      return TMQ_CONF_INVALID;
    }
    conf->sessionTimeoutMs = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "heartbeat.interval.ms") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    if (tmp < 1000 || tmp >= conf->sessionTimeoutMs || code != 0) {
      tqErrorC("invalid value for heartbeat.interval.ms: %s", value);
      return TMQ_CONF_INVALID;
    }
    conf->heartBeatIntervalMs = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "max.poll.interval.ms") == 0) {
    int32_t tmp;
    code = taosStr2int32(value, &tmp);
    if (tmp < 1000 || code != 0) {
      tqErrorC("invalid value for max.poll.interval.ms: %s", value);
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
      tqErrorC("invalid value for auto.offset.reset: %s", value);
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
      tqErrorC("invalid value for msg.with.table.name: %s", value);
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
      tqErrorC("invalid value for experimental.snapshot.enable: %s", value);
      return TMQ_CONF_INVALID;
    }
  }

  if (strcasecmp(key, "td.connect.ip") == 0) {
    void *tmp = taosStrdup(value);
    if (tmp == NULL) {
      tqErrorC("tmq_conf_set out of memory:%d", terrno);
      return TMQ_CONF_INVALID;
    }
    conf->ip = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.user") == 0) {
    void *tmp = taosStrdup(value);
    if (tmp == NULL) {
      tqErrorC("tmq_conf_set out of memory:%d", terrno);
      return TMQ_CONF_INVALID;
    }
    conf->user = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.pass") == 0) {
    void *tmp = taosStrdup(value);
    if (tmp == NULL) {
      tqErrorC("tmq_conf_set out of memory:%d", terrno);
      return TMQ_CONF_INVALID;
    }
    conf->pass = tmp;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.port") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    if (tmp <= 0 || tmp > 65535 || code != 0) {
      tqErrorC("invalid value for td.connect.port: %s", value);
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
      tqErrorC("invalid value for enable.replay: %s", value);
      return TMQ_CONF_INVALID;
    }
  }
  if (strcasecmp(key, "msg.consume.excluded") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    conf->sourceExcluded = (0 == code && tmp != 0) ? TD_REQ_FROM_TAOX : 0;
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "td.connect.db") == 0) {
    return TMQ_CONF_OK;
  }

  if (strcasecmp(key, "msg.enable.batchmeta") == 0) {
    int64_t tmp;
    code = taosStr2int64(value, &tmp);
    conf->enableBatchMeta = (0 == code && tmp != 0) ? true : false;
    return TMQ_CONF_OK;
  }

  tqErrorC("unknown key: %s", key);
  return TMQ_CONF_UNKNOWN;
}

tmq_list_t* tmq_list_new() {
  return (tmq_list_t*)taosArrayInit(0, sizeof(void*));
}

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  if (list == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  char* topic = taosStrdup(src);
  if (topic == NULL) return terrno;
  if (taosArrayPush(container, &topic) == NULL) {
    taosMemoryFree(topic);
    return terrno;
  }
  return 0;
}

void tmq_list_destroy(tmq_list_t* list) {
  if (list == NULL) return;
  SArray* container = &list->container;
  taosArrayDestroyP(container, taosMemoryFree);
}

int32_t tmq_list_get_size(const tmq_list_t* list) {
  if (list == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  const SArray* container = &list->container;
  return taosArrayGetSize(container);
}

char** tmq_list_to_c_array(const tmq_list_t* list) {
  if (list == NULL) {
    return NULL;
  }
  const SArray* container = &list->container;
  return container->pData;
}

static int32_t tmqCommitDone(SMqCommitCbParamSet* pParamSet) {
  int64_t refId = pParamSet->refId;
  int32_t code = 0;
  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
  }

  // if no more waiting rsp
  if (pParamSet->callbackFn != NULL) {
    pParamSet->callbackFn(tmq, pParamSet->code, pParamSet->userParam);
  }

  taosMemoryFree(pParamSet);
  if (tmq != NULL) {
    code = taosReleaseRef(tmqMgmt.rsetId, refId);
  }

  return code;
}

static int32_t commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId) {
  int32_t waitingRspNum = atomic_sub_fetch_32(&pParamSet->waitingRspNum, 1);
  if (waitingRspNum == 0) {
    tqDebugC("consumer:0x%" PRIx64 " topic:%s vgId:%d all commit-rsp received, commit completed", consumerId, pTopic,
             vgId);
    return tmqCommitDone(pParamSet);
  } else {
    tqDebugC("consumer:0x%" PRIx64 " topic:%s vgId:%d commit-rsp received, remain:%d", consumerId, pTopic, vgId,
             waitingRspNum);
  }
  return 0;
}

static int32_t tmqCommitCb(void* param, SDataBuf* pBuf, int32_t code) {
  if (pBuf){
    taosMemoryFreeClear(pBuf->pData);
    taosMemoryFreeClear(pBuf->pEpSet);
  }
  if(param == NULL){
    return TSDB_CODE_INVALID_PARA;
  }
  SMqCommitCbParam*    pParam = (SMqCommitCbParam*)param;
  SMqCommitCbParamSet* pParamSet = (SMqCommitCbParamSet*)pParam->params;

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

  tqErrorC("consumer:0x%" PRIx64 ", total:%d, failed to find topic:%s", tmq->consumerId, numOfTopics, pTopicName);
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
    tqErrorC("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
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

static int32_t innerCommit(tmq_t* tmq, char* pTopicName, STqOffsetVal* offsetVal, SMqClientVg* pVg, SMqCommitCbParamSet* pParamSet){
  int32_t code = 0;
  if (offsetVal->type <= 0) {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    return code;
  }
  if (tOffsetEqual(offsetVal, &pVg->offsetInfo.committedOffset)) {
    code = TSDB_CODE_TMQ_SAME_COMMITTED_VALUE;
    return code;
  }
  char offsetBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetBuf, tListLen(offsetBuf), offsetVal);

  char commitBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(commitBuf, tListLen(commitBuf), &pVg->offsetInfo.committedOffset);

  code = doSendCommitMsg(tmq, pVg->vgId, &pVg->epSet, offsetVal, pTopicName, pParamSet);
  if (code != TSDB_CODE_SUCCESS) {
    tqErrorC("consumer:0x%" PRIx64 " topic:%s on vgId:%d end commit msg failed, send offset:%s committed:%s, code:%s",
             tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf, tstrerror(terrno));
    return code;
  }

  tqDebugC("consumer:0x%" PRIx64 " topic:%s on vgId:%d send commit msg success, send offset:%s committed:%s",
          tmq->consumerId, pTopicName, pVg->vgId, offsetBuf, commitBuf);
  tOffsetCopy(&pVg->offsetInfo.committedOffset, offsetVal);
  return code;
}

static int32_t asyncCommitOffset(tmq_t* tmq, char* pTopicName, int32_t vgId, STqOffsetVal* offsetVal,
                                 tmq_commit_cb* pCommitFp, void* userParam) {
  tqInfoC("consumer:0x%" PRIx64 " do manual commit offset for %s, vgId:%d", tmq->consumerId, pTopicName, vgId);
  SMqCommitCbParamSet* pParamSet = NULL;
  int32_t code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, 0, &pParamSet);
  if (code != 0){
    return code;
  }

  taosRLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  code = getClientVg(tmq, pTopicName, vgId, &pVg);
  if (code == 0) {
    code = innerCommit(tmq, pTopicName, offsetVal, pVg, pParamSet);
  }
  taosRUnLockLatch(&tmq->lock);

  if (code != 0){
    taosMemoryFree(pParamSet);
  }
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

  if (TD_RES_TMQ(pRes) || TD_RES_TMQ_META(pRes) ||
      TD_RES_TMQ_METADATA(pRes) || TD_RES_TMQ_BATCH_META(pRes)) {
    SMqRspObj* pRspObj = (SMqRspObj*)pRes;
    pTopicName = pRspObj->topic;
    vgId = pRspObj->vgId;
    offsetVal = pRspObj->rspOffset;
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

static int32_t innerCommitAll(tmq_t* tmq, SMqCommitCbParamSet* pParamSet){
  int32_t code = 0;
  taosRLockLatch(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tqDebugC("consumer:0x%" PRIx64 " start to commit offset for %d topics", tmq->consumerId, numOfTopics);

  for (int32_t i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      code = TSDB_CODE_TMQ_INVALID_TOPIC;
      goto END;
    }
    int32_t numOfVgroups = taosArrayGetSize(pTopic->vgs);
    tqDebugC("consumer:0x%" PRIx64 " commit offset for topics:%s, numOfVgs:%d", tmq->consumerId, pTopic->topicName, numOfVgroups);
    for (int32_t j = 0; j < numOfVgroups; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        code = terrno;
        goto END;
      }

      code = innerCommit(tmq, pTopic->topicName, &pVg->offsetInfo.endOffset, pVg, pParamSet);
      if (code != 0){
        tqDebugC("consumer:0x%" PRIx64 " topic:%s vgId:%d, no commit, code:%s, current offset version:%" PRId64 ", ordinal:%d/%d",
                 tmq->consumerId, pTopic->topicName, pVg->vgId, tstrerror(code), pVg->offsetInfo.endOffset.version, j + 1, numOfVgroups);
      }
    }
  }
  tqDebugC("consumer:0x%" PRIx64 " total commit:%d for %d topics", tmq->consumerId, pParamSet->waitingRspNum - DEFAULT_COMMIT_CNT,
           numOfTopics);
END:
  taosRUnLockLatch(&tmq->lock);
  return code;
}

static void asyncCommitAllOffsets(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam) {
  int32_t code = 0;
  SMqCommitCbParamSet* pParamSet = NULL;
  // init waitingRspNum as DEFAULT_COMMIT_CNT to prevent concurrency issue
  code = prepareCommitCbParamSet(tmq, pCommitFp, userParam, DEFAULT_COMMIT_CNT, &pParamSet);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " prepareCommitCbParamSet failed, code:%s", tmq->consumerId, tstrerror(code));
    if (pCommitFp != NULL) {
      pCommitFp(tmq, code, userParam);
    }
    return;
  }
  code = innerCommitAll(tmq, pParamSet);
  if (code != 0){
    tqErrorC("consumer:0x%" PRIx64 " innerCommitAll failed, code:%s", tmq->consumerId, tstrerror(code));
  }

  code = commitRspCountDown(pParamSet, tmq->consumerId, "init", -1);
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 " commit rsp count down failed, code:%s", tmq->consumerId, tstrerror(code));
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
      if (tsem2_post(&tmq->rspSem) != 0){
        tqErrorC("consumer:0x%" PRIx64 " failed to post sem, type:%d", tmq->consumerId, type);
      }
    }else{
      taosFreeQitem(pTaskType);
    }
  }

  code = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (code != 0){
    tqErrorC("failed to release ref:%"PRId64 ", type:%d, code:%d", refId, type, code);
  }
}

void tmqAssignAskEpTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__ASK_EP);
}

void tmqReplayTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  tmq_t*  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) return;

  if (tsem2_post(&tmq->rspSem) != 0){
    tqErrorC("consumer:0x%" PRIx64 " failed to post sem, replay", tmq->consumerId);
  }
  int32_t code = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (code != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, code);
  }
}

void tmqAssignDelayedCommitTask(void* param, void* tmrId) {
  int64_t refId = (int64_t)param;
  generateTimedTask(refId, TMQ_DELAYED_TASK__COMMIT);
}

int32_t tmqHbCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (param == NULL || code != 0){
    goto END;
  }

  SMqHbRsp rsp = {0};
  code = tDeserializeSMqHbRsp(pMsg->pData, pMsg->len, &rsp);
  if (code != 0) {
    goto END;
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
            tqInfoC("consumer:0x%" PRIx64 ", has no privilege, topic:%s", tmq->consumerId, privilege->topic);
            pTopicCur->noPrivilege = 1;
          }
        }
      }
    }
    taosWUnLockLatch(&tmq->lock);
    code = taosReleaseRef(tmqMgmt.rsetId, refId);
    if (code != 0){
      tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, code);
    }
  }

  tqClientDebug = rsp.debugFlag;
  tDestroySMqHbRsp(&rsp);

END:
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
  req.pollFlag = atomic_load_8(&tmq->pollFlag);
  req.topics = taosArrayInit(taosArrayGetSize(tmq->clientTopics), sizeof(TopicOffsetRows));
  if (req.topics == NULL) {
    goto END;
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
    tstrncpy(data->topicName, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
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
      tqDebugC("consumer:0x%" PRIx64 ",report offset, group:%s vgId:%d, offset:%s/%" PRId64 ", rows:%" PRId64,
               tmq->consumerId, tmq->groupId, offRows->vgId, buf, offRows->ever, offRows->rows);
    }
  }
  taosRUnLockLatch(&tmq->lock);

  int32_t tlen = tSerializeSMqHbReq(NULL, 0, &req);
  if (tlen < 0) {
    tqErrorC("tSerializeSMqHbReq failed, size:%d", tlen);
    goto END;
  }

  void* pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tqErrorC("failed to malloc MqHbReq msg, code:%d", terrno);
    goto END;
  }

  if (tSerializeSMqHbReq(pReq, tlen, &req) < 0) {
    tqErrorC("tSerializeSMqHbReq %d failed", tlen);
    taosMemoryFree(pReq);
    goto END;
  }

  SMsgSendInfo* sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    taosMemoryFree(pReq);
    goto END;
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
    tqErrorC("tmqSendHbReq asyncSendMsgToServer failed");
  }
  (void)atomic_val_compare_exchange_8(&tmq->pollFlag, 1, 0);

END:
  tDestroySMqHbReq(&req);
  if (tmrId != NULL) {
    bool ret = taosTmrReset(tmqSendHbReq, tmq->heartBeatIntervalMs, param, tmqMgmt.timer, &tmq->hbLiveTimer);
    tqDebugC("reset timer fo tmq hb:%d", ret);
  }
  int32_t ret = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (ret != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, ret);
  }
}

static void defaultCommitCbFn(tmq_t* pTmq, int32_t code, void* param) {
  if (code != 0) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to commit offset, code:%s", pTmq->consumerId, tstrerror(code));
  }
}

static void tmqFreeRspWrapper(SMqRspWrapper* rspWrapper) {
  if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    tDeleteSMqAskEpRsp(&rspWrapper->epRsp);
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    DELETE_POLL_RSP(tDeleteMqDataRsp, &pRsp->dataRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP){
    DELETE_POLL_RSP(tDeleteSTaosxRsp, &pRsp->dataRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    DELETE_POLL_RSP(tDeleteMqMetaRsp,&pRsp->metaRsp)
  } else if (rspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    DELETE_POLL_RSP(tDeleteMqBatchMetaRsp,&pRsp->batchMetaRsp)
  }
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

static void initClientTopicFromRsp(SMqClientTopic* pTopic, SMqSubTopicEp* pTopicEp, SHashObj* pVgOffsetHashMap,
                                   tmq_t* tmq) {
  pTopic->schema = pTopicEp->schema;
  pTopicEp->schema.nCols = 0;
  pTopicEp->schema.pSchema = NULL;

  char    vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
  int32_t vgNumGet = taosArrayGetSize(pTopicEp->vgs);

  tstrncpy(pTopic->topicName, pTopicEp->topic, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pTopic->db, pTopicEp->db, TSDB_DB_FNAME_LEN);

  tqInfoC("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d", tmq->consumerId, pTopic->topicName, vgNumGet);
  pTopic->vgs = taosArrayInit(vgNumGet, sizeof(SMqClientVg));
  if (pTopic->vgs == NULL) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to init vgs for topic:%s", tmq->consumerId, pTopic->topicName);
    return;
  }
  for (int32_t j = 0; j < vgNumGet; j++) {
    SMqSubVgEp* pVgEp = taosArrayGet(pTopicEp->vgs, j);
    if (pVgEp == NULL) {
      continue;
    }
    (void)snprintf(vgKey, sizeof(vgKey), "%s:%d", pTopic->topicName, pVgEp->vgId);
    SVgroupSaveInfo* pInfo = taosHashGet(pVgOffsetHashMap, vgKey, strlen(vgKey));

    STqOffsetVal offsetNew = {0};
    offsetNew.type = tmq->resetOffsetCfg;

    tqInfoC("consumer:0x%" PRIx64 ", update topic:%s, new numOfVgs:%d, num:%d, port:%d", tmq->consumerId,
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
      tqErrorC("consumer:0x%" PRIx64 ", failed to push vg:%d into topic:%s", tmq->consumerId, pVgEp->vgId,
               pTopic->topicName);
      freeClientVg(&clientVg);
    }
  }
}

static void buildNewTopicList(tmq_t* tmq, SArray* newTopics, const SMqAskEpRsp* pRsp){
  SHashObj* pVgOffsetHashMap = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pVgOffsetHashMap == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " taos hash init null, code:%d", tmq->consumerId, terrno);
    return;
  }

  int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
  for (int32_t i = 0; i < topicNumCur; i++) {
    // find old topic
    SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, i);
    if (pTopicCur && pTopicCur->vgs) {
      int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
      tqInfoC("consumer:0x%" PRIx64 ", current vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        if (pVgCur == NULL) {
          continue;
        }
        char vgKey[TSDB_TOPIC_FNAME_LEN + 22] = {0};
        (void)snprintf(vgKey, sizeof(vgKey), "%s:%d", pTopicCur->topicName, pVgCur->vgId);

        char buf[TSDB_OFFSET_LEN] = {0};
        tFormatOffset(buf, TSDB_OFFSET_LEN, &pVgCur->offsetInfo.endOffset);
        tqInfoC("consumer:0x%" PRIx64 ", vgId:%d vgKey:%s, offset:%s", tmq->consumerId, pVgCur->vgId, vgKey, buf);

        SVgroupSaveInfo info = {.currentOffset = pVgCur->offsetInfo.endOffset,
            .seekOffset = pVgCur->offsetInfo.beginOffset,
            .commitOffset = pVgCur->offsetInfo.committedOffset,
            .numOfRows = pVgCur->numOfRows,
            .vgStatus = pVgCur->vgStatus};
        if (taosHashPut(pVgOffsetHashMap, vgKey, strlen(vgKey), &info, sizeof(SVgroupSaveInfo)) != 0) {
          tqErrorC("consumer:0x%" PRIx64 ", failed to put vg:%d into hashmap", tmq->consumerId, pVgCur->vgId);
        }
      }
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pRsp->topics); i++) {
    SMqClientTopic topic = {0};
    SMqSubTopicEp* pTopicEp = taosArrayGet(pRsp->topics, i);
    if (pTopicEp == NULL) {
      continue;
    }
    initClientTopicFromRsp(&topic, pTopicEp, pVgOffsetHashMap, tmq);
    if (taosArrayPush(newTopics, &topic) == NULL) {
      tqErrorC("consumer:0x%" PRIx64 ", failed to push topic:%s into new topics", tmq->consumerId, topic.topicName);
      freeClientTopic(&topic);
    }
  }

  taosHashCleanup(pVgOffsetHashMap);
}

static void doUpdateLocalEp(tmq_t* tmq, int32_t epoch, const SMqAskEpRsp* pRsp) {
  int32_t topicNumGet = taosArrayGetSize(pRsp->topics);
  // vnode transform (epoch == tmq->epoch && topicNumGet != 0)
  // ask ep rsp (epoch == tmq->epoch && topicNumGet == 0)
  if (epoch < tmq->epoch || (epoch == tmq->epoch && topicNumGet == 0)) {
    tqDebugC("consumer:0x%" PRIx64 " no update ep epoch from %d to epoch %d, incoming topics:%d", tmq->consumerId,
             tmq->epoch, epoch, topicNumGet);
    return;
  }

  SArray* newTopics = taosArrayInit(topicNumGet, sizeof(SMqClientTopic));
  if (newTopics == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " taos array init null, code:%d", tmq->consumerId, terrno);
    return;
  }
  tqInfoC("consumer:0x%" PRIx64 " update ep epoch from %d to epoch %d, incoming topics:%d, existed topics:%d",
          tmq->consumerId, tmq->epoch, epoch, topicNumGet, (int)taosArrayGetSize(tmq->clientTopics));

  taosWLockLatch(&tmq->lock);
  if (topicNumGet > 0){
    buildNewTopicList(tmq, newTopics, pRsp);
  }
  // destroy current buffered existed topics info
  if (tmq->clientTopics) {
    taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  }
  tmq->clientTopics = newTopics;
  taosWUnLockLatch(&tmq->lock);

  atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__READY);
  atomic_store_32(&tmq->epoch, epoch);

  tqInfoC("consumer:0x%" PRIx64 " update topic info completed", tmq->consumerId);
}

static int32_t askEpCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqAskEpCbParam* pParam = (SMqAskEpCbParam*)param;
  if (pParam == NULL) {
    goto FAIL;
  }

  tmq_t* tmq = taosAcquireRef(tmqMgmt.rsetId, pParam->refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto FAIL;
  }

  if (code != TSDB_CODE_SUCCESS) {
    tqErrorC("consumer:0x%" PRIx64 ", get topic endpoint error, code:%s", tmq->consumerId, tstrerror(code));
    goto END;
  }

  if (pMsg == NULL) {
    goto END;
  }
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tqDebugC("consumer:0x%" PRIx64 ", recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (pParam->sync) {
    SMqAskEpRsp rsp = {0};
    if (tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp) != NULL) {
      doUpdateLocalEp(tmq, head->epoch, &rsp);
    }
    tDeleteSMqAskEpRsp(&rsp);
  } else {
    SMqRspWrapper* pWrapper = NULL;
    code = taosAllocateQitem(sizeof(SMqRspWrapper), DEF_QITEM, 0, (void**)&pWrapper);
    if (code) {
      goto END;
    }

    pWrapper->tmqRspType = TMQ_MSG_TYPE__EP_RSP;
    pWrapper->epoch = head->epoch;
    (void)memcpy(&pWrapper->epRsp, pMsg->pData, sizeof(SMqRspHead));
    if (tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pWrapper->epRsp) == NULL) {
      tmqFreeRspWrapper((SMqRspWrapper*)pWrapper);
      taosFreeQitem(pWrapper);
    } else {
      code = taosWriteQitem(tmq->mqueue, pWrapper);
      if (code != 0) {
        tmqFreeRspWrapper((SMqRspWrapper*)pWrapper);
        taosFreeQitem(pWrapper);
        tqErrorC("consumer:0x%" PRIx64 " put ep res into mqueue failed, code:%d", tmq->consumerId, code);
      }
    }
  }

  END:
  {
    int32_t ret = taosReleaseRef(tmqMgmt.rsetId, pParam->refId);
    if (ret != 0){
      tqErrorC("failed to release ref:%"PRId64 ", code:%d", pParam->refId, ret);
    }
  }

  FAIL:
  if (pParam && pParam->sync) {
    SAskEpInfo* pInfo = pParam->pParam;
    if (pInfo) {
      pInfo->code = code;
      if (tsem2_post(&pInfo->sem) != 0){
        tqErrorC("failed to post rsp sem askep cb");
      }
    }
  }

  if (pMsg) {
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pMsg->pData);
  }

  return code;
}

static int32_t askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpSet) {
  SMqAskEpReq req = {0};
  req.consumerId = pTmq->consumerId;
  req.epoch = updateEpSet ? -1 : pTmq->epoch;
  tstrncpy(req.cgroup, pTmq->groupId, TSDB_CGROUP_LEN);
  int              code = 0;
  SMqAskEpCbParam* pParam = NULL;
  void*            pReq = NULL;

  int32_t tlen = tSerializeSMqAskEpReq(NULL, 0, &req);
  if (tlen < 0) {
    tqErrorC("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq failed", pTmq->consumerId);
    return TSDB_CODE_INVALID_PARA;
  }

  pReq = taosMemoryCalloc(1, tlen);
  if (pReq == NULL) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to malloc askEpReq msg, size:%d", pTmq->consumerId, tlen);
    return terrno;
  }

  if (tSerializeSMqAskEpReq(pReq, tlen, &req) < 0) {
    tqErrorC("consumer:0x%" PRIx64 ", tSerializeSMqAskEpReq %d failed", pTmq->consumerId, tlen);
    taosMemoryFree(pReq);
    return TSDB_CODE_INVALID_PARA;
  }

  pParam = taosMemoryCalloc(1, sizeof(SMqAskEpCbParam));
  if (pParam == NULL) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to malloc subscribe param", pTmq->consumerId);
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
  tqDebugC("consumer:0x%" PRIx64 " ask ep from mnode,QID:0x%" PRIx64, pTmq->consumerId, sendInfo->requestId);
  return asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &epSet, NULL, sendInfo);
}

void tmqHandleAllDelayedTask(tmq_t* pTmq) {
  STaosQall* qall = NULL;
  int32_t    code = 0;

  code = taosAllocateQall(&qall);
  if (code) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to allocate qall, code:%s", pTmq->consumerId, tstrerror(code));
    return;
  }

  int32_t numOfItems = taosReadAllQitems(pTmq->delayedTask, qall);
  if (numOfItems == 0) {
    taosFreeQall(qall);
    return;
  }

  tqDebugC("consumer:0x%" PRIx64 " handle delayed %d tasks before poll data", pTmq->consumerId, numOfItems);
  int8_t* pTaskType = NULL;
  while (taosGetQitem(qall, (void**)&pTaskType) != 0) {
    if (*pTaskType == TMQ_DELAYED_TASK__ASK_EP) {
      tqDebugC("consumer:0x%" PRIx64 " retrieve ask ep timer", pTmq->consumerId);
      code = askEp(pTmq, NULL, false, false);
      if (code != 0) {
        tqErrorC("consumer:0x%" PRIx64 " failed to ask ep, code:%s", pTmq->consumerId, tstrerror(code));
        continue;
      }
      tqDebugC("consumer:0x%" PRIx64 " retrieve ep from mnode in 1s", pTmq->consumerId);
      bool ret = taosTmrReset(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(pTmq->refId), tmqMgmt.timer,
                              &pTmq->epTimer);
      tqDebugC("reset timer fo tmq ask ep:%d", ret);
    } else if (*pTaskType == TMQ_DELAYED_TASK__COMMIT) {
      tmq_commit_cb* pCallbackFn = (pTmq->commitCb != NULL) ? pTmq->commitCb : defaultCommitCbFn;
      asyncCommitAllOffsets(pTmq, pCallbackFn, pTmq->commitCbUserParam);
      tqDebugC("consumer:0x%" PRIx64 " next commit to vnode(s) in %.2fs", pTmq->consumerId,
               pTmq->autoCommitInterval / 1000.0);
      bool ret = taosTmrReset(tmqAssignDelayedCommitTask, pTmq->autoCommitInterval, (void*)(pTmq->refId), tmqMgmt.timer,
                              &pTmq->commitTimer);
      tqDebugC("reset timer fo commit:%d", ret);
    } else {
      tqErrorC("consumer:0x%" PRIx64 " invalid task type:%d", pTmq->consumerId, *pTaskType);
    }

    taosFreeQitem(pTaskType);
  }

  taosFreeQall(qall);
}

void tmqClearUnhandleMsg(tmq_t* tmq) {
  SMqRspWrapper* rspWrapper = NULL;
  while (taosGetQitem(tmq->qall, (void**)&rspWrapper) != 0) {
    tmqFreeRspWrapper(rspWrapper);
    taosFreeQitem(rspWrapper);
  }

  rspWrapper = NULL;
  if (taosReadAllQitems(tmq->mqueue, tmq->qall) == 0){
    return;
  }
  while (taosGetQitem(tmq->qall, (void**)&rspWrapper) != 0) {
    tmqFreeRspWrapper(rspWrapper);
    taosFreeQitem(rspWrapper);
  }
}

int32_t tmqSubscribeCb(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg) {
    taosMemoryFreeClear(pMsg->pEpSet);
  }

  if (param == NULL) {
    return code;
  }

  SMqSubscribeCbParam* pParam = (SMqSubscribeCbParam*)param;
  pParam->rspErr = code;

  if (tsem2_post(&pParam->rspSem) != 0){
    tqErrorC("failed to post sem, subscribe cb");
  }
  return 0;
}

int32_t tmq_subscription(tmq_t* tmq, tmq_list_t** topics) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  if (*topics == NULL) {
    *topics = tmq_list_new();
    if (*topics == NULL) {
      return terrno;
    }
  }
  taosRLockLatch(&tmq->lock);
  for (int i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
    SMqClientTopic* topic = taosArrayGet(tmq->clientTopics, i);
    if (topic == NULL) {
      tqErrorC("topic is null");
      continue;
    }
    char* tmp = strchr(topic->topicName, '.');
    if (tmp == NULL) {
      tqErrorC("topic name is invalid:%s", topic->topicName);
      continue;
    }
    if (tmq_list_append(*topics, tmp + 1) != 0) {
      tqErrorC("failed to append topic:%s", tmp + 1);
      continue;
    }
  }
  taosRUnLockLatch(&tmq->lock);
  return 0;
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
  if(tsem2_destroy(&tmq->rspSem) != 0) {
    tqErrorC("failed to destroy sem in free tmq");
  }

  taosArrayDestroyEx(tmq->clientTopics, freeClientTopic);
  taos_close_internal(tmq->pTscObj);

  if (tmq->commitTimer) {
    if (!taosTmrStopA(&tmq->commitTimer)) {
      tqErrorC("failed to stop commit timer");
    }
  }
  if (tmq->epTimer) {
    if (!taosTmrStopA(&tmq->epTimer)) {
      tqErrorC("failed to stop ep timer");
    }
  }
  if (tmq->hbLiveTimer) {
    if (!taosTmrStopA(&tmq->hbLiveTimer)) {
      tqErrorC("failed to stop hb timer");
    }
  }
  taosMemoryFree(tmq);

  tqInfoC("consumer:0x%" PRIx64 " closed", id);
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
    tqErrorC("failed to create consumer, groupId:%s", conf->groupId);
    SET_ERROR_MSG_TMQ("malloc tmq failed")
    return NULL;
  }

  const char* user = conf->user == NULL ? TSDB_DEFAULT_USER : conf->user;
  const char* pass = conf->pass == NULL ? TSDB_DEFAULT_PASS : conf->pass;

  pTmq->clientTopics = taosArrayInit(0, sizeof(SMqClientTopic));
  if (pTmq->clientTopics == NULL) {
    tqErrorC("failed to create consumer, groupId:%s", conf->groupId);
    SET_ERROR_MSG_TMQ("malloc client topics failed")
    goto _failed;
  }
  code = taosOpenQueue(&pTmq->mqueue);
  if (code) {
    tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("open queue failed")
    goto _failed;
  }

  code = taosOpenQueue(&pTmq->delayedTask);
  if (code) {
    tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("open delayed task queue failed")
    goto _failed;
  }

  code = taosAllocateQall(&pTmq->qall);
  if (code) {
    tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("allocate qall failed")
    goto _failed;
  }

  if (conf->groupId[0] == 0) {
    tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, tstrerror(code),
             pTmq->groupId);
    SET_ERROR_MSG_TMQ("malloc tmq element failed or group is empty")
    goto _failed;
  }

  // init status
  pTmq->status = TMQ_CONSUMER_STATUS__INIT;
  pTmq->pollCnt = 0;
  pTmq->epoch = 0;
  pTmq->pollFlag = 0;

  // set conf
  tstrncpy(pTmq->clientId, conf->clientId, TSDB_CLIENT_ID_LEN);
  tstrncpy(pTmq->groupId, conf->groupId, TSDB_CGROUP_LEN);
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
    tstrncpy(pTmq->fqdn, "localhost", TSDB_FQDN_LEN);
  }
  if (conf->replayEnable) {
    pTmq->autoCommit = false;
  }
  taosInitRWLatch(&pTmq->lock);

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem2_init(&pTmq->rspSem, 0, 0) != 0) {
    tqErrorC("consumer:0x %" PRIx64 " setup failed since %s, consumer group %s", pTmq->consumerId,
             tstrerror(TAOS_SYSTEM_ERROR(errno)), pTmq->groupId);
    SET_ERROR_MSG_TMQ("init t_sem failed")
    goto _failed;
  }

  // init connection
  code = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ, &pTmq->pTscObj);
  if (code) {
    terrno = code;
    tqErrorC("consumer:0x%" PRIx64 " setup failed since %s, groupId:%s", pTmq->consumerId, terrstr(), pTmq->groupId);
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
  tqInfoC("consumer:0x%" PRIx64 " is setup, refId:%" PRId64
          ", groupId:%s, snapshot:%d, autoCommit:%d, commitInterval:%dms, offset:%s",
          pTmq->consumerId, pTmq->refId, pTmq->groupId, pTmq->useSnapshot, pTmq->autoCommit, pTmq->autoCommitInterval,
          buf);

  return pTmq;

_failed:
  tmqFreeImpl(pTmq);
  return NULL;
}

static int32_t syncAskEp(tmq_t* pTmq) {
  SAskEpInfo* pInfo = taosMemoryMalloc(sizeof(SAskEpInfo));
  if (pInfo == NULL) return terrno;
  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    taosMemoryFree(pInfo);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int32_t code = askEp(pTmq, pInfo, true, false);
  if (code == 0) {
    if (tsem2_wait(&pInfo->sem) != 0){
      tqErrorC("consumer:0x%" PRIx64 ", failed to wait for sem", pTmq->consumerId);
    }
    code = pInfo->code;
  }

  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem sync ask ep");
  }
  taosMemoryFree(pInfo);
  return code;
}

int32_t tmq_subscribe(tmq_t* tmq, const tmq_list_t* topic_list) {
  if (tmq == NULL || topic_list == NULL) return TSDB_CODE_INVALID_PARA;
  const SArray*   container = &topic_list->container;
  int32_t         sz = taosArrayGetSize(container);
  void*           buf = NULL;
  SMsgSendInfo*   sendInfo = NULL;
  SCMSubscribeReq req = {0};
  int32_t         code = 0;

  tqInfoC("consumer:0x%" PRIx64 " cgroup:%s, subscribe %d topics", tmq->consumerId, tmq->groupId, sz);

  req.consumerId = tmq->consumerId;
  tstrncpy(req.clientId, tmq->clientId, TSDB_CLIENT_ID_LEN);
  tstrncpy(req.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
  tstrncpy(req.user, tmq->user, TSDB_USER_LEN);
  tstrncpy(req.fqdn, tmq->fqdn, TSDB_FQDN_LEN);

  req.topicNames = taosArrayInit(sz, sizeof(void*));
  if (req.topicNames == NULL) {
    code = terrno;
    goto END;
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
      code = terrno;
      goto END;
    }
    SName name = {0};
    code = tNameSetDbName(&name, tmq->pTscObj->acctId, topic, strlen(topic));
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " cgroup:%s, failed to set topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      goto END;
    }
    char* topicFName = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    if (topicFName == NULL) {
      code = terrno;
      goto END;
    }

    code = tNameExtractFullName(&name, topicFName);
    if (code) {
      tqErrorC("consumer:0x%" PRIx64 " cgroup:%s, failed to extract topic name, code:%d", tmq->consumerId, tmq->groupId,
               code);
      taosMemoryFree(topicFName);
      goto END;
    }

    if (taosArrayPush(req.topicNames, &topicFName) == NULL) {
      code = terrno;
      taosMemoryFree(topicFName);
      goto END;
    }
    tqInfoC("consumer:0x%" PRIx64 " subscribe topic:%s", tmq->consumerId, topicFName);
  }

  int32_t tlen = tSerializeSCMSubscribeReq(NULL, &req);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    code = terrno;
    goto END;
  }

  void* abuf = buf;
  tlen = tSerializeSCMSubscribeReq(&abuf, &req);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) {
    code = terrno;
    taosMemoryFree(buf);
    goto END;
  }

  SMqSubscribeCbParam param = {.rspErr = 0};
  if (tsem2_init(&param.rspSem, 0, 0) != 0) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    taosMemoryFree(buf);
    taosMemoryFree(sendInfo);
    goto END;
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
    goto END;
  }

  if (tsem2_wait(&param.rspSem) != 0){
    tqErrorC("consumer:0x%" PRIx64 ", failed to wait semaphore in subscribe", tmq->consumerId);
  }
  if(tsem2_destroy(&param.rspSem) != 0) {
    tqErrorC("consumer:0x%" PRIx64 ", failed to destroy semaphore in subscribe", tmq->consumerId);
  }

  if (param.rspErr != 0) {
    code = param.rspErr;
    goto END;
  }

  int32_t retryCnt = 0;
  while ((code = syncAskEp(tmq)) != 0) {
    if (retryCnt++ > SUBSCRIBE_RETRY_MAX_COUNT || code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
      tqErrorC("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry more than 2 minutes, code:%s",
               tmq->consumerId, tstrerror(code));
      if (code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
        code = 0;
      }
      goto END;
    }

    tqInfoC("consumer:0x%" PRIx64 ", mnd not ready for subscribe, retry:%d in 500ms", tmq->consumerId, retryCnt);
    taosMsleep(SUBSCRIBE_RETRY_INTERVAL);
  }

  if (tmq->epTimer == NULL){
    tmq->epTimer = taosTmrStart(tmqAssignAskEpTask, DEFAULT_ASKEP_INTERVAL, (void*)(tmq->refId), tmqMgmt.timer);
  }
  if (tmq->commitTimer == NULL){
    tmq->commitTimer = taosTmrStart(tmqAssignDelayedCommitTask, tmq->autoCommitInterval, (void*)(tmq->refId), tmqMgmt.timer);
  }
  if (tmq->epTimer == NULL || tmq->commitTimer == NULL) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }

END:
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

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  tmq_t*             tmq = NULL;
  SMqRspWrapper*     pRspWrapper = NULL;
  int8_t             rspType = 0;
  int32_t            vgId = 0;
  uint64_t           requestId = 0;
  SMqPollCbParam*    pParam = (SMqPollCbParam*)param;
  if (pMsg == NULL) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (pParam == NULL) {
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto EXIT;
  }
  int64_t refId = pParam->refId;
  vgId = pParam->vgId;
  requestId = pParam->requestId;
  tmq = taosAcquireRef(tmqMgmt.rsetId, refId);
  if (tmq == NULL) {
    code = TSDB_CODE_TMQ_CONSUMER_CLOSED;
    goto EXIT;
  }

  int32_t ret = taosAllocateQitem(sizeof(SMqRspWrapper), DEF_QITEM, 0, (void**)&pRspWrapper);
  if (ret) {
    code = ret;
    tscWarn("consumer:0x%" PRIx64 " msg discard from vgId:%d, since out of memory", tmq->consumerId, vgId);
    goto END;
  }

  if (code != 0) {
    goto END;
  }

  if (pMsg->pData == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " msg discard from vgId:%d, since msg is NULL", tmq->consumerId, vgId);
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }

  int32_t msgEpoch = ((SMqRspHead*)pMsg->pData)->epoch;
  int32_t clientEpoch = atomic_load_32(&tmq->epoch);

  if (msgEpoch != clientEpoch) {
    tqErrorC("consumer:0x%" PRIx64
             " msg discard from vgId:%d since epoch not equal, rsp epoch %d, current epoch %d, reqId:0x%" PRIx64,
             tmq->consumerId, vgId, msgEpoch, clientEpoch, requestId);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto END;
  }
  rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;
  tqDebugC("consumer:0x%" PRIx64 " recv poll rsp, vgId:%d, type %d,QID:0x%" PRIx64, tmq->consumerId, vgId, rspType, requestId);
  if (rspType == TMQ_MSG_TYPE__POLL_DATA_RSP) {
    PROCESS_POLL_RSP(tDecodeMqDataRsp, &pRspWrapper->pollRsp.dataRsp)
  } else if (rspType == TMQ_MSG_TYPE__POLL_META_RSP) {
    PROCESS_POLL_RSP(tDecodeMqMetaRsp, &pRspWrapper->pollRsp.metaRsp)
  } else if (rspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    PROCESS_POLL_RSP(tDecodeSTaosxRsp, &pRspWrapper->pollRsp.dataRsp)
  } else if (rspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    PROCESS_POLL_RSP(tSemiDecodeMqBatchMetaRsp, &pRspWrapper->pollRsp.batchMetaRsp)
  } else {  // invalid rspType
    tqErrorC("consumer:0x%" PRIx64 " invalid rsp msg received, type:%d ignored", tmq->consumerId, rspType);
    code = TSDB_CODE_TSC_INTERNAL_ERROR;
    goto END;
  }
  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->pollRsp.reqId = requestId;
  pRspWrapper->pollRsp.pEpset = pMsg->pEpSet;
  pMsg->pEpSet = NULL;

END:
  if (pRspWrapper) {
    pRspWrapper->code = code;
    pRspWrapper->pollRsp.vgId = vgId;
    tstrncpy(pRspWrapper->pollRsp.topicName, pParam->topicName, TSDB_TOPIC_FNAME_LEN);
    code = taosWriteQitem(tmq->mqueue, pRspWrapper);
    if (code != 0) {
      tmqFreeRspWrapper(pRspWrapper);
      taosFreeQitem(pRspWrapper);
      tqErrorC("consumer:0x%" PRIx64 " put poll res into mqueue failed, code:%d", tmq->consumerId, code);
    } else {
      tqDebugC("consumer:0x%" PRIx64 " put poll res into mqueue, type:%d, vgId:%d, total in queue:%d,QID:0x%" PRIx64,
               tmq ? tmq->consumerId : 0, rspType, vgId, taosQueueItemSize(tmq->mqueue), requestId);
    }
  }


  if (tsem2_post(&tmq->rspSem) != 0){
    tqErrorC("failed to post rsp sem, consumer:0x%" PRIx64, tmq->consumerId);
  }
  ret = taosReleaseRef(tmqMgmt.rsetId, refId);
  if (ret != 0){
    tqErrorC("failed to release ref:%"PRId64 ", code:%d", refId, ret);
  }

EXIT:
  taosMemoryFreeClear(pMsg->pData);
  taosMemoryFreeClear(pMsg->pEpSet);
  return code;
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

void changeByteEndian(char* pData) {
  if (pData == NULL) {
    return;
  }
  char* p = pData;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column
  // length | version:
  int32_t blockVersion = *(int32_t*)p;
  if (blockVersion != BLOCK_VERSION_1) {
    tqErrorC("invalid block version:%d", blockVersion);
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
                                        SMqRspObj* pRspObj) {
  pRspObj->resIter = -1;
  pRspObj->resInfo.totalRows = 0;
  pRspObj->resInfo.precision = TSDB_TIME_PRECISION_MILLI;

  SMqDataRsp* pDataRsp = &pRspObj->dataRsp;
  bool needTransformSchema = !pDataRsp->withSchema;
  if (!pDataRsp->withSchema) {  // withSchema is false if subscribe subquery, true if subscribe db or stable
    pDataRsp->withSchema = true;
    pDataRsp->blockSchema = taosArrayInit(pDataRsp->blockNum, sizeof(void*));
    if (pDataRsp->blockSchema == NULL) {
      tqErrorC("failed to allocate memory for blockSchema");
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
          tqErrorC("failed to push schema into blockSchema");
          continue;
        }
      }
    }
  }
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
    code = terrno;
    taosMemoryFreeClear(msg);
    return code;
  }

  pParam->refId = pTmq->refId;
  tstrncpy(pParam->topicName, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
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

  char offsetFormatBuf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(offsetFormatBuf, tListLen(offsetFormatBuf), &pVg->offsetInfo.endOffset);
  code = asyncSendMsgToServer(pTmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, NULL, sendInfo);
  tqDebugC("consumer:0x%" PRIx64 " send poll to %s vgId:%d, code:%d, epoch %d, req:%s,QID:0x%" PRIx64, pTmq->consumerId,
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
  int32_t code = 0;

  taosWLockLatch(&tmq->lock);
  int32_t numOfTopics = taosArrayGetSize(tmq->clientTopics);
  tqDebugC("consumer:0x%" PRIx64 " start to poll data, numOfTopics:%d", tmq->consumerId, numOfTopics);

  for (int i = 0; i < numOfTopics; i++) {
    SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
    if (pTopic == NULL) {
      continue;
    }
    int32_t numOfVg = taosArrayGetSize(pTopic->vgs);
    if (pTopic->noPrivilege) {
      tqDebugC("consumer:0x%" PRIx64 " has no privilegr for topic:%s", tmq->consumerId, pTopic->topicName);
      continue;
    }
    for (int j = 0; j < numOfVg; j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
      if (pVg == NULL) {
        continue;
      }
      int64_t elapsed = taosGetTimestampMs() - pVg->emptyBlockReceiveTs;
      if (elapsed < EMPTY_BLOCK_POLL_IDLE_DURATION && elapsed >= 0) {  // less than 10ms
        tqDebugC("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for 10ms before start next poll", tmq->consumerId,
                 tmq->epoch, pVg->vgId);
        continue;
      }

      elapsed = taosGetTimestampMs() - pVg->blockReceiveTs;
      if (tmq->replayEnable && elapsed < pVg->blockSleepForReplay && elapsed >= 0) {
        tqDebugC("consumer:0x%" PRIx64 " epoch %d, vgId:%d idle for %" PRId64 "ms before start next poll when replay",
                 tmq->consumerId, tmq->epoch, pVg->vgId, pVg->blockSleepForReplay);
        continue;
      }

      int32_t vgStatus = atomic_val_compare_exchange_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE, TMQ_VG_STATUS__WAIT);
      if (vgStatus == TMQ_VG_STATUS__WAIT) {
        int32_t vgSkipCnt = atomic_add_fetch_32(&pVg->vgSkipCnt, 1);
        tqDebugC("consumer:0x%" PRIx64 " epoch %d wait poll-rsp, skip vgId:%d skip cnt %d", tmq->consumerId, tmq->epoch,
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
  tqDebugC("consumer:0x%" PRIx64 " end to poll data, code:%d", tmq->consumerId, code);
  return code;
}

static void updateVgInfo(SMqClientVg* pVg, STqOffsetVal* reqOffset, STqOffsetVal* rspOffset, int64_t sver, int64_t ever,
                         int64_t consumerId, bool hasData) {
  if (!pVg->seekUpdated) {
    tqDebugC("consumer:0x%" PRIx64 " local offset is update, since seekupdate not set", consumerId);
    if (hasData) {
      tOffsetCopy(&pVg->offsetInfo.beginOffset, reqOffset);
    }
    tOffsetCopy(&pVg->offsetInfo.endOffset, rspOffset);
  } else {
    tqDebugC("consumer:0x%" PRIx64 " local offset is NOT update, since seekupdate is set", consumerId);
  }

  // update the status
  atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);

  // update the valid wal version range
  pVg->offsetInfo.walVerBegin = sver;
  pVg->offsetInfo.walVerEnd = ever + 1;
}

static SMqRspObj* buildRsp(SMqPollRspWrapper* pollRspWrapper){
  typedef union {
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    SMqBatchMetaRsp batchMetaRsp;
  } MEMSIZE;

  SMqRspObj* pRspObj = taosMemoryCalloc(1, sizeof(SMqRspObj));
  if (pRspObj == NULL) {
    tqErrorC("buildRsp:failed to allocate memory");
    return NULL;
  }
  (void)memcpy(&pRspObj->dataRsp, &pollRspWrapper->dataRsp, sizeof(MEMSIZE));
  tstrncpy(pRspObj->topic, pollRspWrapper->topicName, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(pRspObj->db, pollRspWrapper->topicHandle->db, TSDB_DB_FNAME_LEN);
  pRspObj->vgId = pollRspWrapper->vgId;
  (void)memset(&pollRspWrapper->dataRsp, 0, sizeof(MEMSIZE));
  return pRspObj;
}

static void processMqRspError(tmq_t* tmq, SMqRspWrapper* pRspWrapper){
  SMqPollRspWrapper* pollRspWrapper = &pRspWrapper->pollRsp;

  if (pRspWrapper->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {  // for vnode transform
    int32_t code = askEp(tmq, NULL, false, true);
    if (code != 0) {
      tqErrorC("consumer:0x%" PRIx64 " failed to ask ep, code:%s", tmq->consumerId, tstrerror(code));
    }
  } else if (pRspWrapper->code == TSDB_CODE_TMQ_CONSUMER_MISMATCH) {
    int32_t code = askEp(tmq, NULL, false, false);
    if (code != 0) {
      tqErrorC("consumer:0x%" PRIx64 " failed to ask ep, code:%s", tmq->consumerId, tstrerror(code));
    }
  }
  tqInfoC("consumer:0x%" PRIx64 " msg from vgId:%d discarded, since %s", tmq->consumerId, pollRspWrapper->vgId,
          tstrerror(pRspWrapper->code));
  taosWLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
  if (pVg) {
    pVg->emptyBlockReceiveTs = taosGetTimestampMs();
    atomic_store_32(&pVg->vgStatus, TMQ_VG_STATUS__IDLE);
  }
  taosWUnLockLatch(&tmq->lock);
}
static SMqRspObj* processMqRsp(tmq_t* tmq, SMqRspWrapper* pRspWrapper){
  SMqRspObj* pRspObj = NULL;

  if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__EP_RSP) {
    tqDebugC("consumer:0x%" PRIx64 " ep msg received", tmq->consumerId);
    SMqAskEpRsp*        rspMsg = &pRspWrapper->epRsp;
    doUpdateLocalEp(tmq, pRspWrapper->epoch, rspMsg);
    return pRspObj;
  }

  SMqPollRspWrapper* pollRspWrapper = &pRspWrapper->pollRsp;
  taosWLockLatch(&tmq->lock);
  SMqClientVg* pVg = NULL;
  getVgInfo(tmq, pollRspWrapper->topicName, pollRspWrapper->vgId, &pVg);
  if(pVg == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " get vg or topic error, topic:%s vgId:%d", tmq->consumerId,
             pollRspWrapper->topicName, pollRspWrapper->vgId);
    goto END;
  }
  pollRspWrapper->topicHandle = getTopicInfo(tmq, pollRspWrapper->topicName);
  if (pollRspWrapper->pEpset != NULL) {
    pVg->epSet = *pollRspWrapper->pEpset;
  }

  if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP || pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    updateVgInfo(pVg, &pollRspWrapper->dataRsp.reqOffset, &pollRspWrapper->dataRsp.rspOffset, pollRspWrapper->head.walsver, pollRspWrapper->head.walever,
                 tmq->consumerId, pollRspWrapper->dataRsp.blockNum != 0);

    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &pollRspWrapper->rspOffset);
    if (pollRspWrapper->dataRsp.blockNum == 0) {
      tqDebugC("consumer:0x%" PRIx64 " empty block received, vgId:%d, offset:%s, vg total:%" PRId64
                   ", total:%" PRId64 ",QID:0x%" PRIx64,
               tmq->consumerId, pVg->vgId, buf, pVg->numOfRows, tmq->totalRows, pollRspWrapper->reqId);
      pVg->emptyBlockReceiveTs = taosGetTimestampMs();
    } else {
      pRspObj = buildRsp(pollRspWrapper);
      if (pRspObj == NULL) {
        tqErrorC("consumer:0x%" PRIx64 " failed to allocate memory for meta rsp", tmq->consumerId);
        goto END;
      }
      pRspObj->resType = pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_DATA_RSP ? RES_TYPE__TMQ : RES_TYPE__TMQ_METADATA;
      int64_t numOfRows = 0;
      tmqBuildRspFromWrapperInner(pollRspWrapper, pVg, &numOfRows, pRspObj);
      tmq->totalRows += numOfRows;
      pVg->emptyBlockReceiveTs = 0;
      if (tmq->replayEnable) {
        pVg->blockReceiveTs = taosGetTimestampMs();
        pVg->blockSleepForReplay = pRspObj->dataRsp.sleepTime;
        if (pVg->blockSleepForReplay > 0) {
          if (taosTmrStart(tmqReplayTask, pVg->blockSleepForReplay, (void*)(tmq->refId), tmqMgmt.timer) == NULL) {
            tqErrorC("consumer:0x%" PRIx64 " failed to start replay timer, vgId:%d, sleep:%" PRId64,
                     tmq->consumerId, pVg->vgId, pVg->blockSleepForReplay);
          }
        }
      }
      tqDebugC("consumer:0x%" PRIx64 " process poll rsp, vgId:%d, offset:%s, blocks:%d, rows:%" PRId64
                   ", vg total:%" PRId64 ", total:%" PRId64 ",QID:0x%" PRIx64,
               tmq->consumerId, pVg->vgId, buf, pRspObj->dataRsp.blockNum, numOfRows, pVg->numOfRows, tmq->totalRows,
               pollRspWrapper->reqId);
    }
  } else if (pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP || pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_BATCH_META_RSP) {
    updateVgInfo(pVg, &pollRspWrapper->rspOffset, &pollRspWrapper->rspOffset,
                 pollRspWrapper->head.walsver, pollRspWrapper->head.walever, tmq->consumerId, true);


    pRspObj = buildRsp(pollRspWrapper);
    if (pRspObj == NULL) {
      tqErrorC("consumer:0x%" PRIx64 " failed to allocate memory for meta rsp", tmq->consumerId);
      goto END;
    }
    pRspObj->resType = pRspWrapper->tmqRspType == TMQ_MSG_TYPE__POLL_META_RSP ? RES_TYPE__TMQ_META : RES_TYPE__TMQ_BATCH_META;
  }

  END:
  taosWUnLockLatch(&tmq->lock);
  return pRspObj;
}

static void* tmqHandleAllRsp(tmq_t* tmq, int64_t timeout) {
  tqDebugC("consumer:0x%" PRIx64 " start to handle the rsp, total:%d", tmq->consumerId, taosQallItemSize(tmq->qall));

  void* returnVal = NULL;
  while (1) {
    SMqRspWrapper* pRspWrapper = NULL;
    if (taosGetQitem(tmq->qall, (void**)&pRspWrapper) == 0) {
      if (taosReadAllQitems(tmq->mqueue, tmq->qall) == 0){
        return NULL;
      }
      if (taosGetQitem(tmq->qall, (void**)&pRspWrapper) == 0) {
        return NULL;
      }
    }

    tqDebugC("consumer:0x%" PRIx64 " handle rsp, type:%d", tmq->consumerId, pRspWrapper->tmqRspType);
    if (pRspWrapper->code != 0) {
      processMqRspError(tmq, pRspWrapper);
    }else{
      returnVal = processMqRsp(tmq, pRspWrapper);
    }
    tmqFreeRspWrapper(pRspWrapper);
    taosFreeQitem(pRspWrapper);
    if(returnVal != NULL){
      break;
    }
  }

  return returnVal;
}

TAOS_RES* tmq_consumer_poll(tmq_t* tmq, int64_t timeout) {
  if (tmq == NULL) return NULL;

  void*   rspObj = NULL;
  int64_t startTime = taosGetTimestampMs();

  tqDebugC("consumer:0x%" PRIx64 " start to poll at %" PRId64 ", timeout:%" PRId64, tmq->consumerId, startTime,
           timeout);

  // in no topic status, delayed task also need to be processed
  if (atomic_load_8(&tmq->status) == TMQ_CONSUMER_STATUS__INIT) {
    tqInfoC("consumer:0x%" PRIx64 " poll return since consumer is init", tmq->consumerId);
    taosMsleep(500);  //     sleep for a while
    return NULL;
  }

  (void)atomic_val_compare_exchange_8(&tmq->pollFlag, 0, 1);

  while (1) {
    tmqHandleAllDelayedTask(tmq);

    if (tmqPollImpl(tmq, timeout) < 0) {
      tqErrorC("consumer:0x%" PRIx64 " return due to poll error", tmq->consumerId);
    }

    rspObj = tmqHandleAllRsp(tmq, timeout);
    if (rspObj) {
      tqDebugC("consumer:0x%" PRIx64 " return rsp %p", tmq->consumerId, rspObj);
      return (TAOS_RES*)rspObj;
    }

    if (timeout >= 0) {
      int64_t currentTime = taosGetTimestampMs();
      int64_t elapsedTime = currentTime - startTime;
      if (elapsedTime > timeout || elapsedTime < 0) {
        tqDebugC("consumer:0x%" PRIx64 " (epoch %d) timeout, no rsp, start time %" PRId64 ", current time %" PRId64,
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
  tqInfoC("consumer:0x%" PRIx64 " closing poll:%" PRId64 " rows:%" PRId64 " topics:%d, final epoch:%d",
           pTmq->consumerId, pTmq->pollCnt, pTmq->totalRows, numOfTopics, pTmq->epoch);

  tqInfoC("consumer:0x%" PRIx64 " rows dist begin: ", pTmq->consumerId);
  for (int32_t i = 0; i < numOfTopics; ++i) {
    SMqClientTopic* pTopics = taosArrayGet(pTmq->clientTopics, i);
    if (pTopics == NULL) continue;
    tqInfoC("consumer:0x%" PRIx64 " topic:%d", pTmq->consumerId, i);
    int32_t numOfVgs = taosArrayGetSize(pTopics->vgs);
    for (int32_t j = 0; j < numOfVgs; ++j) {
      SMqClientVg* pVg = taosArrayGet(pTopics->vgs, j);
      if (pVg == NULL) continue;
      tqInfoC("topic:%s, %d. vgId:%d rows:%" PRId64, pTopics->topicName, j, pVg->vgId, pVg->numOfRows);
    }
  }
  taosRUnLockLatch(&pTmq->lock);
  tqInfoC("consumer:0x%" PRIx64 " rows dist end", pTmq->consumerId);
}

int32_t tmq_unsubscribe(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t code = 0;
  int8_t status = atomic_load_8(&tmq->status);
  tqInfoC("consumer:0x%" PRIx64 " start to unsubscribe consumer, status:%d", tmq->consumerId, status);

  displayConsumeStatistics(tmq);
  if (status != TMQ_CONSUMER_STATUS__READY) {
    tqInfoC("consumer:0x%" PRIx64 " status:%d, already closed or not in ready state, no need unsubscribe", tmq->consumerId, status);
    goto END;
  }
  if (tmq->autoCommit) {
    code = tmq_commit_sync(tmq, NULL);
    if (code != 0) {
       goto END;
    }
  }
  tmqSendHbReq((void*)(tmq->refId), NULL);

  tmq_list_t* lst = tmq_list_new();
  if (lst == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  code = tmq_subscribe(tmq, lst);
  tmq_list_destroy(lst);
  if(code != 0){
    goto END;
  }

END:
  return code;
}

int32_t tmq_consumer_close(tmq_t* tmq) {
  if (tmq == NULL) return TSDB_CODE_INVALID_PARA;
  tqInfoC("consumer:0x%" PRIx64 " start to close consumer, status:%d", tmq->consumerId, tmq->status);
  int32_t code = tmq_unsubscribe(tmq);
  if (code == 0) {
    atomic_store_8(&tmq->status, TMQ_CONSUMER_STATUS__CLOSED);
    code = taosRemoveRef(tmqMgmt.rsetId, tmq->refId);
    if (code != 0){
      tqErrorC("tmq close failed to remove ref:%" PRId64 ", code:%d", tmq->refId, code);
    }
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
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    char* tmp = strchr(((SMqRspObj*)res)->topic, '.');
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

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res) || TD_RES_TMQ_META(res)) {
    char* tmp = strchr(((SMqRspObj*)res)->db, '.');
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
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res) || TD_RES_TMQ_META(res)) {
    return ((SMqRspObj*)res)->vgId;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int64_t tmq_get_vgroup_offset(TAOS_RES* res) {
  if (res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    STqOffsetVal*     pOffset = &pRspObj->dataRsp.reqOffset;
    if (pOffset->type == TMQ_OFFSET__LOG) {
      return pOffset->version;
    } else {
      tqErrorC("invalid offset type:%d", pOffset->type);
    }
  } else if (TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    if (pRspObj->rspOffset.type == TMQ_OFFSET__LOG) {
      return pRspObj->rspOffset.version;
    }
  } else {
    tqErrorC("invalid tmq type:%d", *(int8_t*)res);
  }

  // data from tsdb, no valid offset info
  return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
}

const char* tmq_get_table_name(TAOS_RES* res) {
  if (res == NULL) {
    return NULL;
  }
  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqRspObj* pRspObj = (SMqRspObj*)res;
    SMqDataRsp* data = &pRspObj->dataRsp;
    if (!data->withTbName || data->blockTbName == NULL || pRspObj->resIter < 0 ||
        pRspObj->resIter >= data->blockNum) {
      return NULL;
    }
    return (const char*)taosArrayGetP(data->blockTbName, pRspObj->resIter);
  }
  return NULL;
}

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* cb, void* param) {
  if (tmq == NULL) {
    tqErrorC("invalid tmq handle, null");
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
  if (tsem2_post(&pInfo->sem) != 0){
    tqErrorC("failed to post rsp sem in commit cb");
  }
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* pRes) {
  if (tmq == NULL) {
    tqErrorC("invalid tmq handle, null");
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;

  SSyncCommitInfo* pInfo = taosMemoryMalloc(sizeof(SSyncCommitInfo));
  if (pInfo == NULL) {
    tqErrorC("failed to allocate memory for sync commit");
    return terrno;
  }
  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    tqErrorC("failed to init sem for sync commit");
    taosMemoryFree(pInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pInfo->code = 0;

  if (pRes == NULL) {
    asyncCommitAllOffsets(tmq, commitCallBackFn, pInfo);
  } else {
    asyncCommitFromResult(tmq, pRes, commitCallBackFn, pInfo);
  }

  if (tsem2_wait(&pInfo->sem) != 0){
    tqErrorC("failed to wait sem for sync commit");
  }
  code = pInfo->code;

  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem for sync commit");
  }
  taosMemoryFree(pInfo);

  tqInfoC("consumer:0x%" PRIx64 " sync res commit done, code:%s", tmq->consumerId, tstrerror(code));
  return code;
}

// wal range will be ok after calling tmq_get_topic_assignment or poll interface
static int32_t checkWalRange(SVgOffsetInfo* offset, int64_t value) {
  if (offset->walVerBegin == -1 || offset->walVerEnd == -1) {
    tqErrorC("Assignment or poll interface need to be called first");
    return TSDB_CODE_TMQ_NEED_INITIALIZED;
  }

  if (value != -1 && (value < offset->walVerBegin || value > offset->walVerEnd)) {
    tqErrorC("invalid seek params, offset:%" PRId64 ", valid range:[%" PRId64 ", %" PRId64 "]", value,
             offset->walVerBegin, offset->walVerEnd);
    return TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE;
  }

  return 0;
}

int32_t tmq_commit_offset_sync(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
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
    tqErrorC("consumer:0x%" PRIx64 " failed to prepare seek operation", tmq->consumerId);
    return terrno;
  }

  if (tsem2_init(&pInfo->sem, 0, 0) != 0) {
    taosMemoryFree(pInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pInfo->code = 0;

  code = asyncCommitOffset(tmq, tname, vgId, &offsetVal, commitCallBackFn, pInfo);
  if (code == 0) {
    if (tsem2_wait(&pInfo->sem) != 0){
      tqErrorC("failed to wait sem for sync commit offset");
    }
    code = pInfo->code;
  }

  if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
  if(tsem2_destroy(&pInfo->sem) != 0) {
    tqErrorC("failed to destroy sem for sync commit offset");
  }
  taosMemoryFree(pInfo);

  tqInfoC("consumer:0x%" PRIx64 " sync send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

  return code;
}

void tmq_commit_offset_async(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb* cb,
                             void* param) {
  int32_t code = 0;
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
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

  tqInfoC("consumer:0x%" PRIx64 " async send commit to vgId:%d, offset:%" PRId64 " code:%s", tmq->consumerId, vgId,
          offset, tstrerror(code));

end:
  if (code != 0 && cb != NULL) {
    if (code == TSDB_CODE_TMQ_SAME_COMMITTED_VALUE) code = TSDB_CODE_SUCCESS;
    cb(tmq, code, param);
  }
}

int32_t tmqGetNextResInfo(TAOS_RES* res, bool convertUcs4, SReqResultInfo** pResInfo) {
  SMqRspObj*  pRspObj = (SMqRspObj*)res;
  SMqDataRsp* data = &pRspObj->dataRsp;

  pRspObj->resIter++;
  if (pRspObj->resIter < data->blockNum) {
    if (data->withSchema) {
      doFreeReqResultInfo(&pRspObj->resInfo);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(data->blockSchema, pRspObj->resIter);
      if (pSW) {
        TAOS_CHECK_RETURN(setResSchemaInfo(&pRspObj->resInfo, pSW->pSchema, pSW->nCols));
      }
    }

    void*   pRetrieve = taosArrayGetP(data->blockData, pRspObj->resIter);
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
    tqErrorC("consumer:0x%" PRIx64 " failed to get the wal info from vgId:%d for topic:%s", pCommon->consumerId,
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
                                       .currentOffset = rsp.rspOffset.version,
                                       .vgId = pParam->vgId};

    (void)taosThreadMutexLock(&pCommon->mutex);
    if (taosArrayPush(pCommon->pList, &assignment) == NULL) {
      tqErrorC("consumer:0x%" PRIx64 " failed to push the wal info from vgId:%d for topic:%s", pCommon->consumerId,
               pParam->vgId, pCommon->pTopicName);
      code = TSDB_CODE_TSC_INTERNAL_ERROR;
    }
    (void)taosThreadMutexUnlock(&pCommon->mutex);
  }

END:
  pCommon->code = code;
  if (total == pParam->totalReq) {
    if (tsem2_post(&pCommon->rsp) != 0) {
      tqErrorC("failed to post semaphore in get wal cb");
    }
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
  taosArrayDestroy(pCommon->pList);
  if(tsem2_destroy(&pCommon->rsp) != 0) {
    tqErrorC("failed to destroy semaphore for topic:%s", pCommon->pTopicName);
  }
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
  if (tsem2_post(&pParam->sem) != 0){
    tqErrorC("failed to post semaphore in tmCommittedCb");
  }
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
    if(tsem2_destroy(&pParam->sem) != 0) {
      tqErrorC("failed to destroy semaphore in get committed from server1");
    }
    taosMemoryFree(pParam);
    return code;
  }

  if (tsem2_wait(&pParam->sem) != 0){
    tqErrorC("failed to wait semaphore in get committed from server");
  }
  code = pParam->code;
  if (code == TSDB_CODE_SUCCESS) {
    if (pParam->vgOffset.offset.val.type == TMQ_OFFSET__LOG) {
      code = pParam->vgOffset.offset.val.version;
    } else {
      tOffsetDestroy(&pParam->vgOffset.offset);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
    }
  }
  if(tsem2_destroy(&pParam->sem) != 0) {
    tqErrorC("failed to destroy semaphore in get committed from server2");
  }
  taosMemoryFree(pParam);

  return code;
}

int64_t tmq_position(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
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
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, position error", tmq->consumerId, type);
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
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d can not be reach here", tmq->consumerId, type);
  }

  tqInfoC("consumer:0x%" PRIx64 " tmq_position vgId:%d position:%" PRId64, tmq->consumerId, vgId, position);
  return position;
}

int64_t tmq_committed(tmq_t* tmq, const char* pTopicName, int32_t vgId) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
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
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
             pOffsetInfo->endOffset.type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  if (isInSnapshotMode(pOffsetInfo->committedOffset.type, tmq->useSnapshot)) {
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, committed error", tmq->consumerId,
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
  tqInfoC("consumer:0x%" PRIx64 " tmq_committed vgId:%d committed:%" PRId64, tmq->consumerId, vgId, committed);
  return committed;
}

int32_t tmq_get_topic_assignment(tmq_t* tmq, const char* pTopicName, tmq_topic_assignment** assignment,
                                 int32_t* numOfAssignment) {
  if (tmq == NULL || pTopicName == NULL || assignment == NULL || numOfAssignment == NULL) {
    tqErrorC("invalid tmq handle, null");
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
    tqErrorC("consumer:0x%" PRIx64 " invalid topic name:%s", tmq->consumerId, pTopicName);
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
      tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, assignment not allowed", tmq->consumerId, type);
      code = TSDB_CODE_TMQ_SNAPSHOT_ERROR;
      goto end;
    }
  }

  *assignment = taosMemoryCalloc(*numOfAssignment, sizeof(tmq_topic_assignment));
  if (*assignment == NULL) {
    tqErrorC("consumer:0x%" PRIx64 " failed to malloc buffer, size:%" PRIzu, tmq->consumerId,
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
    tqInfoC("consumer:0x%" PRIx64 " get assignment from local:%d->%" PRId64, tmq->consumerId, pAssignment->vgId,
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
      code = terrno;
      goto end;
    }
    if (tsem2_init(&pCommon->rsp, 0, 0) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    (void)taosThreadMutexInit(&pCommon->mutex, 0);
    pCommon->pTopicName = taosStrdup(pTopic->topicName);
    if (pCommon->pTopicName == NULL) {
      code = terrno;
      goto end;
    }
    pCommon->consumerId = tmq->consumerId;

    for (int32_t i = 0; i < (*numOfAssignment); ++i) {
      SMqClientVg* pClientVg = taosArrayGet(pTopic->vgs, i);
      if (pClientVg == NULL) {
        continue;
      }
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

      tqInfoC("consumer:0x%" PRIx64 " %s retrieve wal info vgId:%d, epoch %d, req:%s,QID:0x%" PRIx64, tmq->consumerId,
              pTopic->topicName, pClientVg->vgId, tmq->epoch, offsetFormatBuf, req.reqId);
      code = asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pClientVg->epSet, NULL, sendInfo);
      if (code != 0) {
        goto end;
      }
    }

    if (tsem2_wait(&pCommon->rsp) != 0){
      tqErrorC("consumer:0x%" PRIx64 " failed to wait sem in get assignment", tmq->consumerId);
    }
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
        tqInfoC("consumer:0x%" PRIx64 " %s vgId:%d offset is update to:%" PRId64, tmq->consumerId, pTopic->topicName,
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
  if (tsem2_post(&pParam->sem) != 0){
    tqErrorC("failed to post sem in tmqSeekCb");
  }
  return 0;
}

// seek interface have to send msg to server to cancel push handle if needed, because consumer may be in wait status if
// there is no data to poll
int32_t tmq_offset_seek(tmq_t* tmq, const char* pTopicName, int32_t vgId, int64_t offset) {
  if (tmq == NULL || pTopicName == NULL) {
    tqErrorC("invalid tmq handle, null");
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
    tqErrorC("consumer:0x%" PRIx64 " offset type:%d not wal version, seek not allowed", tmq->consumerId, type);
    taosWUnLockLatch(&tmq->lock);
    return TSDB_CODE_TMQ_SNAPSHOT_ERROR;
  }

  code = checkWalRange(pOffsetInfo, offset);
  if (code != 0) {
    taosWUnLockLatch(&tmq->lock);
    return code;
  }

  tqInfoC("consumer:0x%" PRIx64 " seek to %" PRId64 " on vgId:%d", tmq->consumerId, offset, vgId);
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
    return terrno;
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
    if(tsem2_destroy(&pParam->sem) != 0) {
      tqErrorC("consumer:0x%" PRIx64 "destroy rsp sem failed in seek offset", tmq->consumerId);
    }
    taosMemoryFree(pParam);
    return code;
  }

  if (tsem2_wait(&pParam->sem) != 0){
    tqErrorC("consumer:0x%" PRIx64 "wait rsp sem failed in seek offset", tmq->consumerId);
  }
  code = pParam->code;
  if(tsem2_destroy(&pParam->sem) != 0) {
    tqErrorC("consumer:0x%" PRIx64 "destroy rsp sem failed in seek offset", tmq->consumerId);
  }
  taosMemoryFree(pParam);

  tqInfoC("consumer:0x%" PRIx64 "send seek to vgId:%d, return code:%s", tmq->consumerId, vgId, tstrerror(code));

  return code;
}

TAOS* tmq_get_connect(tmq_t* tmq) {
  if (tmq && tmq->pTscObj) {
    return (TAOS*)(&(tmq->pTscObj->id));
  }
  return NULL;
}

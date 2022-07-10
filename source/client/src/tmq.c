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
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"
#include "cJSON.h"

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
  char     clientId[256];
  char     groupId[TSDB_CGROUP_LEN];
  int8_t   autoCommit;
  int8_t   resetOffset;
  int8_t   withTbName;
  int8_t   spEnable;
  int32_t  spBatchSize;
  uint16_t port;
  int32_t  autoCommitInterval;
  char*    ip;
  char*    user;
  char*    pass;
  /*char*          db;*/
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
};

struct tmq_t {
  // conf
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[256];
  int8_t         withTbName;
  int8_t         useSnapshot;
  int8_t         autoCommit;
  int32_t        autoCommitInterval;
  int32_t        resetOffsetCfg;
  int64_t        consumerId;
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
  tmr_h hbTimer;
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

struct tmq_raw_data{
  void    *raw_meta;
  int32_t raw_meta_len;
  int16_t raw_meta_type;
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
  TMQ_DELAYED_TASK__HB = 1,
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

#if 0
typedef struct {
  tmq_t*         tmq;
  int8_t         async;
  int8_t         automatic;
  int8_t         freeOffsets;
  tmq_commit_cb* userCb;
  tsem_t         rspSem;
  int32_t        rspErr;
  SArray*        offsets;
  void*          userParam;
} SMqCommitCbParam;
#endif

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
      conf->spEnable = true;
      return TMQ_CONF_OK;
    } else if (strcmp(value, "false") == 0) {
      conf->spEnable = false;
      return TMQ_CONF_OK;
    } else {
      return TMQ_CONF_INVALID;
    }
  }

  if (strcmp(key, "experimental.snapshot.batch.size") == 0) {
    conf->spBatchSize = atoi(value);
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
  char*   topic = strdup(src);
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

#if 0
int32_t tmqCommitCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SMqCommitCbParam* pParam = (SMqCommitCbParam*)param;
  pParam->rspErr = code;
  if (pParam->async) {
    if (pParam->automatic && pParam->tmq->commitCb) {
      pParam->tmq->commitCb(pParam->tmq, pParam->rspErr, pParam->tmq->commitCbUserParam);
    } else if (!pParam->automatic && pParam->userCb) {
      pParam->userCb(pParam->tmq, pParam->rspErr, pParam->userParam);
    }

    if (pParam->freeOffsets) {
      taosArrayDestroy(pParam->offsets);
    }

    taosMemoryFree(pParam);
  } else {
    tsem_post(&pParam->rspSem);
  }
  return 0;
}
#endif

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

  /*tscDebug("receive offset commit cb of %s on vg %d, offset is %ld", pParam->pOffset->subKey, pParam->->vgId,
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

  tscDebug("consumer %ld commit offset of %s on vg %d, offset is %ld", tmq->consumerId, pOffset->subKey, pVg->vgId,
           pOffset->val.version);

  // TODO: put into cb
  pVg->committedOffsetNew = pVg->currentOffsetNew;

  pMsgSendInfo->requestId = generateRequestId();
  pMsgSendInfo->requestObjRefId = 0;
  pMsgSendInfo->param = pParam;
  pMsgSendInfo->fp = tmqCommitCb2;
  pMsgSendInfo->msgType = TDMT_VND_MQ_COMMIT_OFFSET;
  // send msg

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &pVg->epSet, &transporterId, pMsgSendInfo);
  pParamSet->waitingRspNum++;
  pParamSet->totalRspNum++;
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

    tscDebug("consumer %ld begin commit for topic %s, vgNum %d", tmq->consumerId, pTopic->topicName,
             (int32_t)taosArrayGetSize(pTopic->vgs));

    for (int32_t j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
      SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);

      tscDebug("consumer %ld begin commit for topic %s, vgId %d", tmq->consumerId, pTopic->topicName, pVg->vgId);

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

#if 0
int32_t tmqCommitInner(tmq_t* tmq, const TAOS_RES* msg, int8_t automatic, int8_t async,
                       tmq_commit_cb* userCb, void* userParam) {
  SMqCMCommitOffsetReq req;
  SArray*              pOffsets = NULL;
  void*                buf = NULL;
  SMqCommitCbParam*    pParam = NULL;
  SMsgSendInfo*        sendInfo = NULL;
  int8_t               freeOffsets;
  int32_t              code = -1;

  if (msg == NULL) {
    freeOffsets = 1;
    pOffsets = taosArrayInit(0, sizeof(SMqOffset));
    for (int32_t i = 0; i < taosArrayGetSize(tmq->clientTopics); i++) {
      SMqClientTopic* pTopic = taosArrayGet(tmq->clientTopics, i);
      for (int32_t j = 0; j < taosArrayGetSize(pTopic->vgs); j++) {
        SMqClientVg* pVg = taosArrayGet(pTopic->vgs, j);
        SMqOffset    offset;
        tstrncpy(offset.topicName, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
        tstrncpy(offset.cgroup, tmq->groupId, TSDB_CGROUP_LEN);
        offset.vgId = pVg->vgId;
        offset.offset = pVg->currentOffset;
        taosArrayPush(pOffsets, &offset);
      }
    }
  } else {
    freeOffsets = 0;
    pOffsets = (SArray*)&msg->container;
  }

  req.num = (int32_t)pOffsets->size;
  req.offsets = pOffsets->pData;

  SEncoder encoder;

  tEncoderInit(&encoder, NULL, 0);
  code = tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  if (code < 0) {
    goto END;
  }
  int32_t tlen = encoder.pos;
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    tEncoderClear(&encoder);
    goto END;
  }
  tEncoderClear(&encoder);

  tEncoderInit(&encoder, buf, tlen);
  tEncodeSMqCMCommitOffsetReq(&encoder, &req);
  tEncoderClear(&encoder);

  pParam = taosMemoryCalloc(1, sizeof(SMqCommitCbParam));
  if (pParam == NULL) {
    goto END;
  }
  pParam->tmq = tmq;
  pParam->automatic = automatic;
  pParam->async = async;
  pParam->offsets = pOffsets;
  pParam->freeOffsets = freeOffsets;
  pParam->userCb = userCb;
  pParam->userParam = userParam;
  if (!async) tsem_init(&pParam->rspSem, 0, 0);

  sendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (sendInfo == NULL) goto END;
  sendInfo->msgInfo = (SDataBuf){
      .pData = buf,
      .len = tlen,
      .handle = NULL,
  };

  sendInfo->requestId = generateRequestId();
  sendInfo->requestObjRefId = 0;
  sendInfo->param = pParam;
  sendInfo->fp = tmqCommitCb;
  sendInfo->msgType = TDMT_MND_MQ_COMMIT_OFFSET;

  SEpSet epSet = getEpSet_s(&tmq->pTscObj->pAppInfo->mgmtEp);

  int64_t transporterId = 0;
  asyncSendMsgToServer(tmq->pTscObj->pAppInfo->pTransporter, &epSet, &transporterId, sendInfo);

  if (!async) {
    tsem_wait(&pParam->rspSem);
    code = pParam->rspErr;
    tsem_destroy(&pParam->rspSem);
    taosMemoryFree(pParam);
  } else {
    code = 0;
  }

  // avoid double free if msg is sent
  buf = NULL;

END:
  if (buf) taosMemoryFree(buf);
  /*if (pParam) taosMemoryFree(pParam);*/
  /*if (sendInfo) taosMemoryFree(sendInfo);*/

  if (code != 0 && async) {
    if (automatic) {
      tmq->commitCb(tmq, code, (tmq_topic_vgroup_list_t*)pOffsets, tmq->commitCbUserParam);
    } else {
      userCb(tmq, code, (tmq_topic_vgroup_list_t*)pOffsets, userParam);
    }
  }

  if (!async && freeOffsets) {
    taosArrayDestroy(pOffsets);
  }
  return code;
}
#endif

void tmqAssignDelayedHbTask(void* param, void* tmrId) {
  tmq_t*  tmq = (tmq_t*)param;
  int8_t* pTaskType = taosAllocateQitem(sizeof(int8_t), DEF_QITEM);
  *pTaskType = TMQ_DELAYED_TASK__HB;
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

int32_t tmqHandleAllDelayedTask(tmq_t* tmq) {
  STaosQall* qall = taosAllocateQall();
  taosReadAllQitems(tmq->delayedTask, qall);
  while (1) {
    int8_t* pTaskType = NULL;
    taosGetQitem(qall, (void**)&pTaskType);
    if (pTaskType == NULL) break;

    if (*pTaskType == TMQ_DELAYED_TASK__HB) {
      tmqAskEp(tmq, true);
      taosTmrReset(tmqAssignDelayedHbTask, 1000, tmq, tmqMgmt.timer, &tmq->hbTimer);
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
  pTmq->useSnapshot = conf->spEnable;
  pTmq->autoCommit = conf->autoCommit;
  pTmq->autoCommitInterval = conf->autoCommitInterval;
  pTmq->commitCb = conf->commitCb;
  pTmq->commitCbUserParam = conf->commitCbUserParam;
  pTmq->resetOffsetCfg = conf->resetOffset;

  // assign consumerId
  pTmq->consumerId = tGenIdPI64();

  // init semaphore
  if (tsem_init(&pTmq->rspSem, 0, 0) != 0) {
    goto FAIL;
  }

  // init connection
  pTmq->pTscObj = taos_connect_internal(conf->ip, user, pass, NULL, NULL, conf->port, CONN_TYPE__TMQ);
  if (pTmq->pTscObj == NULL) {
    tsem_destroy(&pTmq->rspSem);
    goto FAIL;
  }

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

  // init hb timer
  if (tmq->hbTimer == NULL) {
    tmq->hbTimer = taosTmrStart(tmqAssignDelayedHbTask, 1000, tmq, tmqMgmt.timer);
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

#if 0
int32_t tmqGetSkipLogNum(tmq_message_t* tmq_message) {
  if (tmq_message == NULL) return 0;
  SMqPollRsp* pRsp = &tmq_message->msg;
  return pRsp->skipLogNum;
}
#endif

int32_t tmqPollCb(void* param, SDataBuf* pMsg, int32_t code) {
  SMqPollCbParam* pParam = (SMqPollCbParam*)param;
  SMqClientVg*    pVg = pParam->pVg;
  SMqClientTopic* pTopic = pParam->pTopic;
  tmq_t*          tmq = pParam->tmq;
  int32_t         vgId = pParam->vgId;
  int32_t         epoch = pParam->epoch;
  taosMemoryFree(pParam);
  if (code != 0) {
    tscWarn("msg discard from vg %d, epoch %d, code:%x", vgId, epoch, code);
    if (pMsg->pData) taosMemoryFree(pMsg->pData);
    if (code == TSDB_CODE_TQ_NO_COMMITTED_OFFSET) {
      SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM);
      if (pRspWrapper == NULL) {
        taosMemoryFree(pMsg->pData);
        tscWarn("msg discard from vg %d, epoch %d since out of memory", vgId, epoch);
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
    tscWarn("msg discard from vg %d since from earlier epoch, rsp epoch %d, current epoch %d", vgId, msgEpoch,
            tmqEpoch);
    tsem_post(&tmq->rspSem);
    taosMemoryFree(pMsg->pData);
    return 0;
  }

  if (msgEpoch != tmqEpoch) {
    tscWarn("mismatch rsp from vg %d, epoch %d, current epoch %d", vgId, msgEpoch, tmqEpoch);
  }

  // handle meta rsp
  int8_t rspType = ((SMqRspHead*)pMsg->pData)->mqMsgType;

  SMqPollRspWrapper* pRspWrapper = taosAllocateQitem(sizeof(SMqPollRspWrapper), DEF_QITEM);
  if (pRspWrapper == NULL) {
    taosMemoryFree(pMsg->pData);
    tscWarn("msg discard from vg %d, epoch %d since out of memory", vgId, epoch);
    goto CREATE_MSG_FAIL;
  }

  pRspWrapper->tmqRspType = rspType;
  pRspWrapper->vgHandle = pVg;
  pRspWrapper->topicHandle = pTopic;

  if (rspType == TMQ_MSG_TYPE__POLL_RSP) {
    SDecoder decoder;
    tDecoderInit(&decoder, POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), pMsg->len - sizeof(SMqRspHead));
    tDecodeSMqDataRsp(&decoder, &pRspWrapper->dataRsp);
    memcpy(&pRspWrapper->dataRsp, pMsg->pData, sizeof(SMqRspHead));
    /*tDecodeSMqDataBlkRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRspWrapper->dataRsp);*/
  } else {
    ASSERT(rspType == TMQ_MSG_TYPE__POLL_META_RSP);
    memcpy(&pRspWrapper->metaRsp, pMsg->pData, sizeof(SMqRspHead));
    tDecodeSMqMetaRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &pRspWrapper->metaRsp);
  }

  taosMemoryFree(pMsg->pData);

  tscDebug("consumer %ld recv poll: vg %d, req offset %ld, rsp offset %ld, type %d", tmq->consumerId, pVg->vgId,
           pRspWrapper->dataRsp.reqOffset.version, pRspWrapper->dataRsp.rspOffset.version, rspType);

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
  tscDebug("consumer %ld update ep epoch %d to epoch %d, topic num: %d", tmq->consumerId, tmq->epoch, epoch,
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
      tscDebug("consumer %ld new vg num: %d", tmq->consumerId, vgNumCur);
      for (int32_t j = 0; j < vgNumCur; j++) {
        SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, j);
        sprintf(vgKey, "%s:%d", pTopicCur->topicName, pVgCur->vgId);
        char buf[50];
        tFormatOffset(buf, 50, &pVgCur->currentOffsetNew);
        tscDebug("consumer %ld epoch %d vg %d vgKey is %s, offset is %s", tmq->consumerId, epoch, pVgCur->vgId, vgKey,
                 buf);
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

    tscDebug("consumer %ld update topic: %s", tmq->consumerId, topic.topicName);

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

      /*tscDebug("consumer %ld(epoch %d) offset of vg %d updated to %ld, vgKey is %s", tmq->consumerId, epoch,*/
      /*pVgEp->vgId, offset, vgKey);*/
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
  tscDebug("consumer %ld update ep epoch %d to epoch %d, topic num: %d", tmq->consumerId, tmq->epoch, epoch,
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

    tscDebug("consumer %ld update topic: %s", tmq->consumerId, topic.topicName);
    int32_t topicNumCur = taosArrayGetSize(tmq->clientTopics);
    for (int32_t j = 0; j < topicNumCur; j++) {
      // find old topic
      SMqClientTopic* pTopicCur = taosArrayGet(tmq->clientTopics, j);
      if (pTopicCur->vgs && strcmp(pTopicCur->topicName, pTopicEp->topic) == 0) {
        int32_t vgNumCur = taosArrayGetSize(pTopicCur->vgs);
        tscDebug("consumer %ld new vg num: %d", tmq->consumerId, vgNumCur);
        if (vgNumCur == 0) break;
        for (int32_t k = 0; k < vgNumCur; k++) {
          SMqClientVg* pVgCur = taosArrayGet(pTopicCur->vgs, k);
          sprintf(vgKey, "%s:%d", topic.topicName, pVgCur->vgId);
          tscDebug("consumer %ld epoch %d vg %d build %s", tmq->consumerId, epoch, pVgCur->vgId, vgKey);
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
      tscDebug("consumer %ld(epoch %d) original offset of vg %d is %ld", tmq->consumerId, epoch, pVgEp->vgId, offset);
      if (pOffset != NULL) {
        offset = *pOffset;
        tscDebug("consumer %ld(epoch %d) receive offset of vg %d, full key is %s", tmq->consumerId, epoch, pVgEp->vgId,
                 vgKey);
      }
      tscDebug("consumer %ld(epoch %d) offset of vg %d updated to %ld", tmq->consumerId, epoch, pVgEp->vgId, offset);
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
    tscError("consumer %ld get topic endpoint error, not ready, wait:%d", tmq->consumerId, pParam->async);
    goto END;
  }

  // tmq's epoch is monotonically increase,
  // so it's safe to discard any old epoch msg.
  // Epoch will only increase when received newer epoch ep msg
  SMqRspHead* head = pMsg->pData;
  int32_t     epoch = atomic_load_32(&tmq->epoch);
  tscDebug("consumer %ld recv ep, msg epoch %d, current epoch %d", tmq->consumerId, head->epoch, epoch);
  if (head->epoch <= epoch) {
    goto END;
  }

  if (!async) {
    SMqAskEpRsp rsp;
    tDecodeSMqAskEpRsp(POINTER_SHIFT(pMsg->pData, sizeof(SMqRspHead)), &rsp);
    /*printf("rsp epoch %ld sz %ld\n", rsp.epoch, rsp.topics->size);*/
    /*printf("tmq epoch %ld sz %ld\n", tmq->epoch, tmq->clientTopics->size);*/
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
    tscTrace("consumer %ld skip ask ep cnt %d", tmq->consumerId, epSkipCnt);
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

  tscDebug("consumer %ld ask ep", tmq->consumerId);

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
        tscTrace("consumer %ld epoch %d skip vg %d skip cnt %d", tmq->consumerId, tmq->epoch, pVg->vgId, vgSkipCnt);
        continue;
        /*if (vgSkipCnt < 10000) continue;*/
#if 0
        if (skipCnt < 30000) {
          continue;
        } else {
        tscDebug("consumer %ld skip vg %d skip too much reset", tmq->consumerId, pVg->vgId);
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
      tscDebug("consumer %ld send poll to %s : vg %d, epoch %d, req offset %s, reqId %lu", tmq->consumerId,
               pTopic->topicName, pVg->vgId, tmq->epoch, offsetFormatBuf, pReq->reqId);
      /*printf("send vg %d %ld\n", pVg->vgId, pVg->currentOffset);*/
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
        /*printf("vg %d offset %ld up to %ld\n", pVg->vgId, pVg->currentOffset, rspMsg->msg.rspOffset);*/
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
        /*printf("vg %d offset %ld up to %ld\n", pVg->vgId, pVg->currentOffset, rspMsg->msg.rspOffset);*/
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
        tscDebug("consumer %ld reset and repoll", tmq->consumerId);
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
        tscDebug("consumer %ld (epoch %d) timeout, no rsp", tmq->consumerId, tmq->epoch);
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

    tmq_list_t* lst = tmq_list_new();
    rsp = tmq_subscribe(tmq, lst);
    tmq_list_destroy(lst);

    if (rsp != 0) {
      return rsp;
    }
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

tmq_raw_data *tmq_get_raw_meta(TAOS_RES* res) {
  if (TD_RES_TMQ_META(res)) {
    tmq_raw_data *raw = taosMemoryCalloc(1, sizeof(tmq_raw_data));
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    raw->raw_meta = pMetaRspObj->metaRsp.metaRsp;
    raw->raw_meta_len = pMetaRspObj->metaRsp.metaRspLen;
    raw->raw_meta_type = pMetaRspObj->metaRsp.resMsgType;
    return raw;
  }
  return NULL;
}

static char *buildCreateTableJson(SSchemaWrapper *schemaRow, SSchemaWrapper* schemaTag, char* name, int64_t id, int8_t t){
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return string;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);

  char uid[32] = {0};
  sprintf(uid, "%"PRIi64, id);
  cJSON* id_ = cJSON_CreateString(uid);
  cJSON_AddItemToObject(json, "id", id_);
  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString(t == TSDB_NORMAL_TABLE ? "normal" : "super");
  cJSON_AddItemToObject(json, "tableType", tableType);
//  cJSON* version = cJSON_CreateNumber(1);
//  cJSON_AddItemToObject(json, "version", version);

  cJSON* columns = cJSON_CreateArray();
  for(int i = 0; i < schemaRow->nCols; i++){
    cJSON* column = cJSON_CreateObject();
    SSchema *s = schemaRow->pSchema + i;
    cJSON* cname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(column, "name", cname);
    cJSON* ctype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(column, "type", ctype);
    if(s->type == TSDB_DATA_TYPE_BINARY){
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON* cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    }else if (s->type == TSDB_DATA_TYPE_NCHAR){
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE)/TSDB_NCHAR_SIZE;
      cJSON* cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    }
    cJSON_AddItemToArray(columns, column);
  }
  cJSON_AddItemToObject(json, "columns", columns);

  cJSON* tags = cJSON_CreateArray();
  for(int i = 0; schemaTag && i < schemaTag->nCols; i++){
    cJSON* tag = cJSON_CreateObject();
    SSchema *s = schemaTag->pSchema + i;
    cJSON* tname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(tag, "name", tname);
    cJSON* ttype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(tag, "type", ttype);
    if(s->type == TSDB_DATA_TYPE_BINARY){
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON* cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    }else if (s->type == TSDB_DATA_TYPE_NCHAR){
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE)/TSDB_NCHAR_SIZE;
      cJSON* cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    }
    cJSON_AddItemToArray(tags, tag);
  }
  cJSON_AddItemToObject(json, "tags", tags);

  string = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);
  return string;
}

static char *processCreateStb(SMqMetaRsp *metaRsp){
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*  string = NULL;

  // decode and process req
  void* data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
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

static char *buildCreateCTableJson(STag* pTag, int64_t sid, char* name, int64_t id){
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return string;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);
  char cid[32] = {0};
  sprintf(cid, "%"PRIi64, id);
  cJSON* cid_ = cJSON_CreateString(cid);
  cJSON_AddItemToObject(json, "id", cid_);

  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString("child");
  cJSON_AddItemToObject(json, "tableType", tableType);

  char sid_[32] = {0};
  sprintf(sid_, "%"PRIi64, sid);
  cJSON* using = cJSON_CreateString(sid_);
  cJSON_AddItemToObject(json, "using", using);
//  cJSON* version = cJSON_CreateNumber(1);
//  cJSON_AddItemToObject(json, "version", version);

  cJSON* tags = cJSON_CreateArray();

  if (tTagIsJson(pTag)) {   // todo
    char* pJson = parseTagDatatoJson(pTag);

    cJSON* tag = cJSON_CreateObject();
    cJSON* tname = cJSON_CreateString("unknown");                 // todo
    cJSON_AddItemToObject(tag, "name", tname);
    cJSON* ttype = cJSON_CreateNumber(TSDB_DATA_TYPE_JSON);
    cJSON_AddItemToObject(tag, "type", ttype);
    cJSON* tvalue = cJSON_CreateString(pJson);
    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
    cJSON_AddItemToObject(json, "tags", tags);

    string = cJSON_PrintUnformatted(json);
    goto end;
  }

  SArray* pTagVals = NULL;
  int32_t code = tTagToValArray(pTag, &pTagVals);
  if (code) {
    goto end;
  }

  for(int i = 0; i < taosArrayGetSize(pTagVals); i++){
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);

    cJSON* tag = cJSON_CreateObject();
//    cJSON* tname = cJSON_CreateNumber(pTagVal->cid);
    cJSON* tname = cJSON_CreateString("unkonwn");    // todo
    cJSON_AddItemToObject(tag, "name", tname);
    cJSON* ttype = cJSON_CreateNumber(pTagVal->type);
    cJSON_AddItemToObject(tag, "type", ttype);

    char* buf = NULL;
    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      buf = taosMemoryCalloc(pTagVal->nData + 1, 1);
      dataConverToStr(buf, pTagVal->type, pTagVal->pData, pTagVal->nData, NULL);
    } else {
      buf = taosMemoryCalloc(32, 1);
      dataConverToStr(buf, pTagVal->type, &pTagVal->i64, tDataTypes[pTagVal->type].bytes, NULL);
    }

    cJSON* tvalue = cJSON_CreateString(buf);
    taosMemoryFree(buf);
    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
  }
  cJSON_AddItemToObject(json, "tags", tags);
  string = cJSON_PrintUnformatted(json);

end:

  cJSON_Delete(json);
  return string;
}

static char *processCreateTable(SMqMetaRsp *metaRsp){
  SDecoder           decoder = {0};
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq     *pCreateReq;
  char              *string = NULL;
  // decode
  void* data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    goto _exit;
  }

  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if(pCreateReq->type == TSDB_CHILD_TABLE){
      string = buildCreateCTableJson((STag*)pCreateReq->ctb.pTag, pCreateReq->ctb.suid, pCreateReq->name, pCreateReq->uid);
    }else if(pCreateReq->type == TSDB_NORMAL_TABLE){
      string = buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->name, pCreateReq->uid, TSDB_NORMAL_TABLE);
    }
  }

  tDecoderClear(&decoder);

  _exit:
  tDecoderClear(&decoder);
  return string;
}

static char *processAlterTable(SMqMetaRsp *metaRsp){
  SDecoder           decoder = {0};
  SVAlterTbReq       vAlterTbReq = {0};
  char              *string = NULL;

  // decode
  void* data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
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

  switch (vAlterTbReq.action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      cJSON* alterType = cJSON_CreateNumber(TSDB_ALTER_TABLE_ADD_COLUMN);
      cJSON_AddItemToObject(json, "alterType", alterType);
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      cJSON_AddItemToObject(json, "colType", colType);

      if(vAlterTbReq.type == TSDB_DATA_TYPE_BINARY){
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON* cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR){
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE)/TSDB_NCHAR_SIZE;
        cJSON* cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_DROP_COLUMN:{
      cJSON* alterType = cJSON_CreateNumber(TSDB_ALTER_TABLE_DROP_COLUMN);
      cJSON_AddItemToObject(json, "alterType", alterType);
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:{
      cJSON* alterType = cJSON_CreateNumber(TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES);
      cJSON_AddItemToObject(json, "alterType", alterType);
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      cJSON_AddItemToObject(json, "colType", colType);
      if(vAlterTbReq.type == TSDB_DATA_TYPE_BINARY){
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON* cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR){
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE)/TSDB_NCHAR_SIZE;
        cJSON* cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:{
      cJSON* alterType = cJSON_CreateNumber(TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME);
      cJSON_AddItemToObject(json, "alterType", alterType);
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colNewName = cJSON_CreateString(vAlterTbReq.colNewName);
      cJSON_AddItemToObject(json, "colNewName", colNewName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:{
      cJSON* alterType = cJSON_CreateNumber(TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
      cJSON_AddItemToObject(json, "alterType", alterType);
      cJSON* tagName = cJSON_CreateString(vAlterTbReq.tagName);
      cJSON_AddItemToObject(json, "colName", tagName);
      cJSON* colValue = cJSON_CreateString("invalid, todo");   // todo
      cJSON_AddItemToObject(json, "colValue", colValue);
      cJSON* isNull = cJSON_CreateBool(vAlterTbReq.isNull);
      cJSON_AddItemToObject(json, "colValueNull", isNull);
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

static char *processDropSTable(SMqMetaRsp *metaRsp){
  SDecoder           decoder = {0};
  SVDropStbReq       req = {0};
  char              *string = NULL;

  // decode
  void* data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
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
  char uid[32] = {0};
  sprintf(uid, "%"PRIi64, req.suid);
  cJSON* id = cJSON_CreateString(uid);
  cJSON_AddItemToObject(json, "id", id);
  cJSON* tableName = cJSON_CreateString(req.name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* tableType = cJSON_CreateString("super");
  cJSON_AddItemToObject(json, "tableType", tableType);

  string = cJSON_PrintUnformatted(json);

  _exit:
  tDecoderClear(&decoder);
  return string;
}

static char *processDropTable(SMqMetaRsp *metaRsp){
  SDecoder           decoder = {0};
  SVDropTbBatchReq   req = {0};
  char              *string = NULL;

  // decode
  void* data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
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

    cJSON* tableName = cJSON_CreateString(pDropTbReq->name);   // todo
    cJSON_AddItemToArray(tableNameList, tableName);
  }
  cJSON_AddItemToObject(json, "tableNameList", tableNameList);

  string = cJSON_PrintUnformatted(json);

  _exit:
  tDecoderClear(&decoder);
  return string;
}

char *tmq_get_json_meta(TAOS_RES *res){
  if (!TD_RES_TMQ_META(res)) {
    return NULL;
  }

  SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
  if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_STB){
    return processCreateStb(&pMetaRspObj->metaRsp);
  }else if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_STB){
    return processCreateStb(&pMetaRspObj->metaRsp);
  }else if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_STB){
    return processDropSTable(&pMetaRspObj->metaRsp);
  }else if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_TABLE){
    return processCreateTable(&pMetaRspObj->metaRsp);
  }else if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_TABLE){
    return processAlterTable(&pMetaRspObj->metaRsp);
  }else if(pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_TABLE){
    return processDropTable(&pMetaRspObj->metaRsp);
  }
  return NULL;
}

static int32_t taosCreateStb(TAOS *taos, void *meta, int32_t metaLen){
  SVCreateStbReq req = {0};
  SDecoder       coder;
  SMCreateStbReq pReq = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = NULL;

  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pTscObj) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    goto end;
  }

  code = buildRequest(pTscObj, "", 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if(!pRequest->pDb){
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void* data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  // build create stable
  pReq.pColumns = taosArrayInit(req.schemaRow.nCols, sizeof(SField));
  for(int32_t i = 0; i < req.schemaRow.nCols; i++){
    SSchema* pSchema = req.schemaRow.pSchema + i;
    SField   field   = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pColumns, &field);
  }
  pReq.pTags = taosArrayInit(req.schemaTag.nCols, sizeof(SField));
  for(int32_t i = 0; i < req.schemaTag.nCols; i++){
    SSchema* pSchema = req.schemaTag.pSchema + i;
    SField   field   = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pTags, &field);
  }
  pReq.colVer = req.schemaRow.version;
  pReq.tagVer = req.schemaTag.version;
  pReq.numOfColumns = req.schemaRow.nCols;
  pReq.numOfTags = req.schemaTag.nCols;
  pReq.commentLen = -1;
  pReq.suid = req.suid;
  pReq.source = 1;

  SName tableName;
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

  SQuery      pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);
  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

  end:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  tDecoderClear(&coder);
  return code;
}

static int32_t taosDropStb(TAOS *taos, void *meta, int32_t metaLen){
  SVDropStbReq req = {0};
  SDecoder     coder;
  SMDropStbReq pReq = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = NULL;

  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pTscObj) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    goto end;
  }

  code = buildRequest(pTscObj, "", 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if(!pRequest->pDb){
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void* data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // build drop stable
  pReq.igNotExists = true;
  pReq.source = 1;
  pReq.suid = req.suid;
  SName tableName;
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

  SQuery      pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);
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
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*) data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosCreateTable(TAOS *taos, void *meta, int32_t metaLen){
  SVCreateTbBatchReq  req             = {0};
  SDecoder            coder           = {0};
  int32_t             code            = TSDB_CODE_SUCCESS;
  SRequestObj        *pRequest        = NULL;
  SQuery             *pQuery          = NULL;
  SHashObj           *pVgroupHashmap  = NULL;
  STscObj            *pTscObj         = acquireTscObj(*(int64_t *)taos);

  if (NULL == pTscObj) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    goto end;
  }

  code = buildRequest(pTscObj, "", 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if(!pRequest->pDb){
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void* data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  SVCreateTbReq     *pCreateReq = NULL;
  SCatalog* pCatalog = NULL;
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
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;

    SVgroupInfo pInfo = {0};
    SName pName;
    toName(pTscObj->acctId, pRequest->pDb, pCreateReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

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
  pQuery->pRoot   = nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, false, NULL);
  pQuery  = NULL;          // no need to free in the end
  code    = pRequest->code;

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

static int32_t taosDropTable(TAOS *taos, void *meta, int32_t metaLen){
  SVDropTbBatchReq    req             = {0};
  SDecoder            coder           = {0};
  int32_t             code            = TSDB_CODE_SUCCESS;
  SRequestObj        *pRequest        = NULL;
  SQuery             *pQuery          = NULL;
  SHashObj           *pVgroupHashmap  = NULL;
  STscObj            *pTscObj         = acquireTscObj(*(int64_t *)taos);

  if (NULL == pTscObj) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    goto end;
  }

  code = buildRequest(pTscObj, "", 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if(!pRequest->pDb){
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void* data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  SVDropTbReq     *pDropReq = NULL;
  SCatalog        *pCatalog = NULL;
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
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;

    SVgroupInfo pInfo = {0};
    SName pName;
    toName(pTscObj->acctId, pRequest->pDb, pDropReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

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
  pQuery->pRoot   = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, false, NULL);
  pQuery  = NULL;          // no need to free in the end
  code    = pRequest->code;

  end:
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

static int32_t taosAlterTable(TAOS *taos, void *meta, int32_t metaLen){
  SVAlterTbReq        req             = {0};
  SDecoder            coder           = {0};
  int32_t             code            = TSDB_CODE_SUCCESS;
  SRequestObj        *pRequest        = NULL;
  SQuery             *pQuery          = NULL;
  SArray             *pArray          = NULL;
  SVgDataBlocks      *pVgData         = NULL;
  STscObj            *pTscObj         = acquireTscObj(*(int64_t *)taos);

  if (NULL == pTscObj) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    goto end;
  }

  code = buildRequest(pTscObj, "", 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if(!pRequest->pDb){
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void* data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVAlterTbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // do not deal TSDB_ALTER_TABLE_UPDATE_OPTIONS
  if(req.action == TSDB_ALTER_TABLE_UPDATE_OPTIONS){
    goto end;
  }

  SCatalog        *pCatalog = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
      .requestId = pRequest->requestId,
      .requestObjRefId = pRequest->self,
      .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  SVgroupInfo pInfo = {0};
  SName pName = {0};
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
  pQuery->pRoot   = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, false, NULL);
  pQuery  = NULL;          // no need to free in the end
  pVgData = NULL;
  pArray  = NULL;
  code    = pRequest->code;

end:
  taosArrayDestroy(pArray);
  if(pVgData) taosMemoryFreeClear(pVgData->pData);
  taosMemoryFreeClear(pVgData);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

int32_t taos_write_raw_meta(TAOS *taos, tmq_raw_data *raw_meta){
  if (!taos || !raw_meta) {
    return TSDB_CODE_INVALID_PARA;
  }

  if(raw_meta->raw_meta_type == TDMT_VND_CREATE_STB) {
    return taosCreateStb(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }else if(raw_meta->raw_meta_type == TDMT_VND_ALTER_STB){
    return taosCreateStb(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }else if(raw_meta->raw_meta_type == TDMT_VND_DROP_STB){
    return taosDropStb(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }else if(raw_meta->raw_meta_type == TDMT_VND_CREATE_TABLE){
    return taosCreateTable(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }else if(raw_meta->raw_meta_type == TDMT_VND_ALTER_TABLE){
    return taosAlterTable(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }else if(raw_meta->raw_meta_type == TDMT_VND_DROP_TABLE){
    return taosDropTable(taos, raw_meta->raw_meta, raw_meta->raw_meta_len);
  }
  return TSDB_CODE_INVALID_PARA;
}

void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb* cb, void* param) {
  tmqCommitInner2(tmq, msg, 0, 1, cb, param);
}

int32_t tmq_commit_sync(tmq_t* tmq, const TAOS_RES* msg) { return tmqCommitInner2(tmq, msg, 0, 0, NULL, NULL); }

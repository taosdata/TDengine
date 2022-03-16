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

#ifndef _TD_MND_DEF_H_
#define _TD_MND_DEF_H_

#include "os.h"

#include "cJSON.h"
#include "scheduler.h"
#include "sync.h"
#include "thash.h"
#include "tlist.h"
#include "tlog.h"
#include "tmsg.h"
#include "trpc.h"
#include "ttimer.h"

#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  MND_AUTH_ACCT_START = 0,
  MND_AUTH_ACCT_USER,
  MND_AUTH_ACCT_DNODE,
  MND_AUTH_ACCT_MNODE,
  MND_AUTH_ACCT_DB,
  MND_AUTH_ACCT_TABLE,
  MND_AUTH_ACCT_MAX
} EAuthAcct;

typedef enum {
  MND_AUTH_OP_START = 0,
  MND_AUTH_OP_CREATE_USER,
  MND_AUTH_OP_ALTER_USER,
  MND_AUTH_OP_DROP_USER,
  MND_AUTH_MAX
} EAuthOp;

typedef enum {
  TRN_STAGE_PREPARE = 0,
  TRN_STAGE_REDO_LOG = 1,
  TRN_STAGE_REDO_ACTION = 2,
  TRN_STAGE_COMMIT = 3,
  TRN_STAGE_COMMIT_LOG = 4,
  TRN_STAGE_UNDO_ACTION = 5,
  TRN_STAGE_UNDO_LOG = 6,
  TRN_STAGE_ROLLBACK = 7,
  TRN_STAGE_FINISHED = 8
} ETrnStage;

typedef enum {
  TRN_TYPE_BASIC_SCOPE = 1000,
  TRN_TYPE_CREATE_USER = 1001,
  TRN_TYPE_ALTER_USER = 1002,
  TRN_TYPE_DROP_USER = 1003,
  TRN_TYPE_CREATE_FUNC = 1004,
  TRN_TYPE_DROP_FUNC = 1005,
  TRN_TYPE_CREATE_SNODE = 1006,
  TRN_TYPE_DROP_SNODE = 1007,
  TRN_TYPE_CREATE_QNODE = 1008,
  TRN_TYPE_DROP_QNODE = 1009,
  TRN_TYPE_CREATE_BNODE = 1010,
  TRN_TYPE_DROP_BNODE = 1011,
  TRN_TYPE_CREATE_MNODE = 1012,
  TRN_TYPE_DROP_MNODE = 1013,
  TRN_TYPE_CREATE_TOPIC = 1014,
  TRN_TYPE_DROP_TOPIC = 1015,
  TRN_TYPE_SUBSCRIBE = 1016,
  TRN_TYPE_REBALANCE = 1017,
  TRN_TYPE_COMMIT_OFFSET = 1018,
  TRN_TYPE_CREATE_STREAM = 1019,
  TRN_TYPE_DROP_STREAM = 1020,
  TRN_TYPE_ALTER_STREAM = 1021,
  TRN_TYPE_BASIC_SCOPE_END,
  TRN_TYPE_GLOBAL_SCOPE = 2000,
  TRN_TYPE_CREATE_DNODE = 2001,
  TRN_TYPE_DROP_DNODE = 2002,
  TRN_TYPE_GLOBAL_SCOPE_END,
  TRN_TYPE_DB_SCOPE = 3000,
  TRN_TYPE_CREATE_DB = 3001,
  TRN_TYPE_ALTER_DB = 3002,
  TRN_TYPE_DROP_DB = 3003,
  TRN_TYPE_SPLIT_VGROUP = 3004,
  TRN_TYPE_MERGE_VGROUP = 3015,
  TRN_TYPE_DB_SCOPE_END,
  TRN_TYPE_STB_SCOPE = 4000,
  TRN_TYPE_CREATE_STB = 4001,
  TRN_TYPE_ALTER_STB = 4002,
  TRN_TYPE_DROP_STB = 4003,
  TRN_TYPE_STB_SCOPE_END,
} ETrnType;

typedef enum { TRN_POLICY_ROLLBACK = 0, TRN_POLICY_RETRY = 1 } ETrnPolicy;

typedef enum {
  DND_REASON_ONLINE = 0,
  DND_REASON_STATUS_MSG_TIMEOUT,
  DND_REASON_STATUS_NOT_RECEIVED,
  DND_REASON_VERSION_NOT_MATCH,
  DND_REASON_DNODE_ID_NOT_MATCH,
  DND_REASON_CLUSTER_ID_NOT_MATCH,
  DND_REASON_STATUS_INTERVAL_NOT_MATCH,
  DND_REASON_TIME_ZONE_NOT_MATCH,
  DND_REASON_LOCALE_NOT_MATCH,
  DND_REASON_CHARSET_NOT_MATCH,
  DND_REASON_OTHERS
} EDndReason;

typedef struct {
  int32_t    id;
  ETrnStage  stage;
  ETrnPolicy policy;
  ETrnType   transType;
  int32_t    code;
  int32_t    failedTimes;
  void*      rpcHandle;
  void*      rpcAHandle;
  void*      rpcRsp;
  int32_t    rpcRspLen;
  SArray*    redoLogs;
  SArray*    undoLogs;
  SArray*    commitLogs;
  SArray*    redoActions;
  SArray*    undoActions;
  int64_t    createdTime;
  int64_t    lastExecTime;
  int64_t    dbUid;
  char       dbname[TSDB_DB_FNAME_LEN];
  char       lastError[TSDB_TRANS_ERROR_LEN];
} STrans;

typedef struct {
  int64_t id;
  char    name[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int64_t updateTime;
} SClusterObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  int64_t    rebootTime;
  int64_t    lastAccessTime;
  int32_t    accessTimes;
  int32_t    numOfVnodes;
  int32_t    numOfSupportVnodes;
  int32_t    numOfCores;
  EDndReason offlineReason;
  uint16_t   port;
  char       fqdn[TSDB_FQDN_LEN];
  char       ep[TSDB_EP_LEN];
} SDnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  ESyncState role;
  int32_t    roleTerm;
  int64_t    roleTime;
  SDnodeObj* pDnode;
} SMnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj* pDnode;
} SQnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj* pDnode;
} SSnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj* pDnode;
} SBnodeObj;

typedef struct {
  int32_t maxUsers;
  int32_t maxDbs;
  int32_t maxStbs;
  int32_t maxTbs;
  int32_t maxTimeSeries;
  int32_t maxStreams;
  int32_t maxFuncs;
  int32_t maxConsumers;
  int32_t maxConns;
  int32_t maxTopics;
  int64_t maxStorage;   // In unit of GB
  int32_t accessState;  // Configured only by command
} SAcctCfg;

typedef struct {
  int32_t numOfUsers;
  int32_t numOfDbs;
  int32_t numOfTimeSeries;
  int32_t numOfStreams;
  int64_t totalStorage;  // Total storage wrtten from this account
  int64_t compStorage;   // Compressed storage on disk
} SAcctInfo;

typedef struct {
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   acctId;
  int32_t   status;
  SAcctCfg  cfg;
  SAcctInfo info;
} SAcctObj;

typedef struct {
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_PASSWORD_LEN];
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int8_t    superUser;
  int32_t   acctId;
  SHashObj* readDbs;
  SHashObj* writeDbs;
} SUserObj;

typedef struct {
  int32_t numOfVgroups;
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRows;
  int32_t maxRows;
  int32_t commitTime;
  int32_t fsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  quorum;
  int8_t  update;
  int8_t  cacheLastRow;
  int8_t  streamMode;
} SDbCfg;

typedef struct {
  char    name[TSDB_DB_FNAME_LEN];
  char    acct[TSDB_USER_LEN];
  char    createUser[TSDB_USER_LEN];
  int64_t createdTime;
  int64_t updateTime;
  int64_t uid;
  int32_t cfgVersion;
  int32_t vgVersion;
  int8_t  hashMethod;  // default is 1
  SDbCfg  cfg;
} SDbObj;

typedef struct {
  int32_t    dnodeId;
  ESyncState role;
} SVnodeGid;

typedef struct {
  int32_t   vgId;
  int64_t   createdTime;
  int64_t   updateTime;
  int32_t   version;
  uint32_t  hashBegin;
  uint32_t  hashEnd;
  char      dbName[TSDB_DB_FNAME_LEN];
  int64_t   dbUid;
  int64_t   numOfTables;
  int64_t   numOfTimeSeries;
  int64_t   totalStorage;
  int64_t   compStorage;
  int64_t   pointsWritten;
  int8_t    compact;
  int8_t    replica;
  int8_t    streamMode;
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
} SVgObj;

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  version;
  int32_t  nextColId;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  SSchema* pColumns;
  SSchema* pTags;
  SRWLatch lock;
  char     comment[TSDB_STB_COMMENT_LEN];
} SStbObj;

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int64_t createdTime;
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  align;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int64_t signature;
  int32_t commentSize;
  int32_t codeSize;
  char*   pComment;
  char*   pCode;
  char    pData[];
} SFuncObj;

typedef struct {
  int64_t id;
  int8_t  type;
  int8_t  replica;
  int16_t numOfColumns;
  int32_t rowSize;
  int32_t numOfRows;
  int32_t numOfReads;
  int32_t payloadLen;
  void*   pIter;
  SMnode* pMnode;
  char    db[TSDB_DB_FNAME_LEN];
  int16_t offset[TSDB_MAX_COLUMNS];
  int32_t bytes[TSDB_MAX_COLUMNS];
  char    payload[];
} SShowObj;

typedef struct {
  int64_t id;
  int8_t  type;
  int8_t  replica;
  int16_t numOfColumns;
  int32_t rowSize;
  int32_t numOfRows;
  int32_t numOfReads;
  int32_t payloadLen;
  void*   pIter;
  SMnode* pMnode;
  char    db[TSDB_DB_FNAME_LEN];
  int16_t offset[TSDB_MAX_COLUMNS];
  int32_t bytes[TSDB_MAX_COLUMNS];
  char    payload[];
} SSysTableRetrieveObj;

typedef struct {
  int32_t vgId;  // -1 for unassigned
  int32_t status;
  SEpSet  epSet;
  int64_t oldConsumerId;
  int64_t consumerId;  // -1 for unassigned
  char*   qmsg;
} SMqConsumerEp;

static FORCE_INLINE int32_t tEncodeSMqConsumerEp(void** buf, const SMqConsumerEp* pConsumerEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pConsumerEp->vgId);
  tlen += taosEncodeFixedI32(buf, pConsumerEp->status);
  tlen += taosEncodeSEpSet(buf, &pConsumerEp->epSet);
  tlen += taosEncodeFixedI64(buf, pConsumerEp->oldConsumerId);
  tlen += taosEncodeFixedI64(buf, pConsumerEp->consumerId);
  tlen += taosEncodeString(buf, pConsumerEp->qmsg);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumerEp(void** buf, SMqConsumerEp* pConsumerEp) {
  buf = taosDecodeFixedI32(buf, &pConsumerEp->vgId);
  buf = taosDecodeFixedI32(buf, &pConsumerEp->status);
  buf = taosDecodeSEpSet(buf, &pConsumerEp->epSet);
  buf = taosDecodeFixedI64(buf, &pConsumerEp->oldConsumerId);
  buf = taosDecodeFixedI64(buf, &pConsumerEp->consumerId);
  buf = taosDecodeString(buf, &pConsumerEp->qmsg);
  return buf;
}

static FORCE_INLINE void tDeleteSMqConsumerEp(SMqConsumerEp* pConsumerEp) {
  if (pConsumerEp) {
    tfree(pConsumerEp->qmsg);
  }
}

typedef struct {
  int64_t consumerId;
  SArray* vgInfo;  // SArray<SMqConsumerEp>
} SMqSubConsumer;

static FORCE_INLINE int32_t tEncodeSMqSubConsumer(void** buf, const SMqSubConsumer* pConsumer) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  int32_t sz = taosArrayGetSize(pConsumer->vgInfo);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp* pCEp = taosArrayGet(pConsumer->vgInfo, i);
    tlen += tEncodeSMqConsumerEp(buf, pCEp);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqSubConsumer(void** buf, SMqSubConsumer* pConsumer) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->vgInfo = taosArrayInit(sz, sizeof(SMqConsumerEp));
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp consumerEp;
    buf = tDecodeSMqConsumerEp(buf, &consumerEp);
    taosArrayPush(pConsumer->vgInfo, &consumerEp);
  }
  return buf;
}

static FORCE_INLINE void tDeleteSMqSubConsumer(SMqSubConsumer* pSubConsumer) {
  if (pSubConsumer->vgInfo) {
    taosArrayDestroyEx(pSubConsumer->vgInfo, (void (*)(void*))tDeleteSMqConsumerEp);
    pSubConsumer->vgInfo = NULL;
  }
}

typedef struct {
  char    key[TSDB_PARTITION_KEY_LEN];
  int64_t offset;
} SMqOffsetObj;

static FORCE_INLINE int32_t tEncodeSMqOffsetObj(void** buf, const SMqOffsetObj* pOffset) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pOffset->key);
  tlen += taosEncodeFixedI64(buf, pOffset->offset);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqOffsetObj(void* buf, SMqOffsetObj* pOffset) {
  buf = taosDecodeStringTo(buf, pOffset->key);
  buf = taosDecodeFixedI64(buf, &pOffset->offset);
  return buf;
}

typedef struct {
  char    key[TSDB_SUBSCRIBE_KEY_LEN];
  int32_t status;
  int32_t vgNum;
  SArray* consumers;      // SArray<SMqSubConsumer>
  SArray* lostConsumers;  // SArray<SMqSubConsumer>
  SArray* unassignedVg;   // SArray<SMqConsumerEp>
} SMqSubscribeObj;

static FORCE_INLINE SMqSubscribeObj* tNewSubscribeObj() {
  SMqSubscribeObj* pSub = calloc(1, sizeof(SMqSubscribeObj));
  if (pSub == NULL) {
    return NULL;
  }

  pSub->consumers = taosArrayInit(0, sizeof(SMqSubConsumer));
  if (pSub->consumers == NULL) {
    goto _err;
  }

  pSub->lostConsumers = taosArrayInit(0, sizeof(SMqSubConsumer));
  if (pSub->lostConsumers == NULL) {
    goto _err;
  }

  pSub->unassignedVg = taosArrayInit(0, sizeof(SMqConsumerEp));
  if (pSub->unassignedVg == NULL) {
    goto _err;
  }

  pSub->key[0] = 0;
  pSub->vgNum = 0;
  pSub->status = 0;

  return pSub;

_err:
  tfree(pSub->consumers);
  tfree(pSub->lostConsumers);
  tfree(pSub->unassignedVg);
  tfree(pSub);
  return NULL;
}

static FORCE_INLINE int32_t tEncodeSubscribeObj(void** buf, const SMqSubscribeObj* pSub) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pSub->key);
  tlen += taosEncodeFixedI32(buf, pSub->vgNum);
  tlen += taosEncodeFixedI32(buf, pSub->status);
  int32_t sz;

  sz = taosArrayGetSize(pSub->consumers);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubConsumer* pSubConsumer = taosArrayGet(pSub->consumers, i);
    tlen += tEncodeSMqSubConsumer(buf, pSubConsumer);
  }

  sz = taosArrayGetSize(pSub->lostConsumers);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubConsumer* pSubConsumer = taosArrayGet(pSub->lostConsumers, i);
    tlen += tEncodeSMqSubConsumer(buf, pSubConsumer);
  }

  sz = taosArrayGetSize(pSub->unassignedVg);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp* pCEp = taosArrayGet(pSub->unassignedVg, i);
    tlen += tEncodeSMqConsumerEp(buf, pCEp);
  }

  return tlen;
}

static FORCE_INLINE void* tDecodeSubscribeObj(void* buf, SMqSubscribeObj* pSub) {
  buf = taosDecodeStringTo(buf, pSub->key);
  buf = taosDecodeFixedI32(buf, &pSub->vgNum);
  buf = taosDecodeFixedI32(buf, &pSub->status);

  int32_t sz;

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->consumers = taosArrayInit(sz, sizeof(SMqSubConsumer));
  if (pSub->consumers == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubConsumer subConsumer = {0};
    buf = tDecodeSMqSubConsumer(buf, &subConsumer);
    taosArrayPush(pSub->consumers, &subConsumer);
  }

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->lostConsumers = taosArrayInit(sz, sizeof(SMqSubConsumer));
  if (pSub->lostConsumers == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubConsumer subConsumer = {0};
    buf = tDecodeSMqSubConsumer(buf, &subConsumer);
    taosArrayPush(pSub->lostConsumers, &subConsumer);
  }

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->unassignedVg = taosArrayInit(sz, sizeof(SMqConsumerEp));
  if (pSub->unassignedVg == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp consumerEp = {0};
    buf = tDecodeSMqConsumerEp(buf, &consumerEp);
    taosArrayPush(pSub->unassignedVg, &consumerEp);
  }
  return buf;
}

static FORCE_INLINE void tDeleteSMqSubscribeObj(SMqSubscribeObj* pSub) {
  if (pSub->consumers) {
    taosArrayDestroyEx(pSub->consumers, (void (*)(void*))tDeleteSMqSubConsumer);
    // taosArrayDestroy(pSub->consumers);
    pSub->consumers = NULL;
  }

  if (pSub->unassignedVg) {
    taosArrayDestroyEx(pSub->unassignedVg, (void (*)(void*))tDeleteSMqConsumerEp);
    // taosArrayDestroy(pSub->unassignedVg);
    pSub->unassignedVg = NULL;
  }
}

typedef struct {
  char     name[TSDB_TOPIC_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  version;
  SRWLatch lock;
  int32_t  sqlLen;
  char*    sql;
  char*    logicalPlan;
  char*    physicalPlan;
} SMqTopicObj;

typedef struct {
  int64_t  consumerId;
  int64_t  connId;
  SRWLatch lock;
  char     cgroup[TSDB_CGROUP_LEN];
  SArray*  currentTopics;        // SArray<char*>
  SArray*  recentRemovedTopics;  // SArray<char*>
  int32_t  epoch;
  // stat
  int64_t pollCnt;
  // status
  int32_t status;
  // heartbeat from the consumer reset hbStatus to 0
  // each checkConsumerAlive msg add hbStatus by 1
  // if checkConsumerAlive > CONSUMER_REBALANCE_CNT, mask to lost
  int32_t hbStatus;
} SMqConsumerObj;

static FORCE_INLINE int32_t tEncodeSMqConsumerObj(void** buf, const SMqConsumerObj* pConsumer) {
  int32_t sz;
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  tlen += taosEncodeFixedI64(buf, pConsumer->connId);
  tlen += taosEncodeFixedI32(buf, pConsumer->epoch);
  tlen += taosEncodeFixedI64(buf, pConsumer->pollCnt);
  tlen += taosEncodeFixedI32(buf, pConsumer->status);
  tlen += taosEncodeString(buf, pConsumer->cgroup);

  sz = taosArrayGetSize(pConsumer->currentTopics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(pConsumer->currentTopics, i);
    tlen += taosEncodeString(buf, topic);
  }

  sz = taosArrayGetSize(pConsumer->recentRemovedTopics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    char* topic = taosArrayGetP(pConsumer->recentRemovedTopics, i);
    tlen += taosEncodeString(buf, topic);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumerObj(void* buf, SMqConsumerObj* pConsumer) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeFixedI64(buf, &pConsumer->connId);
  buf = taosDecodeFixedI32(buf, &pConsumer->epoch);
  buf = taosDecodeFixedI64(buf, &pConsumer->pollCnt);
  buf = taosDecodeFixedI32(buf, &pConsumer->status);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);

  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->currentTopics = taosArrayInit(sz, sizeof(SMqConsumerObj));
  for (int32_t i = 0; i < sz; i++) {
    char* topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->currentTopics, &topic);
  }

  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->recentRemovedTopics = taosArrayInit(sz, sizeof(SMqConsumerObj));
  for (int32_t i = 0; i < sz; i++) {
    char* topic;
    buf = taosDecodeString(buf, &topic);
    taosArrayPush(pConsumer->recentRemovedTopics, &topic);
  }
  return buf;
}

typedef struct {
  char     name[TSDB_TOPIC_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  version;
  int32_t  vgNum;
  SRWLatch lock;
  int8_t   status;
  // int32_t  sqlLen;
  char*   sql;
  char*   logicalPlan;
  char*   physicalPlan;
  SArray* tasks;  // SArray<SArray<SStreamTask>>
} SStreamObj;

int32_t tEncodeSStreamObj(SCoder* pEncoder, const SStreamObj* pObj);
int32_t tDecodeSStreamObj(SCoder* pDecoder, SStreamObj* pObj);

typedef struct SMnodeMsg {
  char    user[TSDB_USER_LEN];
  char    db[TSDB_DB_FNAME_LEN];
  int32_t acctId;
  SMnode* pMnode;
  int64_t createdTime;
  SRpcMsg rpcMsg;
  int32_t contLen;
  void*   pCont;
} SMnodeMsg;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

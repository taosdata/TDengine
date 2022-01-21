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
#include "sync.h"
#include "tmsg.h"
#include "thash.h"
#include "tlist.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"
#include "scheduler.h"

#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int32_t mDebugFlag;

// mnode log function
#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", 255, __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", 255, __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ", 255, __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ", 255, __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}

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
  int32_t    code;
  int32_t    failedTimes;
  void      *rpcHandle;
  void      *rpcAHandle;
  SArray    *redoLogs;
  SArray    *undoLogs;
  SArray    *commitLogs;
  SArray    *redoActions;
  SArray    *undoActions;
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
  SDnodeObj *pDnode;
} SMnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj *pDnode;
} SQnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj *pDnode;
} SSnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj *pDnode;
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
  SHashObj *prohibitDbHash;
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
} SDbCfg;

typedef struct {
  char    name[TSDB_DB_FNAME_LEN];
  char    acct[TSDB_USER_LEN];
  int64_t createdTime;
  int64_t updateTime;
  uint64_t uid;
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
  int32_t   numOfTables;
  int32_t   numOfTimeSeries;
  int64_t   totalStorage;
  int64_t   compStorage;
  int64_t   pointsWritten;
  int8_t    compact;
  int8_t    replica;
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
} SVgObj;

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  uint64_t uid;
  uint64_t dbUid;
  int32_t  version;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  SRWLatch lock;
  SSchema *pSchema;
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
  int64_t sigature;
  int32_t commentSize;
  int32_t codeSize;
  char   *pComment;
  char   *pCode;
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
  void   *pIter;
  SMnode *pMnode;
  char    db[TSDB_DB_FNAME_LEN];
  int16_t offset[TSDB_MAX_COLUMNS];
  int32_t bytes[TSDB_MAX_COLUMNS];
  char    payload[];
} SShowObj;

#if 0
typedef struct SConsumerObj {
  uint64_t uid;
  int64_t  createTime;
  int64_t  updateTime;
  //uint64_t dbUid;
  int32_t  version;
  SRWLatch lock;
  SArray*   topics;
} SConsumerObj;

typedef struct SMqTopicConsumer {
  int64_t consumerId;
  SList*  topicList;
} SMqTopicConsumer;
#endif

typedef struct SMqConsumerEp {
  int32_t      vgId;      // -1 for unassigned
  SEpSet       epSet;
  int64_t      consumerId; // -1 for unassigned
  int64_t      lastConsumerHbTs;
  int64_t      lastVgHbTs;
  int32_t      execLen;
  SSubQueryMsg qExec;
} SMqConsumerEp;

static FORCE_INLINE int32_t tEncodeSMqConsumerEp(void** buf, SMqConsumerEp* pConsumerEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pConsumerEp->vgId);
  tlen += taosEncodeSEpSet(buf, &pConsumerEp->epSet);
  tlen += taosEncodeFixedI64(buf, pConsumerEp->consumerId);
  tlen += tEncodeSSubQueryMsg(buf, &pConsumerEp->qExec);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumerEp(void** buf, SMqConsumerEp* pConsumerEp) {
  buf = taosDecodeFixedI32(buf, &pConsumerEp->vgId);
  buf = taosDecodeSEpSet(buf, &pConsumerEp->epSet);
  buf = taosDecodeFixedI64(buf, &pConsumerEp->consumerId);
  buf = tDecodeSSubQueryMsg(buf, &pConsumerEp->qExec);
  pConsumerEp->execLen = sizeof(SSubQueryMsg) + pConsumerEp->qExec.contentLen;
  return buf;
}

//unit for rebalance
typedef struct SMqSubscribeObj {
  char    key[TSDB_SUBSCRIBE_KEY_LEN];
  int32_t epoch;
  //TODO: replace with priority queue
  int32_t nextConsumerIdx;
  SArray* availConsumer;        // SArray<int64_t> (consumerId)
  SArray* assigned;             // SArray<SMqConsumerEp>
  SArray* unassignedConsumer;   // SArray<SMqConsumerEp>
  SArray* unassignedVg;         // SArray<SMqConsumerEp>
} SMqSubscribeObj;

static FORCE_INLINE SMqSubscribeObj* tNewSubscribeObj() {
  SMqSubscribeObj* pSub = malloc(sizeof(SMqSubscribeObj));
  pSub->key[0] = 0;
  pSub->epoch = 0;
  if (pSub == NULL) {
    return NULL;
  }
  pSub->availConsumer = taosArrayInit(0, sizeof(int64_t));
  if (pSub->availConsumer == NULL) {
    free(pSub);
    return NULL;
  }
  pSub->assigned = taosArrayInit(0, sizeof(SMqConsumerEp));
  if (pSub->assigned == NULL) {
    taosArrayDestroy(pSub->availConsumer);
    free(pSub);
    return NULL;
  }
  pSub->unassignedConsumer = taosArrayInit(0, sizeof(SMqConsumerEp));
  if (pSub->assigned == NULL) {
    taosArrayDestroy(pSub->availConsumer);
    taosArrayDestroy(pSub->unassignedConsumer);
    free(pSub);
    return NULL;
  }
  pSub->unassignedVg = taosArrayInit(0, sizeof(SMqConsumerEp));
  if (pSub->assigned == NULL) {
    taosArrayDestroy(pSub->availConsumer);
    taosArrayDestroy(pSub->unassignedConsumer);
    taosArrayDestroy(pSub->unassignedVg);
    free(pSub);
    return NULL;
  }
  return NULL;
}

static FORCE_INLINE int32_t tEncodeSubscribeObj(void** buf, const SMqSubscribeObj* pSub) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pSub->key);
  tlen += taosEncodeFixedI32(buf, pSub->epoch);
  int32_t sz;

  sz = taosArrayGetSize(pSub->availConsumer);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    int64_t* pConsumerId = taosArrayGet(pSub->availConsumer, i);
    tlen += taosEncodeFixedI64(buf, *pConsumerId);
  }

  sz = taosArrayGetSize(pSub->assigned);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp* pCEp = taosArrayGet(pSub->assigned, i);
    tlen += tEncodeSMqConsumerEp(buf, pCEp);
  }

  sz = taosArrayGetSize(pSub->unassignedConsumer);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp* pCEp = taosArrayGet(pSub->unassignedConsumer, i);
    tlen += tEncodeSMqConsumerEp(buf, pCEp);
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
  buf = taosDecodeFixedI32(buf, &pSub->epoch);

  int32_t sz;

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->assigned = taosArrayInit(sz, sizeof(int64_t));
  if (pSub->assigned == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    int64_t consumerId;
    buf = taosDecodeFixedI64(buf, &consumerId);
    taosArrayPush(pSub->assigned, &consumerId);
  }

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->unassignedConsumer = taosArrayInit(sz, sizeof(SMqConsumerEp));
  if (pSub->unassignedConsumer == NULL) {
    taosArrayDestroy(pSub->assigned);
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp cEp;
    buf = tDecodeSMqConsumerEp(buf, &cEp);
    taosArrayPush(pSub->unassignedConsumer, &cEp);
  }

  buf = taosDecodeFixedI32(buf, &sz);
  pSub->unassignedVg = taosArrayInit(sz, sizeof(SMqConsumerEp));
  if (pSub->unassignedVg == NULL) {
    taosArrayDestroy(pSub->assigned);
    taosArrayDestroy(pSub->unassignedConsumer);
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp cEp;
    buf = tDecodeSMqConsumerEp(buf, &cEp);
    taosArrayPush(pSub->unassignedVg, &cEp);
  }

  return buf;
}

typedef struct SMqCGroup {
  char    name[TSDB_CONSUMER_GROUP_LEN];
  int32_t status;       // 0 - uninitialized, 1 - wait rebalance, 2- normal
  SList  *consumerIds;  // SList<int64_t>
  SList  *idleVGroups;  // SList<int32_t>
} SMqCGroup;

typedef struct SMqTopicObj {
  char      name[TSDB_TOPIC_FNAME_LEN];
  char      db[TSDB_DB_FNAME_LEN];
  int64_t   createTime;
  int64_t   updateTime;
  uint64_t  uid;
  uint64_t  dbUid;
  int32_t   version;
  SRWLatch  lock;
  int32_t   sqlLen;
  char     *sql;
  char     *logicalPlan;
  char     *physicalPlan;
  //SHashObj *cgroups;  // SHashObj<SMqCGroup>
  //SHashObj *consumers;  // SHashObj<SMqConsumerObj>
} SMqTopicObj;

// TODO: add cache and change name to id
typedef struct SMqConsumerTopic {
  char    name[TSDB_TOPIC_FNAME_LEN];
  int32_t epoch;
  //TODO: replace with something with ep
  //SList   *vgroups;  // SList<int32_t>
  //vg assigned to the consumer on the topic
  SArray  *pVgInfo;  // SArray<int32_t>
} SMqConsumerTopic;

static FORCE_INLINE SMqConsumerTopic* tNewConsumerTopic(int64_t consumerId, SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SMqConsumerTopic* pCTopic = malloc(sizeof(SMqConsumerTopic));
  if (pCTopic == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  strcpy(pCTopic->name, pTopic->name);
  pCTopic->epoch = 0;
  pCTopic->pVgInfo = taosArrayInit(0, sizeof(int32_t));

  int32_t unassignedVgSz = taosArrayGetSize(pSub->unassignedVg);
  if (unassignedVgSz > 0) {
    SMqConsumerEp* pCEp = taosArrayPop(pSub->unassignedVg);
    pCEp->consumerId = consumerId;
    taosArrayPush(pCTopic->pVgInfo, &pCEp->vgId);
    taosArrayPush(pSub->assigned, pCEp);
  }
  return pCTopic;
}

static FORCE_INLINE int32_t tEncodeSMqConsumerTopic(void** buf, SMqConsumerTopic* pConsumerTopic) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pConsumerTopic->name);
  tlen += taosEncodeFixedI32(buf, pConsumerTopic->epoch);
  int32_t sz = taosArrayGetSize(pConsumerTopic->pVgInfo);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    int32_t* pVgInfo = taosArrayGet(pConsumerTopic->pVgInfo, i);
    tlen += taosEncodeFixedI32(buf, *pVgInfo);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumerTopic(void* buf, SMqConsumerTopic* pConsumerTopic) {
  buf = taosDecodeStringTo(buf, pConsumerTopic->name);
  buf = taosDecodeFixedI32(buf, &pConsumerTopic->epoch);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumerTopic->pVgInfo = taosArrayInit(sz, sizeof(SMqConsumerTopic));
  for (int32_t i = 0; i < sz; i++) {
    int32_t vgInfo;
    buf = taosDecodeFixedI32(buf, &vgInfo);
    taosArrayPush(pConsumerTopic->pVgInfo, &vgInfo);
  }
  return buf;
}

typedef struct SMqConsumerObj {
  int64_t  consumerId;
  SRWLatch lock;
  char     cgroup[TSDB_CONSUMER_GROUP_LEN];
  SArray  *topics;  // SArray<SMqConsumerTopic>
  //SHashObj *topicHash; //SHashObj<SMqTopicObj>
} SMqConsumerObj;

static FORCE_INLINE int32_t tEncodeSMqConsumerObj(void** buf, const SMqConsumerObj* pConsumer) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  tlen += taosEncodeString(buf, pConsumer->cgroup);
  int32_t sz = taosArrayGetSize(pConsumer->topics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerTopic* pConsumerTopic = taosArrayGet(pConsumer->topics, i);
    tlen += tEncodeSMqConsumerTopic(buf, pConsumerTopic);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumerObj(void* buf, SMqConsumerObj* pConsumer) {
  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->topics = taosArrayInit(sz, sizeof(SMqConsumerObj));
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerTopic cTopic;
    buf = tDecodeSMqConsumerTopic(buf, &cTopic);
    taosArrayPush(pConsumer->topics, &cTopic);
  }
  return buf;
}

typedef struct SMqSubConsumerObj {
  int64_t consumerUid;  // if -1, unassigned
  SList  *vgId;         // SList<int32_t>
} SMqSubConsumerObj;

typedef struct SMqSubCGroupObj {
  char   name[TSDB_CONSUMER_GROUP_LEN];
  SList *consumers;  // SList<SMqConsumerObj>
} SMqSubCGroupObj;

typedef struct SMqSubTopicObj {
  char     name[TSDB_TOPIC_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  version;
  SRWLatch lock;
  int32_t  sqlLen;
  char    *sql;
  char    *logicalPlan;
  char    *physicalPlan;
  SList   *cgroups;  // SList<SMqSubCGroupObj>
} SMqSubTopicObj;

typedef struct SMqConsumerSubObj {
  int64_t topicUid;
  SList  *vgIds;  // SList<int64_t>
} SMqConsumerSubObj;

typedef struct SMqConsumerHbObj {
  int64_t consumerId;
  SList  *consumerSubs;  // SList<SMqConsumerSubObj>
} SMqConsumerHbObj;

typedef struct SMqVGroupSubObj {
  int64_t topicUid;
  SList  *consumerIds;  // SList<int64_t>
} SMqVGroupSubObj;

typedef struct SMqVGroupHbObj {
  int64_t vgId;
  SList  *vgSubs;  // SList<SMqVGroupSubObj>
} SMqVGroupHbObj;

#if 0
typedef struct SCGroupObj {
  char     name[TSDB_TOPIC_NAME_LEN];
  int64_t  createTime;
  int64_t  updateTime;
  uint64_t uid;
  //uint64_t dbUid;
  int32_t  version;
  SRWLatch lock;
  SList*   consumerIds;
} SCGroupObj;
#endif

typedef struct SMnodeMsg {
  char    user[TSDB_USER_LEN];
  char    db[TSDB_DB_FNAME_LEN];
  int32_t acctId;
  SMnode *pMnode;
  int64_t createdTime;
  SRpcMsg rpcMsg;
  int32_t contLen;
  void   *pCont;
} SMnodeMsg;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

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
#include "tstream.h"
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
  TRN_STAGE_ROLLBACK = 3,
  TRN_STAGE_UNDO_ACTION = 4,
  TRN_STAGE_UNDO_LOG = 5,
  TRN_STAGE_COMMIT = 6,
  TRN_STAGE_COMMIT_LOG = 7,
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
  TRN_TYPE_CONSUMER_LOST = 1022,
  TRN_TYPE_CONSUMER_RECOVER = 1023,
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
  TRN_TYPE_CREATE_SMA = 4004,
  TRN_TYPE_DROP_SMA = 4005,
  TRN_TYPE_STB_SCOPE_END,
} ETrnType;

typedef enum {
  TRN_POLICY_ROLLBACK = 0,
  TRN_POLICY_RETRY = 1,
} ETrnPolicy;

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

typedef enum {
  CONSUMER_UPDATE__TOUCH = 1,
  CONSUMER_UPDATE__ADD,
  CONSUMER_UPDATE__REMOVE,
  CONSUMER_UPDATE__LOST,
  CONSUMER_UPDATE__RECOVER,
  CONSUMER_UPDATE__MODIFY,
} ECsmUpdateType;

typedef struct {
  int32_t        id;
  ETrnStage      stage;
  ETrnPolicy     policy;
  ETrnType       type;
  int32_t        code;
  int32_t        failedTimes;
  SRpcHandleInfo rpcInfo;
  void*          rpcRsp;
  int32_t        rpcRspLen;
  SArray*        redoLogs;
  SArray*        undoLogs;
  SArray*        commitLogs;
  SArray*        redoActions;
  SArray*        undoActions;
  int64_t        createdTime;
  int64_t        lastExecTime;
  int64_t        dbUid;
  char           dbname[TSDB_DB_FNAME_LEN];
  char           lastError[TSDB_TRANS_ERROR_LEN];
  int32_t        startFunc;
  int32_t        stopFunc;
  int32_t        paramLen;
  void*          param;
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
  int64_t maxStorage;
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
  int32_t   authVersion;
  SHashObj* readDbs;
  SHashObj* writeDbs;
  SRWLatch  lock;
} SUserObj;

typedef struct {
  int32_t numOfVgroups;
  int32_t numOfStables;
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRows;
  int32_t maxRows;
  int32_t fsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  cacheLastRow;
  int8_t  hashMethod;  // default is 1
  int32_t numOfRetensions;
  SArray* pRetensions;
} SDbCfg;

typedef struct {
  char     name[TSDB_DB_FNAME_LEN];
  char     acct[TSDB_USER_LEN];
  char     createUser[TSDB_USER_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  int64_t  uid;
  int32_t  cfgVersion;
  int32_t  vgVersion;
  SDbCfg   cfg;
  SRWLatch lock;
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
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
} SVgObj;

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  char    stb[TSDB_TABLE_FNAME_LEN];
  char    db[TSDB_DB_FNAME_LEN];
  int64_t createdTime;
  int64_t uid;
  int64_t stbUid;
  int64_t dbUid;
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int8_t  timezone;
  int32_t dstVgId;  // for stream
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int32_t exprLen;  // strlen + 1
  int32_t tagsFilterLen;
  int32_t sqlLen;
  int32_t astLen;
  char*   expr;
  char*   tagsFilter;
  char*   sql;
  char*   ast;
} SSmaObj;

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  version;
  int32_t  nextColId;
  float    xFilesFactor;
  int32_t  delay;
  int32_t  ttl;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  int32_t  commentLen;
  int32_t  ast1Len;
  int32_t  ast2Len;
  SSchema* pColumns;
  SSchema* pTags;
  char*    comment;
  char*    pAst1;
  char*    pAst2;
  SRWLatch lock;
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
} SFuncObj;

typedef struct {
  int64_t        id;
  int8_t         type;
  int8_t         replica;
  int16_t        numOfColumns;
  int32_t        numOfRows;
  void*          pIter;
  SMnode*        pMnode;
  STableMetaRsp* pMeta;
  bool           sysDbRsp;
  char           db[TSDB_DB_FNAME_LEN];
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
  char    key[TSDB_PARTITION_KEY_LEN];
  int64_t dbUid;
  int64_t offset;
} SMqOffsetObj;

int32_t tEncodeSMqOffsetObj(void** buf, const SMqOffsetObj* pOffset);
void*   tDecodeSMqOffsetObj(void* buf, SMqOffsetObj* pOffset);

typedef struct {
  char           name[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  int64_t        createTime;
  int64_t        updateTime;
  int64_t        uid;
  int64_t        dbUid;
  int32_t        version;
  int8_t         subType;  // db or table
  int8_t         withTbName;
  int8_t         withSchema;
  int8_t         withTag;
  SRWLatch       lock;
  int32_t        consumerCnt;
  int32_t        sqlLen;
  int32_t        astLen;
  char*          sql;
  char*          ast;
  char*          physicalPlan;
  SSchemaWrapper schema;
  int32_t        refConsumerCnt;
} SMqTopicObj;

typedef struct {
  int64_t  consumerId;
  char     cgroup[TSDB_CGROUP_LEN];
  char     appId[TSDB_CGROUP_LEN];
  int8_t   updateType;  // used only for update
  int32_t  epoch;
  int32_t  status;
  int32_t  hbStatus;          // hbStatus is not applicable to serialization
  SRWLatch lock;              // lock is used for topics update
  SArray*  currentTopics;     // SArray<char*>
  SArray*  rebNewTopics;      // SArray<char*>
  SArray*  rebRemovedTopics;  // SArray<char*>

  // subscribed by user
  SArray* assignedTopics;  // SArray<char*>

  // data for display
  int32_t pid;
  SEpSet  ep;
  int64_t upTime;
  int64_t subscribeTime;
  int64_t rebalanceTime;
} SMqConsumerObj;

SMqConsumerObj* tNewSMqConsumerObj(int64_t consumerId, char cgroup[TSDB_CGROUP_LEN]);
void            tDeleteSMqConsumerObj(SMqConsumerObj* pConsumer);
int32_t         tEncodeSMqConsumerObj(void** buf, const SMqConsumerObj* pConsumer);
void*           tDecodeSMqConsumerObj(const void* buf, SMqConsumerObj* pConsumer);

typedef struct {
  int32_t vgId;
  char*   qmsg;
  SEpSet  epSet;
} SMqVgEp;

SMqVgEp* tCloneSMqVgEp(const SMqVgEp* pVgEp);
void     tDeleteSMqVgEp(SMqVgEp* pVgEp);
int32_t  tEncodeSMqVgEp(void** buf, const SMqVgEp* pVgEp);
void*    tDecodeSMqVgEp(const void* buf, SMqVgEp* pVgEp);

typedef struct {
  int64_t consumerId;  // -1 for unassigned
  SArray* vgs;         // SArray<SMqVgEp*>
} SMqConsumerEp;

SMqConsumerEp* tCloneSMqConsumerEp(const SMqConsumerEp* pEp);
void           tDeleteSMqConsumerEp(SMqConsumerEp* pEp);
int32_t        tEncodeSMqConsumerEp(void** buf, const SMqConsumerEp* pEp);
void*          tDecodeSMqConsumerEp(const void* buf, SMqConsumerEp* pEp);

typedef struct {
  char      key[TSDB_SUBSCRIBE_KEY_LEN];
  SRWLatch  lock;
  int64_t   dbUid;
  int32_t   vgNum;
  int8_t    subType;
  int8_t    withTbName;
  int8_t    withSchema;
  int8_t    withTag;
  SHashObj* consumerHash;   // consumerId -> SMqConsumerEp
  SArray*   unassignedVgs;  // SArray<SMqVgEp*>
} SMqSubscribeObj;

SMqSubscribeObj* tNewSubscribeObj(const char key[TSDB_SUBSCRIBE_KEY_LEN]);
SMqSubscribeObj* tCloneSubscribeObj(const SMqSubscribeObj* pSub);
void             tDeleteSubscribeObj(SMqSubscribeObj* pSub);
int32_t          tEncodeSubscribeObj(void** buf, const SMqSubscribeObj* pSub);
void*            tDecodeSubscribeObj(const void* buf, SMqSubscribeObj* pSub);

typedef struct {
  int32_t epoch;
  SArray* consumers;  // SArray<SMqConsumerEp*>
} SMqSubActionLogEntry;

SMqSubActionLogEntry* tCloneSMqSubActionLogEntry(SMqSubActionLogEntry* pEntry);
void                  tDeleteSMqSubActionLogEntry(SMqSubActionLogEntry* pEntry);
int32_t               tEncodeSMqSubActionLogEntry(void** buf, const SMqSubActionLogEntry* pEntry);
void*                 tDecodeSMqSubActionLogEntry(const void* buf, SMqSubActionLogEntry* pEntry);

typedef struct {
  char    key[TSDB_SUBSCRIBE_KEY_LEN];
  SArray* logs;  // SArray<SMqSubActionLogEntry*>
} SMqSubActionLogObj;

SMqSubActionLogObj* tCloneSMqSubActionLogObj(SMqSubActionLogObj* pLog);
void                tDeleteSMqSubActionLogObj(SMqSubActionLogObj* pLog);
int32_t             tEncodeSMqSubActionLogObj(void** buf, const SMqSubActionLogObj* pLog);
void*               tDecodeSMqSubActionLogObj(const void* buf, SMqSubActionLogObj* pLog);

typedef struct {
  int32_t           oldConsumerNum;
  const SMqRebInfo* pRebInfo;
} SMqRebInputObj;

typedef struct {
  int64_t  oldConsumerId;
  int64_t  newConsumerId;
  SMqVgEp* pVgEp;
} SMqRebOutputVg;

typedef struct {
  SArray*               rebVgs;            // SArray<SMqRebOutputVg>
  SArray*               newConsumers;      // SArray<int64_t>
  SArray*               removedConsumers;  // SArray<int64_t>
  SArray*               touchedConsumers;  // SArray<int64_t>
  SMqSubscribeObj*      pSub;
  SMqSubActionLogEntry* pLogEntry;
} SMqRebOutputObj;

typedef struct {
  char           name[TSDB_TOPIC_FNAME_LEN];
  char           sourceDb[TSDB_DB_FNAME_LEN];
  char           targetDb[TSDB_DB_FNAME_LEN];
  char           targetSTbName[TSDB_TABLE_FNAME_LEN];
  int64_t        targetStbUid;
  int64_t        createTime;
  int64_t        updateTime;
  int64_t        uid;
  int64_t        dbUid;
  int32_t        version;
  int32_t        vgNum;
  SRWLatch       lock;
  int8_t         status;
  int8_t         createdBy;      // STREAM_CREATED_BY__USER or SMA
  int32_t        fixedSinkVgId;  // 0 for shuffle
  int64_t        smaId;          // 0 for unused
  int8_t         trigger;
  int32_t        triggerParam;
  int64_t        waterMark;
  char*          sql;
  char*          physicalPlan;
  SArray*        tasks;  // SArray<SArray<SStreamTask>>
  SSchemaWrapper outputSchema;
} SStreamObj;

int32_t tEncodeSStreamObj(SEncoder* pEncoder, const SStreamObj* pObj);
int32_t tDecodeSStreamObj(SDecoder* pDecoder, SStreamObj* pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

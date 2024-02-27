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
  MND_OPER_CONNECT = 1,
  MND_OPER_CREATE_ACCT,
  MND_OPER_DROP_ACCT,
  MND_OPER_ALTER_ACCT,
  MND_OPER_CREATE_USER,
  MND_OPER_DROP_USER,
  MND_OPER_ALTER_USER,
  MND_OPER_CREATE_DNODE,
  MND_OPER_DROP_DNODE,
  MND_OPER_CONFIG_DNODE,
  MND_OPER_CREATE_MNODE,
  MND_OPER_DROP_MNODE,
  MND_OPER_CREATE_QNODE,
  MND_OPER_DROP_QNODE,
  MND_OPER_CREATE_SNODE,
  MND_OPER_DROP_SNODE,
  MND_OPER_REDISTRIBUTE_VGROUP,
  MND_OPER_MERGE_VGROUP,
  MND_OPER_SPLIT_VGROUP,
  MND_OPER_BALANCE_VGROUP,
  MND_OPER_CREATE_FUNC,
  MND_OPER_DROP_FUNC,
  MND_OPER_KILL_TRANS,
  MND_OPER_KILL_CONN,
  MND_OPER_KILL_QUERY,
  MND_OPER_CREATE_DB,
  MND_OPER_ALTER_DB,
  MND_OPER_DROP_DB,
  MND_OPER_COMPACT_DB,
  MND_OPER_TRIM_DB,
  MND_OPER_USE_DB,
  MND_OPER_WRITE_DB,
  MND_OPER_READ_DB,
  MND_OPER_READ_OR_WRITE_DB,
  MND_OPER_SHOW_VARIBALES,
  MND_OPER_SUBSCRIBE,
  MND_OPER_CREATE_TOPIC,
  MND_OPER_DROP_TOPIC,
  MND_OPER_CREATE_VIEW,
  MND_OPER_DROP_VIEW,
  MND_OPER_CONFIG_CLUSTER,
} EOperType;

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
  TRN_CONFLICT_NOTHING = 0,
  TRN_CONFLICT_GLOBAL = 1,
  TRN_CONFLICT_DB = 2,
  TRN_CONFLICT_DB_INSIDE = 3,
  TRN_CONFLICT_TOPIC = 4,
  TRN_CONFLICT_TOPIC_INSIDE = 5,
} ETrnConflct;

typedef enum {
  TRN_STAGE_PREPARE = 0,
  TRN_STAGE_REDO_ACTION = 1,
  TRN_STAGE_ROLLBACK = 2,
  TRN_STAGE_UNDO_ACTION = 3,
  TRN_STAGE_COMMIT = 4,
  TRN_STAGE_COMMIT_ACTION = 5,
  TRN_STAGE_FINISH = 6,
  TRN_STAGE_PRE_FINISH = 7
} ETrnStage;

typedef enum {
  TRN_POLICY_ROLLBACK = 0,
  TRN_POLICY_RETRY = 1,
} ETrnPolicy;

typedef enum {
  TRN_EXEC_PARALLEL = 0,
  TRN_EXEC_SERIAL = 1,
} ETrnExec;

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
  DND_REASON_TTL_CHANGE_ON_WRITE_NOT_MATCH,
  DND_REASON_ENABLE_WHITELIST_NOT_MATCH,
  DND_REASON_OTHERS
} EDndReason;

typedef enum {
  CONSUMER_UPDATE_REB = 1,  // update after rebalance
  CONSUMER_ADD_REB,         // add    after rebalance
  CONSUMER_REMOVE_REB,      // remove after rebalance
  CONSUMER_UPDATE_REC,      // update after recover
  CONSUMER_UPDATE_SUB,      // update after subscribe req
} ECsmUpdateType;

typedef struct {
  int32_t       id;
  ETrnStage     stage;
  ETrnPolicy    policy;
  ETrnConflct   conflict;
  ETrnExec      exec;
  EOperType     oper;
  int32_t       code;
  int32_t       failedTimes;
  void*         rpcRsp;
  int32_t       rpcRspLen;
  int32_t       redoActionPos;
  SArray*       prepareActions;
  SArray*       redoActions;
  SArray*       undoActions;
  SArray*       commitActions;
  int64_t       createdTime;
  int64_t       lastExecTime;
  int32_t       lastAction;
  int32_t       lastErrorNo;
  SEpSet        lastEpset;
  tmsg_t        lastMsgType;
  tmsg_t        originRpcType;
  char          dbname[TSDB_TABLE_FNAME_LEN];
  char          stbname[TSDB_TABLE_FNAME_LEN];
  int32_t       startFunc;
  int32_t       stopFunc;
  int32_t       paramLen;
  void*         param;
  char          opername[TSDB_TRANS_OPER_LEN];
  SArray*       pRpcArray;
  SRWLatch      lockRpcArray;
  int64_t       mTraceId;
  TdThreadMutex mutex;
} STrans;

typedef struct {
  int64_t id;
  char    name[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int64_t updateTime;
  int32_t upTime;
} SClusterObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  int64_t    rebootTime;
  int64_t    lastAccessTime;
  int32_t    accessTimes;
  int32_t    numOfVnodes;
  int32_t    numOfOtherNodes;
  int32_t    numOfSupportVnodes;
  int32_t    numOfDiskCfg;
  float      numOfCores;
  int64_t    memTotal;
  int64_t    memAvail;
  int64_t    memUsed;
  EDndReason offlineReason;
  uint16_t   port;
  char       fqdn[TSDB_FQDN_LEN];
  char       ep[TSDB_EP_LEN];
  char       active[TSDB_ACTIVE_KEY_LEN];
  char       connActive[TSDB_CONN_ACTIVE_KEY_LEN];
  char       machineId[TSDB_MACHINE_ID_LEN + 1];
} SDnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  ESyncState syncState;
  SyncTerm   syncTerm;
  bool       syncRestore;
  int64_t    roleTimeMs;
  SDnodeObj* pDnode;
  int32_t    role;
  SyncIndex  lastIndex;
} SMnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj* pDnode;
  SQnodeLoad load;
} SQnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  SDnodeObj* pDnode;
} SSnodeObj;

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
  char          user[TSDB_USER_LEN];
  char          pass[TSDB_PASSWORD_LEN];
  char          acct[TSDB_USER_LEN];
  int64_t       createdTime;
  int64_t       updateTime;
  int8_t        superUser;
  int8_t        sysInfo;
  int8_t        enable;
  int8_t        reserve;
  int32_t       acctId;
  int32_t       authVersion;
  int32_t       passVersion;
  int64_t       ipWhiteListVer;
  SIpWhiteList* pIpWhiteList;

  SHashObj* readDbs;
  SHashObj* writeDbs;
  SHashObj* topics;
  SHashObj* readTbs;
  SHashObj* writeTbs;
  SHashObj* alterTbs;
  SHashObj* readViews;
  SHashObj* writeViews;
  SHashObj* alterViews;
  SHashObj* useDbs;
  SRWLatch  lock;
} SUserObj;

typedef struct {
  int32_t numOfVgroups;
  int32_t numOfStables;
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t keepTimeOffset;
  int32_t minRows;
  int32_t maxRows;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  hashMethod;  // default is 1
  int8_t  cacheLast;
  int8_t  schemaless;
  int16_t hashPrefix;
  int16_t hashSuffix;
  int16_t sstTrigger;
  int32_t tsdbPageSize;
  int32_t numOfRetensions;
  SArray* pRetensions;
  int32_t walRetentionPeriod;
  int32_t walRollPeriod;
  int64_t walRetentionSize;
  int64_t walSegmentSize;
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
  int64_t  stateTs;
  int64_t  compactStartTime;
} SDbObj;

typedef struct {
  int32_t    dnodeId;
  ESyncState syncState;
  int64_t    syncTerm;
  bool       syncRestore;
  bool       syncCanRead;
  int64_t    roleTimeMs;
  int64_t    startTimeMs;
  ESyncRole  nodeRole;
  int32_t    learnerProgress;
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
  int64_t   cacheUsage;
  int64_t   numOfTables;
  int64_t   numOfTimeSeries;
  int64_t   totalStorage;
  int64_t   compStorage;
  int64_t   pointsWritten;
  int8_t    compact;
  int8_t    isTsma;
  int8_t    replica;
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  void*     pTsma;
  int32_t   numOfCachedTables;
  int32_t   syncConfChangeVer;
} SVgObj;

typedef struct {
  char           name[TSDB_TABLE_FNAME_LEN];
  char           stb[TSDB_TABLE_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  char           dstTbName[TSDB_TABLE_FNAME_LEN];
  int64_t        createdTime;
  int64_t        uid;
  int64_t        stbUid;
  int64_t        dbUid;
  int64_t        dstTbUid;
  int8_t         intervalUnit;
  int8_t         slidingUnit;
  int8_t         timezone;
  int32_t        dstVgId;  // for stream
  int64_t        interval;
  int64_t        offset;
  int64_t        sliding;
  int32_t        exprLen;  // strlen + 1
  int32_t        tagsFilterLen;
  int32_t        sqlLen;
  int32_t        astLen;
  char*          expr;
  char*          tagsFilter;
  char*          sql;
  char*          ast;
  SSchemaWrapper schemaRow;  // for dstVgroup
  SSchemaWrapper schemaTag;  // for dstVgroup
} SSmaObj;

typedef struct {
  char    name[TSDB_INDEX_FNAME_LEN];
  char    stb[TSDB_TABLE_FNAME_LEN];
  char    db[TSDB_DB_FNAME_LEN];
  char    dstTbName[TSDB_TABLE_FNAME_LEN];
  char    colName[TSDB_COL_NAME_LEN];
  int64_t createdTime;
  int64_t uid;
  int64_t stbUid;
  int64_t dbUid;
} SIdxObj;

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  createdTime;
  int64_t  updateTime;
  int64_t  uid;
  int64_t  dbUid;
  int32_t  tagVer;
  int32_t  colVer;
  int32_t  smaVer;
  int32_t  nextColId;
  int64_t  maxdelay[2];
  int64_t  watermark[2];
  int32_t  ttl;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  int32_t  numOfFuncs;
  int32_t  commentLen;
  int32_t  ast1Len;
  int32_t  ast2Len;
  SArray*  pFuncs;
  SSchema* pColumns;
  SSchema* pTags;
  char*    comment;
  char*    pAst1;
  char*    pAst2;
  SRWLatch lock;
  int8_t   source;
} SStbObj;

typedef struct {
  char     name[TSDB_FUNC_NAME_LEN];
  int64_t  createdTime;
  int8_t   funcType;
  int8_t   scriptType;
  int8_t   align;
  int8_t   outputType;
  int32_t  outputLen;
  int32_t  bufSize;
  int64_t  signature;
  int32_t  commentSize;
  int32_t  codeSize;
  char*    pComment;
  char*    pCode;
  int32_t  funcVersion;
  SRWLatch lock;
} SFuncObj;

typedef struct {
  int64_t        id;
  int8_t         type;
  int8_t         replica;
  int16_t        numOfColumns;
  int32_t        numOfRows;
  int32_t        curIterPackedRows;
  void*          pIter;
  SMnode*        pMnode;
  STableMetaRsp* pMeta;
  bool           restore;
  bool           sysDbRsp;
  char           db[TSDB_DB_FNAME_LEN];
  char           filterTb[TSDB_TABLE_NAME_LEN];
} SShowObj;

typedef struct {
  char           name[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  char           createUser[TSDB_USER_LEN];
  int64_t        createTime;
  int64_t        updateTime;
  int64_t        uid;
  int64_t        dbUid;
  int32_t        version;
  int8_t         subType;   // column, db or stable
  int8_t         withMeta;  // TODO
  SRWLatch       lock;
  int32_t        sqlLen;
  int32_t        astLen;
  char*          sql;
  char*          ast;
  char*          physicalPlan;
  SSchemaWrapper schema;
  int64_t        stbUid;
  char           stbName[TSDB_TABLE_FNAME_LEN];
  // forbid condition
  int64_t ntbUid;
  SArray* ntbColIds;
  int64_t ctbStbUid;
} SMqTopicObj;

typedef struct {
  int64_t  consumerId;
  char     cgroup[TSDB_CGROUP_LEN];
  char     clientId[256];
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
  int64_t createTime;
  int64_t subscribeTime;
  int64_t rebalanceTime;

  int8_t  withTbName;
  int8_t  autoCommit;
  int32_t autoCommitInterval;
  int32_t resetOffsetCfg;
} SMqConsumerObj;

SMqConsumerObj* tNewSMqConsumerObj(int64_t consumerId, char cgroup[TSDB_CGROUP_LEN]);
void            tDeleteSMqConsumerObj(SMqConsumerObj* pConsumer, bool isDeleted);
int32_t         tEncodeSMqConsumerObj(void** buf, const SMqConsumerObj* pConsumer);
void*           tDecodeSMqConsumerObj(const void* buf, SMqConsumerObj* pConsumer, int8_t sver);

typedef struct {
  int32_t vgId;
  //  char*   qmsg;  // SubPlanToString
  SEpSet epSet;
} SMqVgEp;

SMqVgEp* tCloneSMqVgEp(const SMqVgEp* pVgEp);
void     tDeleteSMqVgEp(SMqVgEp* pVgEp);
int32_t  tEncodeSMqVgEp(void** buf, const SMqVgEp* pVgEp);
void*    tDecodeSMqVgEp(const void* buf, SMqVgEp* pVgEp, int8_t sver);

typedef struct {
  int64_t consumerId;  // -1 for unassigned
  SArray* vgs;         // SArray<SMqVgEp*>
  SArray* offsetRows;  // SArray<OffsetRows*>
} SMqConsumerEp;

// SMqConsumerEp* tCloneSMqConsumerEp(const SMqConsumerEp* pEp);
// void           tDeleteSMqConsumerEp(void* pEp);
int32_t tEncodeSMqConsumerEp(void** buf, const SMqConsumerEp* pEp);
void*   tDecodeSMqConsumerEp(const void* buf, SMqConsumerEp* pEp, int8_t sver);

typedef struct {
  char      key[TSDB_SUBSCRIBE_KEY_LEN];
  SRWLatch  lock;
  int64_t   dbUid;
  int32_t   vgNum;
  int8_t    subType;
  int8_t    withMeta;
  int64_t   stbUid;
  SHashObj* consumerHash;   // consumerId -> SMqConsumerEp
  SArray*   unassignedVgs;  // SArray<SMqVgEp*>
  SArray*   offsetRows;
  char      dbName[TSDB_DB_FNAME_LEN];
  char*     qmsg;  // SubPlanToString
} SMqSubscribeObj;

SMqSubscribeObj* tNewSubscribeObj(const char key[TSDB_SUBSCRIBE_KEY_LEN]);
SMqSubscribeObj* tCloneSubscribeObj(const SMqSubscribeObj* pSub);
void             tDeleteSubscribeObj(SMqSubscribeObj* pSub);
int32_t          tEncodeSubscribeObj(void** buf, const SMqSubscribeObj* pSub);
void*            tDecodeSubscribeObj(const void* buf, SMqSubscribeObj* pSub, int8_t sver);

// typedef struct {
//   int32_t epoch;
//   SArray* consumers;  // SArray<SMqConsumerEp*>
// } SMqSubActionLogEntry;

// SMqSubActionLogEntry* tCloneSMqSubActionLogEntry(SMqSubActionLogEntry* pEntry);
// void                  tDeleteSMqSubActionLogEntry(SMqSubActionLogEntry* pEntry);
// int32_t               tEncodeSMqSubActionLogEntry(void** buf, const SMqSubActionLogEntry* pEntry);
// void*                 tDecodeSMqSubActionLogEntry(const void* buf, SMqSubActionLogEntry* pEntry);
//
// typedef struct {
//   char    key[TSDB_SUBSCRIBE_KEY_LEN];
//   SArray* logs;  // SArray<SMqSubActionLogEntry*>
// } SMqSubActionLogObj;
//
// SMqSubActionLogObj* tCloneSMqSubActionLogObj(SMqSubActionLogObj* pLog);
// void                tDeleteSMqSubActionLogObj(SMqSubActionLogObj* pLog);
// int32_t             tEncodeSMqSubActionLogObj(void** buf, const SMqSubActionLogObj* pLog);
// void*               tDecodeSMqSubActionLogObj(const void* buf, SMqSubActionLogObj* pLog);

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
  SArray*          rebVgs;            // SArray<SMqRebOutputVg>
  SArray*          newConsumers;      // SArray<int64_t>
  SArray*          removedConsumers;  // SArray<int64_t>
  SArray*          modifyConsumers;   // SArray<int64_t>
  SMqSubscribeObj* pSub;
  //  SMqSubActionLogEntry* pLogEntry;
} SMqRebOutputObj;

typedef struct SStreamConf {
  int8_t  igExpired;
  int8_t  trigger;
  int8_t  fillHistory;
  int64_t triggerParam;
  int64_t watermark;
} SStreamConf;

typedef struct {
  char     name[TSDB_STREAM_FNAME_LEN];
  SRWLatch lock;

  // create info
  int64_t createTime;
  int64_t updateTime;
  int32_t version;
  int32_t totalLevel;
  int64_t smaId;  // 0 for unused
  // info
  int64_t     uid;
  int8_t      status;
  SStreamConf conf;
  // source and target
  int64_t sourceDbUid;
  int64_t targetDbUid;
  char    sourceDb[TSDB_DB_FNAME_LEN];
  char    targetDb[TSDB_DB_FNAME_LEN];
  char    targetSTbName[TSDB_TABLE_FNAME_LEN];
  int64_t targetStbUid;

  // fixedSinkVg is not applicable for encode and decode
  SVgObj  fixedSinkVg;
  int32_t fixedSinkVgId;  // 0 for shuffle

  // transformation
  char*   sql;
  char*   ast;
  char*   physicalPlan;
  SArray* tasks;  // SArray<SArray<SStreamTask>>

  SArray* pHTasksList;  // generate the results for already stored ts data
  int64_t hTaskUid;     // stream task for history ts data

  SSchemaWrapper outputSchema;
  SSchemaWrapper tagSchema;

  // 3.0.20
  int64_t checkpointFreq;  // ms
  int64_t currentTick;     // do not serialize
  int64_t deleteMark;
  int8_t  igCheckUpdate;

  // 3.0.5.
  int64_t checkpointId;

  int32_t indexForMultiAggBalance;
  int8_t  subTableWithoutMd5;
  char    reserve[256];

} SStreamObj;

typedef struct SStreamSeq {
  char     name[24];
  uint64_t seq;
  SRWLatch lock;
} SStreamSeq;
int32_t tEncodeSStreamObj(SEncoder* pEncoder, const SStreamObj* pObj);
int32_t tDecodeSStreamObj(SDecoder* pDecoder, SStreamObj* pObj, int32_t sver);
void    tFreeStreamObj(SStreamObj* pObj);

#define VIEW_TYPE_UPDATABLE    (1 << 0)
#define VIEW_TYPE_MATERIALIZED (1 << 1)

typedef struct {
  char     fullname[TSDB_VIEW_FNAME_LEN];
  char     name[TSDB_VIEW_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     user[TSDB_USER_LEN];
  char*    querySql;
  char*    parameters;
  void**   defaultValues;
  char*    targetTable;
  uint64_t viewId;
  uint64_t dbId;
  int64_t  createdTime;
  int32_t  version;
  int8_t   precision;
  int8_t   type;
  int32_t  numOfCols;
  SSchema* pSchema;
  SRWLatch lock;
} SViewObj;

int32_t tEncodeSViewObj(SEncoder* pEncoder, const SViewObj* pObj);
int32_t tDecodeSViewObj(SDecoder* pDecoder, SViewObj* pObj, int32_t sver);
void    tFreeSViewObj(SViewObj* pObj);

typedef struct {
  int32_t compactDetailId;
  int32_t compactId;
  int32_t vgId;
  int32_t dnodeId;
  int32_t numberFileset;
  int32_t finished;
  int64_t startTime;
  int32_t newNumberFileset;
  int32_t newFinished;
} SCompactDetailObj;

typedef struct {
  int32_t compactId;
  char    dbname[TSDB_TABLE_FNAME_LEN];
  int64_t startTime;
  SArray* compactDetail;
} SCompactObj;

// SGrantLogObj
typedef enum {
  GRANT_STATE_INIT = 0,
  GRANT_STATE_UNGRANTED = 1,
  GRANT_STATE_GRANTED = 2,
  GRANT_STATE_EXPIRED = 3,
  GRANT_STATE_REVOKED = 4,
  GRANT_STATE_MAX,
} EGrantState;

typedef enum {
  GRANT_STATE_REASON_INIT = 0,
  GRANT_STATE_REASON_ALTER = 1,     // alter activeCode 'revoked' or 'xxx'
  GRANT_STATE_REASON_MISMATCH = 2,  // dnode machine mismatch
  GRANT_STATE_REASON_EXPIRE = 3,    // expire
  GRANT_STATE_REASON_MAX,
} EGrantStateReason;

#define GRANT_STATE_NUM       30
#define GRANT_ACTIVE_NUM      10
#define GRANT_ACTIVE_HEAD_LEN 30

typedef struct {
  union {
    int64_t u0;
    struct {
      int64_t ts : 40;
      int64_t lastState : 4;
      int64_t state : 4;
      int64_t reason : 8;
      int64_t reserve : 8;
    };
  };
} SGrantState;

typedef struct {
  union {
    int64_t u0;
    struct {
      int64_t ts : 40;
      int64_t reserve : 24;
    };
  };
  char active[GRANT_ACTIVE_HEAD_LEN + 1];
} SGrantActive;

typedef struct {
  union {
    int64_t u0;
    struct {
      int64_t ts : 40;
      int64_t id : 24;
    };
  };
  char machine[TSDB_MACHINE_ID_LEN + 1];
} SGrantMachine;

typedef struct {
  int32_t      id;
  int8_t       nStates;
  int8_t       nActives;
  int64_t      createTime;
  int64_t      updateTime;
  int64_t      upgradeTime;
  SGrantState  states[GRANT_STATE_NUM];
  SGrantActive actives[GRANT_ACTIVE_NUM];
  char*        active;
  SArray*      pMachines;  // SGrantMachine
  SRWLatch     lock;
} SGrantLogObj;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

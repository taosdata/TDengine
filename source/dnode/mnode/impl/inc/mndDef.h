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
} ETrnConflct;

typedef enum {
  TRN_STAGE_PREPARE = 0,
  TRN_STAGE_REDO_ACTION = 1,
  TRN_STAGE_ROLLBACK = 2,
  TRN_STAGE_UNDO_ACTION = 3,
  TRN_STAGE_COMMIT = 4,
  TRN_STAGE_COMMIT_ACTION = 5,
  TRN_STAGE_FINISHED = 6,
  TRN_STAGE_PRE_FINISH = 7
} ETrnStage;

typedef enum {
  TRN_POLICY_ROLLBACK = 0,
  TRN_POLICY_RETRY = 1,
} ETrnPolicy;

typedef enum {
  TRN_EXEC_PRARLLEL = 0,
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
  int32_t     id;
  ETrnStage   stage;
  ETrnPolicy  policy;
  ETrnConflct conflict;
  ETrnExec    exec;
  EOperType   oper;
  int32_t     code;
  int32_t     failedTimes;
  void*       rpcRsp;
  int32_t     rpcRspLen;
  int32_t     redoActionPos;
  SArray*     redoActions;
  SArray*     undoActions;
  SArray*     commitActions;
  int64_t     createdTime;
  int64_t     lastExecTime;
  int32_t     lastAction;
  int32_t     lastErrorNo;
  SEpSet      lastEpset;
  tmsg_t      lastMsgType;
  tmsg_t      originRpcType;
  char        dbname[TSDB_TABLE_FNAME_LEN];
  char        stbname[TSDB_TABLE_FNAME_LEN];
  int32_t     startFunc;
  int32_t     stopFunc;
  int32_t     paramLen;
  void*       param;
  char        opername[TSDB_TRANS_OPER_LEN];
  SArray*     pRpcArray;
  SRWLatch    lockRpcArray;
  int64_t     mTraceId;
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
  float      numOfCores;
  int64_t    memTotal;
  int64_t    memAvail;
  int64_t    memUsed;
  EDndReason offlineReason;
  uint16_t   port;
  char       fqdn[TSDB_FQDN_LEN];
  char       ep[TSDB_EP_LEN];
} SDnodeObj;

typedef struct {
  int32_t    id;
  int64_t    createdTime;
  int64_t    updateTime;
  ESyncState syncState;
  bool       syncRestore;
  int64_t    stateStartTime;
  SDnodeObj* pDnode;
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
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_PASSWORD_LEN];
  char      acct[TSDB_USER_LEN];
  int64_t   createdTime;
  int64_t   updateTime;
  int8_t    superUser;
  int8_t    sysInfo;
  int8_t    enable;
  int8_t    reserve;
  int32_t   acctId;
  int32_t   authVersion;
  SHashObj* readDbs;
  SHashObj* writeDbs;
  SHashObj* topics;
  SHashObj* readTbs;
  SHashObj* writeTbs;
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
  bool       syncRestore;
  bool       syncCanRead;
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
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA];
  void*     pTsma;
  int32_t   numOfCachedTables;
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
  int32_t funcVersion;
  SRWLatch lock;
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
  bool           restore;
  bool           sysDbRsp;
  char           db[TSDB_DB_FNAME_LEN];
  char           filterTb[TSDB_TABLE_NAME_LEN];
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
  char*   qmsg;  // SubPlanToString
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
void           tDeleteSMqConsumerEp(void* pEp);
int32_t        tEncodeSMqConsumerEp(void** buf, const SMqConsumerEp* pEp);
void*          tDecodeSMqConsumerEp(const void* buf, SMqConsumerEp* pEp);

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
  char      dbName[TSDB_DB_FNAME_LEN];
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
  char name[TSDB_STREAM_FNAME_LEN];
  // ctl
  SRWLatch lock;
  // create info
  int64_t createTime;
  int64_t updateTime;
  int32_t version;
  int32_t totalLevel;
  int64_t smaId;  // 0 for unused
  // info
  int64_t uid;
  int8_t  status;
  // config
  int8_t  igExpired;
  int8_t  trigger;
  int8_t  fillHistory;
  int64_t triggerParam;
  int64_t watermark;
  // source and target
  int64_t sourceDbUid;
  int64_t targetDbUid;
  char    sourceDb[TSDB_DB_FNAME_LEN];
  char    targetDb[TSDB_DB_FNAME_LEN];
  char    targetSTbName[TSDB_TABLE_FNAME_LEN];
  int64_t targetStbUid;
  int32_t fixedSinkVgId;  // 0 for shuffle
  // fixedSinkVg is not applicable for encode and decode
  SVgObj fixedSinkVg;

  // transformation
  char*          sql;
  char*          ast;
  char*          physicalPlan;
  SArray*        tasks;  // SArray<SArray<SStreamTask>>
  SSchemaWrapper outputSchema;
  SSchemaWrapper tagSchema;

  // 3.0.20
  int64_t checkpointFreq;  // ms
  int64_t currentTick;     // do not serialize
  int64_t deleteMark;
  int8_t  igCheckUpdate;
} SStreamObj;

int32_t tEncodeSStreamObj(SEncoder* pEncoder, const SStreamObj* pObj);
int32_t tDecodeSStreamObj(SDecoder* pDecoder, SStreamObj* pObj, int32_t sver);
void    tFreeStreamObj(SStreamObj* pObj);

typedef struct {
  char    streamName[TSDB_STREAM_FNAME_LEN];
  int64_t uid;
  int64_t streamUid;
  SArray* childInfo;  // SArray<SStreamChildEpInfo>
} SStreamCheckpointObj;

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DEF_H_*/

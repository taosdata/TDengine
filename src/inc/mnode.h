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

#ifndef TDENGINE_MGMT_H
#define TDENGINE_MGMT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "taosdef.h"
#include "taosmsg.h"
#include "taoserror.h"


#include "sdb.h"
#include "tglobalcfg.h"
#include "thash.h"
#include "tidpool.h"
#include "tlog.h"
#include "tmempool.h"
#include "trpc.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

// internal globals
extern char  version[];
extern void *mgmtTmr;
extern void *mgmtQhandle;
extern void *mgmtTranQhandle;
extern int   mgmtShellConns;
extern int   mgmtDnodeConns;
extern char  mgmtDirectory[];

extern int tsAcctUpdateSize;
extern int tsDbUpdateSize;
extern int tsDnodeUpdateSize;
extern int tsMnodeUpdateSize;
extern int tsUserUpdateSize;
extern int tsVgUpdateSize;

typedef struct {
  uint32_t   privateIp;
  int32_t    sid;
  uint32_t   moduleStatus;
  int32_t    openVnodes;
  int32_t    numOfVnodes;
  int32_t    numOfFreeVnodes;
  int64_t    createdTime;
  uint32_t   publicIp;
  int32_t    status;
  uint32_t   lastAccess;
  uint32_t   rebootTimes;
  uint32_t   lastReboot;       // time stamp for last reboot
  uint16_t   numOfCores;       // from dnode status msg
  uint8_t    alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  uint8_t    reserveStatus;
  uint16_t   numOfTotalVnodes; // from dnode status msg, config information
  uint16_t   unused;
  float      diskAvailable;    // from dnode status msg
  int32_t    bandwidthMb;      // config by user
  int16_t    cpuAvgUsage;      // calc from sys.cpu
  int16_t    memoryAvgUsage;   // calc from sys.mem
  int16_t    diskAvgUsage;     // calc from sys.disk
  int16_t    bandwidthUsage;   // calc from sys.band
  uint32_t   rack;
  uint16_t   idc;
  uint16_t   slot;
  int32_t    customScore;     // config by user
  float      lbScore;         // calc in balance function
  int16_t    lbStatus;         // set in balance function
  int16_t    lastAllocVnode;  // increase while create vnode
  SVnodeLoad vload[TSDB_MAX_VNODES];
  char       reserved[16];
  char       updateEnd[1];
  void *     thandle;
} SDnodeObj;

typedef struct {
  uint32_t ip;
  uint32_t publicIp;
  int32_t  vnode;
} SVnodeGid;

typedef struct {
  int32_t sid;
  int32_t vgId;  // vnode group ID
} STableGid;

typedef struct _tab_obj {
  char      meterId[TSDB_TABLE_ID_LEN + 1];
  uint64_t  uid;
  STableGid gid;

  int32_t sversion;     // schema version
  int64_t createdTime;
  int32_t numOfTags;    // for metric
  int32_t numOfMeters;  // for metric
  int32_t numOfColumns;
  int32_t schemaSize;
  short   nextColId;
  char    tableType : 4;
  char    status : 3;
  char    isDirty : 1;  // if the table change tag column 1 value
  char    reserved[15];
  char    updateEnd[1];

  pthread_rwlock_t rwLock;
  tSkipList *      pSkipList;
  struct _tab_obj *pHead;  // for metric, a link list for all meters created
                           // according to this metric
  char *pTagData;          // TSDB_TABLE_ID_LEN(metric_name)+
                           // tags_value1/tags_value2/tags_value3
  struct _tab_obj *prev, *next;
  char *           pSql;   // pointer to SQL, for SC, null-terminated string
  char *           pReserve1;
  char *           pReserve2;
  char *           schema;
  // SSchema    schema[];
} STabObj;


typedef struct SSuperTableObj {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int32_t  sversion;
  int32_t  numOfTags;
  int32_t  numOfMeters;
  int32_t  numOfColumns;
  int32_t  schemaSize;
  int8_t   reserved[7];
  int8_t   updateEnd[1];


  pthread_rwlock_t rwLock;
  struct SSuperTableObj *prev, *next;
  int16_t  nextColId;

  int8_t   *schema;
} SSuperTableObj;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  char     superTableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int8_t   reserved[7];
  int8_t   updateEnd[1];
  SSuperTableObj *superTable;
} SChildTableObj;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int32_t  sversion;
  int32_t  numOfColumns;
  int32_t  schemaSize;
  int8_t     reserved[3];
  int8_t     updateEnd[1];
  int16_t  nextColId;
  char*    schema;
} SNormalTableObj;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int32_t  sversion;
  int32_t  numOfColumns;
  int32_t  schemaSize;
  int16_t  sqlLen;
  int8_t     reserved[3];
  int8_t     updateEnd[1];
  int16_t  nextColId;
  char*    pSql;  //null-terminated string
  char*    schema;
} SStreamTableObj;


typedef struct _vg_obj {
  uint32_t        vgId;
  char            dbName[TSDB_DB_NAME_LEN];
  int64_t         createdTime;
  uint64_t        lastCreate;
  uint64_t        lastRemove;
  int32_t         numOfVnodes;
  SVnodeGid       vnodeGid[TSDB_VNODES_SUPPORT];
  int32_t         numOfMeters;
  int32_t         lbIp;
  int32_t         lbTime;
  int8_t          lbStatus;
  int8_t            reserved[16];
  int8_t            updateEnd[1];
  struct _vg_obj *prev, *next;
  void *          idPool;
  STabObj **      meterList;
} SVgObj;

typedef struct _db_obj {
  /*
   * this length will cause the storage structure to change, rollback
   */
  char    name[TSDB_DB_NAME_LEN + 1];
  int64_t createdTime;
  SDbCfg  cfg;
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfMetrics;
  uint8_t vgStatus;
  uint8_t dropStatus;
  char    reserved[16];
  char    updateEnd[1];

  STabObj *       pMetric;
  struct _db_obj *prev, *next;
  SVgObj *        pHead;  // empty vgroup first
  SVgObj *        pTail;  // empty vgroup end
  void *          vgTimer;
} SDbObj;

typedef struct _user_obj {
  char              user[TSDB_USER_LEN + 1];
  char              pass[TSDB_KEY_LEN];
  char              acct[TSDB_USER_LEN];
  int64_t           createdTime;
  char              superAuth : 1;
  char              writeAuth : 1;
  char              reserved[16];
  char              updateEnd[1];
  struct _user_obj *prev, *next;
} SUserObj;

typedef struct {
  int32_t numOfUsers;
  int32_t numOfDbs;
  int32_t numOfTimeSeries;
  int32_t numOfPointsPerSecond;
  int32_t numOfConns;
  int32_t numOfQueries;
  int32_t numOfStreams;
  int64_t totalStorage;  // Total storage wrtten from this account
  int64_t compStorage;   // Compressed storage on disk
  int64_t queryTime;
  int64_t totalPoints;
  int64_t inblound;
  int64_t outbound;
  TSKEY   sKey;
  char    accessState;  // Checked by mgmt heartbeat message
} SAcctInfo;

typedef struct {
  char      user[TSDB_USER_LEN + 1];
  char      pass[TSDB_KEY_LEN];
  SAcctCfg  cfg;
  int32_t   acctId;
  int64_t   createdTime;
  char      reserved[15];
  char      updateEnd[1];
  SAcctInfo acctInfo;

  SDbObj *         pHead;
  SUserObj *       pUser;
  struct _connObj *pConn;
  pthread_mutex_t  mutex;
} SAcctObj;

typedef struct _connObj {
  SAcctObj *       pAcct;
  SDbObj *         pDb;
  SUserObj *       pUser;
  char             user[TSDB_USER_LEN];
  uint64_t         stime;               // login time
  char             superAuth : 1;       // super user flag
  char             writeAuth : 1;       // write flag
  char             killConnection : 1;  // kill the connection flag
  uint8_t          usePublicIp : 1;     // if the connection request is publicIp
  uint8_t          reserved : 4;
  uint32_t         queryId;             // query ID to be killed
  uint32_t         streamId;            // stream ID to be killed
  uint32_t         ip;                  // shell IP
  uint16_t         port;                // shell port
  void *           thandle;
  SQList *         pQList;  // query list
  SSList *         pSList;  // stream list
  uint64_t         qhandle;
  struct _connObj *prev, *next;
} SConnObj;

typedef struct {
  char spi;
  char encrypt;
  char secret[TSDB_KEY_LEN];
  char cipheringKey[TSDB_KEY_LEN];
} SSecInfo;

typedef struct {
  char     type;
  void *   pNode;
  short    numOfColumns;
  int      rowSize;
  int      numOfRows;
  int      numOfReads;
  short    offset[TSDB_MAX_COLUMNS];
  short    bytes[TSDB_MAX_COLUMNS];
  void *   signature;
  uint16_t payloadLen; /* length of payload*/
  char     payload[];  /* payload for wildcard match in show tables */
} SShowObj;


#ifdef __cplusplus
}
#endif

#endif

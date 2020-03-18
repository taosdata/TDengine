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
#include "taosdef.h"
#include "tskiplist.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

typedef struct {
  int32_t    dnodeId;
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
  int32_t  dnodeId;
  uint32_t ip;
  uint32_t publicIp;
  int32_t  vnode;
} SVnodeGid;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t   type;
  int8_t   dirty;
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
} STableInfo;

struct _vg_obj;

typedef struct SSuperTableObj {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t   type;
  int8_t   dirty;
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int32_t  sversion;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  int8_t   reserved[5];
  int8_t   updateEnd[1];
  int32_t  numOfTables;
  int16_t  nextColId;
  SSchema *schema;
} SSuperTableObj;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t   type;
  int8_t   dirty;
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  char     superTableId[TSDB_TABLE_ID_LEN + 1];
  int8_t   reserved[1];
  int8_t   updateEnd[1];
  SSuperTableObj *superTable;
} SChildTableObj;

typedef struct {
  char     tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t   type;
  int8_t   dirty;
  uint64_t uid;
  int32_t  sid;
  int32_t  vgId;
  int64_t  createdTime;
  int32_t  sversion;
  int32_t  numOfColumns;
  int32_t  sqlLen;
  int8_t   reserved[3];
  int8_t   updateEnd[1];
  char*    sql;  //null-terminated string
  int16_t  nextColId;
  SSchema* schema;
} SNormalTableObj;

typedef struct _vg_obj {
  uint32_t        vgId;
  char            dbName[TSDB_DB_NAME_LEN + 1];
  int64_t         createdTime;
  uint64_t        lastCreate;
  uint64_t        lastRemove;
  int32_t         numOfVnodes;
  SVnodeGid       vnodeGid[TSDB_VNODES_SUPPORT];
  int32_t         numOfTables;
  int32_t         lbIp;
  int32_t         lbTime;
  int8_t          lbStatus;
  int8_t          reserved[16];
  int8_t          updateEnd[1];
  struct _vg_obj *prev, *next;
  void *          idPool;
  STableInfo **   tableList;
} SVgObj;

typedef struct _db_obj {
  char    name[TSDB_DB_NAME_LEN + 1];
  int8_t  dirty;
  int64_t createdTime;
  SDbCfg  cfg;
  int8_t  dropStatus;
  char    reserved[16];
  char    updateEnd[1];
  struct _db_obj *prev, *next;
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfSuperTables;
  SVgObj *pHead;
  SVgObj *pTail;
} SDbObj;

struct _acctObj;

typedef struct _user_obj {
  char              user[TSDB_USER_LEN + 1];
  char              pass[TSDB_KEY_LEN + 1];
  char              acct[TSDB_USER_LEN + 1];
  int64_t           createdTime;
  int8_t            superAuth;
  int8_t            writeAuth;
  int8_t            reserved[16];
  int8_t            updateEnd[1];
  struct _user_obj *prev, *next;
  struct _acctObj * pAcct;
  SQqueryList *     pQList;  // query list
  SStreamList *     pSList;  // stream list
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
  int64_t sKey;
  int8_t  accessState;   // Checked by mgmt heartbeat message
} SAcctInfo;

typedef struct _acctObj {
  char      user[TSDB_USER_LEN + 1];
  char      pass[TSDB_KEY_LEN + 1];
  SAcctCfg  cfg;
  int32_t   acctId;
  int64_t   createdTime;
  int8_t    reserved[15];
  int8_t    updateEnd[1];
  SAcctInfo acctInfo;
  SDbObj *         pHead;
  SUserObj *       pUser;
  pthread_mutex_t  mutex;
} SAcctObj;

typedef struct {
  int8_t   type;
  char     db[TSDB_DB_NAME_LEN + 1];
  void *   pNode;
  int16_t  numOfColumns;
  int32_t  rowSize;
  int32_t  numOfRows;
  int32_t  numOfReads;
  int16_t  offset[TSDB_MAX_COLUMNS];
  int16_t  bytes[TSDB_MAX_COLUMNS];
  void *   signature;
  uint16_t payloadLen;
  char     payload[];
} SShowObj;

typedef struct {
  uint8_t  msgType;
  int8_t   expected;
  int8_t   received;
  int8_t   successed;
  int32_t  contLen;
  int32_t  code;
  void     *ahandle;
  void     *thandle;
  void     *pCont;
  SDbObj   *pDb;
  SUserObj *pUser;
} SQueuedMsg;

int32_t mgmtInitSystem();
int32_t mgmtStartSystem();
void    mgmtCleanUpSystem();
void    mgmtStopSystem();

extern char  version[];
extern void *tsMgmtTmr;
extern char  tsMnodeDir[];

#ifdef __cplusplus
}
#endif

#endif

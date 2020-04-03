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

struct _vg_obj;
struct _db_obj;
struct _acctObj;

typedef struct {
  int32_t  mnodeId;
  uint32_t privateIp;
  uint32_t publicIp;
  int64_t  createdTime;
  int64_t  lostTime;
  uint64_t dbVersion;
  uint32_t rack;
  uint16_t idc;
  uint16_t slot;
  int8_t   role;
  int8_t   status;
  int8_t   numOfMnodes;
  int32_t  numOfDnodes;
  char     mnodeName[TSDB_DNODE_NAME_LEN + 1];
  int8_t   reserved[15];
  int8_t   updateEnd[1];
  int      syncFd;
  void    *hbTimer;
  void    *pSync;
} SMnodeObj;

typedef struct {
  int32_t    dnodeId;
  uint32_t   privateIp;
  uint32_t   publicIp;
  uint32_t   moduleStatus;
  int64_t    createdTime;
  uint32_t   lastAccess;
  int32_t    openVnodes;
  int32_t    numOfTotalVnodes; // from dnode status msg, config information
  uint32_t   rack;
  uint16_t   idc;
  uint16_t   slot;
  uint16_t   numOfCores;       // from dnode status msg
  int8_t     alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  int8_t     lbStatus;         // set in balance function
  float      lbScore;          // calc in balance function
  int32_t    customScore;      // config by user
  char       dnodeName[TSDB_DNODE_NAME_LEN + 1];
  int8_t     reserved[15];
  int8_t     updateEnd[1];
  SVnodeLoad vload[TSDB_MAX_VNODES];
  int32_t    status;
  uint32_t   lastReboot;       // time stamp for last reboot
  float      diskAvailable;    // from dnode status msg
  int16_t    diskAvgUsage;     // calc from sys.disk
  int16_t    cpuAvgUsage;      // calc from sys.cpu
  int16_t    memoryAvgUsage;   // calc from sys.mem
  int16_t    bandwidthUsage;   // calc from sys.band
} SDnodeObj;

typedef struct {
  int32_t  dnodeId;
  int32_t  vnode;
  uint32_t privateIp;
  uint32_t publicIp;
} SVnodeGid;

typedef struct {
  char   tableId[TSDB_TABLE_ID_LEN + 1];
  int8_t type;
} STableInfo;

typedef struct SSuperTableObj {
  STableInfo info;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    sversion;
  int32_t    numOfColumns;
  int32_t    numOfTags;
  int8_t     reserved[15];
  int8_t     updateEnd[1];
  int32_t    numOfTables;
  int16_t    nextColId;
  SSchema *  schema;
} SSuperTableObj;

typedef struct {
  STableInfo info;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    sversion;     //used by normal table
  int32_t    numOfColumns; //used by normal table
  int32_t    sid;
  int32_t    vgId;
  char       superTableId[TSDB_TABLE_ID_LEN + 1];
  int32_t    sqlLen;
  int8_t     reserved[1]; 
  int8_t     updateEnd[1];
  int16_t    nextColId;    //used by normal table
  char*      sql;          //used by normal table
  SSchema*   schema;       //used by normal table
  SSuperTableObj *superTable;
} SChildTableObj;

typedef struct _vg_obj {
  uint32_t        vgId;
  char            dbName[TSDB_DB_NAME_LEN + 1];
  int64_t         createdTime;
  SVnodeGid       vnodeGid[TSDB_VNODES_SUPPORT];
  int32_t         numOfVnodes;
  int32_t         lbIp;
  int32_t         lbTime;
  int8_t          lbStatus;
  int8_t          reserved[14];
  int8_t          updateEnd[1];
  struct _vg_obj *prev, *next;
  struct _db_obj *pDb;
  int32_t         numOfTables;
  void *          idPool;
  SChildTableObj ** tableList;
} SVgObj;

typedef struct _db_obj {
  char    name[TSDB_DB_NAME_LEN + 1];
  int8_t  dirty;
  int64_t createdTime;
  SDbCfg  cfg;
  int8_t  reserved[15];
  int8_t  updateEnd[1];
  struct _db_obj *prev, *next;
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfSuperTables;
  SVgObj *pHead;
  SVgObj *pTail;
  struct _acctObj *pAcct;
} SDbObj;

typedef struct _user_obj {
  char              user[TSDB_USER_LEN + 1];
  char              pass[TSDB_KEY_LEN + 1];
  char              acct[TSDB_USER_LEN + 1];
  int64_t           createdTime;
  int8_t            superAuth;
  int8_t            writeAuth;
  int8_t            reserved[13];
  int8_t            updateEnd[1];
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
  int8_t    dirty;
  int8_t    reserved[14];
  int8_t    updateEnd[1];
  SAcctInfo acctInfo;
  SDbObj *         pHead;
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
  int8_t   usePublicIp;
  int8_t   received;
  int8_t   successed;
  int8_t   expected;
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

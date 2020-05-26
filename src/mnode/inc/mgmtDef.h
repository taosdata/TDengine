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

#ifndef TDENGINE_MGMT_DEF_H
#define TDENGINE_MGMT_DEF_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"
#include "taosmsg.h"

struct SVgObj;
struct SDbObj;
struct SAcctObj;
struct SUserObj;
struct SMnodeObj;

typedef struct SDnodeObj {
  int32_t    dnodeId;
  uint16_t   dnodePort;
  char       dnodeFqdn[TSDB_FQDN_LEN + 1];
  char       dnodeEp[TSDB_EP_LEN + 1];
  int64_t    createdTime;
  uint32_t   lastAccess;
  int32_t    openVnodes;
  int32_t    totalVnodes;      // from dnode status msg, config information
  int32_t    customScore;      // config by user
  uint16_t   numOfCores;       // from dnode status msg
  int8_t     alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  int8_t     status;           // set in balance function
  int8_t     isMgmt;
  int8_t     reserved[15];
  int8_t     updateEnd[1];
  int32_t    refCount;
  uint32_t   moduleStatus;
  uint32_t   lastReboot;       // time stamp for last reboot
  float      score;            // calc in balance function
  float      diskAvailable;    // from dnode status msg
  int16_t    diskAvgUsage;     // calc from sys.disk
  int16_t    cpuAvgUsage;      // calc from sys.cpu
  int16_t    memoryAvgUsage;   // calc from sys.mem
  int16_t    bandwidthUsage;   // calc from sys.band
} SDnodeObj;

typedef struct SMnodeObj {
  int32_t    mnodeId;
  int64_t    createdTime;
  int8_t     reserved[14];
  int8_t     updateEnd[1];
  int32_t    refCount;
  int8_t     role;
} SMnodeObj;

typedef struct {
  char  *tableId;
  int8_t type;
} STableObj;

typedef struct SSuperTableObj {
  STableObj  info;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    sversion;
  int32_t    tversion;
  int32_t    numOfColumns;
  int32_t    numOfTags;
  int8_t     reserved[15];
  int8_t     updateEnd[1];
  int32_t    refCount;
  int32_t    numOfTables;
  int16_t    nextColId;
  SSchema *  schema;
  void *     vgHash;
} SSuperTableObj;

typedef struct {
  STableObj  info;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    sversion;     //used by normal table
  int32_t    numOfColumns; //used by normal table
  int32_t    sid;
  int32_t    vgId;
  uint64_t   suid;
  int32_t    sqlLen;
  int8_t     reserved[1]; 
  int8_t     updateEnd[1];
  int16_t    nextColId;    //used by normal table
  int32_t    refCount;
  char*      sql;          //used by normal table
  SSchema*   schema;       //used by normal table
  SSuperTableObj *superTable;
} SChildTableObj;

typedef struct {
  int32_t    dnodeId;
  int8_t     role;
  int8_t     reserved[3];
  SDnodeObj* pDnode;
} SVnodeGid;

typedef struct SVgObj {
  uint32_t       vgId;
  char           dbName[TSDB_DB_NAME_LEN + 1];
  int64_t        createdTime;
  SVnodeGid      vnodeGid[TSDB_MAX_REPLICA];
  int32_t        numOfVnodes;
  int32_t        lbDnodeId;
  int32_t        lbTime;
  int8_t         inUse;
  int8_t         reserved[13];
  int8_t         updateEnd[1];
  int32_t        refCount;
  struct SVgObj *prev, *next;
  struct SDbObj *pDb;
  int32_t        numOfTables;
  int64_t        totalStorage;
  int64_t        compStorage;
  int64_t        pointsWritten;
  void *         idPool;
  SChildTableObj **tableList;
} SVgObj;

typedef struct {
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t maxTables;
  int32_t daysPerFile;
  int32_t daysToKeep;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRowsPerFileBlock;
  int32_t maxRowsPerFileBlock;
  int32_t commitTime;
  int8_t  precision;
  int8_t  compression;
  int8_t  walLevel;
  int8_t  replications;
  int8_t  reserved[16];
} SDbCfg;

typedef struct SDbObj {
  char    name[TSDB_DB_NAME_LEN + 1];
  char    acct[TSDB_USER_LEN + 1];
  int64_t createdTime;
  int32_t cfgVersion;
  SDbCfg  cfg;
  int8_t  status;
  int8_t  reserved[14];
  int8_t  updateEnd[1];
  int32_t refCount;
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfSuperTables;
  SVgObj *pHead;
  SVgObj *pTail;
  struct SAcctObj *pAcct;
} SDbObj;

typedef struct SUserObj {
  char              user[TSDB_USER_LEN + 1];
  char              pass[TSDB_KEY_LEN + 1];
  char              acct[TSDB_USER_LEN + 1];
  int64_t           createdTime;
  int8_t            superAuth;
  int8_t            writeAuth;
  int8_t            reserved[13];
  int8_t            updateEnd[1];
  int32_t           refCount;
  struct SAcctObj * pAcct;
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

typedef struct SAcctObj {
  char      user[TSDB_USER_LEN + 1];
  char      pass[TSDB_KEY_LEN + 1];
  SAcctCfg  cfg;
  int32_t   acctId;
  int64_t   createdTime;
  int8_t    status;
  int8_t    reserved[14];
  int8_t    updateEnd[1];
  int32_t   refCount;
  SAcctInfo acctInfo;
  pthread_mutex_t  mutex;
} SAcctObj;

typedef struct {
  int8_t   type;
  char     db[TSDB_DB_NAME_LEN + 1];
  void *   pIter;
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
  int8_t   received;
  int8_t   successed;
  int8_t   expected;
  int8_t   retry;
  int8_t   maxRetry;
  int32_t  contLen;
  int32_t  code;
  void     *ahandle;
  void     *thandle;
  void     *pCont;
  SAcctObj *pAcct;
  SDnodeObj*pDnode;
  SUserObj *pUser;
  SDbObj   *pDb;
  SVgObj   *pVgroup;
  STableObj *pTable;
} SQueuedMsg;

#ifdef __cplusplus
}
#endif

#endif

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

#ifndef TDENGINE_MNODE_DEF_H
#define TDENGINE_MNODE_DEF_H

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

/*
struct define notes:
1. The first field must be the xxxxId field or name field , e.g. 'int32_t dnodeId', 'int32_t mnodeId', 'char name[]', 'char user[]', ...
2. From the dnodeId field to the updataEnd field, these information will be falled disc;
3. The fields behind the updataEnd field can be changed;
*/

typedef struct SClusterObj {
  char    uid[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int8_t  reserved[12];
  int8_t  updateEnd[4];
  int32_t refCount;
} SClusterObj;

typedef struct SDnodeObj {
  int32_t    dnodeId;
  int32_t    openVnodes;
  int64_t    createdTime;
  int32_t    resever0;         // from dnode status msg, config information
  int32_t    customScore;      // config by user
  uint32_t   lastAccess;
  uint16_t   numOfCores;       // from dnode status msg
  uint16_t   dnodePort;
  char       dnodeFqdn[TSDB_FQDN_LEN];
  char       dnodeEp[TSDB_EP_LEN];
  int8_t     alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  int8_t     status;           // set in balance function
  int8_t     isMgmt;
  int8_t     reserve1[11];  
  int8_t     updateEnd[4];
  int32_t    refCount;
  uint32_t   moduleStatus;
  uint32_t   lastReboot;       // time stamp for last reboot
  float      score;            // calc in balance function
  float      diskAvailable;    // from dnode status msg
  int16_t    diskAvgUsage;     // calc from sys.disk
  int16_t    cpuAvgUsage;      // calc from sys.cpu
  int16_t    memoryAvgUsage;   // calc from sys.mem
  int16_t    bandwidthUsage;   // calc from sys.band
  int8_t     offlineReason;
  int8_t     reserved2[1];
} SDnodeObj;

typedef struct SMnodeObj {
  int32_t    mnodeId;
  int8_t     reserved0[4];
  int64_t    createdTime;
  int8_t     reserved1[4];
  int8_t     updateEnd[4];
  int32_t    refCount;
  int8_t     role;
  int8_t     reserved2[3];
} SMnodeObj;

typedef struct STableObj {
  char  *tableId;
  int8_t type;
} STableObj;

typedef struct SSTableObj {
  STableObj  info; 
  int8_t     reserved0[9]; // for fill struct STableObj to 4byte align
  int16_t    nextColId;
  int32_t    sversion;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    tversion;
  int32_t    numOfColumns;
  int32_t    numOfTags;
  int8_t     updateEnd[4];
  int32_t    refCount;
  int32_t    numOfTables;
  SSchema *  schema;
  void *     vgHash;
} SSTableObj;

typedef struct {
  STableObj  info;  
  int8_t     reserved0[9]; // for fill struct STableObj to 4byte align
  int16_t    nextColId;    //used by normal table
  int32_t    sversion;     //used by normal table  
  uint64_t   uid;
  uint64_t   suid;
  int64_t    createdTime;
  int32_t    numOfColumns; //used by normal table
  int32_t    tid;
  int32_t    vgId;
  int32_t    sqlLen;
  int8_t     updateEnd[4];
  int32_t    refCount;
  char*      sql;          //used by normal table
  SSchema*   schema;       //used by normal table
  SSTableObj*superTable;
} SCTableObj;

typedef struct {
  int32_t    dnodeId;
  int8_t     role;
  int8_t     vver[3];  // To ensure compatibility, 3 bits are used to represent the remainder of 64 bit version
  SDnodeObj *pDnode;
} SVnodeGid;

typedef struct SVgObj {
  uint32_t       vgId;
  int32_t        numOfVnodes;
  int64_t        createdTime;
  int32_t        lbDnodeId;
  int32_t        lbTime;
  char           dbName[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t         inUse;
  int8_t         accessState;
  int8_t         status;
  int8_t         reserved0[4];
  SVnodeGid      vnodeGid[TSDB_MAX_REPLICA];
  int32_t        vgCfgVersion;
  int8_t         reserved1[8];
  int8_t         updateEnd[4];
  int32_t        refCount;
  int32_t        numOfTables;
  int64_t        totalStorage;
  int64_t        compStorage;
  int64_t        pointsWritten;
  struct SDbObj *pDb;
  void *         idPool;
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
  int32_t fsyncPeriod;
  int8_t  precision;
  int8_t  compression;
  int8_t  walLevel;
  int8_t  replications;
  int8_t  quorum;
  int8_t  update;
  int8_t  cacheLastRow;
  int8_t  dbType;
  int16_t partitions;
  int8_t  reserved[7];
} SDbCfg;

typedef struct SDbObj {
  char    name[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t  reserved0[4];
  char    acct[TSDB_USER_LEN];
  int64_t createdTime;
  int32_t dbCfgVersion;
  SDbCfg  cfg;
  int8_t  status;
  int8_t  reserved1[11];
  int8_t  updateEnd[4];
  int32_t refCount;
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfSuperTables;
  int32_t vgListSize; 
  int32_t vgListIndex;
  SVgObj **vgList;
  struct SAcctObj *pAcct;
  pthread_mutex_t  mutex;
} SDbObj;

typedef struct SUserObj {
  char              user[TSDB_USER_LEN];
  char              pass[TSDB_KEY_LEN];
  char              acct[TSDB_USER_LEN];
  int64_t           createdTime;
  int8_t            superAuth;
  int8_t            writeAuth;
  int8_t            reserved[10];
  int8_t            updateEnd[4];
  int32_t           refCount;
  struct SAcctObj * pAcct;
} SUserObj;

typedef struct SFuncObj {
  char              name[TSDB_FUNC_NAME_LEN];
  char              path[128];
  int32_t           contLen;
  char              cont[TSDB_FUNC_CODE_LEN];
  int64_t           createdTime;
  uint8_t           resType;
  int16_t           resBytes;
  int64_t           sig;         // partial md5 sign
  int16_t           type;        // [lua script|so|js]
  int8_t            reserved[64];
  int8_t            updateEnd[4];
  int32_t           refCount;
} SFuncObj;

typedef struct {
  int64_t totalStorage;  // Total storage wrtten from this account
  int64_t compStorage;   // Compressed storage on disk
  int64_t queryTime;
  int64_t totalPoints;
  int64_t inblound;
  int64_t outbound;
  int64_t sKey;
  int32_t numOfUsers;
  int32_t numOfDbs;
  int32_t numOfTimeSeries;
  int32_t numOfPointsPerSecond;
  int32_t numOfConns;
  int32_t numOfQueries;
  int32_t numOfStreams;
  int8_t  accessState;   // Checked by mgmt heartbeat message
  int8_t  reserved[3];
} SAcctInfo;

typedef struct SAcctObj {
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_KEY_LEN];
  SAcctCfg  cfg;
  int64_t   createdTime;
  int32_t   acctId;
  int8_t    status;
  int8_t    reserved0[7];
  int8_t    updateEnd[4];
  int32_t   refCount;
  int8_t    reserved1[4];
  SAcctInfo acctInfo;
  pthread_mutex_t  mutex;
} SAcctObj;

typedef struct {
  char     db[TSDB_DB_NAME_LEN];
  int8_t   type;
  int16_t  numOfColumns;
  int32_t  index;
  int32_t  rowSize;
  int32_t  numOfRows;
  void *   pIter;
  void **  ppShow;
  int16_t  offset[TSDB_MAX_COLUMNS];
  int32_t  bytes[TSDB_MAX_COLUMNS];
  int32_t  numOfReads;
  int8_t   maxReplica;
  int8_t   reserved0[1];
  uint16_t payloadLen;
  char     payload[];
} SShowObj;

#ifdef __cplusplus
}
#endif

#endif

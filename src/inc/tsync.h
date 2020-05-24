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

#ifndef TDENGINE_SYNC_H
#define TDENGINE_SYNC_H

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_SYNC_MAX_REPLICA 5

typedef enum _TAOS_SYNC_ROLE {
  TAOS_SYNC_ROLE_OFFLINE,
  TAOS_SYNC_ROLE_UNSYNCED,
  TAOS_SYNC_ROLE_SLAVE,
  TAOS_SYNC_ROLE_MASTER,
} ESyncRole;

typedef enum _TAOS_SYNC_STATUS {
  TAOS_SYNC_STATUS_INIT,
  TAOS_SYNC_STATUS_START,
  TAOS_SYNC_STATUS_FILE,
  TAOS_SYNC_STATUS_CACHE,
} ESyncStatus;

typedef struct {
  uint32_t  nodeId;    // node ID assigned by TDengine
  uint16_t  nodePort;  // node sync Port
  char      nodeFqdn[TSDB_FQDN_LEN]; // node FQDN  
} SNodeInfo;

typedef struct {
  int8_t     quorum;    // number of confirms required, >=1 
  int8_t     replica;   // number of replications, >=1
  SNodeInfo  nodeInfo[TAOS_SYNC_MAX_REPLICA];
} SSyncCfg;

typedef struct {
  int       selfIndex;
  uint32_t  nodeId[TAOS_SYNC_MAX_REPLICA];
  int       role[TAOS_SYNC_MAX_REPLICA];  
} SNodesRole;
 
// if name is empty(name[0] is zero), get the file from index or after, used by master
// if name is provided(name[0] is not zero), get the named file at the specified index, used by unsynced node
// it returns the file magic number and size, if file not there, magic shall be 0.
typedef uint32_t (*FGetFileInfo)(void *ahandle, char *name, uint32_t *index, int32_t *size, uint64_t *fversion); 

// get the wal file from index or after
// return value, -1: error, 1:more wal files, 0:last WAL. if name[0]==0, no WAL file
typedef int      (*FGetWalInfo)(void *ahandle, char *name, uint32_t *index); 
 
// when a forward pkt is received, call this to handle data 
typedef int      (*FWriteToCache)(void *ahandle, void *pHead, int type);

// when forward is confirmed by peer, master call this API to notify app
typedef void     (*FConfirmForward)(void *ahandle, void *mhandle, int32_t code);

// when role is changed, call this to notify app
typedef void     (*FNotifyRole)(void *ahandle, int8_t role);

// when data file is synced successfully, notity app
typedef void     (*FNotifyFileSynced)(void *ahandle, uint64_t fversion);

typedef struct {
  int32_t    vgId;      // vgroup ID
  uint64_t   version;   // initial version
  SSyncCfg   syncCfg;   // configuration from mgmt
  char       path[128]; // path to the file
 
  void      *ahandle;   // handle provided by APP 
  FGetFileInfo    getFileInfo;
  FGetWalInfo     getWalInfo;
  FWriteToCache   writeToCache;
  FConfirmForward confirmForward;
  FNotifyRole     notifyRole;
  FNotifyFileSynced notifyFileSynced;
} SSyncInfo;

typedef void* tsync_h;

tsync_h syncStart(const SSyncInfo *);
void    syncStop(tsync_h shandle);
int32_t syncReconfig(tsync_h shandle, const SSyncCfg *);
int32_t syncForwardToPeer(tsync_h shandle, void *pHead, void *mhandle, int qtype);
void    syncConfirmForward(tsync_h shandle, uint64_t version, int32_t code);
void    syncRecover(tsync_h shandle);      // recover from other nodes:
int     syncGetNodesRole(tsync_h shandle, SNodesRole *);

extern  char *syncRole[];

//global configurable parameters
extern  int   tsMaxSyncNum;
extern  int   tsSyncTcpThreads;
extern  int   tsMaxWatchFiles;
extern  int   tsSyncTimer;
extern  int   tsMaxFwdInfo; 
extern  int   sDebugFlag;
extern  char  tsArbitrator[];
extern  uint16_t tsSyncPort;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SYNC_H

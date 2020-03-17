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

#define TAOS_QTYPE_RPC      0
#define TAOS_QTYPE_FWD      1
#define TAOS_QTYPE_WAL      2 

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
  int8_t    msgType;
  int8_t    reserved[3];
  int32_t   len;
  uint64_t  version;
  uint64_t  cksum;
  char      cont[];
} SWalHead;

typedef struct {
  uint32_t  nodeId;    // node ID assigned by TDengine
  uint32_t  nodeIp;    // node IP address
  char      name[TSDB_FILENAME_LEN]; // external node name 
} SNodeInfo;

typedef struct {
  int       selfIndex;
  uint32_t  nodeId[TAOS_SYNC_MAX_REPLICA];
  int       role[TAOS_SYNC_MAX_REPLICA];  
} SNodesRole;
  
typedef struct {
  char       label[20]; // for debug purpose 
  char       path[128]; // path to the file
  int8_t     replica;   // number of replications
  int8_t     quorum; 
  int32_t    vgId;      // vgroup ID
  void      *ahandle;   // handle provided by APP 
  uint64_t   version;   // initial version
 
  // if name is null, get the file from index or after, used by master
  // if name is provided, get the named file at the specified index, used by unsynced node
  // it returns the file magic number, if file not there, magic shall be 0.
  uint32_t   (*getFileInfo)(char *name, int *index, int *size); 

  // get the wal file from index or after
  // return value, -1: error, 1:more wal files, 0:last WAL, or no WAL if name[0] == 0
  int        (*getWalInfo)(char *name, int *index); 

  int        (*writeToCache)(void *ahandle, SWalHead *, int type);
  void       (*confirmFwd)(void *ahandle, void *mhandle, int32_t code);
  void       (*notifyRole)(void *ahandle, int8_t role);
  SNodeInfo  nodeInfo[TAOS_SYNC_MAX_REPLICA];
} SSyncInfo;

typedef void* tsync_h;

tsync_h syncStart(SSyncInfo *);
void    syncStop(tsync_h );
int     syncReconfig(tsync_h, SSyncInfo *);
int     syncForwardToPeer(tsync_h, SWalHead *pHead, void *mhandle);
void    syncAckForward(tsync_h, uint64_t version, int32_t code);
void    syncRecover(tsync_h );      // recover from other nodes:
int     syncGetNodesRole(tsync_h, SNodesRole *);

extern  char *syncRole[];

extern  int   tsMaxSyncNum;
extern  int   tsSyncTcpThreads;
extern  int   tsMaxWatchFiles;
extern  short tsSyncPort;


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SYNC_H

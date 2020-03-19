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

#ifndef TDENGINE_SYNCINT_H
#define TDENGINE_SYNCINT_H

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_SMSG_SYNC_DATA    1
#define TAOS_SMSG_FORWARD      2
#define TAOS_SMSG_FWDACK       3
#define TAOS_SMSG_SYNC_REQ     4 
#define TAOS_SMSG_SYNC_RSP     5
#define TAOS_SMSG_SYNC_MUST    6
#define TAOS_SMSG_STATUS       7

#define nodeRole    pNode->peerInfo[pNode->selfIndex]->role
#define nodeVersion pNode->peerInfo[pNode->selfIndex]->version
#define nodeSStatus pNode->peerInfo[pNode->selfIndex]->sstatus

#pragma pack(push, 1)

typedef struct {
  char     type;        // msg type
  char     pversion;    // protocol version
  char     reserved[6]; // not used
  int32_t  vgId;        // vg ID
  int32_t  len;         // content length
  char     cont[];      // message content starts from here
} SSyncHead;

typedef struct {
  int8_t    role;
  uint64_t  version;
} SPeerStatus;

typedef struct {
  int8_t      role;
  int8_t      ack;
  uint64_t    version;
  SPeerStatus peersStatus[];
} SPeersStatus;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  uint32_t  magic;
  int32_t   index;
  int32_t   size;
} SFileInfo;

typedef struct {
  int8_t    sync;
} SFileAck;

typedef struct {
  uint64_t  version;
  int32_t   code;
} SFwdAck;
  
#pragma pack(pop)

typedef struct {
  char           *buffer;
  int             bufferSize;
  char           *offset;
  int             forwards;
  int             code;
} SRecvBuffer;

typedef struct {
  uint64_t  version;
  void     *mhandle;
  int8_t    acks;
  int8_t    nacks;
  int8_t    confirmed;
  int32_t   code;
  uint64_t  time;
} SFwdInfo;

typedef struct {
  int       first;
  int       last;
  int       fwds;  // number of forwards
  SFwdInfo  fwdInfo[];
} SSyncFwds;

typedef struct _syncPeer {
  int32_t     nodeId;
  uint32_t    ip;
  char        ipstr[20];  // peer ip string
  int8_t      role;
  int8_t      sstatus;    // sync status
  uint64_t    version;
  int         syncFd;
  int         peerFd;     // forward FD
  void       *timer;
  void       *pThread;
  int         notifyFd;
  int         watchNum;
  int        *watchFd;
  int8_t      refCount;   // reference count
  struct _sync_node *pSyncNode;
} SSyncPeer;

typedef struct _sync_node {
  char         label[20];
  char         path[128];
  int8_t       replica;
  int8_t       quorum;
  uint32_t     vgId;
  void        *ahandle;
  uint32_t   (*getFileInfo)(char *name, int *index, int *size);
  int        (*getWalInfo)(char *name, int *index);
  int        (*writeToCache)(void *ahandle, SWalHead *, int type); 
  void       (*confirmForward)(void *ahandle, void *mhandle, int32_t code);
  void       (*notifyRole)(void *ahandle, int8_t role);
  int8_t       selfIndex;
  SSyncPeer   *peerInfo[TAOS_SYNC_MAX_REPLICA];
  SSyncPeer   *pMaster;
  int8_t       refCount;
  SRecvBuffer *pRecv;
  SSyncFwds   *pSyncFwds;  // saved forward info if quorum >1
  void        *pFwdTimer;
  pthread_mutex_t mutex;
} SSyncNode;

extern int  tsSyncNum;
extern int  tsMaxWatchFiles;

void *syncRetrieveData(void *param);
void *syncRestoreData(void *param);
int   syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead);
void  syncRestartConnection(SSyncPeer *pPeer);
void  syncBroadcastStatus(SSyncNode *pNode);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H

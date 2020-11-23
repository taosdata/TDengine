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

#define sFatal(...) { if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYN FATAL ", sDebugFlag, __VA_ARGS__); }}
#define sError(...) { if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYN ERROR ", sDebugFlag, __VA_ARGS__); }}
#define sWarn(...)  { if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN ", sDebugFlag, __VA_ARGS__); }}
#define sInfo(...)  { if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sDebug(...) { if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sTrace(...) { if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}

typedef enum {
  TAOS_SMSG_SYNC_DATA   = 1,
  TAOS_SMSG_FORWARD     = 2,
  TAOS_SMSG_FORWARD_RSP = 3,
  TAOS_SMSG_SYNC_REQ    = 4,
  TAOS_SMSG_SYNC_RSP    = 5,
  TAOS_SMSG_SYNC_MUST   = 6,
  TAOS_SMSG_STATUS      = 7
} ESyncMsgType;

#define SYNC_MAX_SIZE (TSDB_MAX_WAL_SIZE + sizeof(SWalHead) + sizeof(SSyncHead) + 16)
#define SYNC_RECV_BUFFER_SIZE (5*1024*1024)
#define SYNC_FWD_TIMER  300
#define SYNC_ROLE_TIMER 10000

#define nodeRole    pNode->peerInfo[pNode->selfIndex]->role
#define nodeVersion pNode->peerInfo[pNode->selfIndex]->version
#define nodeSStatus pNode->peerInfo[pNode->selfIndex]->sstatus

#pragma pack(push, 1)

typedef struct {
  char     type;        // msg type
  char     pversion;    // protocol version
  char     reserved[6]; // not used
  int32_t  vgId;        // vg ID
  int32_t  len;         // content length, does not include head
  // char     cont[];      // message content starts from here
} SSyncHead;

typedef struct {
  SSyncHead syncHead;
  uint16_t  port;
  char      fqdn[TSDB_FQDN_LEN];
  int32_t   sourceId;  // only for arbitrator
} SFirstPkt;

typedef struct {
  int8_t    role;
  uint64_t  version;
} SPeerStatus;

typedef struct {
  int8_t      role;
  int8_t      ack;
  int8_t      type;
  int8_t      reserved[3];
  uint16_t    tranId;
  uint64_t    version;
  SPeerStatus peersStatus[];
} SPeersStatus;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  uint32_t  magic;
  uint32_t  index;
  uint64_t  fversion;
  int64_t   size;
} SFileInfo;

typedef struct {
  int8_t    sync;
} SFileAck;

typedef struct {
  uint64_t  version;
  int32_t   code;
} SFwdRsp;
  
#pragma pack(pop)

typedef struct {
  char *  buffer;
  int32_t bufferSize;
  char *  offset;
  int32_t forwards;
  int32_t code;
} SRecvBuffer;

typedef struct {
  uint64_t  version;
  void     *mhandle;
  int8_t    acks;
  int8_t    nacks;
  int8_t    confirmed;
  int32_t   code;
  int64_t   time;
} SFwdInfo;

typedef struct {
  int32_t  first;
  int32_t  last;
  int32_t  fwds;  // number of forwards
  SFwdInfo fwdInfo[];
} SSyncFwds;

typedef struct SsyncPeer {
  int32_t  nodeId;
  uint32_t ip;
  uint16_t port;
  int8_t   role;
  int8_t   sstatus;               // sync status
  char     fqdn[TSDB_FQDN_LEN];   // peer ip string
  char     id[TSDB_EP_LEN + 32];  // peer vgId + end point
  uint64_t version;
  uint64_t sversion;        // track the peer version in retrieve process
  int32_t  syncFd;
  int32_t  peerFd;          // forward FD
  int32_t  numOfRetrieves;  // number of retrieves tried
  int32_t  fileChanged;     // a flag to indicate file is changed during retrieving process
  void *   timer;
  void *   pConn;
  int32_t  notifyFd;
  int32_t  watchNum;
  int32_t *watchFd;
  int32_t  refCount;  // reference count
  struct   SSyncNode *pSyncNode;
} SSyncPeer;

typedef struct SSyncNode {
  char         path[TSDB_FILENAME_LEN];
  int8_t       replica;
  int8_t       quorum;
  int8_t       selfIndex;
  uint32_t     vgId;
  int64_t      rid;
  SSyncPeer *  peerInfo[TAOS_SYNC_MAX_REPLICA + 1];  // extra one for arbitrator
  SSyncPeer *  pMaster;
  SRecvBuffer *pRecv;
  SSyncFwds *  pSyncFwds;  // saved forward info if quorum >1
  void *       pFwdTimer;
  void *       pRoleTimer;
  FGetFileInfo      getFileInfo;
  FGetWalInfo       getWalInfo;
  FWriteToCache     writeToCache;
  FConfirmForward   confirmForward;
  FNotifyRole       notifyRole;
  FNotifyFlowCtrl   notifyFlowCtrl;
  FNotifyFileSynced notifyFileSynced;
  pthread_mutex_t   mutex;
} SSyncNode;

// sync module global
extern int32_t tsSyncNum;
extern char    tsNodeFqdn[TSDB_FQDN_LEN];

void *syncRetrieveData(void *param);
void *syncRestoreData(void *param);
int32_t syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead);
void    syncRestartConnection(SSyncPeer *pPeer);
void    syncBroadcastStatus(SSyncNode *pNode);
void    syncAddPeerRef(SSyncPeer *pPeer);
int32_t syncDecPeerRef(SSyncPeer *pPeer);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H

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

#ifndef TDENGINE_SYNC_INT_H
#define TDENGINE_SYNC_INT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "syncMsg.h"
#include "twal.h"

#define sFatal(...) { if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYN FATAL ", sDebugFlag, __VA_ARGS__); }}
#define sError(...) { if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYN ERROR ", sDebugFlag, __VA_ARGS__); }}
#define sWarn(...)  { if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN ", sDebugFlag, __VA_ARGS__); }}
#define sInfo(...)  { if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sDebug(...) { if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sTrace(...) { if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}

#define SYNC_TCP_THREADS 2
#define SYNC_MAX_NUM 2

#define SYNC_MAX_SIZE (TSDB_MAX_WAL_SIZE + sizeof(SWalHead) + sizeof(SSyncHead) + 16)
#define SYNC_RECV_BUFFER_SIZE (5*1024*1024)

#define SYNC_MAX_FWDS 4096
#define SYNC_FWD_TIMER 300
#define SYNC_ROLE_TIMER 15000             // ms
#define SYNC_CHECK_INTERVAL 1000          // ms
#define SYNC_WAIT_AFTER_CHOOSE_MASTER 10  // ms

#define nodeRole    pNode->peerInfo[pNode->selfIndex]->role
#define nodeVersion pNode->peerInfo[pNode->selfIndex]->version
#define nodeSStatus pNode->peerInfo[pNode->selfIndex]->sstatus

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
  uint64_t lastFileVer;     // track the file version while retrieve
  uint64_t lastWalVer;      // track the wal version while retrieve
  SOCKET   syncFd;
  SOCKET   peerFd;          // forward FD
  int32_t  numOfRetrieves;  // number of retrieves tried
  int32_t  fileChanged;     // a flag to indicate file is changed during retrieving process
  int32_t  refCount;
  int8_t   isArb;
  int64_t  rid;
  void *   timer;
  void *   pConn;
  struct   SSyncNode *pSyncNode;
} SSyncPeer;

typedef struct SSyncNode {
  char         path[TSDB_FILENAME_LEN];
  int8_t       replica;
  int8_t       quorum;
  int8_t       selfIndex;
  uint32_t     vgId;
  int32_t      refCount;
  int64_t      rid;
  SSyncPeer *  peerInfo[TAOS_SYNC_MAX_REPLICA + 1];  // extra one for arbitrator
  SSyncPeer *  pMaster;
  SRecvBuffer *pRecv;
  SSyncFwds *  pSyncFwds;  // saved forward info if quorum >1
  void *       pFwdTimer;
  void *       pRoleTimer;
  void *       pTsdb;
  FGetWalInfo       getWalInfoFp;
  FWriteToCache     writeToCacheFp;
  FConfirmForward   confirmForward;
  FNotifyRole       notifyRoleFp;
  FNotifyFlowCtrl   notifyFlowCtrlFp;
  FStartSyncFile    startSyncFileFp;
  FStopSyncFile     stopSyncFileFp;
  FGetVersion       getVersionFp;
  FSendFile         sendFileFp;
  FRecvFile         recvFileFp;
  pthread_mutex_t   mutex;
} SSyncNode;

// sync module global
extern int32_t tsSyncNum;
extern char    tsNodeFqdn[TSDB_FQDN_LEN];
extern char *  syncStatus[];

void *     syncRetrieveData(void *param);
void *     syncRestoreData(void *param);
int32_t    syncSaveIntoBuffer(SSyncPeer *pPeer, SWalHead *pHead);
void       syncRestartConnection(SSyncPeer *pPeer);
void       syncBroadcastStatus(SSyncNode *pNode);
uint32_t   syncResolvePeerFqdn(SSyncPeer *pPeer);
SSyncPeer *syncAcquirePeer(int64_t rid);
void       syncReleasePeer(SSyncPeer *pPeer);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H

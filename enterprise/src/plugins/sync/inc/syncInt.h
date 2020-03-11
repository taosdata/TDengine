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

#define TAOS_SMSG_SYNC_DATA 1
#define TAOS_SMSG_FORWARD   2
#define TAOS_SMSG_SYNC_REQ  3
#define TAOS_SMSG_SYNC_RSP  4
#define TAOS_SMSG_SYNC_MUST 5
#define TAOS_SMSG_STATUS    6

#pragma pack(push, 1)

typedef struct {
  char     type;        // msg type
  char     pversion;    // protocol version
  char     reserved[6]; // not used
  int32_t  vgId;        // vg ID
  int32_t  len;         // content length
  uint64_t version;     // latest version
  char     cont[];      // message content starts from here
} SSyncHead;

typedef struct {
  char     type;
  char     version;
  int16_t  reserved;
  int32_t  vgId;
} SFirstPkt;

typedef struct {
  int8_t    status;
  uint64_t  version;
} SPeerState;

typedef struct {
  int8_t     status;
  int8_t     ack;
  uint64_t   version;
  SPeerState peerStates[];
} SPeerStatus;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  uint32_t  magic;
  int32_t   index;
  int32_t   size;
} SFileInfo;

typedef struct {
  int8_t    sync;
} SFileAck;

#pragma pack(pop)

typedef struct {
  char           *buffer;
  int             bufferSize;
  char           *offset;
  int             forwards;
  int             code;
  pthread_mutex_t mutex;
} SRecvBuffer;

typedef struct _syncPeer {
  int32_t     nodeId;
  uint32_t    ip;
  char        ipstr[20];  // peer ip string
  int         status;
  uint64_t    version;
  int         syncFd;
  int         peerFd;     // forward FD
  void       *hbTimer;
  void       *syncTimer;
  void       *pThread;
  int8_t      refCount;   // reference count
  struct _sync_obj *pSyncObj;
} SSyncPeer;

typedef struct _sync_obj {
  char         label[20];
  int8_t       replica;
  int8_t       quorum;
  int64_t      version;
  uint32_t     vgId;
  void        *ahandle;
  uint32_t   (*getFileInfo)(char *name, int *index, int *size);
  int        (*writeToCache)(void *ahandle, uint64_t version, void *cont, int len);
  int        (*getWalInfo)(char *name, int *index);
  int8_t       selfIndex;
  int8_t       status;
  SSyncPeer   *peerInfo[TAOS_SYNC_MAX_REPLICA];
  int8_t       refCount;
  SRecvBuffer *pRecv;
  pthread_mutex_t vmutex;
} SSyncObj;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H

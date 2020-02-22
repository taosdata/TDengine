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

#ifndef TDENGINE_VNODEPEER_H
#define TDENGINE_VNODEPEER_H

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_VMSG_SYNC_DATA 1
#define TSDB_VMSG_FORWARD 2
#define TSDB_VMSG_SYNC_REQ 3
#define TSDB_VMSG_SYNC_RSP 4
#define TSDB_VMSG_SYNC_MUST 5
#define TSDB_VMSG_STATUS 6

#pragma pack(push, 1)

typedef struct {
  char  type;
  char  version;
  short sourceVid;
  short destVid;
} SFirstPkt;

typedef struct {
  uint64_t lastCreate;
  uint64_t lastRemove;
  uint32_t fileId;
  uint64_t fmagic[];
} SSyncMsg;

typedef struct {
  char     status;
  uint64_t version;
} SPeerState;

typedef struct {
  char       status : 6;
  char       ack : 2;
  char       commitInProcess;
  int32_t    fileId;  // ID for corrupted file, 0 means no corrupted file
  uint64_t   version;
  SPeerState peerStates[];
} SPeerStatus;

#pragma pack(pop)

typedef struct _thread_obj {
  pthread_t thread;
  int       threadId;
  int       pollFd;
  int       numOfFds;
} SThreadObj;

typedef struct {
  int          numOfThreads;
  SThreadObj **pThread;
  pthread_t    thread;
  int          threadId;
} SThreadPool;

typedef struct _vnodePeer {
  void *      signature;
  int         ownId;  // own vnode ID
  uint32_t    ip;
  char        ipstr[20];  // ip string
  int         vid;
  int         status;
  int         syncStatus;
  int32_t     fileId;  // 0 means no corrupted file
  uint64_t    version;
  int         commitInProcess;
  int         syncFd;
  int         peerFd;  // forward FD
  void *      hbTimer;
  void *      syncTimer;
  SThreadObj *pThread;
} SVnodePeer;

typedef struct {
  SVnodePeer *pVPeer;
  uint64_t    lastCreate;
  uint64_t    lastRemove;
  uint32_t    fileId;
  uint64_t    fmagic[];
} SSyncCmd;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H

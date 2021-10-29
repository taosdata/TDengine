/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef _TD_LIBS_SYNC_INT_H
#define _TD_LIBS_SYNC_INT_H

#include "thash.h"
#include "os.h"
#include "sync.h"
#include "raft.h"
#include "tlog.h"

#define TAOS_SYNC_MAX_WORKER 3

typedef struct SSyncWorker {
  pthread_t thread;
} SSyncWorker;

struct SSyncNode {
  pthread_mutex_t   mutex;
  int32_t      refCount;
  SyncGroupId   vgId;
  SSyncRaft raft;
  void* syncTimer;
};

typedef struct SSyncManager {
  pthread_mutex_t   mutex;

  // worker threads
  SSyncWorker worker[TAOS_SYNC_MAX_WORKER];

  // sync net worker
  SSyncWorker netWorker;

  // vgroup hash table
  SHashObj* vgroupTable;

  // timer manager
  void* syncTimerManager;

} SSyncManager;

extern SSyncManager* gSyncManager;

#define syncFatal(...) do { if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYNC FATAL ", 255, __VA_ARGS__); }}     while(0)
#define syncError(...) do { if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYNC ERROR ", 255, __VA_ARGS__); }}     while(0)
#define syncWarn(...)  do { if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYNC WARN ", 255, __VA_ARGS__); }}      while(0)
#define syncInfo(...)  do { if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYNC ", 255, __VA_ARGS__); }}           while(0)
#define syncDebug(...) do { if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYNC ", sDebugFlag, __VA_ARGS__); }} while(0)
#define syncTrace(...) do { if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYNC ", sDebugFlag, __VA_ARGS__); }} while(0)

#endif  /* _TD_LIBS_SYNC_INT_H */
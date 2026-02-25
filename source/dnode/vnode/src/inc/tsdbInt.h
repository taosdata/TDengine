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

#ifndef _TD_VNODE_TSDB_INT_H_
#define _TD_VNODE_TSDB_INT_H_

#include "../tsdb/tsdbDataFileRW.h"
#include "../tsdb/tsdbFS2.h"
#include "../tsdb/tsdbFSetRW.h"
#include "../tsdb/tsdbIter.h"
#include "../tsdb/tsdbSttFileRW.h"

typedef struct {
  STsdb      *tsdb;
  int32_t     szPage;
  int32_t     nodeId;  // node id of leader vnode in ss migration
  STimeWindow tw;

  // lastCommit time when the task is scheduled, we will compare it with the
  // fileset last commit time at the start of the task execution, if mismatch,
  // we know there are new commits after the task is scheduled.
  TSKEY   lastCommit;
  int64_t cid;

  STFileSet   *fset;
  TFileOpArray fopArr;
} SRTNer;

typedef struct {
  STsdb      *tsdb;
  STimeWindow tw;  // unit is second
  TSKEY       lastCommit;
  int32_t     nodeId;  // node id of leader vnode in ss migration
  int32_t     fid;
  int8_t      optrType;     // ETsdbOpType
  int8_t      triggerType;  // ETriggerType
  SVATaskID   taskid;
} SRtnArg;

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  optrType;
  int64_t cid;
  int64_t compactVersion;

  STFileSet   *fset;
  TFileOpArray fopArr[1];

  struct {
    int32_t expLevel;
    // reader
    SDataFileReader    *dataReader;
    TSttFileReaderArray sttReaderArr[1];

    // iter & merger
    TTsdbIterArray dataIterArr[1];
    SIterMerger   *dataIterMerger;
    TTsdbIterArray tombIterArr[1];
    SIterMerger   *tombIterMerger;

    // writer
    SFSetWriter *writer;

    TABLEID tbid[1];
    SHashObj *pKeepHashObj;  // SHashObj<suid, keep>

    // skyline
    SArray  *aSkyLine;
    int32_t  iSkyLine;
    TSDBKEY *pDKey;
    TSDBKEY  dKey;
  } ctx[1];
} SCompactor2;

typedef struct {
  STsdb    *tsdb;
  int32_t   fid;
  ETsdbOpType type;
  bool        force;
  SVATaskID taskid;
} SCompactArg;

typedef struct {
  int32_t   fid;
  SVATaskID taskId;
  int64_t   fileSize;
} SRetentionState;

struct SRetentionMonitor {
  int64_t startTimeSec;  // start time of seconds
  TARRAY2(SRetentionState) stateArr;
  int64_t totalSize;
  int64_t finishedSize;
  int64_t lastUpdateFinishedSizeTime;
  int32_t totalTasks;
  int8_t  killed;
};

int32_t tsdbDoRollup(SRTNer *rtner);
int32_t tsdbDoSsMigrate(SRTNer *rtner);
void    tsdbRetentionCancel(void *arg);
int32_t tsdbRetention(void *arg);

FORCE_INLINE bool tsdbRetentionTaskKilled(STsdb *tsdb) { return atomic_load_8(&tsdb->pRetentionMonitor->killed) != 0; }

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_VNODE_TSDB_INT_H_*/

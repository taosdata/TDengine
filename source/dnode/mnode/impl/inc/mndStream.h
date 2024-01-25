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

#ifndef _TD_MND_STREAM_H_
#define _TD_MND_STREAM_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamTransInfo {
  int64_t     startTime;
  int64_t     streamUid;
  const char *name;
  int32_t     transId;
} SStreamTransInfo;

// time to generated the checkpoint, if now() - checkpointTs >= tsCheckpointInterval, this checkpoint will be discard
// to avoid too many checkpoints for a taskk in the waiting list
typedef struct SCheckpointCandEntry {
  char *  pName;
  int64_t streamId;
  int64_t checkpointTs;
  int64_t checkpointId;
} SCheckpointCandEntry;

typedef struct SStreamTransMgmt {
  SHashObj *pDBTrans;
  SHashObj *pWaitingList;  // stream id list, of which timed checkpoint failed to be issued due to the trans conflict.
} SStreamTransMgmt;

typedef struct SStreamExecInfo {
  SArray          *pNodeList;
  int64_t          ts;  // snapshot ts
  SStreamTransMgmt transMgmt;
  SHashObj        *pTaskMap;
  SArray          *pTaskList;
  TdThreadMutex    lock;
  SHashObj        *pTransferStateStreams;
} SStreamExecInfo;

#define MND_STREAM_CREATE_NAME      "stream-create"
#define MND_STREAM_CHECKPOINT_NAME  "stream-checkpoint"
#define MND_STREAM_PAUSE_NAME       "stream-pause"
#define MND_STREAM_RESUME_NAME      "stream-resume"
#define MND_STREAM_DROP_NAME        "stream-drop"
#define MND_STREAM_TASK_RESET_NAME  "stream-task-reset"
#define MND_STREAM_TASK_UPDATE_NAME "stream-task-update"

extern SStreamExecInfo execInfo;

int32_t     mndInitStream(SMnode *pMnode);
void        mndCleanupStream(SMnode *pMnode);
SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName);
void        mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);
int32_t     mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t     mndPersistStream(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);

int32_t mndStreamRegisterTrans(STrans* pTrans, const char* pTransName, int64_t streamUid);
int32_t mndAddtoCheckpointWaitingList(SStreamObj *pStream, int64_t checkpointId);
bool    mndStreamTransConflictCheck(SMnode *pMnode, int64_t streamUid, const char *pTransName, bool lock);
int32_t mndStreamGetRelTrans(SMnode *pMnode, int64_t streamUid);

// for sma
// TODO refactor
int32_t mndDropStreamTasks(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);
int32_t mndPersistDropStreamLog(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream);

int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/

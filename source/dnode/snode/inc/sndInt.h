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

#ifndef _TD_SNODE_INT_H_
#define _TD_SNODE_INT_H_

#include "os.h"

#include "tlog.h"
#include "tmsg.h"
#include "tqueue.h"
#include "trpc.h"

#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  STREAM_STATUS__RUNNING = 1,
  STREAM_STATUS__STOPPED,
  STREAM_STATUS__CREATING,
  STREAM_STATUS__STOPING,
  STREAM_STATUS__RESTORING,
  STREAM_STATUS__DELETING,
};

enum {
  STREAM_TASK_STATUS__RUNNING = 1,
  STREAM_TASK_STATUS__STOP,
};

typedef struct {
  SHashObj* pHash;  // taskId -> streamTask
} SStreamMeta;

typedef struct SSnode {
  SStreamMeta* pMeta;
  SSnodeOpt    cfg;
} SSnode;

typedef struct {
  int64_t streamId;
  int32_t taskId;
  int32_t IdxInLevel;
  int32_t level;
} SStreamTaskInfo;

typedef struct {
  SStreamTaskInfo meta;
  int8_t          status;
  void*           executor;
  void*           stateStore;
  // storage handle
} SStreamTask;

int32_t sndCreateTask();
int32_t sndDropTaskOfStream(int64_t streamId);

int32_t sndStopTaskOfStream(int64_t streamId);
int32_t sndResumeTaskOfStream(int64_t streamId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SNODE_INT_H_*/

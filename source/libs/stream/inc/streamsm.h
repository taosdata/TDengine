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

#ifndef TDENGINE_STREAMSM_H
#define TDENGINE_STREAMSM_H

#include "tstream.h"

#ifdef __cplusplus
extern "C" {
#endif

// moore finite state machine for stream task
typedef struct SStreamTaskState {
  ETaskStatus state;
  char*       name;
} SStreamTaskState;

typedef enum EStreamTaskEvent {
  TASK_EVENT_INIT = 0x1,
  TASK_EVENT_START = 0x2,
  TASK_EVENT_STOP = 0x3,
  TASK_EVENT_GEN_CHECKPOINT = 0x4,
  TASK_EVENT_PAUSE = 0x5,
  TASK_EVENT_RESUME = 0x6,
  TASK_EVENT_HALT = 0x7,
  TASK_EVENT_TRANS_STATE = 0x8,
  TASK_EVENT_SCAN_TSDB = 0x9,
  TASK_EVENT_SCAN_WAL = 0x10,
} EStreamTaskEvent;

typedef int32_t (*__state_trans_fn)(SStreamTask*);

typedef struct STaskStateTrans {
  SStreamTaskState state;
  EStreamTaskEvent event;
  SStreamTaskState next;
  __state_trans_fn pAction;
} STaskStateTrans;

struct SStreamTaskSM {
  SStreamTaskState current;
  SArray*          pTransList;  // SArray<STaskStateTrans>
  int64_t          stateTs;
  SStreamTask*     pTask;
  STaskStateTrans* pActiveTrans;
};

typedef struct SStreamEventInfo {
  EStreamTaskEvent event;
  const char*      name;
  bool             isTrans;
} SStreamEventInfo;

SStreamTaskSM* streamCreateStateMachine(SStreamTask* pTask);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMSM_H

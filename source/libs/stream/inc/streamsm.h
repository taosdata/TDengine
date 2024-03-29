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
typedef int32_t (*__state_trans_fn)(SStreamTask*);
typedef int32_t (*__state_trans_succ_fn)(SStreamTask*);

typedef struct SAttachedEventInfo {
  ETaskStatus      status;  // required status that this event can be handled
  EStreamTaskEvent event;   // the delayed handled event
  void*            pParam;
  void*            pFn;
} SAttachedEventInfo;

typedef struct STaskStateTrans {
  bool                  autoInvokeEndFn;
  SStreamTaskState      state;
  EStreamTaskEvent      event;
  SStreamTaskState      next;
  __state_trans_fn      pAction;
  __state_trans_succ_fn pSuccAction;
  SAttachedEventInfo    attachEvent;
} STaskStateTrans;

struct SStreamTaskSM {
  SStreamTask*     pTask;
  STaskStateTrans* pActiveTrans;
  int64_t          startTs;
  SStreamTaskState current;
  struct {
    SStreamTaskState state;
    EStreamTaskEvent evt;
  } prev;
  // register the next handled event, if current state is not allowed to handle this event
  SArray* pWaitingEventList;
};

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMSM_H

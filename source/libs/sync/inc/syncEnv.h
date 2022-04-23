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

#ifndef _TD_LIBS_SYNC_ENV_H
#define _TD_LIBS_SYNC_ENV_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "taosdef.h"
#include "trpc.h"
#include "ttimer.h"

#define TIMER_MAX_MS 0x7FFFFFFF
#define ENV_TICK_TIMER_MS 1000
#define PING_TIMER_MS 1000
#define ELECT_TIMER_MS_MIN 500
#define ELECT_TIMER_MS_MAX (ELECT_TIMER_MS_MIN * 2)
#define ELECT_TIMER_MS_RANGE (ELECT_TIMER_MS_MAX - ELECT_TIMER_MS_MIN)
#define HEARTBEAT_TIMER_MS 100

#define EMPTY_RAFT_ID ((SRaftId){.addr = 0, .vgId = 0})

typedef struct SSyncEnv {
  // tick timer
  tmr_h             pEnvTickTimer;
  int32_t           envTickTimerMS;
  uint64_t          envTickTimerLogicClock;  // if use queue, should pass logic clock into queue item
  uint64_t          envTickTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpEnvTickTimer;  // Timer Fp
  uint64_t          envTickTimerCounter;

  // timer manager
  tmr_h pTimerManager;

  // other resources shared by SyncNodes
  // ...

} SSyncEnv;

extern SSyncEnv* gSyncEnv;

int32_t syncEnvStart();
int32_t syncEnvStop();
int32_t syncEnvStartTimer();
int32_t syncEnvStopTimer();

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_ENV_H*/

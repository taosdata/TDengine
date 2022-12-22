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

#include "syncInt.h"

#define TIMER_MAX_MS       0x7FFFFFFF
#define ENV_TICK_TIMER_MS  1000
#define PING_TIMER_MS      5000
#define ELECT_TIMER_MS_MIN 2500
#define HEARTBEAT_TIMER_MS 1000
#define HEARTBEAT_TICK_NUM 20

typedef struct SSyncEnv {
  uint8_t isStart;

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

SSyncEnv* syncEnv();
bool      syncIsInit();

int64_t    syncNodeAdd(SSyncNode* pNode);
void       syncNodeRemove(int64_t rid);
SSyncNode* syncNodeAcquire(int64_t rid);
void       syncNodeRelease(SSyncNode* pNode);

int64_t           syncHbTimerDataAdd(SSyncHbTimerData* pData);
void              syncHbTimerDataRemove(int64_t rid);
SSyncHbTimerData* syncHbTimerDataAcquire(int64_t rid);
void              syncHbTimerDataRelease(SSyncHbTimerData* pData);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_ENV_H*/

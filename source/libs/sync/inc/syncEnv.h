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

typedef struct SSyncEnv {
  tmr_h pEnvTickTimer;
  tmr_h pTimerManager;
  char  name[128];

} SSyncEnv;

extern SSyncEnv* gSyncEnv;

int32_t syncEnvStart();

int32_t syncEnvStop();

tmr_h syncEnvStartTimer(TAOS_TMR_CALLBACK fp, int mseconds, void* param);

void syncEnvStopTimer(tmr_h* pTimer);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_ENV_H*/

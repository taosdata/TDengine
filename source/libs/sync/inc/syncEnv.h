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

typedef struct SSyncEnv {
  void *pTimer;
  void *pTimerManager;
} SSyncEnv;

int32_t syncEnvStart();

int32_t syncEnvStop();

static int32_t doSyncEnvStart(SSyncEnv *pSyncEnv);

static int32_t doSyncEnvStop(SSyncEnv *pSyncEnv);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_ENV_H*/

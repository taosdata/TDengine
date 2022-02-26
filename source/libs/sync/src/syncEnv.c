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

#include "syncEnv.h"
#include <assert.h>

SSyncEnv *gSyncEnv = NULL;

int32_t syncEnvStart() {
  int32_t ret;
  gSyncEnv = (SSyncEnv *)malloc(sizeof(SSyncEnv));
  assert(gSyncEnv != NULL);
  ret = doSyncEnvStart(gSyncEnv);
  return ret;
}

int32_t syncEnvStop() {
  int32_t ret = doSyncEnvStop(gSyncEnv);
  return ret;
}

static int32_t doSyncEnvStart(SSyncEnv *pSyncEnv) { return 0; }

static int32_t doSyncEnvStop(SSyncEnv *pSyncEnv) { return 0; }

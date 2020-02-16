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

#ifndef TDENGINE_DNODE_SYSTEM_H
#define TDENGINE_DNODE_SYSTEM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "dnode.h"

typedef enum {
  TSDB_DNODE_RUN_STATUS_INITIALIZE,
  TSDB_DNODE_RUN_STATUS_RUNING,
  TSDB_DNODE_RUN_STATUS_STOPPED
} SDnodeRunStatus;

extern int32_t (*dnodeInitPeers)(int32_t numOfThreads);
extern int32_t (*dnodeCheckSystem)();
extern int32_t (*dnodeInitStorage)();
extern void (*dnodeCleanupStorage)();
extern void (*dnodeParseParameterK)();
extern int32_t tsMaxQueues;


int32_t dnodeInitSystem();
void dnodeCleanUpSystem();
void dnodeInitPlugins();

SDnodeRunStatus dnodeGetRunStatus();
void dnodeSetRunStatus(SDnodeRunStatus status);
void dnodeCheckDataDirOpenned(const char *dir);
void dnodeLockVnodes();
void dnodeUnLockVnodes();

#ifdef __cplusplus
}
#endif

#endif

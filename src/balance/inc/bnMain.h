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

#ifndef TDENGINE_BALANCE_MAIN_H
#define TDENGINE_BALANCE_MAIN_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnodeInt.h"
#include "mnodeDnode.h"

typedef struct {
  int32_t     size;
  int32_t     maxSize;
  SDnodeObj **list;
} SBnDnodes;

typedef struct {
  void *          timer;
  bool            stop;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  pthread_t       thread;
} SBnThread;

typedef struct {
  pthread_mutex_t mutex;
} SBnMgmt;

int32_t bnInit();
void    bnCleanUp();
bool    bnStart();
void    bnCheckStatus();
void    bnCheckModules();

extern SBnDnodes tsBnDnodes;
extern void *tsMnodeTmr;

#ifdef __cplusplus
}
#endif

#endif

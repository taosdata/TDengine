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

#ifndef TDENGINE_TMODULE_H
#define TDENGINE_TMODULE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

enum _module {
  TSDB_MOD_MGMT,
  TSDB_MOD_HTTP,
  TSDB_MOD_MONITOR,
  TSDB_MOD_MAX
};

#define tsetModuleStatus(mod) \
  { tsModuleStatus |= (1 << mod); }
#define tclearModuleStatus(mod) \
  { tsModuleStatus &= ~(1 << mod); }

typedef struct {
  char  *name;
  int  (*initFp)();
  void (*cleanUpFp)();
  int  (*startFp)();
  void (*stopFp)();
  int    num;
  int    curNum;
  int    equalVnodeNum;
} SModule;

extern uint32_t tsModuleStatus;
extern SModule tsModule[];

#ifdef __cplusplus
}
#endif

#endif

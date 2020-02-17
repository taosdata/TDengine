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

#ifndef TDENGINE_DNODE_SHELL_H
#define TDENGINE_DNODE_SHELL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdint.h>
#include "dnode.h"

typedef struct {
  int      sid;
  uint32_t ip;
  uint16_t port;
  int32_t  count;             // track the number of imports
  int32_t  code;              // track the code of imports
  int32_t  numOfTotalPoints;  // track the total number of points imported
  void    *thandle;           // handle from TAOS layer
  void    *qhandle;
} SShellObj;

int32_t dnodeInitShell();

void dnodeCleanupShell();

//SDnodeStatisInfo dnodeGetStatisInfo()

#ifdef __cplusplus
}
#endif

#endif

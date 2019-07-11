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

#ifndef __MONITOR_SYSTEM_H__
#define __MONITOR_SYSTEM_H__

#include <stdbool.h>

int  monitorInitSystem();
int  monitorStartSystem();
void monitorStopSystem();
void monitorCleanUpSystem();

typedef struct {
  int selectReqNum;
  int insertReqNum;
  int httpReqNum;
} SCountInfo;

extern void (*monitorCountReqFp)(SCountInfo *info);

#endif
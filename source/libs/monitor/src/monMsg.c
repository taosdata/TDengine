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

#define _DEFAULT_SOURCE
#include "monInt.h"
#include "tcoding.h"
#include "tencode.h"

int32_t tSerializeSMonMmInfo(void *buf, int32_t bufLen, SMonMmInfo *pInfo);
int32_t tDeserializeSMonMmInfo(void *buf, int32_t bufLen, SMonMmInfo *pInfo);
void    tFreeSMonMmInfo(SMonMmInfo *pInfo) {
  taosArrayDestroy(pInfo->logs.logs);
  pInfo->logs.logs = NULL;
}

int32_t tSerializeSMonVmInfo(void *buf, int32_t bufLen, SMonVmInfo *pInfo);
int32_t tDeserializeSMonVMmInfo(void *buf, int32_t bufLen, SMonVmInfo *pInfo);
void    tFreeSMonVmInfo(SMonVmInfo *pInfo) {
  taosArrayDestroy(pInfo->logs.logs);
  pInfo->logs.logs = NULL;
}

int32_t tSerializeSMonQmInfo(void *buf, int32_t bufLen, SMonQmInfo *pInfo);
int32_t tDeserializeSMonQMmInfo(void *buf, int32_t bufLen, SMonQmInfo *pInfo);
void    tFreeSMonQmInfo(SMonQmInfo *pInfo) {
  taosArrayDestroy(pInfo->logs.logs);
  pInfo->logs.logs = NULL;
}

int32_t tSerializeSMonSmInfo(void *buf, int32_t bufLen, SMonSmInfo *pInfo);
int32_t tDeserializeSMonSmInfo(void *buf, int32_t bufLen, SMonSmInfo *pInfo);
void    tFreeSMonSmInfo(SMonSmInfo *pInfo) {
  taosArrayDestroy(pInfo->logs.logs);
  pInfo->logs.logs = NULL;
}

int32_t tSerializeSMonBmInfo(void *buf, int32_t bufLen, SMonBmInfo *pInfo);
int32_t tDeserializeSMonBmInfo(void *buf, int32_t bufLen, SMonBmInfo *pInfo);
void    tFreeSMonBmInfo(SMonBmInfo *pInfo) {
  taosArrayDestroy(pInfo->logs.logs);
  pInfo->logs.logs = NULL;
}
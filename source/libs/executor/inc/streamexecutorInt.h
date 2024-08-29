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
#ifndef STREAM_EXECUTORINT_H
#define STREAM_EXECUTORINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "tutil.h"

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type);
bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo);
void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H

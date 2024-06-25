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

#include "executorInt.h"

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type) {
  if (type != STREAM_GET_ALL && type != STREAM_CHECKPOINT) {
    pBasicInfo->updateOperatorInfo = true;
  }
}

bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo) {
  return pBasicInfo->updateOperatorInfo;
}

void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo) {
  pBasicInfo->updateOperatorInfo = false;
}

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
#include "tdatablock.h"

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

int32_t initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  pBasicInfo->primaryPkIndex = -1;
  pBasicInfo->updateOperatorInfo = false;
  pBasicInfo->pEventInfo = taosArrayInit(4, sizeof(SSessionKey));
  if (pBasicInfo->pEventInfo == NULL) {
    return terrno;
  }
  return createSpecialDataBlock(STREAM_EVENT_OPEN_WINDOW, &pBasicInfo->pEventRes);
}

void destroyStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  blockDataDestroy(pBasicInfo->pEventRes);
  pBasicInfo->pEventRes = NULL;
  taosArrayDestroy(pBasicInfo->pEventInfo);
  pBasicInfo->pEventInfo = NULL;
}

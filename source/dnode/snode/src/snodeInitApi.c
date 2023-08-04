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

#include "storageapi.h"
#include "tstreamUpdate.h"
#include "streamState.h"

static void initStateStoreAPI(SStateStore* pStore);
static void initFunctionStateStore(SFunctionStateStore* pStore);

void initStreamStateAPI(SStorageAPI* pAPI) {
  initStateStoreAPI(&pAPI->stateStore);
  initFunctionStateStore(&pAPI->functionStore);
}

void initStateStoreAPI(SStateStore* pStore) {
  pStore->streamFileStateInit = streamFileStateInit;
  pStore->updateInfoDestoryColseWinSBF = updateInfoDestoryColseWinSBF;

  pStore->streamStateGetByPos = streamStateGetByPos;

  pStore->streamStatePutParName = streamStatePutParName;
  pStore->streamStateGetParName = streamStateGetParName;

  pStore->streamStateAddIfNotExist = streamStateAddIfNotExist;
  pStore->streamStateReleaseBuf = streamStateReleaseBuf;
  pStore->streamStateFreeVal = streamStateFreeVal;

  pStore->streamStatePut = streamStatePut;
  pStore->streamStateGet = streamStateGet;
  pStore->streamStateCheck = streamStateCheck;
  pStore->streamStateGetByPos = streamStateGetByPos;
  pStore->streamStateDel = streamStateDel;
  pStore->streamStateClear = streamStateClear;
  pStore->streamStateSaveInfo = streamStateSaveInfo;
  pStore->streamStateGetInfo = streamStateGetInfo;
  pStore->streamStateSetNumber = streamStateSetNumber;

  pStore->streamStateFillPut = streamStateFillPut;
  pStore->streamStateFillGet = streamStateFillGet;
  pStore->streamStateFillDel = streamStateFillDel;

  pStore->streamStateCurNext = streamStateCurNext;
  pStore->streamStateCurPrev = streamStateCurPrev;

  pStore->streamStateGetAndCheckCur = streamStateGetAndCheckCur;
  pStore->streamStateSeekKeyNext = streamStateSeekKeyNext;
  pStore->streamStateFillSeekKeyNext = streamStateFillSeekKeyNext;
  pStore->streamStateFillSeekKeyPrev = streamStateFillSeekKeyPrev;
  pStore->streamStateFreeCur = streamStateFreeCur;

  pStore->streamStateGetGroupKVByCur = streamStateGetGroupKVByCur;
  pStore->streamStateGetKVByCur = streamStateGetKVByCur;

  pStore->streamStateSessionAddIfNotExist = streamStateSessionAddIfNotExist;
  pStore->streamStateSessionPut = streamStateSessionPut;
  pStore->streamStateSessionGet = streamStateSessionGet;
  pStore->streamStateSessionDel = streamStateSessionDel;
  pStore->streamStateSessionClear = streamStateSessionClear;
  pStore->streamStateSessionGetKVByCur = streamStateSessionGetKVByCur;
  pStore->streamStateStateAddIfNotExist = streamStateStateAddIfNotExist;
  pStore->streamStateSessionGetKeyByRange = streamStateSessionGetKeyByRange;

  pStore->updateInfoInit = updateInfoInit;
  pStore->updateInfoFillBlockData = updateInfoFillBlockData;
  pStore->updateInfoIsUpdated = updateInfoIsUpdated;
  pStore->updateInfoIsTableInserted = updateInfoIsTableInserted;
  pStore->updateInfoDestroy = updateInfoDestroy;
  pStore->windowSBfDelete = windowSBfDelete;
  pStore->windowSBfAdd = windowSBfAdd;

  pStore->updateInfoInitP = updateInfoInitP;
  pStore->updateInfoAddCloseWindowSBF = updateInfoAddCloseWindowSBF;
  pStore->updateInfoDestoryColseWinSBF = updateInfoDestoryColseWinSBF;
  pStore->updateInfoSerialize = updateInfoSerialize;
  pStore->updateInfoDeserialize = updateInfoDeserialize;

  pStore->streamStateSessionSeekKeyNext = streamStateSessionSeekKeyNext;
  pStore->streamStateSessionSeekKeyCurrentPrev = streamStateSessionSeekKeyCurrentPrev;
  pStore->streamStateSessionSeekKeyCurrentNext = streamStateSessionSeekKeyCurrentNext;

  pStore->streamFileStateInit = streamFileStateInit;

  pStore->streamFileStateDestroy = streamFileStateDestroy;
  pStore->streamFileStateClear = streamFileStateClear;
  pStore->needClearDiskBuff = needClearDiskBuff;

  pStore->streamStateOpen = streamStateOpen;
  pStore->streamStateClose = streamStateClose;
  pStore->streamStateBegin = streamStateBegin;
  pStore->streamStateCommit = streamStateCommit;
  pStore->streamStateDestroy= streamStateDestroy;
  pStore->streamStateDeleteCheckPoint = streamStateDeleteCheckPoint;
  pStore->streamStateReloadInfo = streamStateReloadInfo;
}

void initFunctionStateStore(SFunctionStateStore* pStore) {
  pStore->streamStateFuncPut = streamStateFuncPut;
  pStore->streamStateFuncGet = streamStateFuncGet;
}
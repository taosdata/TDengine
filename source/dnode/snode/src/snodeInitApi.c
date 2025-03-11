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
#include "streamState.h"
#include "tstreamUpdate.h"

static void initStateStoreAPI(SStateStore* pStore);
static void initFunctionStateStore(SFunctionStateStore* pStore);

void initStreamStateAPI(SStorageAPI* pAPI) {
  initStateStoreAPI(&pAPI->stateStore);
  initFunctionStateStore(&pAPI->functionStore);
}

void initStateStoreAPI(SStateStore* pStore) {
  pStore->streamFileStateInit = streamFileStateInit;
  pStore->updateInfoDestoryColseWinSBF = updateInfoDestoryColseWinSBF;

  pStore->streamStatePutParName = streamStatePutParName;
  pStore->streamStateGetParName = streamStateGetParName;
  pStore->streamStateDeleteParName = streamStateDeleteParName;
  pStore->streamStateSetParNameInvalid = streamStateSetParNameInvalid;

  pStore->streamStateAddIfNotExist = streamStateAddIfNotExist;
  pStore->streamStateReleaseBuf = streamStateReleaseBuf;
  pStore->streamStateClearBuff = streamStateClearBuff;
  pStore->streamStateFreeVal = streamStateFreeVal;

  pStore->streamStatePut = streamStatePut;
  pStore->streamStateGet = streamStateGet;
  pStore->streamStateCheck = streamStateCheck;
  pStore->streamStateGetByPos = streamStateGetByPos;
  pStore->streamStateDel = streamStateDel;
  pStore->streamStateDelByGroupId = streamStateDelByGroupId;
  pStore->streamStateClear = streamStateClear;
  pStore->streamStateSaveInfo = streamStateSaveInfo;
  pStore->streamStateGetInfo = streamStateGetInfo;
  pStore->streamStateGetNumber = streamStateGetNumber;
  pStore->streamStateDeleteInfo = streamStateDeleteInfo;
  pStore->streamStateSetNumber = streamStateSetNumber;
  pStore->streamStateGetPrev = streamStateGetPrev;
  pStore->streamStateGetAllPrev = streamStateGetAllPrev;

  pStore->streamStateFillPut = streamStateFillPut;
  pStore->streamStateFillGet = streamStateFillGet;
  pStore->streamStateFillAddIfNotExist = streamStateFillAddIfNotExist;
  pStore->streamStateFillDel = streamStateFillDel;
  pStore->streamStateFillGetNext = streamStateFillGetNext;
  pStore->streamStateFillGetPrev = streamStateFillGetPrev;

  pStore->streamStateCurNext = streamStateCurNext;
  pStore->streamStateCurPrev = streamStateCurPrev;

  pStore->streamStateGetAndCheckCur = streamStateGetAndCheckCur;
  pStore->streamStateSeekKeyNext = streamStateSeekKeyNext;
  pStore->streamStateFillSeekKeyNext = streamStateFillSeekKeyNext;
  pStore->streamStateFillSeekKeyPrev = streamStateFillSeekKeyPrev;
  pStore->streamStateFreeCur = streamStateFreeCur;

  pStore->streamStateFillGetGroupKVByCur = streamStateFillGetGroupKVByCur;
  pStore->streamStateGetKVByCur = streamStateGetKVByCur;

  pStore->streamStateClearExpiredState = streamStateClearExpiredState;
  pStore->streamStateClearExpiredSessionState = streamStateClearExpiredSessionState;
  pStore->streamStateSetRecFlag = streamStateSetRecFlag;
  pStore->streamStateGetRecFlag = streamStateGetRecFlag;

  pStore->streamStateSessionAddIfNotExist = streamStateSessionAddIfNotExist;
  pStore->streamStateSessionPut = streamStateSessionPut;
  pStore->streamStateSessionGet = streamStateSessionGet;
  pStore->streamStateSessionDel = streamStateSessionDel;
  pStore->streamStateSessionReset = streamStateSessionReset;
  pStore->streamStateSessionClear = streamStateSessionClear;
  pStore->streamStateSessionGetKVByCur = streamStateSessionGetKVByCur;
  pStore->streamStateStateAddIfNotExist = streamStateStateAddIfNotExist;
  pStore->streamStateSessionGetKeyByRange = streamStateSessionGetKeyByRange;
  pStore->streamStateCountGetKeyByRange = streamStateCountGetKeyByRange;
  pStore->streamStateSessionAllocWinBuffByNextPosition = streamStateSessionAllocWinBuffByNextPosition;
  pStore->streamStateSessionSaveToDisk = streamStateSessionSaveToDisk;
  pStore->streamStateFlushReaminInfoToDisk = streamStateFlushReaminInfoToDisk;
  pStore->streamStateSessionDeleteAll = streamStateSessionDeleteAll;

  pStore->updateInfoInit = updateInfoInit;
  pStore->updateInfoFillBlockData = updateInfoFillBlockData;
  pStore->updateInfoIsUpdated = updateInfoIsUpdated;
  pStore->updateInfoIsTableInserted = updateInfoIsTableInserted;
  pStore->updateInfoDestroy = updateInfoDestroy;
  pStore->windowSBfDelete = windowSBfDelete;
  pStore->windowSBfAdd = windowSBfAdd;
  pStore->isIncrementalTimeStamp = isIncrementalTimeStamp;

  pStore->updateInfoInitP = updateInfoInitP;
  pStore->updateInfoAddCloseWindowSBF = updateInfoAddCloseWindowSBF;
  pStore->updateInfoDestoryColseWinSBF = updateInfoDestoryColseWinSBF;
  pStore->updateInfoSerialize = updateInfoSerialize;
  pStore->updateInfoDeserialize = updateInfoDeserialize;

  pStore->streamStateSessionSeekKeyPrev = streamStateSessionSeekKeyPrev;
  pStore->streamStateSessionSeekKeyNext = streamStateSessionSeekKeyNext;
  pStore->streamStateCountSeekKeyPrev = streamStateCountSeekKeyPrev;
  pStore->streamStateSessionSeekKeyCurrentPrev = streamStateSessionSeekKeyCurrentPrev;
  pStore->streamStateSessionSeekKeyCurrentNext = streamStateSessionSeekKeyCurrentNext;

  pStore->streamStateGroupPut = streamStateGroupPut;
  pStore->streamStateGroupGetCur = streamStateGroupGetCur;
  pStore->streamStateGroupCurNext = streamStateGroupCurNext;
  pStore->streamStateGroupGetKVByCur = streamStateGroupGetKVByCur;

  pStore->streamFileStateDestroy = streamFileStateDestroy;
  pStore->streamFileStateClear = streamFileStateClear;
  pStore->needClearDiskBuff = needClearDiskBuff;

  pStore->streamStateGetAndSetTsData = streamStateGetAndSetTsData;
  pStore->streamStateTsDataCommit = streamStateTsDataCommit;
  pStore->streamStateInitTsDataState = streamStateInitTsDataState;
  pStore->streamStateDestroyTsDataState = streamStateDestroyTsDataState;
  pStore->streamStateRecoverTsData = streamStateRecoverTsData;
  pStore->streamStateReloadTsDataState = streamStateReloadTsDataState;
  pStore->streamStateMergeAndSaveScanRange = streamStateMergeAndSaveScanRange;
  pStore->streamStateMergeAllScanRange = streamStateMergeAllScanRange;
  pStore->streamStatePopScanRange = streamStatePopScanRange;

  pStore->streamStateCheckSessionState = streamStateCheckSessionState;
  pStore->streamStateGetLastStateCur = streamStateGetLastStateCur;
  pStore->streamStateLastStateCurNext = streamStateLastStateCurNext;
  pStore->streamStateNLastStateGetKVByCur = streamStateNLastStateGetKVByCur;
  pStore->streamStateGetLastSessionStateCur = streamStateGetLastSessionStateCur;
  pStore->streamStateLastSessionStateCurNext = streamStateLastSessionStateCurNext;
  pStore->streamStateNLastSessionStateGetKVByCur = streamStateNLastSessionStateGetKVByCur;

  pStore->streamStateOpen = streamStateOpen;
  pStore->streamStateRecalatedOpen = streamStateRecalatedOpen;
  pStore->streamStateClose = streamStateClose;
  pStore->streamStateBegin = streamStateBegin;
  pStore->streamStateCommit = streamStateCommit;
  pStore->streamStateDestroy = streamStateDestroy;
  pStore->streamStateReloadInfo = streamStateReloadInfo;
  pStore->streamStateCopyBackend = streamStateCopyBackend;
}

void initFunctionStateStore(SFunctionStateStore* pStore) {
  pStore->streamStateFuncPut = streamStateFuncPut;
  pStore->streamStateFuncGet = streamStateFuncGet;
}

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

#include "meta.h"
#include "storageapi.h"
#include "tstreamUpdate.h"
#include "vnodeInt.h"

static void initTsdbReaderAPI(TsdReader* pReader);
static void initMetadataAPI(SStoreMeta* pMeta);
static void initTqAPI(SStoreTqReader* pTq);
static void initStateStoreAPI(SStateStore* pStore);
static void initMetaReaderAPI(SStoreMetaReader* pMetaReader);
static void initMetaFilterAPI(SMetaDataFilterAPI* pFilter);
static void initFunctionStateStore(SFunctionStateStore* pStore);
static void initCacheFn(SStoreCacheReader* pCache);
static void initSnapshotFn(SStoreSnapshotFn* pSnapshot);

void initStorageAPI(SStorageAPI* pAPI) {
  initTsdbReaderAPI(&pAPI->tsdReader);
  initMetadataAPI(&pAPI->metaFn);
  initStateStoreAPI(&pAPI->stateStore);
  initMetaReaderAPI(&pAPI->metaReaderFn);
  initMetaFilterAPI(&pAPI->metaFilter);
  initTqAPI(&pAPI->tqReaderFn);
  initFunctionStateStore(&pAPI->functionStore);
  initCacheFn(&pAPI->cacheFn);
  initSnapshotFn(&pAPI->snapshotFn);
}

void initTsdbReaderAPI(TsdReader* pReader) {
  pReader->tsdReaderOpen = (int32_t(*)(void*, SQueryTableDataCond*, void*, int32_t, SSDataBlock*, void**, const char*,
                                       SHashObj**))tsdbReaderOpen2;
  pReader->tsdReaderClose = tsdbReaderClose2;

  pReader->tsdNextDataBlock = tsdbNextDataBlock2;

  pReader->tsdReaderRetrieveDataBlock = tsdbRetrieveDataBlock2;
  pReader->tsdReaderReleaseDataBlock = tsdbReleaseDataBlock2;

  pReader->tsdReaderRetrieveBlockSMAInfo = tsdbRetrieveDatablockSMA2;

  pReader->tsdReaderNotifyClosing = tsdbReaderSetCloseFlag;
  pReader->tsdReaderResetStatus = tsdbReaderReset2;

  pReader->tsdReaderGetDataBlockDistInfo = tsdbGetFileBlocksDistInfo2;
  pReader->tsdReaderGetNumOfInMemRows = tsdbGetNumOfRowsInMemTable2;  // todo this function should be moved away

  pReader->tsdSetQueryTableList = tsdbSetTableList2;
  pReader->tsdSetReaderTaskId = (void (*)(void*, const char*))tsdbReaderSetId2;

  pReader->tsdSetFilesetDelimited = (void (*)(void*))tsdbSetFilesetDelimited;
  pReader->tsdSetSetNotifyCb = (void (*)(void*, TsdReaderNotifyCbFn, void*))tsdbReaderSetNotifyCb;
}

void initMetadataAPI(SStoreMeta* pMeta) {
  pMeta->isTableExisted = metaIsTableExist;

  pMeta->openTableMetaCursor = metaOpenTbCursor;
  pMeta->closeTableMetaCursor = metaCloseTbCursor;
  pMeta->pauseTableMetaCursor = metaPauseTbCursor;
  pMeta->resumeTableMetaCursor = metaResumeTbCursor;
  pMeta->cursorNext = metaTbCursorNext;
  pMeta->cursorPrev = metaTbCursorPrev;

  pMeta->getBasicInfo = vnodeGetInfo;
  pMeta->getNumOfChildTables = metaGetStbStats;

  pMeta->getChildTableList = vnodeGetCtbIdList;

  pMeta->storeGetIndexInfo = vnodeGetIdx;
  pMeta->getInvertIndex = vnodeGetIvtIdx;

  pMeta->extractTagVal = (const void* (*)(const void*, int16_t, STagVal*))metaGetTableTagVal;
  pMeta->getTableTags = metaGetTableTags;
  pMeta->getTableTagsByUid = metaGetTableTagsByUids;

  pMeta->getTableUidByName = metaGetTableUidByName;
  pMeta->getTableTypeByName = metaGetTableTypeByName;
  pMeta->getTableNameByUid = metaGetTableNameByUid;

  pMeta->getTableSchema = vnodeGetTableSchema;
  pMeta->storeGetTableList = vnodeGetTableList;

  pMeta->getCachedTableList = metaGetCachedTableUidList;
  pMeta->putCachedTableList = metaUidFilterCachePut;

  pMeta->metaGetCachedTbGroup = metaGetCachedTbGroup;
  pMeta->metaPutTbGroupToCache = metaPutTbGroupToCache;

  pMeta->openCtbCursor = metaOpenCtbCursor;
  pMeta->resumeCtbCursor = metaResumeCtbCursor;
  pMeta->pauseCtbCursor = metaPauseCtbCursor;
  pMeta->closeCtbCursor = metaCloseCtbCursor;
  pMeta->ctbCursorNext = metaCtbCursorNext;
}

void initTqAPI(SStoreTqReader* pTq) {
  pTq->tqReaderOpen = tqReaderOpen;
  pTq->tqReaderSetColIdList = tqReaderSetColIdList;

  pTq->tqReaderClose = tqReaderClose;
  pTq->tqReaderSeek = tqReaderSeek;
  pTq->tqRetrieveBlock = tqRetrieveDataBlock;

  pTq->tqReaderNextBlockInWal = tqNextBlockInWal;

  pTq->tqNextBlockImpl = tqNextBlockImpl;  // todo remove it

  pTq->tqReaderAddTables = tqReaderAddTbUidList;
  pTq->tqReaderSetQueryTableList = tqReaderSetTbUidList;

  pTq->tqReaderRemoveTables = tqReaderRemoveTbUidList;

  pTq->tqReaderIsQueriedTable = tqReaderIsQueriedTable;
  pTq->tqReaderCurrentBlockConsumed = tqCurrentBlockConsumed;

  pTq->tqReaderGetWalReader = tqGetWalReader;              // todo remove it
  pTq->tqReaderRetrieveTaosXBlock = tqRetrieveTaosxBlock;  // todo remove it

  pTq->tqReaderSetSubmitMsg = tqReaderSetSubmitMsg;  // todo remove it
  pTq->tqGetResultBlock = tqGetResultBlock;

  pTq->tqReaderNextBlockFilterOut = tqNextDataBlockFilterOut;
  pTq->tqGetResultBlockTime = tqGetResultBlockTime;

  pTq->tqGetStreamExecProgress = tqGetStreamExecInfo;
  }

void initStateStoreAPI(SStateStore* pStore) {
  pStore->streamFileStateInit = streamFileStateInit;
  pStore->updateInfoDestoryColseWinSBF = updateInfoDestoryColseWinSBF;

  pStore->streamStatePutParName = streamStatePutParName;
  pStore->streamStateGetParName = streamStateGetParName;

  pStore->streamStateAddIfNotExist = streamStateAddIfNotExist;
  pStore->streamStateReleaseBuf = streamStateReleaseBuf;
  pStore->streamStateClearBuff = streamStateClearBuff;
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
  pStore->streamStateSessionReset = streamStateSessionReset;
  pStore->streamStateSessionClear = streamStateSessionClear;
  pStore->streamStateSessionGetKVByCur = streamStateSessionGetKVByCur;
  pStore->streamStateStateAddIfNotExist = streamStateStateAddIfNotExist;
  pStore->streamStateSessionGetKeyByRange = streamStateSessionGetKeyByRange;
  pStore->streamStateCountGetKeyByRange = streamStateCountGetKeyByRange;
  pStore->streamStateSessionAllocWinBuffByNextPosition = streamStateSessionAllocWinBuffByNextPosition;

  pStore->streamStateCountWinAddIfNotExist = streamStateCountWinAddIfNotExist;
  pStore->streamStateCountWinAdd = streamStateCountWinAdd;

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

  pStore->streamStateSessionSeekKeyNext = streamStateSessionSeekKeyNext;
  pStore->streamStateCountSeekKeyPrev = streamStateCountSeekKeyPrev;
  pStore->streamStateSessionSeekKeyCurrentPrev = streamStateSessionSeekKeyCurrentPrev;
  pStore->streamStateSessionSeekKeyCurrentNext = streamStateSessionSeekKeyCurrentNext;

  pStore->streamFileStateDestroy = streamFileStateDestroy;
  pStore->streamFileStateClear = streamFileStateClear;
  pStore->needClearDiskBuff = needClearDiskBuff;

  pStore->streamStateOpen = streamStateOpen;
  pStore->streamStateClose = streamStateClose;
  pStore->streamStateBegin = streamStateBegin;
  pStore->streamStateCommit = streamStateCommit;
  pStore->streamStateDestroy = streamStateDestroy;
  pStore->streamStateDeleteCheckPoint = streamStateDeleteCheckPoint;
  pStore->streamStateReloadInfo = streamStateReloadInfo;
  pStore->streamStateCopyBackend = streamStateCopyBackend;
}

void initMetaReaderAPI(SStoreMetaReader* pMetaReader) {
  pMetaReader->initReader = _metaReaderInit;
  pMetaReader->clearReader = metaReaderClear;

  pMetaReader->getTableEntryByUid = metaReaderGetTableEntryByUid;

  pMetaReader->getEntryGetUidCache = metaReaderGetTableEntryByUidCache;
  pMetaReader->getTableEntryByName = metaGetTableEntryByName;

  pMetaReader->readerReleaseLock = metaReaderReleaseLock;
}

void initMetaFilterAPI(SMetaDataFilterAPI* pFilter) {
  pFilter->metaFilterCreateTime = metaFilterCreateTime;
  pFilter->metaFilterTableIds = metaFilterTableIds;
  pFilter->metaFilterTableName = metaFilterTableName;
  pFilter->metaFilterTtl = metaFilterTtl;
}

void initFunctionStateStore(SFunctionStateStore* pStore) {
  pStore->streamStateFuncPut = streamStateFuncPut;
  pStore->streamStateFuncGet = streamStateFuncGet;
}

void initCacheFn(SStoreCacheReader* pCache) {
  pCache->openReader = tsdbCacherowsReaderOpen;
  pCache->closeReader = tsdbCacherowsReaderClose;
  pCache->retrieveRows = tsdbRetrieveCacheRows;
  pCache->reuseReader = tsdbReuseCacherowsReader;
}

void initSnapshotFn(SStoreSnapshotFn* pSnapshot) {
  pSnapshot->setForSnapShot = setForSnapShot;
  pSnapshot->destroySnapshot = destroySnapContext;
  pSnapshot->getMetaTableInfoFromSnapshot = getMetaTableInfoFromSnapshot;
  pSnapshot->getTableInfoFromSnapshot = getTableInfoFromSnapshot;
}

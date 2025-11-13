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
#include "vnodeInt.h"

static void initTsdbReaderAPI(TsdReader* pReader);
static void initMetadataAPI(SStoreMeta* pMeta);
static void initTqAPI(SStoreTqReader* pTq);
static void initMetaReaderAPI(SStoreMetaReader* pMetaReader);
static void initMetaFilterAPI(SMetaDataFilterAPI* pFilter);
static void initCacheFn(SStoreCacheReader* pCache);
static void initSnapshotFn(SStoreSnapshotFn* pSnapshot);

void initStorageAPI(SStorageAPI* pAPI) {
  initTsdbReaderAPI(&pAPI->tsdReader);
  initMetadataAPI(&pAPI->metaFn);
  initMetaReaderAPI(&pAPI->metaReaderFn);
  initMetaFilterAPI(&pAPI->metaFilter);
  initTqAPI(&pAPI->tqReaderFn);
  initCacheFn(&pAPI->cacheFn);
  initSnapshotFn(&pAPI->snapshotFn);
}

void initTsdbReaderAPI(TsdReader* pReader) {
  pReader->tsdReaderOpen = tsdbReaderOpen2;
  pReader->tsdReaderClose = tsdbReaderClose2;

  pReader->tsdNextDataBlock = tsdbNextDataBlock2;

  pReader->tsdReaderRetrieveDataBlock = tsdbRetrieveDataBlock2;
  pReader->tsdReaderReleaseDataBlock = tsdbReleaseDataBlock2;

  pReader->tsdReaderRetrieveBlockSMAInfo = tsdbRetrieveDatablockSMA2;

  pReader->tsdReaderNotifyClosing = tsdbReaderSetCloseFlag;
  pReader->tsdReaderResetStatus = tsdbReaderReset2;

  pReader->tsdReaderGetDataBlockDistInfo = tsdbGetFileBlocksDistInfo2;
  pReader->tsdReaderGetDatablock = tsdbGetDataBlock;
  pReader->tsdReaderSetDatablock = tsdbSetDataBlock;
  pReader->tsdReaderGetNumOfInMemRows = tsdbGetNumOfRowsInMemTable2;  // todo this function should be moved away

  pReader->tsdSetQueryTableList = tsdbSetTableList2;
  pReader->tsdSetReaderTaskId = tsdbReaderSetId;

  pReader->tsdSetFilesetDelimited = (void (*)(void*))tsdbSetFilesetDelimited;
  pReader->tsdSetSetNotifyCb = (void (*)(void*, TsdReaderNotifyCbFn, void*))tsdbReaderSetNotifyCb;

  // file set iterate
  pReader->fileSetReaderOpen = tsdbFileSetReaderOpen;
  pReader->fileSetReadNext = tsdbFileSetReaderNext;
  pReader->fileSetGetEntryField = tsdbFileSetGetEntryField;
  pReader->fileSetReaderClose = tsdbFileSetReaderClose;

  // retrieve first/last ts for each table
  pReader->tsdCreateFirstLastTsIter = tsdbCreateFirstLastTsIter;
  pReader->tsdNextFirstLastTsBlock = tsdbNextFirstLastTsBlock;
  pReader->tsdDestroyFirstLastTsIter = tsdbDestroyFirstLastTsIter;
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
  pMeta->getTableTypeSuidByName = metaGetTableTypeSuidByName;
  pMeta->getTableNameByUid = metaGetTableNameByUid;

  pMeta->getTableSchema = vnodeGetTableSchema;
  pMeta->storeGetTableList = vnodeGetTableList;

  pMeta->getCachedTableList = metaGetCachedTableUidList;
  pMeta->putCachedTableList = metaUidFilterCachePut;
  pMeta->getStableCachedTableList = metaStableTagFilterCacheGet;
  pMeta->putStableCachedTableList = metaStableTagFilterCachePut;

  pMeta->metaGetCachedTbGroup = metaGetCachedTbGroup;
  pMeta->metaPutTbGroupToCache = metaPutTbGroupToCache;

  pMeta->openCtbCursor = metaOpenCtbCursor;
  pMeta->resumeCtbCursor = metaResumeCtbCursor;
  pMeta->pauseCtbCursor = metaPauseCtbCursor;
  pMeta->closeCtbCursor = metaCloseCtbCursor;
  pMeta->ctbCursorNext = metaCtbCursorNext;
  pMeta->getDBSize = vnodeGetDBSize;

  pMeta->metaGetCachedRefDbs = metaGetCachedRefDbs;
  pMeta->metaPutRefDbsToCache = metaPutRefDbsToCache;
}

void initTqAPI(SStoreTqReader* pTq) {
#ifdef USE_TQ
  pTq->tqReaderOpen = tqReaderOpen;
  pTq->tqReaderSetColIdList = tqReaderSetColIdList;

  pTq->tqReaderClose = tqReaderClose;
  pTq->tqReaderSeek = tqReaderSeek;

  pTq->tqGetTablePrimaryKey = tqGetTablePrimaryKey;
  pTq->tqSetTablePrimaryKey = tqSetTablePrimaryKey;
  pTq->tqReaderNextBlockInWal = tqNextBlockInWal;

  pTq->tqNextBlockImpl = tqNextBlockImpl;  // todo remove it

  pTq->tqReaderAddTables = tqReaderAddTbUidList;
  pTq->tqReaderSetQueryTableList = tqReaderSetTbUidList;

  pTq->tqReaderRemoveTables = tqReaderRemoveTbUidList;

  pTq->tqReaderIsQueriedTable = tqReaderIsQueriedTable;
  pTq->tqReaderCurrentBlockConsumed = tqCurrentBlockConsumed;

  pTq->tqReaderGetWalReader = tqGetWalReader;  // todo remove it

  pTq->tqReaderSetSubmitMsg = tqReaderSetSubmitMsg;  // todo remove it
  pTq->tqGetResultBlock = tqGetResultBlock;

  //  pTq->tqReaderNextBlockFilterOut = tqNextDataBlockFilterOut;
  pTq->tqGetResultBlockTime = tqGetResultBlockTime;

  pTq->tqReaderSetVtableInfo = tqReaderSetVtableInfo;
#endif
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

void initCacheFn(SStoreCacheReader* pCache) {
  pCache->openReader = tsdbCacherowsReaderOpen;
  pCache->closeReader = tsdbCacherowsReaderClose;
  pCache->retrieveRows = tsdbRetrieveCacheRows;
  pCache->reuseReader = tsdbReuseCacherowsReader;
}

void initSnapshotFn(SStoreSnapshotFn* pSnapshot) {
  pSnapshot->taosXGetTablePrimaryKey = taosXGetTablePrimaryKey;
  pSnapshot->taosXSetTablePrimaryKey = taosXSetTablePrimaryKey;
  pSnapshot->setForSnapShot = setForSnapShot;
  pSnapshot->destroySnapshot = destroySnapContext;
  pSnapshot->getMetaTableInfoFromSnapshot = getMetaTableInfoFromSnapshot;
  pSnapshot->getTableInfoFromSnapshot = getTableInfoFromSnapshot;
}

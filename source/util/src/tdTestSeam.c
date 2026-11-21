/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/*
 * Storage for the unit-test seams declared in util/tdTestSeam.h.
 *
 * This file is part of libutil, which goes into taosd and into
 * libtaosd_shared.so, so the pointers live next to the engine code that
 * consults them.  A test assigns a replacement and the engine's own callers
 * observe it, whether the test links the engine statically or through the DSO.
 *
 * Without BUILD_TEST_SEAM the file compiles to nothing and product binaries are
 * unaffected.
 */

#include <stddef.h>

#include "tdTestSeam.h"

#ifdef BUILD_TEST_SEAM

void *gTdSeam_taosArrayAddBatch = NULL;

void *gTdSeam_tdbTbcNext = NULL;
void *gTdSeam_tdbTbcMoveTo = NULL;
void *gTdSeam_tdbTbcMoveToNext = NULL;

void *gTdSeam_metaFetchEntryByUid = NULL;
void *gTdSeam_metaFetchEntryFree = NULL;
void *gTdSeam_metaDropTable2 = NULL;
void *gTdSeam_metaRollbackAlterTable = NULL;
void *gTdSeam_metaTxnIdxDelete = NULL;
void *gTdSeam_metaScanTxnEntries = NULL;
void *gTdSeam_metaScanTxnMetaEntries = NULL;
void *gTdSeam_metaTxnMetaDelete = NULL;
void *gTdSeam_metaTxnMetaGet = NULL;
void *gTdSeam_metaTxnMetaUpsert = NULL;
void *gTdSeam_metaMarkTableTxnStatus = NULL;
void *gTdSeam_metaBumpVtableGen = NULL;
void *gTdSeam_metaRLock = NULL;
void *gTdSeam_metaULock = NULL;

void *gTdSeam_vnodeAsync = NULL;

void *gTdSeam_dmMarkWrapper = NULL;
void *gTdSeam_dmReleaseWrapper = NULL;
void *gTdSeam_rpcOpen = NULL;
void *gTdSeam_rpcSendRequest = NULL;
void *gTdSeam_rpcSendResponse = NULL;
void *gTdSeam_rpcFreeCont = NULL;
void *gTdSeam_rpcSetIpWhite = NULL;
void *gTdSeam_rpcSetTimeIpWhite = NULL;
void (*gTdSeam_msmDeployPhase)(const char *phase, const char *streamName) = NULL;
void *gTdSeam_streamHbHandleRspErr = NULL;
void *gTdSeam_streamHbProcessRspMsg = NULL;
void *gTdSeam_taosAnalyUpdate = NULL;
void *gTdSeam_tSingleWorkerCleanup = NULL;
void *gTdSeam_qStreamGetReaderInfo = NULL;
void *gTdSeam_syncIsReadyForRead = NULL;
void *gTdSeam_initStorageAPI = NULL;
void *gTdSeam_qDestroyTask = NULL;
void *gTdSeam_qCreateStreamExecTaskInfo = NULL;
void *gTdSeam_qNeedReset = NULL;
void *gTdSeam_qResetTableScan = NULL;
void *gTdSeam_qFinishReaderGroupRead = NULL;
void *gTdSeam_qSetTaskId = NULL;
void *gTdSeam_qEnableReaderGroupOutput = NULL;
void *gTdSeam_qUpdateOperatorParam = NULL;
void *gTdSeam_qSetStreamGen = NULL;
void *gTdSeam_setTaskScalarExtraInfo = NULL;
void *gTdSeam_qExecTaskOpt = NULL;
void *gTdSeam_tDeserializeSUpdateIpWhiteDual = NULL;
void *gTdSeam_tDeserializeRetrieveAnalyticAlgoRsp = NULL;
void *gTdSeam_tDeserializeSRetrieveDateTimeWhiteListRsp = NULL;

#endif /* BUILD_TEST_SEAM */

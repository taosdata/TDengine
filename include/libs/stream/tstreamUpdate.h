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
#ifndef _TSTREAMUPDATE_H_
#define _TSTREAMUPDATE_H_

#include "storageapi.h"
#include "taosdef.h"
#include "tarray.h"
#include "tcommon.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t updateInfoInitP(SInterval* pInterval, int64_t watermark, bool igUp, int8_t pkType, int32_t pkLen,
                        SUpdateInfo** ppInfo);
int32_t updateInfoInit(int64_t interval, int32_t precision, int64_t watermark, bool igUp, int8_t pkType, int32_t pkLen,
                       SUpdateInfo** ppInfo);
int32_t updateInfoFillBlockData(SUpdateInfo* pInfo, SSDataBlock* pBlock, int32_t primaryTsCol, int32_t primaryKeyCol,
                                TSKEY* pMaxResTs);
bool    updateInfoIsUpdated(SUpdateInfo* pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len);
bool    updateInfoIsTableInserted(SUpdateInfo* pInfo, int64_t tbUid);
void    updateInfoDestroy(SUpdateInfo* pInfo);
void    updateInfoAddCloseWindowSBF(SUpdateInfo* pInfo);
void    updateInfoDestoryColseWinSBF(SUpdateInfo* pInfo);
int32_t updateInfoSerialize(SEncoder* pEncoder, const SUpdateInfo* pInfo);
int32_t updateInfoDeserialize(SDecoder* pDeCoder, SUpdateInfo* pInfo);
void    windowSBfDelete(SUpdateInfo* pInfo, uint64_t count);
int32_t windowSBfAdd(SUpdateInfo* pInfo, uint64_t count);
bool    isIncrementalTimeStamp(SUpdateInfo* pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TSTREAMUPDATE_H_ */

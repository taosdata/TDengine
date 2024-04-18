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
#ifndef _TD_BLOB_INT_H_
#define _TD_BLOB_INT_H_

#include "blob.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t tPutBlobHdr(uint8_t *p, const SBlobDataHdr *pHdr);
int32_t tPutBlobData(uint8_t *p, const SBlobData *pBlob);
int32_t tPutSubmitBlobData(uint8_t *p, const SSubmitBlobData *pSubmitBlobData);

int32_t tGetBlobHdr(uint8_t *p, SBlobDataHdr *pHdr);
int32_t tGetBlobData(uint8_t *p, SBlobData *pBlob);
int32_t tGetSubmitBlobData(uint8_t *p, SSubmitBlobData *pSubmitBlobData);

#ifdef __cplusplus
}
#endif

#endif /*_TD_BLOB_H_*/

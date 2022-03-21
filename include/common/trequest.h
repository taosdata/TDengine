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

#ifndef _TD_COMMON_REQUEST_H_
#define _TD_COMMON_REQUEST_H_

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SRequest      SRequest;
typedef struct SReqBatch     SReqBatch;
typedef struct SReqBatchIter SReqBatchIter;

// SRequest

// SReqBatch

// SReqBatchIter
void            tdInitRBIter(SReqBatchIter *pIter, SReqBatch *pReqBatch);
const SRequest *tdRBIterNext(SReqBatchIter *pIter);
void            tdClearRBIter(SReqBatchIter *pIter);

/* ------------------------ TYPES DEFINITION ------------------------ */
struct SReqBatchIter {
  int32_t    iReq;
  SReqBatch *pReqBatch;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_REQUEST_H_*/
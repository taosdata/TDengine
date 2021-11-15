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

#ifndef _TD_TRANSACTION_INT_H_
#define _TD_TRANSACTION_INT_H_

#include "os.h"
#include "trn.h"
#include "tglobal.h"
#include "tarray.h"
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", 255, __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", 255, __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN ", 255, __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND ", 255, __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND ", mDebugFlag, __VA_ARGS__); }}

#define TRN_VER 1
#define TRN_DEFAULT_ARRAY_SIZE 8

typedef enum {
  TRN_STAGE_PREPARE = 1,
  TRN_STAGE_EXECUTE = 2,
  TRN_STAGE_COMMIT = 3,
  TRN_STAGE_ROLLBACK = 4,
  TRN_STAGE_RETRY = 5
} ETrnStage;

typedef struct STrans {
  int32_t id;
  int8_t  stage;
  int8_t  policy;
  void   *rpcHandle;
  SArray *redoLogs;
  SArray *undoLogs;
  SArray *commitLogs;
  SArray *redoActions;
  SArray *undoActions;
} STrans;

SSdbRaw *trnActionEncode(STrans *pTrans);
STrans  *trnActionDecode(SSdbRaw *pRaw);
int32_t  trnActionInsert(STrans *pTrans);
int32_t  trnActionDelete(STrans *pTrans);
int32_t  trnActionUpdate(STrans *pSrcTrans, STrans *pDstTrans);
int32_t  trnGenerateTransId();

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSACTION_INT_H_*/

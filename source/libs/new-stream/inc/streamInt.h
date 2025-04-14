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
#ifdef USE_STREAM
#ifndef TDENGINE_STREAM_INT_H
#define TDENGINE_STREAM_INT_H

#include "executor.h"
#include "query.h"
#include "trpc.h"
#include "stream.h"
#include "tref.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_HB_INTERVAL_MS             1000


typedef struct SStreamHbInfo {
  tmr_h        hbTmr;
  int32_t      tickCounter;
  int32_t      hbCount;
  int64_t      hbStart;
  int64_t      msgSendTs;
  SStreamHbMsg hbMsg;
} SStreamHbInfo;

typedef struct SStreamMgmtInfo {
  void*         timer;
  SStreamHbInfo hb;
} SStreamMgmtInfo;


#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_STREAM_INT_H */
#endif /* USE_STREAM */

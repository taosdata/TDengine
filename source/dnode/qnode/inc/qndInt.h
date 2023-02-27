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

#ifndef _TD_QNODE_INT_H_
#define _TD_QNODE_INT_H_

#include "os.h"

#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"

#include "qnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SQueueWorker SQHandle;

typedef struct SQnode {
  int32_t   qndId;
  SMsgCb    msgCb;
  SQHandle* pQuery;
} SQnode;

#ifdef __cplusplus
}
#endif

#endif /*_TD_QNODE_INT_H_*/

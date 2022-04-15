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

#ifndef _TD_VNODE_DEF_H_
#define _TD_VNODE_DEF_H_

#include "executor.h"
#include "filter.h"
#include "qworker.h"
#include "sync.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdbInt.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tlosertree.h"
#include "tmallocator.h"
#include "tskiplist.h"
#include "tstream.h"
#include "ttime.h"
#include "ttimer.h"
#include "wal.h"

#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeInfo   SVnodeInfo;
typedef struct SMeta        SMeta;
typedef struct STsdb        STsdb;
typedef struct STQ          STQ;
typedef struct SVState      SVState;
typedef struct SVBufPool    SVBufPool;
typedef struct SQWorkerMgmt SQHandle;

#define TD_VNODE_ID(PVNODE) (PVNODE)->config.vgId

#define VND_META_DIR "meta"
#define VND_TSDB_DIR "tsdb"
#define VND_WAL_DIR  "wal"
#define VND_TQ_DIR   "tq"

typedef struct {
  int8_t  streamType;  // sma or other
  int8_t  dstType;
  int16_t padding;
  int32_t smaId;
  int64_t tbUid;
  int64_t lastReceivedVer;
  int64_t lastCommittedVer;
} SStreamSinkInfo;

typedef struct {
  SVnode*   pVnode;
  SHashObj* pHash;  // streamId -> SStreamSinkInfo
} SSink;

// SVState
struct SVState {
  int64_t processed;
  int64_t committed;
  int64_t applied;
};

struct SVnodeInfo {
  SVnodeCfg config;
  SVState   state;
};

struct SVnode {
  char*      path;
  SVnodeCfg  config;
  SVState    state;
  STfs*      pTfs;
  SMsgCb     msgCb;
  SVBufPool* pBufPool;
  SMeta*     pMeta;
  STsdb*     pTsdb;
  SWal*      pWal;
  STQ*       pTq;
  SSink*     pSink;
  tsem_t     canCommit;
  SQHandle*  pQuery;
};

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

#include "vnd.h"

#include "meta.h"

#include "tsdb.h"

#include "tq.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/

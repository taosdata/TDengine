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

#ifndef _TD_TSDB_DEF_H_
#define _TD_TSDB_DEF_H_

#include "tmallocator.h"
#include "meta.h"
#include "tcompression.h"
#include "tglobal.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "tskiplist.h"
#include "ttime.h"

#include "tsdb.h"
#include "tsdbDBDef.h"
#include "tsdbCommit.h"
#include "tsdbFS.h"
#include "tsdbFile.h"
#include "tsdbLog.h"
#include "tsdbMemTable.h"
#include "tsdbMemory.h"
#include "tsdbOptions.h"
#include "tsdbReadImpl.h"
#include "tsdbSma.h"


#ifdef __cplusplus
extern "C" {
#endif

struct STsdb {
  int32_t               vgId;
  bool                  repoLocked;
  pthread_mutex_t       mutex;
  char *                path;
  STsdbCfg              config;
  STsdbMemTable *       mem;
  STsdbMemTable *       imem;
  SRtn                  rtn;
  SMemAllocatorFactory *pmaf;
  STsdbFS *             fs;
  SMeta *               pMeta;
  STfs *                pTfs;
  SSmaEnv *             pTSmaEnv;
  SSmaEnv *             pRSmaEnv;
};

#define REPO_ID(r)        ((r)->vgId)
#define REPO_CFG(r)       (&(r)->config)
#define REPO_FS(r)        (r)->fs
#define IS_REPO_LOCKED(r) (r)->repoLocked

int tsdbLockRepo(STsdb *pTsdb);
int tsdbUnlockRepo(STsdb *pTsdb);

static FORCE_INLINE STSchema *tsdbGetTableSchemaImpl(STable *pTable, bool lock, bool copy, int32_t version) {
  return pTable->pSchema;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_DEF_H_*/
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

#ifndef _TD_TSDB_INT_H_
#define _TD_TSDB_INT_H_

#include "os.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcompression.h"
#include "tdataformat.h"
#include "tfs.h"
#include "thash.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tlog.h"
#include "tsdbMemory.h"
#include "tskiplist.h"

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// Log
#include "tsdbLog.h"
// Meta
#include "tsdbMeta.h"
// // Buffer
// #include "tsdbBuffer.h"
// MemTable
#include "tsdbMemTable.h"
// File
#include "tsdbFile.h"
// FS
#include "tsdbFS.h"
// ReadImpl
#include "tsdbReadImpl.h"
// Commit
#include "tsdbCommit.h"
// Compact
#include "tsdbCompact.h"

#include "tsdbRowMergeBuf.h"
// Main definitions
struct STsdb {
  uint8_t    state;
  STsdbCfg   config;
  STsdbStat  stat;
  STsdbMeta* tsdbMeta;
  SMemTable* mem;
  SMemTable* imem;
  STsdbFS*   fs;
  SRtn       rtn;
  SMergeBuf  mergeBuf;  // used when update=2
};

#define REPO_ID(r) (r)->config.tsdbId
#define REPO_CFG(r) (&((r)->config))
#define REPO_FS(r) ((r)->fs)
#define IS_REPO_LOCKED(r) (r)->repoLocked
#define TSDB_SUBMIT_MSG_HEAD_SIZE sizeof(SSubmitMsg)

int             tsdbLockRepo(STsdb* pRepo);
int             tsdbUnlockRepo(STsdb* pRepo);
STsdbMeta*      tsdbGetMeta(STsdb* pRepo);
int             tsdbCheckCommit(STsdb* pRepo);
int             tsdbRestoreInfo(STsdb* pRepo);
UNUSED_FUNC int tsdbCacheLastData(STsdb* pRepo, STsdbCfg* oldCfg);
int32_t         tsdbLoadLastCache(STsdb* pRepo, STable* pTable);
void            tsdbGetRootDir(int repoid, char dirName[]);
void            tsdbGetDataDir(int repoid, char dirName[]);

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_INT_H_ */

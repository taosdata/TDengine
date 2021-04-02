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

// // TODO: remove the include
// #include <errno.h>
// #include <fcntl.h>
// #include <limits.h>
// #include <inttypes.h>
// #include <sys/stat.h>
// #include <sys/types.h>
// #include <semaphore.h>
// #include <dirent.h>

#include "os.h"
#include "tlog.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tskiplist.h"
#include "tdataformat.h"
#include "tcoding.h"
#include "tscompression.h"
#include "tlockfree.h"
#include "tlist.h"
#include "hash.h"
#include "tarray.h"
#include "tfs.h"
#include "tsocket.h"

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// Log
#include "tsdbLog.h"
// Meta
#include "tsdbMeta.h"
// Buffer
#include "tsdbBuffer.h"
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
// Commit Queue
#include "tsdbCommitQueue.h"
// Main definitions
struct STsdbRepo {
  uint8_t state;

  STsdbCfg        config;
  STsdbAppH       appH;
  STsdbStat       stat;
  STsdbMeta*      tsdbMeta;
  STsdbBufPool*   pPool;
  SMemTable*      mem;
  SMemTable*      imem;
  STsdbFS*        fs;
  tsem_t          readyToCommit;
  pthread_mutex_t mutex;
  bool            repoLocked;
  int32_t         code;  // Commit code
};

#define REPO_ID(r) (r)->config.tsdbId
#define REPO_CFG(r) (&((r)->config))
#define REPO_FS(r) ((r)->fs)
#define IS_REPO_LOCKED(r) (r)->repoLocked
#define TSDB_SUBMIT_MSG_HEAD_SIZE sizeof(SSubmitMsg)

int        tsdbLockRepo(STsdbRepo* pRepo);
int        tsdbUnlockRepo(STsdbRepo* pRepo);
STsdbMeta* tsdbGetMeta(STsdbRepo* pRepo);
int        tsdbCheckCommit(STsdbRepo* pRepo);
int        tsdbRestoreInfo(STsdbRepo* pRepo);
void       tsdbGetRootDir(int repoid, char dirName[]);
void       tsdbGetDataDir(int repoid, char dirName[]);

static FORCE_INLINE STsdbBufBlock* tsdbGetCurrBufBlock(STsdbRepo* pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode* pNode = listTail(pRepo->mem->bufBlockList);
  if (pNode == NULL) return NULL;

  STsdbBufBlock* pBufBlock = NULL;
  tdListNodeGetData(pRepo->mem->bufBlockList, pNode, (void*)(&pBufBlock));

  return pBufBlock;
}

static FORCE_INLINE int tsdbGetNextMaxTables(int tid) {
  ASSERT(tid >= 1 && tid <= TSDB_MAX_TABLES);
  int maxTables = TSDB_INIT_NTABLES;
  while (true) {
    maxTables = MIN(maxTables, TSDB_MAX_TABLES);
    if (tid <= maxTables) break;
    maxTables *= 2;
  }

  return maxTables + 1;
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_INT_H_ */
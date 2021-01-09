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

// TODO: remove the include
#include <errno.h>
#include <inttypes.h>

#include "os.h"
#include "tlog.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tskiplist.h"
#include "tdataformat.h"
#include "tlockfree.h"
#include "tlist.h"
#include "hash.h"
#include "tarray.h"
#include "tfs.h"

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STsdbRepo STsdbRepo;

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
  int8_t state;

  char*           rootDir;
  STsdbCfg        config;
  STsdbAppH       appH;
  STsdbStat       stat;
  STsdbMeta*      tsdbMeta;
  STsdbBufPool*   pPool;
  SMemTable*      mem;
  SMemTable*      imem;
  STsdbFS*        fs;
  sem_t           readyToCommit;
  pthread_mutex_t mutex;
  bool            repoLocked;
  int32_t         code;  // Commit code
};

#define REPO_ID(r) (r)->config.tsdbId
#define REPO_CFG(r) (&((r)->config))
#define IS_REPO_LOCKED(r) (r)->repoLocked
#define TSDB_SUBMIT_MSG_HEAD_SIZE sizeof(SSubmitMsg)

char*       tsdbGetMetaFileName(char* rootDir);
void        tsdbGetDataFileName(char* rootDir, int vid, int fid, int type, char* fname);
int         tsdbLockRepo(STsdbRepo* pRepo);
int         tsdbUnlockRepo(STsdbRepo* pRepo);
char*       tsdbGetDataDirName(char* rootDir);
int         tsdbGetNextMaxTables(int tid);
STsdbMeta*  tsdbGetMeta(TSDB_REPO_T* pRepo);
STsdbFileH* tsdbGetFile(TSDB_REPO_T* pRepo);
int         tsdbCheckCommit(STsdbRepo* pRepo);

static FORCE_INLINE STsdbBufBlock* tsdbGetCurrBufBlock(STsdbRepo* pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode* pNode = listTail(pRepo->mem->bufBlockList);
  if (pNode == NULL) return NULL;

  STsdbBufBlock* pBufBlock = NULL;
  tdListNodeGetData(pRepo->mem->bufBlockList, pNode, (void*)(&pBufBlock));

  return pBufBlock;
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_INT_H_ */
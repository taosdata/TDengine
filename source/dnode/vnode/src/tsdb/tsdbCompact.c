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

#include "tsdb.h"

// typedef struct {
// } SMemDIter;

typedef struct {
  SArray    *aBlockIdx;  // SArray<SBlockIdx>
  SMapData   mDataBlk;   // SMapData<SDataBlk>
  SBlockData bData;
  int32_t    iBlockIdx;
  int32_t    iDataBlk;
  int32_t    iRow;
} SDataDIter;

typedef struct {
  SArray    *aSttBlk;  // SArray<SSttBlk>
  SBlockData bData;
  int32_t    iSttBlk;
  int32_t    iRow;
} SSttDIter;

typedef struct {
  int32_t  flag;
  SRowInfo rowInfo;
  char     handle[];
} STsdbDataIter;

typedef struct {
  STsdb        *pTsdb;
  STsdbFS       fs;
  int64_t       cid;
  int32_t       fid;
  SDataFReader *pReader;
  SDFileSet    *pDFileSet;
  SRBTree       rtree;
} STsdbCompactor;

#define TSDB_FLG_DEEP_COMPACT 0x1

// ITER =========================
static int32_t tsdbDataIterOpen(STsdbDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

static void tsdbDataIterClose(STsdbDataIter *pIter) {
  // TODO
}

static int32_t tsdbDataIterNext(STsdbDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

// COMPACT =========================
static int32_t tsdbBeginCompact(STsdb *pTsdb, STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->pTsdb = pTsdb;

  code = tsdbFSCopy(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  pCompactor->fid = INT32_MIN;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAbortCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbDeepCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  code = tsdbDataFReaderOpen(&pCompactor->pReader, pTsdb, pCompactor->pDFileSet);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbShallowCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbCompact(STsdb *pTsdb, int32_t flag) {
  int32_t code = 0;
  int32_t lino = 0;

  // Check if can do compact (TODO)

  // Do compact
  STsdbCompactor compactor = {0};

  code = tsdbBeginCompact(pTsdb, &compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  while (true) {
    compactor.pDFileSet = (SDFileSet *)taosArraySearch(compactor.fs.aDFileSet, &compactor.fid, tDFileSetCmprFn, TD_GT);
    if (compactor.pDFileSet == NULL) break;

    compactor.fid = compactor.pDFileSet->fid;

    if (flag & TSDB_FLG_DEEP_COMPACT) {
      code = tsdbDeepCompact(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbShallowCompact(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    tsdbAbortCompact(&compactor);
  } else {
    tsdbCommitCompact(&compactor);
  }
  return code;
}

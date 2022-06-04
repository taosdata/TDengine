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

typedef struct {
  SMemTable *pMemTable;
  SArray    *aBlkIdx;
} SCommitH;

static int32_t tsdbStartCommit(SCommitH *pCHandle, STsdb *pTsdb);
static int32_t tsdbEndCommit(SCommitH *pCHandle);
static int32_t tsdbCommitToFile(SCommitH *pCHandle, int32_t fid);

int32_t tsdbBegin2(STsdb *pTsdb) {
  int32_t code = 0;

  ASSERT(pTsdb->mem == NULL);
  code = tsdbMemTableCreate2(pTsdb, (SMemTable **)&pTsdb->mem);
  if (code) {
    tsdbError("vgId:%d failed to begin TSDB since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
    goto _exit;
  }

_exit:
  return code;
}

int32_t tsdbCommit2(STsdb *pTsdb) {
  int32_t  code = 0;
  SCommitH ch = {0};

  // start to commit
  code = tsdbStartCommit(&ch, pTsdb);
  if (code) {
    goto _exit;
  }

  // commit
  int32_t sfid;  // todo
  int32_t efid;  // todo
  for (int32_t fid = sfid; fid <= efid; fid++) {
    code = tsdbCommitToFile(&ch, fid);
    if (code) {
      goto _err;
    }
  }

  // end commit
  code = tsdbEndCommit(&ch);
  if (code) {
    goto _exit;
  }

_exit:
  return code;

_err:
  // TODO: rollback
  return code;
}

static int32_t tsdbStartCommit(SCommitH *pCHandle, STsdb *pTsdb) {
  int32_t code = 0;

  ASSERT(pTsdb->imem == NULL && pTsdb->mem);
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  // TODO
  return code;
}

static int32_t tsdbEndCommit(SCommitH *pCHandle) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitTableData(SCommitH *pCHandle, SMemData *pMemData, SBlockIdx *pBlockIdx) {
  int32_t      code = 0;
  SMemDataIter iter = {0};

  if (pMemData && pBlockIdx) {
    // merge
  } else if (pMemData) {
    // new one
  } else {
    // save old ones
  }

  return code;
}

static int32_t tsdbTableIdCmprFn(const void *p1, const void *p2) {
  TABLEID *pId1 = (TABLEID *)p1;
  TABLEID *pId2 = (TABLEID *)p2;

  if (pId1->suid < pId2->suid) {
    return -1;
  } else if (pId1->suid > pId2->suid) {
    return 1;
  }

  if (pId1->uid < pId2->uid) {
    return -1;
  } else if (pId1->uid > pId2->uid) {
    return 1;
  }

  return 0;
}
static int32_t tsdbCommitToFile(SCommitH *pCHandle, int32_t fid) {
  int32_t      code = 0;
  SMemDataIter iter = {0};
  TSDBROW     *pRow = NULL;
  int8_t       hasData = 0;
  TSKEY        fidSKey;
  TSKEY        fidEKey;
  int32_t      iMemData = 0;
  int32_t      nMemData = taosArrayGetSize(pCHandle->pMemTable->aMemData);
  int32_t      iBlockIdx = 0;
  int32_t      nBlockIdx;

  // check if there are data in the time range
  for (; iMemData < nMemData; iMemData++) {
    SMemData *pMemData = (SMemData *)taosArrayGetP(pCHandle->pMemTable->aMemData, iMemData);
    tsdbMemDataIterOpen(&iter, &(TSDBKEY){.ts = fidSKey, .version = 0}, 0);
    tsdbMemDataIterGet(&iter, &pRow);

    if (pRow->tsRow.ts >= fidSKey && pRow->tsRow.ts <= fidEKey) {
      hasData = 1;
      break;
    }
  }

  if (!hasData) return code;

  // create or open the file to commit(todo)

  // loop to commit each table data
  nBlockIdx = 0;
  for (;;) {
    if (iBlockIdx >= nBlockIdx && iMemData >= nMemData) break;

    SMemData  *pMemData = NULL;
    SBlockIdx *pBlockIdx = NULL;
    if (iMemData < nMemData) {
      pMemData = (SMemData *)taosArrayGetP(pCHandle->pMemTable->aMemData, iBlockIdx);
    }
    if (iBlockIdx < nBlockIdx) {
      // pBlockIdx
    }

    if (pMemData && pBlockIdx) {
      int32_t c = tsdbTableIdCmprFn(&(TABLEID){.suid = pMemData->suid, .uid = pMemData->uid},
                                    &(TABLEID){.suid = pBlockIdx->suid, .uid = pBlockIdx->uid});
      if (c == 0) {
        iMemData++;
        iBlockIdx++;
      } else if (c < 0) {
        pBlockIdx = NULL;
        iMemData++;
      } else {
        pMemData = NULL;
        iBlockIdx++;
      }
    } else {
      if (pMemData) {
        iMemData++;
      } else {
        iBlockIdx++;
      }
    }

    code = tsdbCommitTableData(pCHandle, pMemData, pBlockIdx);
    if (code) {
      goto _err;
    }
  }

  return code;

_err:
  return code;
}
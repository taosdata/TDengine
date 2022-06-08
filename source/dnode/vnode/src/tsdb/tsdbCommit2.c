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
  SMemTable2 *pMemTable;
  int32_t     minutes;
  int8_t      precision;
  TSKEY       nCommitKey;
  int32_t     fid;
  TSKEY       minKey;
  TSKEY       maxKey;
  SReadH      readh;
  SDFileSet   wSet;
  SArray     *aBlkIdx;
  SArray     *aSupBlk;
  SArray     *aSubBlk;
  SArray     *aDelInfo;
} SCommitH;

static int32_t tsdbCommitStart(SCommitH *pCHandle, STsdb *pTsdb);
static int32_t tsdbCommitEnd(SCommitH *pCHandle);
static int32_t tsdbCommitImpl(SCommitH *pCHandle);

int32_t tsdbBegin2(STsdb *pTsdb) {
  int32_t code = 0;

  ASSERT(pTsdb->mem == NULL);
  code = tsdbMemTableCreate2(pTsdb, (SMemTable2 **)&pTsdb->mem);
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
  code = tsdbCommitStart(&ch, pTsdb);
  if (code) {
    goto _exit;
  }

  // commit
  code = tsdbCommitImpl(&ch);
  if (code) {
    goto _err;
  }

  // end commit
  code = tsdbCommitEnd(&ch);
  if (code) {
    goto _exit;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d failed to commit since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitStart(SCommitH *pCHandle, STsdb *pTsdb) {
  int32_t     code = 0;
  SMemTable2 *pMemTable = (SMemTable2 *)pTsdb->mem;

  tsdbInfo("vgId:%d start to commit", TD_VID(pTsdb->pVnode));

  // switch to commit
  ASSERT(pTsdb->imem == NULL && pTsdb->mem);
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;

  // open handle
  pCHandle->pMemTable = pMemTable;
  pCHandle->minutes = pTsdb->keepCfg.days;
  pCHandle->precision = pTsdb->keepCfg.precision;
  pCHandle->nCommitKey = pMemTable->minKey.ts;

  code = tsdbInitReadH(&pCHandle->readh, pTsdb);
  if (code) {
    goto _err;
  }
  pCHandle->aBlkIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCHandle->aBlkIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pCHandle->aSupBlk = taosArrayInit(0, sizeof(SBlock));
  if (pCHandle->aSupBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pCHandle->aSubBlk = taosArrayInit(0, sizeof(SBlock));
  if (pCHandle->aSubBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pCHandle->aDelInfo = taosArrayInit(0, sizeof(SDelInfo));
  if (pCHandle->aDelInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // start FS transaction
  tsdbStartFSTxn(pTsdb, 0, 0);

  return code;

_err:
  return code;
}

static int32_t tsdbCommitEnd(SCommitH *pCHandle) {
  int32_t     code = 0;
  STsdb      *pTsdb = pCHandle->pMemTable->pTsdb;
  SMemTable2 *pMemTable = (SMemTable2 *)pTsdb->imem;

  // end transaction
  code = tsdbEndFSTxn(pTsdb);
  if (code) {
    goto _err;
  }

  // close handle
  taosArrayClear(pCHandle->aDelInfo);
  taosArrayClear(pCHandle->aSubBlk);
  taosArrayClear(pCHandle->aSupBlk);
  taosArrayClear(pCHandle->aBlkIdx);
  tsdbDestroyReadH(&pCHandle->readh);

  // destroy memtable (todo: unref it)
  pTsdb->imem = NULL;
  tsdbMemTableDestroy2(pMemTable);

  tsdbInfo("vgId:%d commit over", TD_VID(pTsdb->pVnode));
  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableStart(SCommitH *pCHandle) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitTableEnd(SCommitH *pCHandle) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitTable(SCommitH *pCHandle, SMemData *pMemData, SBlockIdx *pBlockIdx) {
  int32_t      code = 0;
  SMemDataIter iter = {0};

  // commit table start
  code = tsdbCommitTableStart(pCHandle);
  if (code) {
    goto _err;
  }

  // commit table impl
  if (pMemData && pBlockIdx) {
    // TODO
  } else if (pMemData) {
    // TODO
  } else {
    // TODO
  }

  // commit table end
  code = tsdbCommitTableEnd(pCHandle);
  if (code) {
    goto _err;
  }

  return code;

_err:
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

static int32_t tsdbWriteBlockIdx(SDFile *pFile, SArray *pArray, uint8_t **ppBuf) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitFileStart(SCommitH *pCHandle) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCHandle->pMemTable->pTsdb;
  SDFileSet *pSet = NULL;

  taosArrayClear(pCHandle->aBlkIdx);

  return code;
}

static int32_t tsdbCommitFileEnd(SCommitH *pCHandle) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitFile(SCommitH *pCHandle) {
  int32_t    code = 0;
  SMemData  *pMemData;
  SBlockIdx *pBlockIdx;
  int32_t    iMemData;
  int32_t    nMemData;
  int32_t    iBlockIdx;
  int32_t    nBlockIdx;

  // commit file start
  code = tsdbCommitFileStart(pCHandle);
  if (code) {
    goto _err;
  }

  // commit file impl
  iMemData = 0;
  nMemData = taosArrayGetSize(pCHandle->pMemTable->aMemData);
  iBlockIdx = 0;
  nBlockIdx = 0;  // todo

  for (;;) {
    if (iMemData >= nMemData && iBlockIdx >= nBlockIdx) break;

    pMemData = NULL;
    pBlockIdx = NULL;
    if (iMemData < nMemData) {
      pMemData = (SMemData *)taosArrayGetP(pCHandle->pMemTable->aMemData, iMemData);
    }
    if (iBlockIdx < nBlockIdx) {
      // pBlockIdx = ;
    }

    if (pMemData && pBlockIdx) {
      int32_t c = tsdbTableIdCmprFn(pMemData, pBlockIdx);
      if (c < 0) {
        iMemData++;
        pBlockIdx = NULL;
      } else if (c == 0) {
        iMemData++;
        iBlockIdx++;
      } else {
        iBlockIdx++;
        pMemData = NULL;
      }
    } else {
      if (pMemData) {
        iMemData++;
      } else {
        iBlockIdx++;
      }
    }

    code = tsdbCommitTable(pCHandle, pMemData, pBlockIdx);
    if (code) {
      goto _err;
    }
  }

  // commit file end
  code = tsdbCommitFileEnd(pCHandle);
  if (code) {
    goto _err;
  }

  return code;

_err:
  return code;
}

static int32_t tsdbCommitData(SCommitH *pCHandle) {
  int32_t code = 0;
  int32_t fid;

  if (pCHandle->pMemTable->nRows == 0) goto _exit;

  // loop to commit to each file
  for (;;) {
    if (pCHandle->nCommitKey == TSKEY_MAX) break;

    pCHandle->fid = TSDB_KEY_FID(pCHandle->nCommitKey, pCHandle->minutes, pCHandle->precision);
    tsdbGetFidKeyRange(pCHandle->minutes, pCHandle->precision, pCHandle->fid, &pCHandle->minKey, &pCHandle->maxKey);
    code = tsdbCommitFile(pCHandle);
    if (code) {
      goto _err;
    }
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t delInfoCmprFn(const void *p1, const void *p2) {
  SDelInfo *pDelInfo1 = (SDelInfo *)p1;
  SDelInfo *pDelInfo2 = (SDelInfo *)p2;

  if (pDelInfo1->suid < pDelInfo2->suid) {
    return -1;
  } else if (pDelInfo1->suid > pDelInfo2->suid) {
    return 1;
  }

  if (pDelInfo1->uid < pDelInfo2->uid) {
    return -1;
  } else if (pDelInfo1->uid > pDelInfo2->uid) {
    return 1;
  }

  if (pDelInfo1->version < pDelInfo2->version) {
    return -1;
  } else if (pDelInfo1->version > pDelInfo2->version) {
    return 1;
  }

  return 0;
}
static int32_t tsdbCommitDelete(SCommitH *pCHandle) {
  int32_t   code = 0;
  SDelInfo  delInfo;
  SMemData *pMemData;

  if (pCHandle->pMemTable->nDelOp == 0) goto _exit;

  // load del array (todo)

  // loop to append SDelInfo
  for (int32_t iMemData = 0; iMemData < taosArrayGetSize(pCHandle->pMemTable->aMemData); iMemData++) {
    pMemData = (SMemData *)taosArrayGetP(pCHandle->pMemTable->aMemData, iMemData);

    for (SDelOp *pDelOp = pMemData->delOpHead; pDelOp; pDelOp = pDelOp->pNext) {
      delInfo = (SDelInfo){.suid = pMemData->suid,
                           .uid = pMemData->uid,
                           .version = pDelOp->version,
                           .sKey = pDelOp->sKey,
                           .eKey = pDelOp->eKey};
      if (taosArrayPush(pCHandle->aDelInfo, &delInfo) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
    }
  }

  taosArraySort(pCHandle->aDelInfo, delInfoCmprFn);

  // write to new file

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitCache(SCommitH *pCHandle) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitImpl(SCommitH *pCHandle) {
  int32_t code = 0;

  // commit data
  code = tsdbCommitData(pCHandle);
  if (code) {
    goto _err;
  }

  // commit delete
  code = tsdbCommitDelete(pCHandle);
  if (code) {
    goto _err;
  }

  // commit cache if need (todo)
  if (0) {
    code = tsdbCommitCache(pCHandle);
    if (code) {
      goto _err;
    }
  }

  return code;

_err:
  return code;
}
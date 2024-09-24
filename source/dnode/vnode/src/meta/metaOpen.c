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

#include "meta.h"
#include "vnd.h"

static int tbDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int skmDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int ctbIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int uidIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int smaIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int taskIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

static int btimeIdxCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int ncolIdxCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

static void metaInitLock(SMeta *pMeta) {
  TdThreadRwlockAttr attr;
  (void)taosThreadRwlockAttrInit(&attr);
  (void)taosThreadRwlockAttrSetKindNP(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  (void)taosThreadRwlockInit(&pMeta->lock, &attr);
  (void)taosThreadRwlockAttrDestroy(&attr);
  return;
}
static void metaDestroyLock(SMeta *pMeta) { (void)taosThreadRwlockDestroy(&pMeta->lock); }

static void metaCleanup(SMeta **ppMeta);

int32_t metaOpen(SVnode *pVnode, SMeta **ppMeta, int8_t rollback) {
  SMeta  *pMeta = NULL;
  int32_t code = 0;
  int32_t lino;
  int32_t offset;
  char    path[TSDB_FILENAME_LEN] = {0};
  char    indexFullPath[128] = {0};

  // create handle
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, path, TSDB_FILENAME_LEN);
  offset = strlen(path);
  snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VNODE_META_DIR);

  if ((pMeta = taosMemoryCalloc(1, sizeof(*pMeta) + strlen(path) + 1)) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  metaInitLock(pMeta);

  pMeta->path = (char *)&pMeta[1];
  strcpy(pMeta->path, path);
  (void)taosRealPath(pMeta->path, NULL, strlen(path) + 1);

  pMeta->pVnode = pVnode;

  // create path if not created yet
  code = taosMkDir(pMeta->path);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open env
  code = tdbOpen(pMeta->path, pVnode->config.szPage, pVnode->config.szCache, &pMeta->pEnv, rollback,
                 pVnode->config.tdbEncryptAlgorithm, pVnode->config.tdbEncryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pTbDb
  code = tdbTbOpen("table.db", sizeof(STbDbKey), -1, tbDbKeyCmpr, pMeta->pEnv, &pMeta->pTbDb, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pSkmDb
  code = tdbTbOpen("schema.db", sizeof(SSkmDbKey), -1, skmDbKeyCmpr, pMeta->pEnv, &pMeta->pSkmDb, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pUidIdx
  code = tdbTbOpen("uid.idx", sizeof(tb_uid_t), sizeof(SUidIdxVal), uidIdxKeyCmpr, pMeta->pEnv, &pMeta->pUidIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pNameIdx
  code = tdbTbOpen("name.idx", -1, sizeof(tb_uid_t), NULL, pMeta->pEnv, &pMeta->pNameIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pCtbIdx
  code = tdbTbOpen("ctb.idx", sizeof(SCtbIdxKey), -1, ctbIdxKeyCmpr, pMeta->pEnv, &pMeta->pCtbIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pSuidIdx
  code = tdbTbOpen("suid.idx", sizeof(tb_uid_t), 0, uidIdxKeyCmpr, pMeta->pEnv, &pMeta->pSuidIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  sprintf(indexFullPath, "%s/%s", pMeta->path, "invert");
  TAOS_UNUSED(taosMkDir(indexFullPath));

  SIndexOpts opts = {.cacheSize = 8 * 1024 * 1024};
  code = indexOpen(&opts, indexFullPath, (SIndex **)&pMeta->pTagIvtIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdbTbOpen("tag.idx", -1, 0, tagIdxKeyCmpr, pMeta->pEnv, &pMeta->pTagIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pTtlMgr ("ttlv1.idx")
  char logPrefix[128] = {0};
  sprintf(logPrefix, "vgId:%d", TD_VID(pVnode));
  code = ttlMgrOpen(&pMeta->pTtlMgr, pMeta->pEnv, 0, logPrefix, tsTtlFlushThreshold);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pSmaIdx
  code = tdbTbOpen("sma.idx", sizeof(SSmaIdxKey), 0, smaIdxKeyCmpr, pMeta->pEnv, &pMeta->pSmaIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // idx table create time
  code = tdbTbOpen("ctime.idx", sizeof(SBtimeIdxKey), 0, btimeIdxCmpr, pMeta->pEnv, &pMeta->pBtimeIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // idx num of col, normal table only
  code = tdbTbOpen("ncol.idx", sizeof(SNcolIdxKey), 0, ncolIdxCmpr, pMeta->pEnv, &pMeta->pNcolIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdbTbOpen("stream.task.db", sizeof(int64_t), -1, taskIdxKeyCmpr, pMeta->pEnv, &pMeta->pStreamDb, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaCacheOpen(pMeta);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaInitTbFilterCache(pMeta);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    metaCleanup(&pMeta);
    *ppMeta = NULL;
  } else {
    metaDebug("vgId:%d %s success", TD_VID(pVnode), __func__);
    *ppMeta = pMeta;
  }
  return code;
}

int32_t metaUpgrade(SVnode *pVnode, SMeta **ppMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino;
  SMeta  *pMeta = *ppMeta;

  if (ttlMgrNeedUpgrade(pMeta->pEnv)) {
    code = metaBegin(pMeta, META_BEGIN_HEAP_OS);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = ttlMgrUpgrade(pMeta->pTtlMgr, pMeta);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaCommit(pMeta, pMeta->txn);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    metaCleanup(ppMeta);
  }
  return code;
}

void metaClose(SMeta **ppMeta) {
  metaCleanup(ppMeta);
  return;
}

int metaAlterCache(SMeta *pMeta, int32_t nPage) {
  int32_t code = 0;
  metaWLock(pMeta);
  code = tdbAlter(pMeta->pEnv, nPage);
  metaULock(pMeta);

  if (code) {
    metaError("vgId:%d %s failed since %s", TD_VID(pMeta->pVnode), __func__, tstrerror(code));
  }
  return code;
}

void metaRLock(SMeta *pMeta) {
  metaTrace("meta rlock %p", &pMeta->lock);
  if (taosThreadRwlockRdlock(&pMeta->lock) != 0) {
    metaError("vgId:%d failed to lock %p", TD_VID(pMeta->pVnode), &pMeta->lock);
  }
}

void metaWLock(SMeta *pMeta) {
  metaTrace("meta wlock %p", &pMeta->lock);
  if (taosThreadRwlockWrlock(&pMeta->lock) != 0) {
    metaError("vgId:%d failed to lock %p", TD_VID(pMeta->pVnode), &pMeta->lock);
  }
}

void metaULock(SMeta *pMeta) {
  metaTrace("meta ulock %p", &pMeta->lock);
  if (taosThreadRwlockUnlock(&pMeta->lock) != 0) {
    metaError("vgId:%d failed to unlock %p", TD_VID(pMeta->pVnode), &pMeta->lock);
  }
}

static void metaCleanup(SMeta **ppMeta) {
  SMeta *pMeta = *ppMeta;
  if (pMeta) {
    if (pMeta->pEnv) metaAbort(pMeta);
    if (pMeta->pCache) metaCacheClose(pMeta);
#ifdef BUILD_NO_CALL
    if (pMeta->pIdx) metaCloseIdx(pMeta);
#endif
    if (pMeta->pStreamDb) tdbTbClose(pMeta->pStreamDb);
    if (pMeta->pNcolIdx) tdbTbClose(pMeta->pNcolIdx);
    if (pMeta->pBtimeIdx) tdbTbClose(pMeta->pBtimeIdx);
    if (pMeta->pSmaIdx) tdbTbClose(pMeta->pSmaIdx);
    if (pMeta->pTtlMgr) ttlMgrClose(pMeta->pTtlMgr);
    if (pMeta->pTagIvtIdx) indexClose(pMeta->pTagIvtIdx);
    if (pMeta->pTagIdx) tdbTbClose(pMeta->pTagIdx);
    if (pMeta->pCtbIdx) tdbTbClose(pMeta->pCtbIdx);
    if (pMeta->pSuidIdx) tdbTbClose(pMeta->pSuidIdx);
    if (pMeta->pNameIdx) tdbTbClose(pMeta->pNameIdx);
    if (pMeta->pUidIdx) tdbTbClose(pMeta->pUidIdx);
    if (pMeta->pSkmDb) tdbTbClose(pMeta->pSkmDb);
    if (pMeta->pTbDb) tdbTbClose(pMeta->pTbDb);
    if (pMeta->pEnv) tdbClose(pMeta->pEnv);
    metaDestroyLock(pMeta);

    taosMemoryFreeClear(*ppMeta);
  }
}

static int tbDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STbDbKey *pTbDbKey1 = (STbDbKey *)pKey1;
  STbDbKey *pTbDbKey2 = (STbDbKey *)pKey2;

  if (pTbDbKey1->version > pTbDbKey2->version) {
    return 1;
  } else if (pTbDbKey1->version < pTbDbKey2->version) {
    return -1;
  }

  if (pTbDbKey1->uid > pTbDbKey2->uid) {
    return 1;
  } else if (pTbDbKey1->uid < pTbDbKey2->uid) {
    return -1;
  }

  return 0;
}

static int skmDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  SSkmDbKey *pSkmDbKey1 = (SSkmDbKey *)pKey1;
  SSkmDbKey *pSkmDbKey2 = (SSkmDbKey *)pKey2;

  if (pSkmDbKey1->uid > pSkmDbKey2->uid) {
    return 1;
  } else if (pSkmDbKey1->uid < pSkmDbKey2->uid) {
    return -1;
  }

  if (pSkmDbKey1->sver > pSkmDbKey2->sver) {
    return 1;
  } else if (pSkmDbKey1->sver < pSkmDbKey2->sver) {
    return -1;
  }

  return 0;
}

static int uidIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  tb_uid_t uid1 = *(tb_uid_t *)pKey1;
  tb_uid_t uid2 = *(tb_uid_t *)pKey2;

  if (uid1 > uid2) {
    return 1;
  } else if (uid1 < uid2) {
    return -1;
  }

  return 0;
}

static int ctbIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  SCtbIdxKey *pCtbIdxKey1 = (SCtbIdxKey *)pKey1;
  SCtbIdxKey *pCtbIdxKey2 = (SCtbIdxKey *)pKey2;

  if (pCtbIdxKey1->suid > pCtbIdxKey2->suid) {
    return 1;
  } else if (pCtbIdxKey1->suid < pCtbIdxKey2->suid) {
    return -1;
  }

  if (pCtbIdxKey1->uid > pCtbIdxKey2->uid) {
    return 1;
  } else if (pCtbIdxKey1->uid < pCtbIdxKey2->uid) {
    return -1;
  }

  return 0;
}

static int tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STagIdxKey *pTagIdxKey1 = (STagIdxKey *)pKey1;
  STagIdxKey *pTagIdxKey2 = (STagIdxKey *)pKey2;
  tb_uid_t    uid1 = 0, uid2 = 0;
  int         c;

  // compare suid
  if (pTagIdxKey1->suid > pTagIdxKey2->suid) {
    return 1;
  } else if (pTagIdxKey1->suid < pTagIdxKey2->suid) {
    return -1;
  }

  // compare column id
  if (pTagIdxKey1->cid > pTagIdxKey2->cid) {
    return 1;
  } else if (pTagIdxKey1->cid < pTagIdxKey2->cid) {
    return -1;
  }

  if (pTagIdxKey1->type != pTagIdxKey2->type) {
    metaError("meta/open: incorrect tag idx type.");
    return TSDB_CODE_FAILED;
  }

  // check NULL, NULL is always the smallest
  if (pTagIdxKey1->isNull && !pTagIdxKey2->isNull) {
    return -1;
  } else if (!pTagIdxKey1->isNull && pTagIdxKey2->isNull) {
    return 1;
  } else if (!pTagIdxKey1->isNull && !pTagIdxKey2->isNull) {
    // all not NULL, compr tag vals
    __compar_fn_t func = getComparFunc(pTagIdxKey1->type, 0);
    if (func == NULL) {
      metaError("meta/open: %s", terrstr());
      return TSDB_CODE_FAILED;
    }
    c = func(pTagIdxKey1->data, pTagIdxKey2->data);
    if (c) return c;
  }

  // both null or tag values are equal, then continue to compare uids
  if (IS_VAR_DATA_TYPE(pTagIdxKey1->type)) {
    uid1 = *(tb_uid_t *)(pTagIdxKey1->data + varDataTLen(pTagIdxKey1->data));
    uid2 = *(tb_uid_t *)(pTagIdxKey2->data + varDataTLen(pTagIdxKey2->data));
  } else {
    uid1 = *(tb_uid_t *)(pTagIdxKey1->data + tDataTypes[pTagIdxKey1->type].bytes);
    uid2 = *(tb_uid_t *)(pTagIdxKey2->data + tDataTypes[pTagIdxKey2->type].bytes);
  }

  // compare uid
  if (uid1 < uid2) {
    return -1;
  } else if (uid1 > uid2) {
    return 1;
  } else {
    return 0;
  }

  return 0;
}

static int btimeIdxCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  SBtimeIdxKey *pBtimeIdxKey1 = (SBtimeIdxKey *)pKey1;
  SBtimeIdxKey *pBtimeIdxKey2 = (SBtimeIdxKey *)pKey2;
  if (pBtimeIdxKey1->btime > pBtimeIdxKey2->btime) {
    return 1;
  } else if (pBtimeIdxKey1->btime < pBtimeIdxKey2->btime) {
    return -1;
  }

  if (pBtimeIdxKey1->uid > pBtimeIdxKey2->uid) {
    return 1;
  } else if (pBtimeIdxKey1->uid < pBtimeIdxKey2->uid) {
    return -1;
  }

  return 0;
}

static int ncolIdxCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  SNcolIdxKey *pNcolIdxKey1 = (SNcolIdxKey *)pKey1;
  SNcolIdxKey *pNcolIdxKey2 = (SNcolIdxKey *)pKey2;

  if (pNcolIdxKey1->ncol > pNcolIdxKey2->ncol) {
    return 1;
  } else if (pNcolIdxKey1->ncol < pNcolIdxKey2->ncol) {
    return -1;
  }

  if (pNcolIdxKey1->uid > pNcolIdxKey2->uid) {
    return 1;
  } else if (pNcolIdxKey1->uid < pNcolIdxKey2->uid) {
    return -1;
  }

  return 0;
}

static int smaIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  SSmaIdxKey *pSmaIdxKey1 = (SSmaIdxKey *)pKey1;
  SSmaIdxKey *pSmaIdxKey2 = (SSmaIdxKey *)pKey2;

  if (pSmaIdxKey1->uid > pSmaIdxKey2->uid) {
    return 1;
  } else if (pSmaIdxKey1->uid < pSmaIdxKey2->uid) {
    return -1;
  }

  if (pSmaIdxKey1->smaUid > pSmaIdxKey2->smaUid) {
    return 1;
  } else if (pSmaIdxKey1->smaUid < pSmaIdxKey2->smaUid) {
    return -1;
  }

  return 0;
}

static int taskIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  int32_t uid1 = *(int32_t *)pKey1;
  int32_t uid2 = *(int32_t *)pKey2;

  if (uid1 > uid2) {
    return 1;
  } else if (uid1 < uid2) {
    return -1;
  }

  return 0;
}

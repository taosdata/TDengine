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

static int tbDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int skmDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int ctbIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int ttlIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int uidIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int smaIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

static int32_t metaInitLock(SMeta *pMeta) { return taosThreadRwlockInit(&pMeta->lock, NULL); }
static int32_t metaDestroyLock(SMeta *pMeta) { return taosThreadRwlockDestroy(&pMeta->lock); }

int metaOpen(SVnode *pVnode, SMeta **ppMeta) {
  SMeta *pMeta = NULL;
  int    ret;
  int    slen;

  *ppMeta = NULL;

  // create handle
  slen = strlen(tfsGetPrimaryPath(pVnode->pTfs)) + strlen(pVnode->path) + strlen(VNODE_META_DIR) + 3;
  if ((pMeta = taosMemoryCalloc(1, sizeof(*pMeta) + slen)) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  metaInitLock(pMeta);
  pMeta->path = (char *)&pMeta[1];
  sprintf(pMeta->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
          VNODE_META_DIR);
  pMeta->pVnode = pVnode;

  // create path if not created yet
  taosMkDir(pMeta->path);

  // open env
  ret = tdbOpen(pMeta->path, pVnode->config.szPage, pVnode->config.szCache, &pMeta->pEnv);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta env since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTbDb
  ret = tdbTbOpen("table.db", sizeof(STbDbKey), -1, tbDbKeyCmpr, pMeta->pEnv, &pMeta->pTbDb);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta table db since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pSkmDb
  ret = tdbTbOpen("schema.db", sizeof(SSkmDbKey), -1, skmDbKeyCmpr, pMeta->pEnv, &pMeta->pSkmDb);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta schema db since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pUidIdx
  ret = tdbTbOpen("uid.idx", sizeof(tb_uid_t), sizeof(int64_t), uidIdxKeyCmpr, pMeta->pEnv, &pMeta->pUidIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta uid idx since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pNameIdx
  ret = tdbTbOpen("name.idx", -1, sizeof(tb_uid_t), NULL, pMeta->pEnv, &pMeta->pNameIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta name index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pCtbIdx
  ret = tdbTbOpen("ctb.idx", sizeof(SCtbIdxKey), 0, ctbIdxKeyCmpr, pMeta->pEnv, &pMeta->pCtbIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta child table index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTagIdx
  ret = tdbTbOpen("tag.idx", -1, 0, tagIdxKeyCmpr, pMeta->pEnv, &pMeta->pTagIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta tag index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTtlIdx
  ret = tdbTbOpen("ttl.idx", sizeof(STtlIdxKey), 0, ttlIdxKeyCmpr, pMeta->pEnv, &pMeta->pTtlIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta ttl index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pSmaIdx
  ret = tdbTbOpen("sma.idx", sizeof(SSmaIdxKey), 0, smaIdxKeyCmpr, pMeta->pEnv, &pMeta->pSmaIdx);
  if (ret < 0) {
    metaError("vgId:%d failed to open meta sma index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open index
  if (metaOpenIdx(pMeta) < 0) {
    metaError("vgId:%d failed to open meta index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  metaDebug("vgId:%d meta is opened", TD_VID(pVnode));

  *ppMeta = pMeta;
  return 0;

_err:
  if (pMeta->pIdx) metaCloseIdx(pMeta);
  if (pMeta->pSmaIdx) tdbTbClose(pMeta->pSmaIdx);
  if (pMeta->pTtlIdx) tdbTbClose(pMeta->pTtlIdx);
  if (pMeta->pTagIdx) tdbTbClose(pMeta->pTagIdx);
  if (pMeta->pCtbIdx) tdbTbClose(pMeta->pCtbIdx);
  if (pMeta->pNameIdx) tdbTbClose(pMeta->pNameIdx);
  if (pMeta->pUidIdx) tdbTbClose(pMeta->pUidIdx);
  if (pMeta->pSkmDb) tdbTbClose(pMeta->pSkmDb);
  if (pMeta->pTbDb) tdbTbClose(pMeta->pTbDb);
  if (pMeta->pEnv) tdbClose(pMeta->pEnv);
  metaDestroyLock(pMeta);
  taosMemoryFree(pMeta);
  return -1;
}

int metaClose(SMeta *pMeta) {
  if (pMeta) {
    if (pMeta->pIdx) metaCloseIdx(pMeta);
    if (pMeta->pSmaIdx) tdbTbClose(pMeta->pSmaIdx);
    if (pMeta->pTtlIdx) tdbTbClose(pMeta->pTtlIdx);
    if (pMeta->pTagIdx) tdbTbClose(pMeta->pTagIdx);
    if (pMeta->pCtbIdx) tdbTbClose(pMeta->pCtbIdx);
    if (pMeta->pNameIdx) tdbTbClose(pMeta->pNameIdx);
    if (pMeta->pUidIdx) tdbTbClose(pMeta->pUidIdx);
    if (pMeta->pSkmDb) tdbTbClose(pMeta->pSkmDb);
    if (pMeta->pTbDb) tdbTbClose(pMeta->pTbDb);
    if (pMeta->pEnv) tdbClose(pMeta->pEnv);
    metaDestroyLock(pMeta);
    taosMemoryFree(pMeta);
  }

  return 0;
}

int32_t metaRLock(SMeta *pMeta) { return taosThreadRwlockRdlock(&pMeta->lock); }

int32_t metaWLock(SMeta *pMeta) { return taosThreadRwlockWrlock(&pMeta->lock); }

int32_t metaULock(SMeta *pMeta) { return taosThreadRwlockUnlock(&pMeta->lock); }

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
  tb_uid_t    uid1, uid2;
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

  ASSERT(pTagIdxKey1->type == pTagIdxKey2->type);

  // check NULL, NULL is always the smallest
  if (pTagIdxKey1->isNull && !pTagIdxKey2->isNull) {
    return -1;
  } else if (!pTagIdxKey1->isNull && pTagIdxKey2->isNull) {
    return 1;
  } else if (!pTagIdxKey1->isNull && !pTagIdxKey2->isNull) {
    // all not NULL, compr tag vals
    c = doCompare(pTagIdxKey1->data, pTagIdxKey2->data, pTagIdxKey1->type, 0);
    if (c) return c;

    if (IS_VAR_DATA_TYPE(pTagIdxKey1->type)) {
      uid1 = *(tb_uid_t *)(pTagIdxKey1->data + varDataTLen(pTagIdxKey1->data));
      uid2 = *(tb_uid_t *)(pTagIdxKey2->data + varDataTLen(pTagIdxKey2->data));
    } else {
      uid1 = *(tb_uid_t *)(pTagIdxKey1->data + tDataTypes[pTagIdxKey1->type].bytes);
      uid2 = *(tb_uid_t *)(pTagIdxKey2->data + tDataTypes[pTagIdxKey2->type].bytes);
    }
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

static int ttlIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STtlIdxKey *pTtlIdxKey1 = (STtlIdxKey *)pKey1;
  STtlIdxKey *pTtlIdxKey2 = (STtlIdxKey *)pKey2;

  if (pTtlIdxKey1->dtime > pTtlIdxKey2->dtime) {
    return 1;
  } else if (pTtlIdxKey1->dtime < pTtlIdxKey2->dtime) {
    return -1;
  }

  if (pTtlIdxKey1->uid > pTtlIdxKey2->uid) {
    return 1;
  } else if (pTtlIdxKey1->uid < pTtlIdxKey2->uid) {
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

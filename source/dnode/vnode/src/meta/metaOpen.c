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

  pMeta->path = (char *)&pMeta[1];
  sprintf(pMeta->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
          VNODE_META_DIR);
  pMeta->pVnode = pVnode;

  // create path if not created yet
  taosMkDir(pMeta->path);

  // open env
  ret = tdbEnvOpen(pMeta->path, pVnode->config.szPage, pVnode->config.szCache, &pMeta->pEnv);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta env since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTbDb
  ret = tdbDbOpen("table.db", sizeof(STbDbKey), -1, tbDbKeyCmpr, pMeta->pEnv, &pMeta->pTbDb);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta table db since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pSkmDb
  ret = tdbDbOpen("schema.db", sizeof(SSkmDbKey), -1, skmDbKeyCmpr, pMeta->pEnv, &pMeta->pSkmDb);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta schema db since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pUidIdx
  ret = tdbDbOpen("uid.idx", sizeof(tb_uid_t), sizeof(int64_t), uidIdxKeyCmpr, pMeta->pEnv, &pMeta->pUidIdx);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta uid idx since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pNameIdx
  ret = tdbDbOpen("name.idx", -1, sizeof(tb_uid_t), NULL, pMeta->pEnv, &pMeta->pNameIdx);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta name index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pCtbIdx
  ret = tdbDbOpen("ctb.idx", sizeof(SCtbIdxKey), 0, ctbIdxKeyCmpr, pMeta->pEnv, &pMeta->pCtbIdx);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta child table index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTagIdx
  ret = tdbDbOpen("tag.idx", -1, 0, tagIdxKeyCmpr, pMeta->pEnv, &pMeta->pTagIdx);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta tag index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open pTtlIdx
  ret = tdbDbOpen("ttl.idx", sizeof(STtlIdxKey), 0, ttlIdxKeyCmpr, pMeta->pEnv, &pMeta->pTtlIdx);
  if (ret < 0) {
    metaError("vgId: %d failed to open meta ttl index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  // open index
  if (metaOpenIdx(pMeta) < 0) {
    metaError("vgId: %d failed to open meta index since %s", TD_VID(pVnode), tstrerror(terrno));
    goto _err;
  }

  metaDebug("vgId: %d meta is opened", TD_VID(pVnode));

  *ppMeta = pMeta;
  return 0;

_err:
  if (pMeta->pIdx) metaCloseIdx(pMeta);
  if (pMeta->pTtlIdx) tdbDbClose(pMeta->pTtlIdx);
  if (pMeta->pTagIdx) tdbDbClose(pMeta->pTagIdx);
  if (pMeta->pCtbIdx) tdbDbClose(pMeta->pCtbIdx);
  if (pMeta->pNameIdx) tdbDbClose(pMeta->pNameIdx);
  if (pMeta->pNameIdx) tdbDbClose(pMeta->pUidIdx);
  if (pMeta->pSkmDb) tdbDbClose(pMeta->pSkmDb);
  if (pMeta->pTbDb) tdbDbClose(pMeta->pTbDb);
  if (pMeta->pEnv) tdbEnvClose(pMeta->pEnv);
  taosMemoryFree(pMeta);
  return -1;
}

int metaClose(SMeta *pMeta) {
  if (pMeta) {
    if (pMeta->pIdx) metaCloseIdx(pMeta);
    if (pMeta->pTtlIdx) tdbDbClose(pMeta->pTtlIdx);
    if (pMeta->pTagIdx) tdbDbClose(pMeta->pTagIdx);
    if (pMeta->pCtbIdx) tdbDbClose(pMeta->pCtbIdx);
    if (pMeta->pNameIdx) tdbDbClose(pMeta->pNameIdx);
    if (pMeta->pNameIdx) tdbDbClose(pMeta->pUidIdx);
    if (pMeta->pSkmDb) tdbDbClose(pMeta->pSkmDb);
    if (pMeta->pTbDb) tdbDbClose(pMeta->pTbDb);
    if (pMeta->pEnv) tdbEnvClose(pMeta->pEnv);
    taosMemoryFree(pMeta);
  }

  return 0;
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
  int8_t     *p1, *p2;
  int8_t      type;
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

  // compare value
  p1 = pTagIdxKey1->data;
  p2 = pTagIdxKey2->data;
  ASSERT(p1[0] == p2[0]);
  type = p1[0];

  p1++;
  p2++;

  c = doCompare(p1, p2, type, 0);
  if (c) return c;

  if (IS_VAR_DATA_TYPE(type)) {
    p1 = p1 + varDataTLen(p1);
    p2 = p2 + varDataTLen(p2);
  } else {
    p1 = p1 + tDataTypes[type].bytes;
    p2 = p2 + tDataTypes[type].bytes;
  }

  // compare suid
  if (*(tb_uid_t *)p1 > *(tb_uid_t *)p2) {
    return 1;
  } else if (*(tb_uid_t *)p1 < *(tb_uid_t *)p2) {
    return -1;
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

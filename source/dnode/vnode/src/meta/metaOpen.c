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

#ifndef NO_UNALIGNED_ACCESS
#define TDB_KEY_ALIGN(k1, k2, kType)
#else
#define TDB_KEY_ALIGN(k1, k2, kType)   \
  kType _k1, _k2;                      \
  if (((uintptr_t)(k1) & 7)) {         \
    memcpy(&_k1, (k1), sizeof(kType)); \
    (k1) = &_k1;                       \
  }                                    \
  if (((uintptr_t)(k2) & 7)) {         \
    memcpy(&_k2, (k2), sizeof(kType)); \
    (k2) = &_k2;                       \
  }
#endif

static int tbDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int skmDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int ctbIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
int        tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
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

static void doScan(SMeta *pMeta) {
  TBC    *cursor = NULL;
  int32_t code;

  // open file to write
  char path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN - 1, "%s%s", pMeta->path, TD_DIRSEP "scan.txt");
  TdFilePtr fp = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (fp == NULL) {
    metaError("failed to open file:%s, reason:%s", path, tstrerror(terrno));
    return;
  }

  code = tdbTbcOpen(pMeta->pTbDb, &cursor, NULL);
  if (code) {
    if (taosCloseFile(&fp) != 0) {
      metaError("failed to close file:%s, reason:%s", path, tstrerror(terrno));
    }
    metaError("failed to open table.db cursor, reason:%s", tstrerror(terrno));
    return;
  }

  code = tdbTbcMoveToFirst(cursor);
  if (code) {
    if (taosCloseFile(&fp) != 0) {
      metaError("failed to close file:%s, reason:%s", path, tstrerror(terrno));
    }
    tdbTbcClose(cursor);
    metaError("failed to move to first, reason:%s", tstrerror(terrno));
    return;
  }

  for (;;) {
    const void *pKey;
    int         kLen;
    const void *pVal;
    int         vLen;
    if (tdbTbcGet(cursor, &pKey, &kLen, &pVal, &vLen) < 0) {
      break;
    }

    // decode entry
    SDecoder   dc = {0};
    SMetaEntry me = {0};

    tDecoderInit(&dc, (uint8_t *)pVal, vLen);

    if (metaDecodeEntry(&dc, &me) < 0) {
      tDecoderClear(&dc);
      break;
    }

    // skip deleted entry
    if (tdbTbGet(pMeta->pUidIdx, &me.uid, sizeof(me.uid), NULL, NULL) == 0) {
      // print entry
      char buf[1024] = {0};
      if (me.type == TSDB_SUPER_TABLE) {
        snprintf(buf, sizeof(buf) - 1, "type: super table, version:%" PRId64 " uid: %" PRId64 " name: %s\n", me.version,
                 me.uid, me.name);

      } else if (me.type == TSDB_CHILD_TABLE) {
        snprintf(buf, sizeof(buf) - 1,
                 "type: child table, version:%" PRId64 " uid: %" PRId64 " name: %s suid:%" PRId64 "\n", me.version,
                 me.uid, me.name, me.ctbEntry.suid);
      } else {
        snprintf(buf, sizeof(buf) - 1, "type: normal table, version:%" PRId64 " uid: %" PRId64 " name: %s\n",
                 me.version, me.uid, me.name);
      }

      if (taosWriteFile(fp, buf, strlen(buf)) < 0) {
        metaError("failed to write file:%s, reason:%s", path, tstrerror(terrno));
        tDecoderClear(&dc);
        break;
      }
    }

    tDecoderClear(&dc);

    if (tdbTbcMoveToNext(cursor) < 0) {
      break;
    }
  }

  tdbTbcClose(cursor);

  // close file
  if (taosFsyncFile(fp) < 0) {
    metaError("failed to fsync file:%s, reason:%s", path, tstrerror(terrno));
  }
  if (taosCloseFile(&fp) < 0) {
    metaError("failed to close file:%s, reason:%s", path, tstrerror(terrno));
  }
}

int32_t metaOpenImpl(SVnode *pVnode, SMeta **ppMeta, const char *metaDir, int8_t rollback) {
  SMeta  *pMeta = NULL;
  int32_t code = 0;
  int32_t lino;
  int32_t offset;
  int32_t pathLen = 0;
  char    path[TSDB_FILENAME_LEN] = {0};
  char    indexFullPath[128] = {0};

  // create handle
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, path, TSDB_FILENAME_LEN);
  offset = strlen(path);
  snprintf(path + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, metaDir);

  if (strncmp(metaDir, VNODE_META_TMP_DIR, strlen(VNODE_META_TMP_DIR)) == 0) {
    taosRemoveDir(path);
  }

  pathLen = strlen(path) + 1;
  if ((pMeta = taosMemoryCalloc(1, sizeof(*pMeta) + pathLen)) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  metaInitLock(pMeta);

  pMeta->path = (char *)&pMeta[1];
  tstrncpy(pMeta->path, path, pathLen);
  int32_t ret = taosRealPath(pMeta->path, NULL, strlen(path) + 1);

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

  (void)tsnprintf(indexFullPath, sizeof(indexFullPath), "%s/%s", pMeta->path, "invert");
  ret = taosMkDir(indexFullPath);

  SIndexOpts opts = {.cacheSize = 8 * 1024 * 1024};
  code = indexOpen(&opts, indexFullPath, (SIndex **)&pMeta->pTagIvtIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdbTbOpen("tag.idx", -1, 0, tagIdxKeyCmpr, pMeta->pEnv, &pMeta->pTagIdx, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open pTtlMgr ("ttlv1.idx")
  char logPrefix[128] = {0};
  (void)tsnprintf(logPrefix, sizeof(logPrefix), "vgId:%d", TD_VID(pVnode));
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

#if 0
  // Do NOT remove this code, it is used to do debug stuff
  doScan(pMeta);
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    metaCleanup(&pMeta);
    *ppMeta = NULL;
  } else {
    metaDebug("vgId:%d %s success", TD_VID(pVnode), __func__);
    *ppMeta = pMeta;
  }
  TAOS_RETURN(code);
}

void vnodeGetMetaPath(SVnode *pVnode, const char *metaDir, char *fname) {
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, fname, TSDB_FILENAME_LEN);
  int32_t offset = strlen(fname);
  snprintf(fname + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, metaDir);
}

bool generateNewMeta = false;

static int32_t metaGenerateNewMeta(SMeta **ppMeta) {
  SMeta  *pNewMeta = NULL;
  SMeta  *pMeta = *ppMeta;
  SVnode *pVnode = pMeta->pVnode;

  metaInfo("vgId:%d start to generate new meta", TD_VID(pMeta->pVnode));

  // Open a new meta for organization
  int32_t code = metaOpenImpl(pMeta->pVnode, &pNewMeta, VNODE_META_TMP_DIR, false);
  if (code) {
    return code;
  }

  code = metaBegin(pNewMeta, META_BEGIN_HEAP_NIL);
  if (code) {
    return code;
  }

#if 1
  // i == 0, scan super table
  // i == 1, scan normal table and child table
  for (int i = 0; i < 2; i++) {
    TBC    *uidCursor = NULL;
    int32_t counter = 0;

    code = tdbTbcOpen(pMeta->pUidIdx, &uidCursor, NULL);
    if (code) {
      metaError("vgId:%d failed to open uid index cursor, reason:%s", TD_VID(pVnode), tstrerror(code));
      return code;
    }

    code = tdbTbcMoveToFirst(uidCursor);
    if (code) {
      metaError("vgId:%d failed to move to first, reason:%s", TD_VID(pVnode), tstrerror(code));
      tdbTbcClose(uidCursor);
      return code;
    }

    for (;;) {
      const void *pKey;
      int         kLen;
      const void *pVal;
      int         vLen;

      if (tdbTbcGet(uidCursor, &pKey, &kLen, &pVal, &vLen) < 0) {
        break;
      }

      tb_uid_t    uid = *(tb_uid_t *)pKey;
      SUidIdxVal *pUidIdxVal = (SUidIdxVal *)pVal;
      if ((i == 0 && (pUidIdxVal->suid && pUidIdxVal->suid == uid))          // super table
          || (i == 1 && (pUidIdxVal->suid == 0 || pUidIdxVal->suid != uid))  // normal table and child table
      ) {
        counter++;
        if (i == 0) {
          metaInfo("vgId:%d counter:%d new meta handle %s table uid:%" PRId64, TD_VID(pVnode), counter, "super", uid);
        } else {
          metaInfo("vgId:%d counter:%d new meta handle %s table uid:%" PRId64, TD_VID(pVnode), counter,
                   pUidIdxVal->suid == 0 ? "normal" : "child", uid);
        }

        // fetch table entry
        void *value = NULL;
        int   valueSize = 0;
        if (tdbTbGet(pMeta->pTbDb,
                     &(STbDbKey){
                         .version = pUidIdxVal->version,
                         .uid = uid,
                     },
                     sizeof(uid), &value, &valueSize) == 0) {
          SDecoder   dc = {0};
          SMetaEntry me = {0};
          tDecoderInit(&dc, value, valueSize);
          if (metaDecodeEntry(&dc, &me) == 0) {
            if (me.type == TSDB_CHILD_TABLE &&
                tdbTbGet(pMeta->pUidIdx, &me.ctbEntry.suid, sizeof(me.ctbEntry.suid), NULL, NULL) != 0) {
              metaError("vgId:%d failed to get super table uid:%" PRId64 " for child table uid:%" PRId64,
                        TD_VID(pVnode), me.ctbEntry.suid, uid);
            } else if (metaHandleEntry2(pNewMeta, &me) != 0) {
              metaError("vgId:%d failed to handle entry, uid:%" PRId64, TD_VID(pVnode), uid);
            }
          }
          tDecoderClear(&dc);
        }
        tdbFree(value);
      }

      code = tdbTbcMoveToNext(uidCursor);
      if (code) {
        metaError("vgId:%d failed to move to next, reason:%s", TD_VID(pVnode), tstrerror(code));
        return code;
      }
    }

    tdbTbcClose(uidCursor);
  }
#else
  TBC *cursor = NULL;

  code = tdbTbcOpen(pMeta->pTbDb, &cursor, NULL);
  if (code) {
    metaError("vgId:%d failed to open table.db cursor, reason:%s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  code = tdbTbcMoveToFirst(cursor);
  if (code) {
    metaError("vgId:%d failed to move to first, reason:%s", TD_VID(pVnode), tstrerror(code));
    tdbTbcClose(cursor);
    return code;
  }

  while (true) {
    const void *pKey;
    int         kLen;
    const void *pVal;
    int         vLen;

    if (tdbTbcGet(cursor, &pKey, &kLen, &pVal, &vLen) < 0) {
      break;
    }

    STbDbKey  *pKeyEntry = (STbDbKey *)pKey;
    SDecoder   dc = {0};
    SMetaEntry me = {0};

    tDecoderInit(&dc, (uint8_t *)pVal, vLen);
    if (metaDecodeEntry(&dc, &me) < 0) {
      tDecoderClear(&dc);
      break;
    }

    if (metaHandleEntry2(pNewMeta, &me) != 0) {
      metaError("vgId:%d failed to handle entry, uid:%" PRId64, TD_VID(pVnode), pKeyEntry->uid);
      tDecoderClear(&dc);
      break;
    }
    tDecoderClear(&dc);

    code = tdbTbcMoveToNext(cursor);
    if (code) {
      metaError("vgId:%d failed to move to next, reason:%s", TD_VID(pVnode), tstrerror(code));
      break;
    }
  }

  tdbTbcClose(cursor);

#endif

  code = metaCommit(pNewMeta, pNewMeta->txn);
  if (code) {
    metaError("vgId:%d failed to commit, reason:%s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  code = metaFinishCommit(pNewMeta, pNewMeta->txn);
  if (code) {
    metaError("vgId:%d failed to finish commit, reason:%s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  if ((code = metaBegin(pNewMeta, META_BEGIN_HEAP_NIL)) != 0) {
    metaError("vgId:%d failed to begin new meta, reason:%s", TD_VID(pVnode), tstrerror(code));
  }
  metaClose(&pNewMeta);
  metaInfo("vgId:%d finish to generate new meta", TD_VID(pVnode));

  // Commit the new metadata
  char metaDir[TSDB_FILENAME_LEN] = {0};
  char metaTempDir[TSDB_FILENAME_LEN] = {0};
  char metaBackupDir[TSDB_FILENAME_LEN] = {0};

  vnodeGetMetaPath(pVnode, VNODE_META_DIR, metaDir);
  vnodeGetMetaPath(pVnode, VNODE_META_TMP_DIR, metaTempDir);
  vnodeGetMetaPath(pVnode, VNODE_META_BACKUP_DIR, metaBackupDir);

  metaClose(ppMeta);
  if (taosRenameFile(metaDir, metaBackupDir) != 0) {
    metaError("vgId:%d failed to rename old meta to backup, reason:%s", TD_VID(pVnode), tstrerror(terrno));
    return terrno;
  }

  // rename the new meta to old meta
  if (taosRenameFile(metaTempDir, metaDir) != 0) {
    metaError("vgId:%d failed to rename new meta to old meta, reason:%s", TD_VID(pVnode), tstrerror(terrno));
    return terrno;
  }

  code = metaOpenImpl(pVnode, ppMeta, VNODE_META_DIR, false);
  if (code) {
    metaError("vgId:%d failed to open new meta, reason:%s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  metaInfo("vgId:%d successfully opened new meta", TD_VID(pVnode));

  return 0;
}

int32_t metaOpen(SVnode *pVnode, SMeta **ppMeta, int8_t rollback) {
  int32_t code = TSDB_CODE_SUCCESS;
  char    metaDir[TSDB_FILENAME_LEN] = {0};
  char    metaBackupDir[TSDB_FILENAME_LEN] = {0};
  char    metaTempDir[TSDB_FILENAME_LEN] = {0};

  vnodeGetMetaPath(pVnode, VNODE_META_DIR, metaDir);
  vnodeGetMetaPath(pVnode, VNODE_META_BACKUP_DIR, metaBackupDir);
  vnodeGetMetaPath(pVnode, VNODE_META_TMP_DIR, metaTempDir);

  bool metaExists = taosCheckExistFile(metaDir);
  bool metaBackupExists = taosCheckExistFile(metaBackupDir);
  bool metaTempExists = taosCheckExistFile(metaTempDir);

  if ((!metaBackupExists && !metaExists && metaTempExists)     //
      || (metaBackupExists && !metaExists && !metaTempExists)  //
      || (metaBackupExists && metaExists && metaTempExists)    //
  ) {
    metaError("vgId:%d, invalid meta state, please check!", TD_VID(pVnode));
    TAOS_RETURN(TSDB_CODE_FAILED);
  } else if (!metaBackupExists && metaExists && metaTempExists) {
    taosRemoveDir(metaTempDir);
  } else if (metaBackupExists && !metaExists && metaTempExists) {
    code = taosRenameFile(metaTempDir, metaDir);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
      TAOS_RETURN(code);
    }
    taosRemoveDir(metaBackupDir);
  } else if (metaBackupExists && metaExists && !metaTempExists) {
    taosRemoveDir(metaBackupDir);
  }

  // Do open meta
  code = metaOpenImpl(pVnode, ppMeta, VNODE_META_DIR, rollback);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  if (generateNewMeta) {
    code = metaGenerateNewMeta(ppMeta);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
      TAOS_RETURN(code);
    }
  }

  return TSDB_CODE_SUCCESS;
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
    metaInfo("vgId:%d meta clean up, path:%s", TD_VID(pMeta->pVnode), pMeta->path);
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

  TDB_KEY_ALIGN(pTbDbKey1, pTbDbKey2, STbDbKey);

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

  TDB_KEY_ALIGN(pSkmDbKey1, pSkmDbKey2, SSkmDbKey);

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
  tb_uid_t uid1 = taosGetInt64Aligned((int64_t*)pKey1);
  tb_uid_t uid2 = taosGetInt64Aligned((int64_t*)pKey2);

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

  TDB_KEY_ALIGN(pCtbIdxKey1, pCtbIdxKey2, SCtbIdxKey);

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

int tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  STagIdxKey *pTagIdxKey1 = (STagIdxKey *)pKey1;
  STagIdxKey *pTagIdxKey2 = (STagIdxKey *)pKey2;
  tb_uid_t    uid1 = 0, uid2 = 0;
  int         c;

  TDB_KEY_ALIGN(pTagIdxKey1, pTagIdxKey2, STagIdxKey);

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

  TDB_KEY_ALIGN(pBtimeIdxKey1, pBtimeIdxKey2, SBtimeIdxKey);

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

  TDB_KEY_ALIGN(pNcolIdxKey1, pNcolIdxKey2, SNcolIdxKey);

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

  TDB_KEY_ALIGN(pSmaIdxKey1, pSmaIdxKey2, SSmaIdxKey);

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

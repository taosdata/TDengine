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

/*
 * meta-reader: A diagnostic / repair tool for TDengine meta tdb files.
 *
 * All tdb tables live inside a single "main.tdb" file (USE_MAINDB mode).
 * The tool opens the tdb environment directly, then uses the same key
 * structures as the meta module.
 *
 * Usage:
 *
 *   meta-reader <meta-dir> list
 *       Scan uid.idx for every child table and print uid, suid, name.
 *
 *   meta-reader <meta-dir> update-suid <uid> <new-suid>
 *       Change the suid of a single child table (uid.idx, table.db,
 *       ctb.idx updated atomically).
 *
 *   meta-reader <meta-dir> batch-update-suid <old-suid> <new-suid>
 *       Change the suid of ALL child tables whose suid == old-suid.
 *       Reads are done first (no write-while-iterate), then all writes
 *       are applied in one atomic transaction.
 *
 *   meta-reader <meta-dir> debug
 *       Print the main-db catalog (all logical table names + pgno),
 *       then count entries in uid.idx and ctb.idx.
 *
 * <meta-dir>  path to the meta directory, e.g. /var/lib/taos/vnode/vnode2/meta
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "meta.h"       /* SUidIdxVal, SCtbIdxKey, STbDbKey, SMetaEntry */
#include "tdbInt.h"     /* SBtInfo, pMainDb, USE_MAINDB internals        */

/* -------------------------------------------------------------------------
 * Simple malloc/free wrappers for tdbBegin's function-pointer signature.
 * (TDengine #defines malloc/free to forbidden sentinels, so we use the
 *  underlying taosMemMalloc / taosMemFree.)
 * ---------------------------------------------------------------------- */
static void *toolMalloc(void *arg, size_t size) { return taosMemMalloc((int64_t)size); }
static void  toolFree(void *arg, void *ptr)     { taosMemFree(ptr); }

/* -------------------------------------------------------------------------
 * Key comparators (mirrors of the static functions in metaOpen.c).
 * ---------------------------------------------------------------------- */
static int tbDbKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  const STbDbKey *k1 = (const STbDbKey *)pKey1;
  const STbDbKey *k2 = (const STbDbKey *)pKey2;
  if (k1->version > k2->version) return 1;
  if (k1->version < k2->version) return -1;
  if (k1->uid > k2->uid) return 1;
  if (k1->uid < k2->uid) return -1;
  return 0;
}

static int uidIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  tb_uid_t uid1 = *(const tb_uid_t *)pKey1;
  tb_uid_t uid2 = *(const tb_uid_t *)pKey2;
  if (uid1 > uid2) return 1;
  if (uid1 < uid2) return -1;
  return 0;
}

static int ctbIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  const SCtbIdxKey *k1 = (const SCtbIdxKey *)pKey1;
  const SCtbIdxKey *k2 = (const SCtbIdxKey *)pKey2;
  if (k1->suid > k2->suid) return 1;
  if (k1->suid < k2->suid) return -1;
  if (k1->uid > k2->uid) return 1;
  if (k1->uid < k2->uid) return -1;
  return 0;
}

/* -------------------------------------------------------------------------
 * Context: open/close the required tdb tables.
 * ---------------------------------------------------------------------- */
typedef struct {
  TDB *pEnv;
  TTB *pTbDb;   /* table.db  – encoded SMetaEntry  */
  TTB *pUidIdx; /* uid.idx   – SUidIdxVal           */
  TTB *pCtbIdx; /* ctb.idx   – SCtbIdxKey / tags    */
} SMetaReaderCtx;

static int metaReaderOpen(SMetaReaderCtx *r, const char *metaDir) {
  int rc;

  rc = tdbOpen(metaDir, 4096, 256, &r->pEnv, 0, 0, "");
  if (rc != 0) {
    fprintf(stderr, "tdbOpen(%s) failed: rc=%d\n", metaDir, rc);
    return rc;
  }

  rc = tdbTbOpen("table.db", sizeof(STbDbKey), -1, tbDbKeyCmpr, r->pEnv, &r->pTbDb, 0);
  if (rc != 0) { fprintf(stderr, "open table.db failed: rc=%d\n", rc); return rc; }

  rc = tdbTbOpen("uid.idx", sizeof(tb_uid_t), sizeof(SUidIdxVal), uidIdxKeyCmpr, r->pEnv, &r->pUidIdx, 0);
  if (rc != 0) { fprintf(stderr, "open uid.idx failed: rc=%d\n", rc); return rc; }

  rc = tdbTbOpen("ctb.idx", sizeof(SCtbIdxKey), -1, ctbIdxKeyCmpr, r->pEnv, &r->pCtbIdx, 0);
  if (rc != 0) { fprintf(stderr, "open ctb.idx failed: rc=%d\n", rc); return rc; }

  return 0;
}

static void metaReaderClose(SMetaReaderCtx *r) {
  if (r->pCtbIdx) tdbTbClose(r->pCtbIdx);
  if (r->pUidIdx) tdbTbClose(r->pUidIdx);
  if (r->pTbDb)   tdbTbClose(r->pTbDb);
  if (r->pEnv)    tdbClose(r->pEnv);
}

/* -------------------------------------------------------------------------
 * Helper: look up the latest table.db entry for a uid.
 * Caller must tdbFree(*ppVal) when done.
 * ---------------------------------------------------------------------- */
static int lookupEntryByUid(SMetaReaderCtx *r, tb_uid_t uid, void **ppVal, int *pVLen) {
  void      *pUidIdxBuf = NULL;
  int        uLen = 0;
  int        rc;

  rc = tdbTbGet(r->pUidIdx, &uid, sizeof(uid), &pUidIdxBuf, &uLen);
  if (rc != 0) return rc;

  SUidIdxVal *pIdxVal = (SUidIdxVal *)pUidIdxBuf;
  STbDbKey    tbKey   = { .version = pIdxVal->version, .uid = uid };
  tdbFree(pUidIdxBuf);

  rc = tdbTbGet(r->pTbDb, &tbKey, sizeof(tbKey), ppVal, pVLen);
  return rc;
}

/* -------------------------------------------------------------------------
 * Command: list
 *
 * Scan uid.idx.  For each entry where val.suid != 0 && val.suid != uid the
 * table is a child table.  Then decode the full entry from table.db to get
 * the name.  (This mirrors metaGenerateNewMeta / doScan in metaOpen.c.)
 * ---------------------------------------------------------------------- */
static int cmdList(SMetaReaderCtx *r) {
  TBC *pCur = NULL;
  int  rc;
  int  count = 0;

  rc = tdbTbcOpen(r->pUidIdx, &pCur, NULL);
  if (rc != 0) {
    fprintf(stderr, "Failed to open uid.idx cursor: rc=%d\n", rc);
    return rc;
  }

  rc = tdbTbcMoveToFirst(pCur);
  if (rc != 0) {
    /* nothing in the index */
    tdbTbcClose(pCur);
    printf("(uid.idx is empty)\n");
    return 0;
  }

  printf("%-20s  %-20s  %s\n",
         "uid", "suid", "name");
  printf("%-20s  %-20s  %s\n",
         "--------------------", "--------------------", "----");

  for (;;) {
    const void *pKey = NULL;  int kLen = 0;
    const void *pVal = NULL;  int vLen = 0;

    if (tdbTbcGet(pCur, &pKey, &kLen, &pVal, &vLen) < 0) break;

    tb_uid_t    uid     = *(const tb_uid_t *)pKey;
    SUidIdxVal *pIdxVal = (SUidIdxVal *)pVal;
    tb_uid_t    suid    = pIdxVal->suid;

    /* child table: suid is set and differs from its own uid */
    if (suid != 0 && suid != uid) {
      char  name[TSDB_TABLE_NAME_LEN] = "<unknown>";
      void *pEntryBuf = NULL;
      int   eBufLen   = 0;

      if (lookupEntryByUid(r, uid, &pEntryBuf, &eBufLen) == 0) {
        SDecoder   dc = {0};
        SMetaEntry me = {0};
        tDecoderInit(&dc, (uint8_t *)pEntryBuf, eBufLen);
        if (metaDecodeEntry(&dc, &me) == 0 && me.name != NULL) {
          tstrncpy(name, me.name, TSDB_TABLE_NAME_LEN);
        }
        tDecoderClear(&dc);
        tdbFree(pEntryBuf);
      }

      printf("%-20" PRId64 "  %-20" PRId64 "  %s\n", uid, suid, name);
      count++;
    }

    if (tdbTbcMoveToNext(pCur) < 0) break;
  }

  tdbTbcClose(pCur);
  printf("--- total %d child tables ---\n", count);
  return 0;
}

/* -------------------------------------------------------------------------
 * Command: debug
 *
 * 1. Dump the main-db catalog (all logical table names registered in tdb).
 * 2. Count total entries in uid.idx.
 * 3. Count total entries in ctb.idx.
 * ---------------------------------------------------------------------- */
static int cmdDebug(SMetaReaderCtx *r) {
  TBC *pCur = NULL;
  int  rc;

#ifdef USE_MAINDB
  /* ---- 1. Scan pMainDb catalog ---- */
  printf("=== main.tdb catalog ===\n");
  printf("%-40s  %s\n", "table-name", "root-pgno");
  printf("%-40s  %s\n", "----------------------------------------", "---------");

  rc = tdbTbcOpen(r->pEnv->pMainDb, &pCur, NULL);
  if (rc != 0) {
    fprintf(stderr, "Cannot open pMainDb cursor: rc=%d\n", rc);
  } else {
    if (tdbTbcMoveToFirst(pCur) == 0) {
      for (;;) {
        const void *pKey = NULL; int kLen = 0;
        const void *pVal = NULL; int vLen = 0;
        if (tdbTbcGet(pCur, &pKey, &kLen, &pVal, &vLen) < 0) break;
        const SBtInfo *info = (const SBtInfo *)pVal;
        printf("%-40.*s  %u  (nLevel=%d, nData=%d)\n",
               kLen, (const char *)pKey, (unsigned)info->root, info->nLevel, info->nData);
        if (tdbTbcMoveToNext(pCur) < 0) break;
      }
    } else {
      printf("(empty)\n");
    }
    tdbTbcClose(pCur);
    pCur = NULL;
  }
#else
  printf("(USE_MAINDB not defined – separate files mode)\n");
#endif

  /* ---- 2. Count uid.idx ---- */
  printf("\n=== uid.idx ===\n");
  int uidTotal = 0, ctbCount = 0, stbCount = 0, ntbCount = 0;

  rc = tdbTbcOpen(r->pUidIdx, &pCur, NULL);
  if (rc == 0) {
    if (tdbTbcMoveToFirst(pCur) == 0) {
      for (;;) {
        const void *pKey = NULL; int kLen = 0;
        const void *pVal = NULL; int vLen = 0;
        if (tdbTbcGet(pCur, &pKey, &kLen, &pVal, &vLen) < 0) break;
        tb_uid_t    uid  = *(const tb_uid_t *)pKey;
        SUidIdxVal *pIdx = (SUidIdxVal *)pVal;
        uidTotal++;
        if (pIdx->suid == 0) ntbCount++;
        else if (pIdx->suid == uid) stbCount++;
        else ctbCount++;
        if (tdbTbcMoveToNext(pCur) < 0) break;
      }
    }
    tdbTbcClose(pCur);
    pCur = NULL;
  }
  printf("total=%d  (super=%d  child=%d  normal=%d)\n",
         uidTotal, stbCount, ctbCount, ntbCount);

  /* ---- 3. Count ctb.idx ---- */
  printf("\n=== ctb.idx ===\n");
  int ctbIdxCount = 0;

  rc = tdbTbcOpen(r->pCtbIdx, &pCur, NULL);
  if (rc == 0) {
    if (tdbTbcMoveToFirst(pCur) == 0) {
      for (;;) {
        const void *pKey = NULL; int kLen = 0;
        const void *pVal = NULL; int vLen = 0;
        if (tdbTbcGet(pCur, &pKey, &kLen, &pVal, &vLen) < 0) break;
        ctbIdxCount++;
        if (tdbTbcMoveToNext(pCur) < 0) break;
      }
    }
    tdbTbcClose(pCur);
    pCur = NULL;
  }
  printf("total=%d entries\n", ctbIdxCount);

  return 0;
}

/* -------------------------------------------------------------------------
 * Command: update-suid <uid> <new_suid>
 *
 * Atomically update three tdb tables inside one write transaction:
 *   1. uid.idx   – change val.suid
 *   2. ctb.idx   – delete {old_suid, uid}, insert {new_suid, uid}
 *   3. table.db  – re-encode entry with new suid and upsert
 * ---------------------------------------------------------------------- */
static int cmdUpdateSuid(SMetaReaderCtx *r, tb_uid_t uid, tb_uid_t newSuid) {
  int rc = 0;

  /* ---- Read current uid.idx entry ---- */
  void *pUidIdxBuf = NULL;
  int   uLen = 0;
  rc = tdbTbGet(r->pUidIdx, &uid, sizeof(uid), &pUidIdxBuf, &uLen);
  if (rc != 0) {
    fprintf(stderr, "uid %" PRId64 " not found in uid.idx\n", uid);
    return rc;
  }

  SUidIdxVal uidIdxVal;
  memcpy(&uidIdxVal, pUidIdxBuf, sizeof(uidIdxVal));
  tdbFree(pUidIdxBuf);

  if (uidIdxVal.suid == 0 || uidIdxVal.suid == uid) {
    fprintf(stderr, "uid %" PRId64 " is not a child table (suid=%" PRId64 ")\n",
            uid, uidIdxVal.suid);
    return -1;
  }

  tb_uid_t oldSuid = uidIdxVal.suid;

  /* ---- Read table.db entry ---- */
  STbDbKey tbKey   = { .version = uidIdxVal.version, .uid = uid };
  void    *pEntry  = NULL;
  int      entryLen = 0;
  rc = tdbTbGet(r->pTbDb, &tbKey, sizeof(tbKey), &pEntry, &entryLen);
  if (rc != 0) {
    fprintf(stderr, "uid %" PRId64 " not found in table.db\n", uid);
    return rc;
  }

  SDecoder   dc = {0};
  SMetaEntry me = {0};
  tDecoderInit(&dc, (uint8_t *)pEntry, entryLen);
  rc = metaDecodeEntry(&dc, &me);
  tdbFree(pEntry);
  if (rc != 0) {
    fprintf(stderr, "metaDecodeEntry failed: rc=%d\n", rc);
    tDecoderClear(&dc);
    return rc;
  }

  if (me.type != TSDB_CHILD_TABLE) {
    fprintf(stderr, "uid %" PRId64 " is type %d, not a child table\n", uid, me.type);
    tDecoderClear(&dc);
    return -1;
  }

  printf("uid=%" PRId64 "  name=%s  suid %" PRId64 " -> %" PRId64 "\n",
         uid, me.name ? me.name : "<?>", oldSuid, newSuid);

  /* Patch suid in-memory */
  me.ctbEntry.suid = newSuid;

  /* Re-encode */
  int      newBufLen = 0;
  void    *newBuf    = NULL;
  SEncoder encoder   = {0};
  tEncodeSize(metaEncodeEntry, &me, newBufLen, rc);
  if (rc != 0) {
    fprintf(stderr, "tEncodeSize failed: rc=%d\n", rc);
    tDecoderClear(&dc);
    return rc;
  }
  newBuf = taosMemMalloc(newBufLen);
  if (!newBuf) {
    fprintf(stderr, "OOM\n");
    tDecoderClear(&dc);
    return -1;
  }
  tEncoderInit(&encoder, (uint8_t *)newBuf, newBufLen);
  rc = metaEncodeEntry(&encoder, &me);
  tEncoderClear(&encoder);
  tDecoderClear(&dc);
  if (rc != 0) {
    fprintf(stderr, "metaEncodeEntry failed: rc=%d\n", rc);
    taosMemFree(newBuf);
    return rc;
  }

  /* ---- Open a write transaction ---- */
  TXN *pTxn = NULL;
  rc = tdbBegin(r->pEnv, &pTxn, toolMalloc, toolFree, NULL,
                TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (rc != 0) {
    fprintf(stderr, "tdbBegin failed: rc=%d\n", rc);
    taosMemFree(newBuf);
    return rc;
  }

  /* 1. uid.idx */
  uidIdxVal.suid = newSuid;
  rc = tdbTbUpsert(r->pUidIdx, &uid, sizeof(uid), &uidIdxVal, sizeof(uidIdxVal), pTxn);
  if (rc != 0) { fprintf(stderr, "upsert uid.idx failed: rc=%d\n", rc); goto _abort; }

  /* 2. ctb.idx: delete old key, insert new key (preserve tag value) */
  SCtbIdxKey oldCtbKey = { .suid = oldSuid, .uid = uid };
  void *pTagBuf = NULL; int tagLen = 0;
  (void)tdbTbGet(r->pCtbIdx, &oldCtbKey, sizeof(oldCtbKey), &pTagBuf, &tagLen);
  (void)tdbTbDelete(r->pCtbIdx, &oldCtbKey, sizeof(oldCtbKey), pTxn);

  SCtbIdxKey newCtbKey = { .suid = newSuid, .uid = uid };
  if (pTagBuf && tagLen > 0) {
    rc = tdbTbInsert(r->pCtbIdx, &newCtbKey, sizeof(newCtbKey), pTagBuf, tagLen, pTxn);
  } else {
    rc = tdbTbInsert(r->pCtbIdx, &newCtbKey, sizeof(newCtbKey), NULL, 0, pTxn);
  }
  tdbFree(pTagBuf);
  if (rc != 0) { fprintf(stderr, "insert ctb.idx failed: rc=%d\n", rc); goto _abort; }

  /* 3. table.db */
  rc = tdbTbUpsert(r->pTbDb, &tbKey, sizeof(tbKey), newBuf, newBufLen, pTxn);
  if (rc != 0) { fprintf(stderr, "upsert table.db failed: rc=%d\n", rc); goto _abort; }

  /* ---- Commit ---- */
  rc = tdbCommit(r->pEnv, pTxn);
  if (rc != 0) { fprintf(stderr, "tdbCommit failed: rc=%d\n", rc); goto _abort; }
  rc = tdbPostCommit(r->pEnv, pTxn);
  if (rc != 0) { fprintf(stderr, "tdbPostCommit failed: rc=%d\n", rc); }

  taosMemFree(newBuf);
  printf("Done.\n");
  return 0;

_abort:
  tdbAbort(r->pEnv, pTxn);
  taosMemFree(newBuf);
  return rc;
}

/* -------------------------------------------------------------------------
 * Helper: encode a single SMetaEntry to a heap buffer.
 * Returns encoded length (> 0) on success, or -1 on error.
 * Caller owns the returned buffer and must taosMemFree() it.
 * ---------------------------------------------------------------------- */
static int encodeMetaEntry(const SMetaEntry *pMe, void **ppBuf) {
  int      bufLen = 0;
  int      rc     = 0;
  SEncoder enc    = {0};

  tEncodeSize(metaEncodeEntry, pMe, bufLen, rc);
  if (rc != 0) return -1;

  void *buf = taosMemMalloc(bufLen);
  if (!buf) return -1;

  tEncoderInit(&enc, (uint8_t *)buf, bufLen);
  rc = metaEncodeEntry(&enc, pMe);
  tEncoderClear(&enc);
  if (rc != 0) { taosMemFree(buf); return -1; }

  *ppBuf = buf;
  return bufLen;
}

/* -------------------------------------------------------------------------
 * Helper: apply one child-table suid update inside an open transaction.
 * All three indexes are updated atomically (same pTxn).
 * ---------------------------------------------------------------------- */
typedef struct {
  tb_uid_t   uid;
  tb_uid_t   oldSuid;
  tb_uid_t   newSuid;
  SUidIdxVal uidIdxVal;   /* copy from uid.idx – already patched (suid=newSuid) */
  void      *pNewEntry;   /* re-encoded table.db value, owned by caller        */
  int        newEntryLen;
  STbDbKey   tbKey;
  void      *pTagBuf;     /* ctb.idx value (tag data), may be NULL             */
  int        tagLen;
} SUpdateItem;

static int applyUpdateItem(SMetaReaderCtx *r, const SUpdateItem *it, TXN *pTxn) {
  int rc = 0;

  /* uid.idx */
  rc = tdbTbUpsert(r->pUidIdx, &it->uid, sizeof(it->uid),
                   &it->uidIdxVal, sizeof(it->uidIdxVal), pTxn);
  if (rc != 0) { fprintf(stderr, "uid.idx upsert uid=%" PRId64 " failed: %d\n", it->uid, rc); return rc; }

  /* ctb.idx: delete old key (oldSuid, uid) then insert (newSuid, uid) */
  SCtbIdxKey oldKey = { .suid = it->oldSuid, .uid = it->uid };
  SCtbIdxKey newKey = { .suid = it->newSuid, .uid = it->uid };
  (void)tdbTbDelete(r->pCtbIdx, &oldKey, sizeof(oldKey), pTxn);
  if (it->pTagBuf && it->tagLen > 0) {
    rc = tdbTbInsert(r->pCtbIdx, &newKey, sizeof(newKey), it->pTagBuf, it->tagLen, pTxn);
  } else {
    rc = tdbTbInsert(r->pCtbIdx, &newKey, sizeof(newKey), NULL, 0, pTxn);
  }
  if (rc != 0) { fprintf(stderr, "ctb.idx insert uid=%" PRId64 " failed: %d\n", it->uid, rc); return rc; }

  /* table.db */
  rc = tdbTbUpsert(r->pTbDb, &it->tbKey, sizeof(it->tbKey),
                   it->pNewEntry, it->newEntryLen, pTxn);
  if (rc != 0) { fprintf(stderr, "table.db upsert uid=%" PRId64 " failed: %d\n", it->uid, rc); return rc; }

  return 0;
}

/* -------------------------------------------------------------------------
 * Command: batch-update-suid <old-suid> <new-suid>
 *
 * Two-pass algorithm (safe – never modifies while iterating):
 *
 *   Pass 1 (read-only)
 *     Scan uid.idx.  For every child table where val.suid == old_suid,
 *     read the table.db entry, decode it, re-encode with new_suid, and
 *     save the prepared SUpdateItem into a dynamic array.
 *
 *   Pass 2 (single write transaction)
 *     Apply every SUpdateItem in the array: update uid.idx, ctb.idx and
 *     table.db, then commit once.
 *
 * A single transaction keeps the change atomic and avoids repeated
 * commit overhead when there are many tables.
 * ---------------------------------------------------------------------- */
static int cmdBatchUpdateSuid(SMetaReaderCtx *r, tb_uid_t oldSuid, tb_uid_t newSuid) {
  int rc = 0;

  if (oldSuid == newSuid) {
    fprintf(stderr, "old-suid and new-suid are identical, nothing to do.\n");
    return 0;
  }

  /* ---- Dynamic array of SUpdateItem ---- */
  int          capacity = 256;
  int          nItems   = 0;
  SUpdateItem *items    = (SUpdateItem *)taosMemMalloc(capacity * sizeof(SUpdateItem));
  if (!items) { fprintf(stderr, "OOM\n"); return -1; }

/* Cleanup helper */
#define FREE_ITEMS(n) \
  do { \
    for (int _i = 0; _i < (n); _i++) { \
      taosMemFree(items[_i].pNewEntry); \
      tdbFree(items[_i].pTagBuf); \
    } \
    taosMemFree(items); \
  } while (0)

  /* ===== Pass 1: collect matching entries ===== */
  TBC *pCur = NULL;
  rc = tdbTbcOpen(r->pUidIdx, &pCur, NULL);
  if (rc != 0) { fprintf(stderr, "Cannot open uid.idx cursor: %d\n", rc); FREE_ITEMS(0); return rc; }

  if (tdbTbcMoveToFirst(pCur) != 0) {
    printf("uid.idx is empty, nothing to update.\n");
    tdbTbcClose(pCur);
    FREE_ITEMS(0);
    return 0;
  }

  for (;;) {
    const void *pKey = NULL; int kLen = 0;
    const void *pVal = NULL; int vLen = 0;
    if (tdbTbcGet(pCur, &pKey, &kLen, &pVal, &vLen) < 0) break;

    tb_uid_t    uid     = *(const tb_uid_t *)pKey;
    SUidIdxVal *pIdxVal = (SUidIdxVal *)pVal;

    /* Skip entries that do not match old_suid */
    if (pIdxVal->suid != oldSuid) {
      if (tdbTbcMoveToNext(pCur) < 0) break;
      continue;
    }

    /* ---- Read and decode table.db entry ---- */
    STbDbKey tbKey = { .version = pIdxVal->version, .uid = uid };
    void *pEntryBuf = NULL; int entryLen = 0;
    rc = tdbTbGet(r->pTbDb, &tbKey, sizeof(tbKey), &pEntryBuf, &entryLen);
    if (rc != 0) {
      fprintf(stderr, "WARN: uid=%" PRId64 " not found in table.db, skipping\n", uid);
      if (tdbTbcMoveToNext(pCur) < 0) break;
      continue;
    }

    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, (uint8_t *)pEntryBuf, entryLen);
    rc = metaDecodeEntry(&dc, &me);
    tdbFree(pEntryBuf);
    if (rc != 0 || me.type != TSDB_CHILD_TABLE) {
      tDecoderClear(&dc);
      fprintf(stderr, "WARN: uid=%" PRId64 " decode failed or not ctb, skipping\n", uid);
      if (tdbTbcMoveToNext(pCur) < 0) break;
      continue;
    }

    /* Patch suid */
    me.ctbEntry.suid = newSuid;

    /* Re-encode */
    void *pNewEntry = NULL;
    int   newEntryLen = encodeMetaEntry(&me, &pNewEntry);
    tDecoderClear(&dc);
    if (newEntryLen < 0) {
      fprintf(stderr, "WARN: uid=%" PRId64 " re-encode failed, skipping\n", uid);
      if (tdbTbcMoveToNext(pCur) < 0) break;
      continue;
    }

    /* Read ctb.idx tag value */
    SCtbIdxKey oldCtbKey = { .suid = oldSuid, .uid = uid };
    void *pTagBuf = NULL; int tagLen = 0;
    (void)tdbTbGet(r->pCtbIdx, &oldCtbKey, sizeof(oldCtbKey), &pTagBuf, &tagLen);

    /* Grow items array if needed */
    if (nItems == capacity) {
      capacity *= 2;
      SUpdateItem *tmp = (SUpdateItem *)taosMemMalloc(capacity * sizeof(SUpdateItem));
      if (!tmp) {
        taosMemFree(pNewEntry);
        tdbFree(pTagBuf);
        FREE_ITEMS(nItems);
        tdbTbcClose(pCur);
        fprintf(stderr, "OOM while collecting items\n");
        return -1;
      }
      memcpy(tmp, items, nItems * sizeof(SUpdateItem));
      taosMemFree(items);
      items = tmp;
    }

    SUidIdxVal patchedVal = *pIdxVal;
    patchedVal.suid = newSuid;

    items[nItems++] = (SUpdateItem){
        .uid        = uid,
        .oldSuid    = oldSuid,
        .newSuid    = newSuid,
        .uidIdxVal  = patchedVal,
        .pNewEntry  = pNewEntry,
        .newEntryLen= newEntryLen,
        .tbKey      = tbKey,
        .pTagBuf    = pTagBuf,
        .tagLen     = tagLen,
    };

    if (tdbTbcMoveToNext(pCur) < 0) break;
  }
  tdbTbcClose(pCur);

  printf("Found %d child table(s) with suid=%" PRId64 ", applying update -> %" PRId64 " ...\n",
         nItems, oldSuid, newSuid);

  if (nItems == 0) {
    FREE_ITEMS(0);
    return 0;
  }

  /* ===== Pass 2: single write transaction ===== */
  TXN *pTxn = NULL;
  rc = tdbBegin(r->pEnv, &pTxn, toolMalloc, toolFree, NULL,
                TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (rc != 0) {
    fprintf(stderr, "tdbBegin failed: %d\n", rc);
    FREE_ITEMS(nItems);
    return rc;
  }

  int nOk = 0;
  for (int i = 0; i < nItems; i++) {
    rc = applyUpdateItem(r, &items[i], pTxn);
    if (rc != 0) {
      fprintf(stderr, "Update failed at item %d (uid=%" PRId64 "), aborting transaction\n",
              i, items[i].uid);
      tdbAbort(r->pEnv, pTxn);
      FREE_ITEMS(nItems);
      return rc;
    }
    nOk++;
  }

  rc = tdbCommit(r->pEnv, pTxn);
  if (rc != 0) {
    fprintf(stderr, "tdbCommit failed: %d\n", rc);
    tdbAbort(r->pEnv, pTxn);
    FREE_ITEMS(nItems);
    return rc;
  }
  rc = tdbPostCommit(r->pEnv, pTxn);
  if (rc != 0) {
    fprintf(stderr, "tdbPostCommit failed: %d\n", rc);
  }

  FREE_ITEMS(nItems);
#undef FREE_ITEMS

  printf("Done. %d child table(s) updated.\n", nOk);
  return 0;
}

/* -------------------------------------------------------------------------
 * main
 * ---------------------------------------------------------------------- */
static void usage(const char *prog) {
  fprintf(stderr,
          "Usage:\n"
          "  %s <meta-dir> list\n"
          "      Print all child tables: uid, suid, name\n\n"
          "  %s <meta-dir> update-suid <uid> <new-suid>\n"
          "      Update the suid of a single child table\n\n"
          "  %s <meta-dir> batch-update-suid <old-suid> <new-suid>\n"
          "      Batch-update ALL child tables whose suid == <old-suid>\n"
          "      (single atomic transaction)\n\n"
          "  %s <meta-dir> debug\n"
          "      Dump main-db catalog and count uid.idx / ctb.idx entries\n\n"
          "Example:\n"
          "  %s /var/lib/taos/vnode/vnode2/meta batch-update-suid 1 2\n",
          prog, prog, prog, prog, prog);
}

int main(int argc, char *argv[]) {
  if (argc < 3) { usage(argv[0]); return 1; }

  const char *metaDir = argv[1];
  const char *cmd     = argv[2];

  SMetaReaderCtx r = {0};
  if (metaReaderOpen(&r, metaDir) != 0) return 2;

  int rc = 0;
  if (strcmp(cmd, "list") == 0) {
    rc = cmdList(&r);
  } else if (strcmp(cmd, "debug") == 0) {
    rc = cmdDebug(&r);
  } else if (strcmp(cmd, "update-suid") == 0) {
    if (argc < 5) {
      fprintf(stderr, "update-suid needs <uid> and <new-suid>\n");
      usage(argv[0]); rc = 1;
    } else {
      tb_uid_t uid     = (tb_uid_t)atoll(argv[3]);
      tb_uid_t newSuid = (tb_uid_t)atoll(argv[4]);
      rc = cmdUpdateSuid(&r, uid, newSuid);
    }
  } else if (strcmp(cmd, "batch-update-suid") == 0) {
    if (argc < 5) {
      fprintf(stderr, "batch-update-suid needs <old-suid> and <new-suid>\n");
      usage(argv[0]); rc = 1;
    } else {
      tb_uid_t oldSuid = (tb_uid_t)atoll(argv[3]);
      tb_uid_t newSuid = (tb_uid_t)atoll(argv[4]);
      rc = cmdBatchUpdateSuid(&r, oldSuid, newSuid);
    }
  } else {
    fprintf(stderr, "Unknown command: %s\n", cmd);
    usage(argv[0]); rc = 1;
  }

  metaReaderClose(&r);
  return rc;
}

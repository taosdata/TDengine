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

// walTxn.c — .txn file management for CDC txn-atomic lazy load.
//
// Each .txn file is a sibling of the corresponding .log file (same firstVer, different suffix).
// It stores only txn-tagged WAL entries (DDL + TXN_BEGIN/COMMIT/ROLLBACK) in the same binary
// format as .log.  Because there are no INSERT entries, lazy load reads are fast.
//
// Atomicity guarantee:
//   txn.pending (16 bytes) records the firstTxnWalIndex of the current fsync batch:
//     Step A: write+fsync txn.pending  (before .log fsync)
//     Step B: fsync .log               (existing)
//     Step C: fsync .txn               (new)
//     Step D: clear txn.pending        (after Step C)
//   On restart: if txn.pending is valid, scan .log from firstTxnWalIndex to committedVer
//               and copy any missing txn entries into .txn.

#include "walInt.h"

// Retention tuning knobs for walTxnFilesTrim:
//   extra hours: keep .txn files no older than (oldest .log createTs - WAL_TXN_TRIM_EXTRA_HOURS)
//   extra files: keep at most (logCount + WAL_TXN_TRIM_EXTRA_FILES) .txn files
#define WAL_TXN_TRIM_EXTRA_HOURS 4
#define WAL_TXN_TRIM_EXTRA_FILES 6

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Open the .txn write file for the segment whose firstVer is segFirstVer.
// Creates the file if it does not exist; positions at the end for appending.
static TdFilePtr walTxnOpenWriteFile(SWal *pWal, int64_t segFirstVer) {
  char fnameStr[WAL_FILE_LEN];
  walBuildTxnName(pWal, segFirstVer, fnameStr);
  TdFilePtr fd = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_APPEND);
  if (fd == NULL) {
    wError("vgId:%d, failed to open .txn file %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
  }
  return fd;
}

// Read and validate txn.pending.  Returns true if a valid non-cleared record was found
// and sets *pFirstIdx to firstTxnWalIndex.  Returns false on invalid/cleared file.
static bool walTxnPendingRead(SWal *pWal, int64_t *pFirstIdx) {
  if (pWal->txnPendingFd == NULL) return false;

  STxnPendingFile rec = {0};
  if (taosLSeekFile(pWal->txnPendingFd, 0, SEEK_SET) < 0) return false;
  if (taosReadFile(pWal->txnPendingFd, &rec, sizeof(rec)) != (int64_t)sizeof(rec)) return false;
  if (rec.magic != WAL_TXN_PENDING_MAGIC) return false;

  uint32_t expected = taosCalcChecksum(0, (uint8_t *)&rec.firstTxnWalIndex, sizeof(int64_t));
  if (rec.crc32 != expected) return false;
  if (rec.firstTxnWalIndex <= 0) return false;  // 0 = cleared

  *pFirstIdx = rec.firstTxnWalIndex;
  return true;
}

// Overwrite txn.pending with the given firstTxnWalIndex (0 = clear).
// Returns 0 on success, non-zero on I/O error.
static int32_t walTxnPendingWrite(SWal *pWal, int64_t firstTxnWalIndex, bool doFsync) {
  if (pWal->txnPendingFd == NULL) return TSDB_CODE_FAILED;

  STxnPendingFile rec = {
      .magic = WAL_TXN_PENDING_MAGIC,
      .firstTxnWalIndex = firstTxnWalIndex,
  };
  rec.crc32 = taosCalcChecksum(0, (uint8_t *)&rec.firstTxnWalIndex, sizeof(int64_t));

  if (taosLSeekFile(pWal->txnPendingFd, 0, SEEK_SET) < 0) {
    wError("vgId:%d, seek txn.pending failed since %s", pWal->cfg.vgId, terrstr());
    return terrno;
  }
  if (taosWriteFile(pWal->txnPendingFd, &rec, sizeof(rec)) != (int64_t)sizeof(rec)) {
    wError("vgId:%d, write txn.pending failed since %s", pWal->cfg.vgId, terrstr());
    return terrno;
  }
  if (doFsync) {
    if (taosFsyncFile(pWal->txnPendingFd) != 0) {
      wError("vgId:%d, fsync txn.pending failed since %s", pWal->cfg.vgId, terrstr());
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

// Check if walIndex ver is already present in the .txn file at txnFd.
// Scan the file sequentially from the start.  Returns true if found.
static bool walTxnFileContainsVer(TdFilePtr txnFd, int64_t ver) {
  if (txnFd == NULL) return false;
  if (taosLSeekFile(txnFd, 0, SEEK_SET) < 0) return false;

  SWalCkHead head;
  while (taosReadFile(txnFd, &head, sizeof(head)) == (int64_t)sizeof(head)) {
    if (head.magic != WAL_MAGIC) break;  // corrupted or end
    if (head.head.version == ver) return true;
    // Skip body
    int32_t bodyLen = head.head.bodyLen;
    if (bodyLen < 0 || bodyLen > WAL_SCAN_BUF_SIZE) break;  // sanity
    if (taosLSeekFile(txnFd, bodyLen, SEEK_CUR) < 0) break;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

int32_t walTxnFilesOpen(SWal *pWal) {
  if (!pWal->cfg.enableTxnFile) return TSDB_CODE_SUCCESS;

  atomic_store_64(&pWal->firstPendingTxnIndex, -1);

  // Initialize txnBeginIndexMap (txnId → first walIndex; tracks IS_BEGIN state for in-flight txns).
  pWal->txnBeginIndexMap =
      taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pWal->txnBeginIndexMap == NULL) {
    wError("vgId:%d, failed to alloc txnBeginIndexMap", pWal->cfg.vgId);
    return terrno;
  }

  // Open txn.pending (create if needed).
  char pendingPath[WAL_FILE_LEN];
  walBuildTxnPendingName(pWal, pendingPath);
  pWal->txnPendingFd = taosOpenFile(pendingPath, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE);
  if (pWal->txnPendingFd == NULL) {
    wError("vgId:%d, failed to open txn.pending %s since %s", pWal->cfg.vgId, pendingPath, terrstr());
    taosHashCleanup(pWal->txnBeginIndexMap);
    pWal->txnBeginIndexMap = NULL;
    return terrno;
  }

  // Open current .txn write file (same firstVer as the last .log segment).
  int32_t sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  if (sz > 0) {
    SWalFileInfo *pLast = (SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet);
    pWal->txnWriteFd = walTxnOpenWriteFile(pWal, pLast->firstVer);
    if (pWal->txnWriteFd == NULL) {
      (void)taosCloseFile(&pWal->txnPendingFd);
      taosHashCleanup(pWal->txnBeginIndexMap);
      pWal->txnBeginIndexMap = NULL;
      return terrno;
    }
  }
  // If fileInfoSet is empty, txnWriteFd stays NULL; it will be opened in walTxnFilesRotate
  // when the first .log segment is created.

  wDebug("vgId:%d, walTxnFilesOpen done, segments:%d", pWal->cfg.vgId, sz);
  return TSDB_CODE_SUCCESS;
}

void walTxnFilesClose(SWal *pWal) {
  if (!pWal->cfg.enableTxnFile) return;

  // Flush current .txn write file before closing.
  if (pWal->txnWriteFd != NULL) {
    if (taosFsyncFile(pWal->txnWriteFd) != 0) {
      wWarn("vgId:%d, fsync .txn on close failed since %s", pWal->cfg.vgId, terrstr());
    }
    (void)taosCloseFile(&pWal->txnWriteFd);
    pWal->txnWriteFd = NULL;
  }

  // Flush txn.pending (ensure cleared state is on disk).
  if (pWal->txnPendingFd != NULL) {
    if (taosFsyncFile(pWal->txnPendingFd) != 0) {
      wWarn("vgId:%d, fsync txn.pending on close failed since %s", pWal->cfg.vgId, terrstr());
    }
    (void)taosCloseFile(&pWal->txnPendingFd);
    pWal->txnPendingFd = NULL;
  }

  taosHashCleanup(pWal->txnBeginIndexMap);
  pWal->txnBeginIndexMap = NULL;
  atomic_store_64(&pWal->firstPendingTxnIndex, -1);
}

// Called from walWriteImpl for every txn-tagged entry.
// txnExtFlags: WAL_TXN_EXT_* flags for this entry (e.g. WAL_TXN_EXT_IS_BEGIN).
// encBuf/encBodyLen: the encrypted body buffer (non-NULL only when encryption is enabled).
// When encryption is disabled: body/bodyLen are the original data (WAL_TXN_HDR_SIZE prefix NOT included).
void walTxnWriteEntry(SWal *pWal, int64_t index, uint64_t txnExtFlags, txn_id_t txnId, const void *body,
                      int32_t bodyLen, const void *encBuf, int32_t encBodyLen) {
  if (!pWal->cfg.enableTxnFile || pWal->txnWriteFd == NULL) return;

  // Track first pending txn index for this batch (used in Step A).
  // Set unconditionally so that txn.pending covers this entry even if the write fails.
  if (atomic_load_64(&pWal->firstPendingTxnIndex) < 0) {
    atomic_store_64(&pWal->firstPendingTxnIndex, index);
  }

  // Write the same SWalCkHead as was written to .log (already built in pWal->writeHead).
  if (taosWriteFile(pWal->txnWriteFd, &pWal->writeHead, sizeof(SWalCkHead)) != (int64_t)sizeof(SWalCkHead)) {
    wError("vgId:%d, .txn write header failed for index:%" PRId64 " since %s", pWal->cfg.vgId, index, terrstr());
    return;
  }

  bool writeOk;
  if (encBuf != NULL) {
    // Encrypted path: encBuf already contains [txnExtFlags:8B][txnId:8B][body] encrypted.
    writeOk = (taosWriteFile(pWal->txnWriteFd, encBuf, encBodyLen) == (int64_t)encBodyLen);
  } else {
    // Non-encrypted path: write [txnExtFlags:8B][txnId:8B][body].
    writeOk = (taosWriteFile(pWal->txnWriteFd, &txnExtFlags, sizeof(uint64_t)) == (int64_t)sizeof(uint64_t));
    if (writeOk) {
      writeOk = (taosWriteFile(pWal->txnWriteFd, &txnId, sizeof(txn_id_t)) == (int64_t)sizeof(txn_id_t));
    }
    if (writeOk && bodyLen > 0) {
      writeOk = (taosWriteFile(pWal->txnWriteFd, body, bodyLen) == (int64_t)bodyLen);
    }
  }
  if (!writeOk) {
    wError("vgId:%d, .txn write body failed for index:%" PRId64 " since %s", pWal->cfg.vgId, index, terrstr());
  }
}

// Step A: write txn.pending with the firstPendingTxnIndex for this batch.
// Must be called BEFORE .log fsync (Step B).
void walTxnPreFsync(SWal *pWal) {
  if (!pWal->cfg.enableTxnFile) return;

  int64_t firstIdx = atomic_load_64(&pWal->firstPendingTxnIndex);
  if (firstIdx < 0) return;  // no txn entries in this batch

  if (walTxnPendingWrite(pWal, firstIdx, true) != 0) {
    wWarn("vgId:%d, walTxnPreFsync: failed to write txn.pending, recovery may be degraded", pWal->cfg.vgId);
  }
}

// Steps C+D: fsync .txn, then clear txn.pending.
// Must be called AFTER .log fsync (Step B).
void walTxnPostFsync(SWal *pWal) {
  if (!pWal->cfg.enableTxnFile) return;

  // Step C: fsync .txn file.
  if (pWal->txnWriteFd != NULL) {
    if (taosFsyncFile(pWal->txnWriteFd) != 0) {
      wWarn("vgId:%d, .txn fsync failed since %s", pWal->cfg.vgId, terrstr());
    }
  }

  // Step D: clear txn.pending (no fsync needed; overwritten on next Step A or walClose).
  int64_t firstIdx = atomic_load_64(&pWal->firstPendingTxnIndex);
  if (firstIdx >= 0) {
    (void)walTxnPendingWrite(pWal, 0, false);
    atomic_store_64(&pWal->firstPendingTxnIndex, -1);
  }
}

// Called from walRollImpl: close the old .txn segment (fsynced by walRollImpl for .log),
// then open a new one with newFirstVer.
void walTxnFilesRotate(SWal *pWal, int64_t newFirstVer) {
  if (!pWal->cfg.enableTxnFile) return;

  // Post-snapshot recovery: walTxnFilesClose (called from walRestoreFromSnapshot) nulled
  // txnBeginIndexMap and txnPendingFd. Re-initialize them now so that IS_BEGIN marking
  // works correctly for all transactions written after snapshot apply.
  // Snapshots are taken at clean commit points — no in-flight transactions cross the
  // snapshot boundary — so an empty map is the correct starting state. New transactions
  // will have their first message correctly marked with IS_BEGIN as they arrive.
  if (pWal->txnBeginIndexMap == NULL) {
    pWal->txnBeginIndexMap =
        taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  }
  if (pWal->txnPendingFd == NULL) {
    char pendingPath[WAL_FILE_LEN];
    walBuildTxnPendingName(pWal, pendingPath);
    pWal->txnPendingFd = taosOpenFile(pendingPath, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE);
    if (pWal->txnPendingFd == NULL) {
      wWarn("vgId:%d, walTxnFilesRotate: failed to re-open txn.pending since %s", pWal->cfg.vgId, terrstr());
    }
    atomic_store_64(&pWal->firstPendingTxnIndex, -1);
  }

  if (pWal->txnWriteFd != NULL) {
    // The caller (walRollImpl) already fsynced .log; fsync .txn to match durability.
    if (taosFsyncFile(pWal->txnWriteFd) != 0) {
      wWarn("vgId:%d, .txn fsync on rotate failed since %s", pWal->cfg.vgId, terrstr());
    }
    (void)taosCloseFile(&pWal->txnWriteFd);
    pWal->txnWriteFd = NULL;
  }

  pWal->txnWriteFd = walTxnOpenWriteFile(pWal, newFirstVer);
  if (pWal->txnWriteFd == NULL) {
    wError("vgId:%d, failed to open new .txn file for firstVer:%" PRId64, pWal->cfg.vgId, newFirstVer);
  }
}

// Truncate the current open .txn write file to remove entries with version > truncVer.
// Positions the file at end for subsequent appends.
int32_t walTxnTruncateCurrent(SWal *pWal, int64_t truncVer) {
  if (!pWal->cfg.enableTxnFile || pWal->txnWriteFd == NULL) return TSDB_CODE_SUCCESS;

  if (taosLSeekFile(pWal->txnWriteFd, 0, SEEK_SET) < 0) {
    wError("vgId:%d, walTxnTruncateCurrent: seek failed since %s", pWal->cfg.vgId, terrstr());
    return TSDB_CODE_SUCCESS;  // non-fatal; best-effort
  }

  int64_t truncOffset = -1;
  int64_t curOffset   = 0;
  SWalCkHead head;

  while (taosReadFile(pWal->txnWriteFd, &head, sizeof(head)) == (int64_t)sizeof(head)) {
    if (head.magic != WAL_MAGIC) break;
    if (head.head.version > truncVer) {
      truncOffset = curOffset;
      break;
    }
    curOffset += (int64_t)sizeof(SWalCkHead) + head.head.bodyLen;
    if (head.head.bodyLen > 0) {
      if (taosLSeekFile(pWal->txnWriteFd, head.head.bodyLen, SEEK_CUR) < 0) break;
    }
  }

  if (truncOffset >= 0) {
    wInfo("vgId:%d, walTxnTruncateCurrent: truncating at offset:%" PRId64 " (truncVer:%" PRId64 ")",
          pWal->cfg.vgId, truncOffset, truncVer);
    if (taosFtruncateFile(pWal->txnWriteFd, truncOffset) < 0) {
      wError("vgId:%d, .txn truncate failed since %s", pWal->cfg.vgId, terrstr());
      return terrno;
    }
  }
  // Always reposition at end for subsequent appends.
  int64_t seekRet = taosLSeekFile(pWal->txnWriteFd, 0, SEEK_END);
  if (seekRet < 0) {
    wError("vgId:%d, .txn seek to end failed since %s", pWal->cfg.vgId, terrstr());
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

// Rebuild txnBeginIndexMap by scanning all .txn files.
// After scan: map contains only txnIds whose first entry (IS_BEGIN) was seen
// but no COMMIT/ROLLBACK has been observed — i.e., in-flight transactions.
// Returns 0 on success, or a TSDB error code if file I/O fails (seek error indicates corruption).
static int32_t walTxnRebuildBeginIndexMap(SWal *pWal) {
  if (pWal->txnBeginIndexMap == NULL) return TSDB_CODE_SUCCESS;
  taosHashClear(pWal->txnBeginIndexMap);

  int32_t logCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  char    fnameStr[WAL_FILE_LEN];

  for (int32_t i = 0; i < logCount; i++) {
    SWalFileInfo *pInfo = (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i);
    walBuildTxnName(pWal, pInfo->firstVer, fnameStr);
    TdFilePtr fd = taosOpenFile(fnameStr, TD_FILE_READ);
    if (fd == NULL) continue;

    SWalCkHead head;
    while (taosReadFile(fd, &head, sizeof(head)) == (int64_t)sizeof(head)) {
      if (head.magic != WAL_MAGIC) break;
      int32_t bodyLen = head.head.bodyLen;
      if (bodyLen < (int32_t)WAL_TXN_HDR_SIZE) {
        if (bodyLen > 0) {
          if (taosLSeekFile(fd, bodyLen, SEEK_CUR) < 0) {
            wError("vgId:%d, walTxnRebuildBeginIndexMap: seek in .txn file failed since %s", pWal->cfg.vgId, terrstr());
            (void)taosCloseFile(&fd);
            return terrno;
          }
        }
        continue;
      }
      uint64_t txnExtFlags = 0;
      int64_t  txnId       = 0;
      if (taosReadFile(fd, &txnExtFlags, sizeof(uint64_t)) != (int64_t)sizeof(uint64_t)) break;
      if (taosReadFile(fd, &txnId, sizeof(int64_t)) != (int64_t)sizeof(int64_t)) break;
      int32_t remLen = bodyLen - (int32_t)WAL_TXN_HDR_SIZE;
      if (remLen > 0) {
        if (taosLSeekFile(fd, remLen, SEEK_CUR) < 0) {
          wError("vgId:%d, walTxnRebuildBeginIndexMap: seek in .txn file failed since %s", pWal->cfg.vgId, terrstr());
          (void)taosCloseFile(&fd);
          return terrno;
        }
      }

      tmsg_t msgType = head.head.msgType;
      if (txnExtFlags & WAL_TXN_EXT_IS_BEGIN) {
        int64_t walIdx = head.head.version;
        int32_t putCode = taosHashPut(pWal->txnBeginIndexMap, &txnId, sizeof(int64_t), &walIdx, sizeof(int64_t));
        if (putCode != 0) {
          wError("vgId:%d, walTxnRebuildBeginIndexMap: taosHashPut failed txnId:%" PRId64 " since %s",
                 pWal->cfg.vgId, txnId, tstrerror(putCode));
          (void)taosCloseFile(&fd);
          return putCode;
        }
      }
      if (msgType == TDMT_VND_TXN_COMMIT || msgType == TDMT_VND_TXN_ROLLBACK) {
        int32_t removeCode = taosHashRemove(pWal->txnBeginIndexMap, &txnId, sizeof(int64_t));
        if (removeCode != 0) {
          wDebug("vgId:%d, walTxnRebuildBeginIndexMap: taosHashRemove failed txnId:%" PRId64 " (likely not in map)",
                 pWal->cfg.vgId, txnId);
        }
      }
    }

    (void)taosCloseFile(&fd);
  }

  wDebug("vgId:%d, walTxnRebuildBeginIndexMap: %d in-flight txns", pWal->cfg.vgId,
         taosHashGetSize(pWal->txnBeginIndexMap));
  return TSDB_CODE_SUCCESS;
}

// Startup recovery — called after main WAL recovery has determined committedVer.
// Step 1: truncate .txn if it has speculative entries (walIndex > committedVer).
// Step 1.5: rebuild txnBeginIndexMap from existing .txn files.
// Step 2: use txn.pending to recover any entries missing from .txn.
int32_t walTxnFilesRecover(SWal *pWal, int64_t committedVer) {
  if (!pWal->cfg.enableTxnFile || committedVer < 0) return TSDB_CODE_SUCCESS;

  // ---- Step 1: truncate speculative entries from .txn ----
  (void)walTxnTruncateCurrent(pWal, committedVer);

  // ---- Step 1.5: rebuild txnBeginIndexMap from committed .txn files ----
  int32_t code = walTxnRebuildBeginIndexMap(pWal);
  if (code != TSDB_CODE_SUCCESS) {
    wError("vgId:%d, walTxnFilesRecover: walTxnRebuildBeginIndexMap failed since %s", pWal->cfg.vgId, tstrerror(code));
    return code;
  }

  // ---- Step 2: recover missing txn entries using txn.pending ----
  int64_t firstTxnIdx = 0;
  if (!walTxnPendingRead(pWal, &firstTxnIdx)) {
    wDebug("vgId:%d, walTxnFilesRecover: no valid txn.pending, nothing to recover", pWal->cfg.vgId);
    return TSDB_CODE_SUCCESS;
  }
  if (firstTxnIdx > committedVer) {
    // The batch recorded in txn.pending was not committed (crash between A and B).
    wDebug("vgId:%d, walTxnFilesRecover: pending firstIdx:%" PRId64 " > committedVer:%" PRId64 ", skip",
           pWal->cfg.vgId, firstTxnIdx, committedVer);
    goto clear_pending;
  }

  wInfo("vgId:%d, walTxnFilesRecover: scanning .log [%" PRId64 ", %" PRId64 "] for missing .txn entries",
        pWal->cfg.vgId, firstTxnIdx, committedVer);

  {
    // Use O(1) seek into .log via .idx to find the starting offset, then read sequentially.
    SWalReader *pReader = walOpenReader(pWal, 0);
    if (pReader == NULL) {
      wError("vgId:%d, walTxnFilesRecover: failed to open WAL reader since %s", pWal->cfg.vgId, terrstr());
      goto clear_pending;
    }

    for (int64_t ver = firstTxnIdx; ver <= committedVer; ver++) {
      int32_t code = walFetchHead(pReader, ver);
      if (code < 0) break;

      SWalCont *pHead = &pReader->pHead->head;
      if (!WAL_IS_TXN_MSG(pHead)) {
        (void)walSkipFetchBody(pReader);
        continue;
      }

      // Check if already in .txn (idempotent: crash C→D case).
      if (walTxnFileContainsVer(pWal->txnWriteFd, ver)) {
        (void)walSkipFetchBody(pReader);
        wDebug("vgId:%d, walTxnFilesRecover: ver:%" PRId64 " already in .txn, skip", pWal->cfg.vgId, ver);
        continue;
      }

      code = walFetchBody(pReader);
      if (code < 0) break;

      pHead = &pReader->pHead->head;
      uint64_t    txnExtFlags = walContTxnExtFlags(pHead);
      txn_id_t    txnId       = walContTxnId(pHead);
      const char *rawBody     = walContBody(pHead);
      int32_t     rawBodyLen  = walContBodyLen(pHead);

      walTxnWriteEntry(pWal, ver, txnExtFlags, txnId, rawBody, rawBodyLen, NULL, 0);
      wDebug("vgId:%d, walTxnFilesRecover: recovered ver:%" PRId64 " msgType:%d", pWal->cfg.vgId, ver,
             (int)pHead->msgType);
    }

    walCloseReader(pReader);
  }

  if (pWal->txnWriteFd != NULL) {
    if (taosFsyncFile(pWal->txnWriteFd) != 0) {
      wWarn("vgId:%d, fsync .txn after recovery failed since %s", pWal->cfg.vgId, terrstr());
    }
  }

clear_pending:
  // Clear txn.pending regardless — we've handled whatever was pending.
  (void)walTxnPendingWrite(pWal, 0, true);
  atomic_store_64(&pWal->firstPendingTxnIndex, -1);
  wInfo("vgId:%d, walTxnFilesRecover: done", pWal->cfg.vgId);
  return TSDB_CODE_SUCCESS;
}

// Trim .txn files that satisfy all three Retention conditions:
//   A: file createTs < (oldest .log createTs - WAL_TXN_TRIM_EXTRA_HOURS)
//   B: .txn file count > .log file count + WAL_TXN_TRIM_EXTRA_FILES
//   C: file's WAL range does not overlap the keepVersion-protected region
// Called after walEndSnapshot trims .log files.
void walTxnFilesTrim(SWal *pWal) {
  if (!pWal->cfg.enableTxnFile) return;

  int32_t logCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  if (logCount == 0) return;

  // Condition A: reference time = oldest .log createTs, minus WAL_TXN_TRIM_EXTRA_HOURS.
  SWalFileInfo *pOldest = (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, 0);
  int64_t       oldestLogCreateTs = pOldest->createTs;  // seconds
  int64_t       cutoffTs = oldestLogCreateTs - (int64_t)WAL_TXN_TRIM_EXTRA_HOURS * 3600;

  // Scan for .txn files. When pWal->pTfs is bound, a .txn file may live on any level-0
  // mount point (same reason as walScanActualLogFiles in walMeta.c) -- scanning only
  // pWal->path here would make trim under-count txnCount, so files on non-primary disks
  // would never be discovered and never trimmed (a slow disk-space leak, not silent data
  // loss like the walCheckAndRepairMeta bug, but still wrong).
  SArray *txnFirstVers = taosArrayInit(16, sizeof(int64_t));
  if (txnFirstVers == NULL) return;

  if (pWal->pTfs == NULL) {
    TdDirPtr pDir = taosOpenDir(pWal->path);
    if (pDir == NULL) {
      taosArrayDestroy(txnFirstVers);
      return;
    }
    TdDirEntryPtr pEntry;
    while ((pEntry = taosReadDir(pDir)) != NULL) {
      const char *name = taosGetDirEntryName(pEntry);
      if (taosDirEntryIsDir(pEntry)) continue;
      int64_t firstVer = -1;
      if (sscanf(name, "%" PRId64 "." WAL_TXN_SUFFIX, &firstVer) != 1) continue;
      if (firstVer < 0) continue;
      if (taosArrayPush(txnFirstVers, &firstVer) == NULL) continue;
    }
    (void)taosCloseDir(&pDir);
  } else {
    STfsDir *pTDir = NULL;
    if (tfsOpendir(pWal->pTfs, pWal->relDir, &pTDir) != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(txnFirstVers);
      return;
    }
    char bname[TSDB_FILENAME_LEN];
    for (const STfsFile *pFile = NULL; (pFile = tfsReaddir(pTDir)) != NULL;) {
      if (taosIsDir(pFile->aname)) continue;
      tstrncpy(bname, pFile->aname, sizeof(bname));
      char   *name = taosDirEntryBaseName(bname);
      int64_t firstVer = -1;
      if (sscanf(name, "%" PRId64 "." WAL_TXN_SUFFIX, &firstVer) != 1) continue;
      if (firstVer < 0) continue;
      if (taosArrayPush(txnFirstVers, &firstVer) == NULL) continue;
    }
    tfsClosedir(pTDir);
  }

  taosArraySort(txnFirstVers, compareInt64Val);
  int32_t txnCount = (int32_t)taosArrayGetSize(txnFirstVers);

  // Condition B: only trim if txnCount > logCount + WAL_TXN_TRIM_EXTRA_FILES.
  int32_t excess = txnCount - (logCount + WAL_TXN_TRIM_EXTRA_FILES);
  if (excess <= 0) {
    taosArrayDestroy(txnFirstVers);
    return;
  }

  // Condition C: keepVersion guard.
  // If keepVersion >= 0, a CDC or DDL transaction holds a keep-constraint on the WAL.
  // .txn file[i] covers versions [firstVer[i], firstVer[i+1]-1].  We must not trim
  // any file whose range overlaps [keepVersion, ∞), i.e. where firstVer[i+1] > keepVersion.
  // Read keepVersion once (caller holds pWal->mutex via walEndSnapshot path).
  int64_t keepVer = pWal->keepVersion;  // -1 = no constraint

  // Trim the oldest `excess` .txn files, provided they satisfy Conditions A and C.
  for (int32_t i = 0; i < excess; i++) {
    int64_t firstVer = *(int64_t *)taosArrayGet(txnFirstVers, i);

    // Condition C: skip files whose WAL range might contain protected versions.
    if (keepVer >= 0) {
      int64_t nextFirstVer = (i + 1 < txnCount)
                             ? *(int64_t *)taosArrayGet(txnFirstVers, i + 1)
                             : INT64_MAX;
      if (nextFirstVer > keepVer) {
        // This file may contain entries at or after keepVer. Stop trimming.
        wDebug("vgId:%d, walTxnFilesTrim: stopping at firstVer:%" PRId64 " (keepVer:%" PRId64 ")",
               pWal->cfg.vgId, firstVer, keepVer);
        break;
      }
    }

    // Condition A: get the createTs of this .txn file.
    char    fnameStr[WAL_FILE_LEN];
    int64_t mtime = 0;
    walBuildTxnName(pWal, firstVer, fnameStr);
    if (taosStatFile(fnameStr, NULL, &mtime, NULL) < 0) continue;
    // Use .log segment createTs for the same firstVer; fall back to file mtime.
    int64_t txnCreateTs = 0;
    for (int32_t j = 0; j < logCount; j++) {
      SWalFileInfo *pInfo = (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, j);
      if (pInfo->firstVer == firstVer) {
        txnCreateTs = pInfo->createTs;
        break;
      }
    }
    if (txnCreateTs == 0) {
      txnCreateTs = mtime;
    }

    if (txnCreateTs > cutoffTs) {
      // Condition A not satisfied; stop — all subsequent files are newer.
      break;
    }

    // Skip the .txn file that corresponds to the currently open write segment.
    int32_t      writeCur = pWal->writeCur;
    SWalFileInfo *pCur = (writeCur >= 0) ? (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, writeCur) : NULL;
    if (pCur != NULL && pCur->firstVer == firstVer) continue;

    wInfo("vgId:%d, walTxnFilesTrim: removing %s", pWal->cfg.vgId, fnameStr);
    (void)taosRemoveFile(fnameStr);
  }

  taosArrayDestroy(txnFirstVers);
}

// ---------------------------------------------------------------------------
// .txn snapshot reader / writer  (SNAP_DATA_TXN_WAL)
// ---------------------------------------------------------------------------
//
// Wire format per block (one file per walSnapTxnRead call):
//   [int64_t firstVer : 8B][int64_t fileSize : 8B][uint8_t data : fileSize]

struct SWalTxnSnapReader {
  SWal    *pWal;
  int64_t  snapVer;
  int32_t  fileIdx;    // next index into pFileVers
  SArray  *pFileVers;  // sorted SArray<int64_t> of .txn file firstVers to send
};

int32_t walSnapTxnReaderOpen(SWal *pWal, int64_t snapVer, SWalTxnSnapReader **ppReader) {
  SWalTxnSnapReader *pReader = taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) return terrno;

  pReader->pWal     = pWal;
  pReader->snapVer  = snapVer;
  pReader->fileIdx  = 0;
  pReader->pFileVers = taosArrayInit(16, sizeof(int64_t));
  if (pReader->pFileVers == NULL) {
    taosMemoryFree(pReader);
    return terrno;
  }

  int32_t logCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  char    fnameStr[WAL_FILE_LEN];
  for (int32_t i = 0; i < logCount; i++) {
    SWalFileInfo *pInfo = (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i);
    if (pInfo->firstVer > snapVer) break;
    walBuildTxnName(pWal, pInfo->firstVer, fnameStr);
    if (taosCheckExistFile(fnameStr)) {
      if (taosArrayPush(pReader->pFileVers, &pInfo->firstVer) == NULL) {
        taosArrayDestroy(pReader->pFileVers);
        taosMemoryFree(pReader);
        return terrno;
      }
    }
  }

  *ppReader = pReader;
  wDebug("vgId:%d, walSnapTxnReaderOpen: snapVer:%" PRId64 " files:%d", pWal->cfg.vgId, snapVer,
         (int)taosArrayGetSize(pReader->pFileVers));
  return TSDB_CODE_SUCCESS;
}

// Read the next .txn file into a heap-allocated buffer.
// *ppData = NULL and return 0 when all files have been sent.
// Caller must taosMemoryFree(*ppData) after use.
int32_t walSnapTxnRead(SWalTxnSnapReader *pReader, uint8_t **ppData, uint32_t *pLen) {
  *ppData = NULL;
  *pLen   = 0;

  SWal   *pWal      = pReader->pWal;
  int32_t totalFiles = (int32_t)taosArrayGetSize(pReader->pFileVers);
  char    fnameStr[WAL_FILE_LEN];

  while (pReader->fileIdx < totalFiles) {
    int64_t firstVer = *(int64_t *)taosArrayGet(pReader->pFileVers, pReader->fileIdx++);
    walBuildTxnName(pWal, firstVer, fnameStr);

    int64_t fileSize = 0;
    if (taosStatFile(fnameStr, &fileSize, NULL, NULL) < 0 || fileSize <= 0) continue;

    uint32_t totalLen = (uint32_t)(sizeof(int64_t) * 2 + fileSize);
    uint8_t *pBuf     = taosMemoryMalloc(totalLen);
    if (pBuf == NULL) return terrno;

    (void)memcpy(pBuf, &firstVer, sizeof(int64_t));
    (void)memcpy(pBuf + sizeof(int64_t), &fileSize, sizeof(int64_t));

    TdFilePtr fd = taosOpenFile(fnameStr, TD_FILE_READ);
    if (fd == NULL) {
      taosMemoryFree(pBuf);
      wWarn("vgId:%d, walSnapTxnRead: skip %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
      continue;
    }
    int64_t nRead = taosReadFile(fd, pBuf + sizeof(int64_t) * 2, fileSize);
    (void)taosCloseFile(&fd);
    if (nRead != fileSize) {
      taosMemoryFree(pBuf);
      wWarn("vgId:%d, walSnapTxnRead: short read %s, skip", pWal->cfg.vgId, fnameStr);
      continue;
    }

    *ppData = pBuf;
    *pLen   = totalLen;
    wDebug("vgId:%d, walSnapTxnRead: sent .txn firstVer:%" PRId64 " size:%" PRId64, pWal->cfg.vgId, firstVer,
           fileSize);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;  // done
}

void walSnapTxnReaderClose(SWalTxnSnapReader **ppReader) {
  if (ppReader == NULL || *ppReader == NULL) return;
  taosArrayDestroy((*ppReader)->pFileVers);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
}

// Receive a .txn file from the leader and write it to disk.
int32_t walSnapTxnWrite(SWal *pWal, uint8_t *pData, uint32_t len) {
  if (len < (uint32_t)(sizeof(int64_t) * 2)) {
    wError("vgId:%d, walSnapTxnWrite: block too short (%u)", pWal->cfg.vgId, len);
    return TSDB_CODE_FAILED;
  }

  int64_t firstVer = 0;
  int64_t fileSize = 0;
  (void)memcpy(&firstVer, pData, sizeof(int64_t));
  (void)memcpy(&fileSize, pData + sizeof(int64_t), sizeof(int64_t));

  if (fileSize <= 0 || (uint32_t)(sizeof(int64_t) * 2 + fileSize) != len) {
    wError("vgId:%d, walSnapTxnWrite: invalid block firstVer:%" PRId64 " fileSize:%" PRId64, pWal->cfg.vgId,
           firstVer, fileSize);
    return TSDB_CODE_FAILED;
  }

  char fnameStr[WAL_FILE_LEN];
  walBuildTxnName(pWal, firstVer, fnameStr);

  TdFilePtr fd = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (fd == NULL) {
    wError("vgId:%d, walSnapTxnWrite: open %s failed since %s", pWal->cfg.vgId, fnameStr, terrstr());
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (taosWriteFile(fd, pData + sizeof(int64_t) * 2, fileSize) != fileSize) {
    wError("vgId:%d, walSnapTxnWrite: write %s failed since %s", pWal->cfg.vgId, fnameStr, terrstr());
    code = terrno;
  } else if (taosFsyncFile(fd) != 0) {
    wError("vgId:%d, walSnapTxnWrite: fsync %s failed since %s", pWal->cfg.vgId, fnameStr, terrstr());
    code = terrno;
  }
  (void)taosCloseFile(&fd);

  if (code == TSDB_CODE_SUCCESS) {
    wDebug("vgId:%d, walSnapTxnWrite: wrote .txn firstVer:%" PRId64 " size:%" PRId64, pWal->cfg.vgId, firstVer,
           fileSize);
  }
  return code;
}

// ---------------------------------------------------------------------------
// Public API: sequential read of .txn files in [beginVer, endVer]
// ---------------------------------------------------------------------------

// Public API: sequential read of .txn files in [beginVer, endVer], invoking cb per entry.
// Used by txnMgrReloadFromWal for fast eager load (no INSERT entries to skip).
// Side effect: updates pWal->txnBeginIndexMap as entries are read, so that IS_BEGIN state
// is rebuilt simultaneously with the cache load — no separate rebuild pass needed.
int32_t walTxnReadRange(SWal *pWal, int64_t beginVer, int64_t endVer, FWalTxnEntryCb cb, void *arg) {
  if (pWal == NULL || cb == NULL || beginVer > endVer) return TSDB_CODE_SUCCESS;
  if (!pWal->cfg.enableTxnFile) return TSDB_CODE_SUCCESS;

  int32_t logCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  if (logCount == 0) return TSDB_CODE_SUCCESS;

  char fnameStr[WAL_FILE_LEN];

  for (int32_t i = 0; i < logCount; i++) {
    SWalFileInfo *pInfo = (SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i);

    // Determine effective lastVer for this segment.
    int64_t segLastVer = (i + 1 < logCount)
                             ? ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i + 1))->firstVer - 1
                             : pWal->vers.lastVer;

    // Skip segments entirely before beginVer or after endVer.
    if (segLastVer < beginVer) continue;
    if (pInfo->firstVer > endVer) break;

    walBuildTxnName(pWal, pInfo->firstVer, fnameStr);
    TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ);
    if (pFile == NULL) {
      wWarn("vgId:%d, walTxnReadRange: cannot open %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
      continue;
    }

    SWalCkHead *pHead = (SWalCkHead *)taosMemoryMalloc(sizeof(SWalCkHead) + WAL_SCAN_BUF_SIZE);
    if (pHead == NULL) {
      (void)taosCloseFile(&pFile);
      return terrno;
    }

    while (taosReadFile(pFile, pHead, sizeof(SWalCkHead)) == (int64_t)sizeof(SWalCkHead)) {
      if (pHead->magic != WAL_MAGIC) break;

      int64_t  ver     = pHead->head.version;
      int32_t  bodyLen = pHead->head.bodyLen;
      tmsg_t   msgType = pHead->head.msgType;

      if (ver > endVer) break;

      if (bodyLen < 0 || bodyLen > WAL_SCAN_BUF_SIZE) {
        wWarn("vgId:%d, walTxnReadRange: abnormal bodyLen:%d at ver:%" PRId64, pWal->cfg.vgId, bodyLen, ver);
        break;
      }

      // Read body.
      if (bodyLen > 0 && taosReadFile(pFile, pHead->head.body, bodyLen) != (int64_t)bodyLen) break;

      if (ver < beginVer) continue;

      // Extract txnId and stripped body via helpers.
      txn_id_t    txnId       = walContTxnId(&pHead->head);
      const char *body        = walContBody(&pHead->head);
      int32_t     strippedLen = walContBodyLen(&pHead->head);

      // Side effect: keep txnBeginIndexMap in sync so IS_BEGIN state is rebuilt
      // as part of the same scan — no separate walTxnRebuildBeginIndexMap pass needed.
      if (txnId != 0 && pWal->txnBeginIndexMap != NULL) {
        uint64_t txnExtFlags = walContTxnExtFlags(&pHead->head);
        if (txnExtFlags & WAL_TXN_EXT_IS_BEGIN) {
          int32_t putCode = taosHashPut(pWal->txnBeginIndexMap, &txnId, sizeof(txnId), &ver, sizeof(ver));
          if (putCode != 0) {
            wError("vgId:%d, walTxnReadRange: txnBeginIndexMap put failed txnId:%" PRId64 " ver:%" PRId64 " since %s",
                   pWal->cfg.vgId, txnId, ver, tstrerror(putCode));
            taosMemoryFree(pHead);
            (void)taosCloseFile(&pFile);
            return putCode;
          }
        }
        if (msgType == TDMT_VND_TXN_COMMIT || msgType == TDMT_VND_TXN_ROLLBACK) {
          int32_t removeCode = taosHashRemove(pWal->txnBeginIndexMap, &txnId, sizeof(txnId));
          if (removeCode != 0) {
            wWarn("vgId:%d, walTxnReadRange: txnBeginIndexMap remove failed txnId:%" PRId64 " (likely not in map)",
                  pWal->cfg.vgId, txnId);
          }
        }
      }

      int32_t code = cb(ver, msgType, txnId, body, strippedLen, arg);
      if (code != 0) {
        taosMemoryFree(pHead);
        (void)taosCloseFile(&pFile);
        return code;
      }
    }

    taosMemoryFree(pHead);
    (void)taosCloseFile(&pFile);
  }

  return TSDB_CODE_SUCCESS;
}

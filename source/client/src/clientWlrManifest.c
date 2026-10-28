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
 * Workload Recorder -- session manifest (DS 6.15.1).
 *
 * The manifest is an accelerator and a summary, never a prerequisite: a parser
 * that finds it missing or corrupt rebuilds the same information by scanning the
 * segments' file headers. That is deliberate, because the manifest is the one file
 * that must be rewritten as the session progresses, so it is also the one most
 * likely to be caught half-written by a crash.
 *
 * Update protocol, per DS 6.15.1:
 *
 *   1. write <manifest>.tmp
 *   2. taosFsyncFile the temp file
 *   3. taosRenameFile over the real manifest
 *
 * Step 2 is what makes the swap safe: after it, a crash leaves either the old
 * manifest or a complete new one, never a partial one. The DS also listed a fourth
 * step, fsync of the directory, to make the rename itself durable. That is not
 * implemented and cannot be portably: taosFsyncFile takes a TdFilePtr and Windows
 * cannot produce one for a directory, so doing it would require the module's first
 * platform-specific branch (DS 4.3.1). The cost is bounded -- a power loss may lose
 * the rename, leaving the previous manifest, which the rebuild path already
 * handles.
 */

#include "clientWlrInt.h"

#include "tjson.h"

static void wlrHexSessionId(const uint8_t *id, char *out, size_t outLen) {
  static const char kHex[] = "0123456789abcdef";
  size_t            i;
  if (outLen < (size_t)WLR_SESSION_ID_LEN * 2 + 1) {
    if (outLen > 0) out[0] = 0;
    return;
  }
  for (i = 0; i < WLR_SESSION_ID_LEN; ++i) {
    out[i * 2] = kHex[(id[i] >> 4) & 0x0F];
    out[i * 2 + 1] = kHex[id[i] & 0x0F];
  }
  out[WLR_SESSION_ID_LEN * 2] = 0;
}

static int32_t wlrManifestPath(SWlrManager *mgr, char *out, size_t outLen) {
  int32_t n = snprintf(out, outLen, "%s%s" WLR_MANIFEST_NAME_FMT, mgr->recordDir, TD_DIRSEP,
                       mgr->processStartWallNs, mgr->pid, mgr->manifestCollision);
  return (n > 0 && (size_t)n < outLen) ? 0 : -1;
}

/*
 * Build the manifest JSON.
 *
 * The four completeness flags are independent on purpose (DS 6.15.1). In
 * particular hasUnfinishedCalls is NOT a defect: a BEGIN with no END is exactly how
 * a hung call is captured, and replay accepts it without --allow-incomplete. Only
 * captureComplete=false requires that flag.
 */
static char *wlrBuildManifestJson(SWlrManager *mgr, bool gracefullyClosed) {
  SJson *root = NULL;
  SJson *segs = NULL;
  SJson *seg = NULL;
  char  *text = NULL;
  char   sessionHex[WLR_SESSION_ID_LEN * 2 + 1] = {0};
  int32_t activeCount;
  bool    overflow;
  bool    segmentNamesPartial;

  root = tjsonCreateObject();
  if (root == NULL) return NULL;

  wlrHexSessionId(mgr->sessionId, sessionHex, sizeof(sessionHex));

  if (tjsonAddIntegerToObject(root, "formatMajor", WLR_FORMAT_MAJOR) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "formatMinor", WLR_FORMAT_MINOR) != 0) goto _err;
  if (tjsonAddStringToObject(root, "sessionId", sessionHex) != 0) goto _err;
  if (tjsonAddStringToObject(root, "clockDomainId", mgr->clockDomainId) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "pid", mgr->pid) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "processStartWallNs", (uint64_t)mgr->processStartWallNs) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "processStartMonoNs", (uint64_t)mgr->processStartMonoNs) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "bufferBytes", (uint64_t)mgr->bufferBytes) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "eventsWritten", (uint64_t)atomic_load_64(&mgr->eventsWritten)) != 0)
    goto _err;
  if (tjsonAddIntegerToObject(root, "bytesWritten", (uint64_t)atomic_load_64(&mgr->bytesWritten)) != 0)
    goto _err;
  if (tjsonAddIntegerToObject(root, "backpressureNs", (uint64_t)atomic_load_64(&mgr->backpressureNs)) != 0)
    goto _err;

  /* Segment list. Only the index and name are recorded here: sizes and event ranges
   * live in each segment's own header, and duplicating them would create two
   * sources of truth that can disagree. */
  segs = tjsonCreateArray();
  if (segs == NULL) goto _err;
  {
    uint32_t i;
    uint32_t manifestSegmentCount = mgr->segIndex >= WLR_MAX_TRACKED_SEGMENTS
                                        ? WLR_MAX_TRACKED_SEGMENTS
                                        : mgr->segIndex + 1;
    segmentNamesPartial = (uint64_t)mgr->segIndex + 1 > WLR_MAX_TRACKED_SEGMENTS;
    for (i = 0; i < manifestSegmentCount; ++i) {
      char name[TSDB_FILENAME_LEN] = {0};
      /* The collision suffix this segment was CREATED with, not the current counter:
       * wlrOpenSegment bumps that permanently on a losing exclusive create, so using it
       * for every index would rename already-written segments to files that do not
       * exist. Names past the tracked window are omitted and advertised as partial. */
      uint32_t coll = mgr->segCollisions[i];
      int32_t nameLen = snprintf(name, sizeof(name), WLR_SEGMENT_NAME_FMT, mgr->processStartWallNs,
                                 mgr->pid, coll, i);
      if (nameLen <= 0 || (size_t)nameLen >= sizeof(name)) {
        tscWarn("WLR: segment name truncated for index %u; omitting manifest entry", i);
        continue;
      }
      seg = tjsonCreateObject();
      if (seg == NULL) goto _err;
      if (tjsonAddIntegerToObject(seg, "index", i) != 0) goto _err;
      if (tjsonAddStringToObject(seg, "file", name) != 0) goto _err;
      if (tjsonAddItemToArray(segs, seg) != 0) goto _err;
      /* A parent owns a child only after a successful attach. Clear the local
       * immediately so _err deletes only nodes that were not adopted. */
      seg = NULL;
    }
  }
  if (tjsonAddItemToObject(root, "segments", segs) != 0) goto _err;
  segs = NULL;
  if (tjsonAddBoolToObject(root, "segmentNamesPartial", segmentNamesPartial) != 0) goto _err;
  if (segmentNamesPartial) {
    tscWarn("WLR: manifest omits segment names beyond %u; replay must scan the directory",
            (uint32_t)WLR_MAX_TRACKED_SEGMENTS);
  }

  activeCount = 0;
  if (mgr->activeLockReady) {
    (void)taosThreadMutexLock(&mgr->activeLock);
    activeCount = mgr->activeCount;
    (void)taosThreadMutexUnlock(&mgr->activeLock);
  } else {
    tscWarn("WLR: active-call lock unavailable; unfinished-call list omitted from manifest");
  }
  overflow = !mgr->activeLockReady || atomic_load_32(&mgr->activeOverflow) != 0;

  /* structurallyComplete is asserted only for what this writer produced: frames were
   * written whole, in sequence. A reader still verifies it independently, since only
   * the reader can see truncation or media damage. */
  if (tjsonAddBoolToObject(root, "structurallyComplete", true) != 0) goto _err;
  /* False when fail-open or an incomplete shutdown drain made events known missing. */
  if (tjsonAddBoolToObject(root, "captureComplete",
                           atomic_load_32(&mgr->captureIncomplete) == 0) != 0)
    goto _err;
  if (tjsonAddBoolToObject(root, "gracefullyClosed", gracefullyClosed) != 0) goto _err;
  if (tjsonAddBoolToObject(root, "hasUnfinishedCalls", activeCount > 0) != 0) goto _err;
  if (tjsonAddIntegerToObject(root, "unfinishedCallCount", (uint64_t)activeCount) != 0) goto _err;
  /* Stated explicitly so a partial list is never mistaken for a complete one. */
  if (tjsonAddBoolToObject(root, "unfinishedCallListPartial", overflow) != 0) goto _err;

  /* Config summary. Never the password -- the recorder does not receive one, and the
   * manifest must not become the place a credential appears (DS 6.15.1, FS 6). */
  if (tjsonAddStringToObject(root, "recordDir", mgr->recordDir) != 0) goto _err;

  text = tjsonToString(root);
  if (text == NULL) tscWarn("WLR: failed to serialize manifest JSON");

_err:
  if (seg != NULL) tjsonDelete(seg);
  if (segs != NULL) tjsonDelete(segs);
  tjsonDelete(root);
  return text;
}

void wlrWriteManifest(SWlrManager *mgr, bool gracefullyClosed) {
  char      path[PATH_MAX + TSDB_FILENAME_LEN] = {0};
  char      tmpPath[PATH_MAX + TSDB_FILENAME_LEN + 8] = {0};
  char     *json = NULL;
  TdFilePtr f = NULL;
  int64_t   len;

  if (mgr->recordDir[0] == 0) return;
  if (wlrManifestPath(mgr, path, sizeof(path)) != 0) {
    tscWarn("WLR: manifest path too long, skipping manifest");
    return;
  }
  int32_t tmpPathLen = snprintf(tmpPath, sizeof(tmpPath), "%s.tmp", path);
  if (tmpPathLen < 0 || tmpPathLen >= (int32_t)sizeof(tmpPath)) {
    tscWarn("WLR: manifest temp path too long, skipping manifest");
    return;
  }

  json = wlrBuildManifestJson(mgr, gracefullyClosed);
  if (json == NULL) {
    tscWarn("WLR: failed to build manifest JSON object");
    return;
  }
  len = (int64_t)strlen(json);
#ifdef WLR_TEST_HOOKS
  wlrTestWaitFinalIo(mgr, WLR_TEST_FINAL_MANIFEST);
#endif

  /* A crash may leave the old temp file behind. Remove that unpublished artifact,
   * then recreate it with POSIX mode 0600 or a Windows private DACL. The final
   * manifest remains untouched until the fsync+rename below succeeds. */
  if (taosCheckExistFile(tmpPath) && taosRemoveFile(tmpPath) != 0) {
    tscWarn("WLR: cannot remove stale manifest temp file %s", tmpPath);
    taosMemoryFree(json);
    return;
  }
  f = taosOpenPrivateFile(tmpPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_EXCL);
  if (f == NULL) {
    tscWarn("WLR: cannot open %s, manifest not updated", tmpPath);
    taosMemoryFree(json);
    return;
  }

  if (taosWriteFile(f, json, len) != len) {
    tscWarn("WLR: short write on manifest temp file");
    (void)taosCloseFile(&f);
    (void)taosRemoveFile(tmpPath);
    taosMemoryFree(json);
    return;
  }

  /* Fsync BEFORE the rename. Without this the rename could become visible while the
   * contents are still in page cache, which is precisely the half-written manifest
   * the temp-file dance exists to prevent.
   *
   * A failed fsync is therefore not ignorable: renaming anyway publishes exactly the
   * torn manifest this sequence exists to avoid. Abandon the temp file and leave any
   * previous manifest alone -- a stale complete manifest beats a partial current one,
   * and the rebuild path already handles staleness. */
  if (taosFsyncFile(f) != 0) {
    tscError("WLR: fsync failed on manifest temp file, not publishing");
    (void)taosCloseFile(&f);
    (void)taosRemoveFile(tmpPath);
    taosMemoryFree(json);
    return;
  }
  (void)taosCloseFile(&f);
  taosMemoryFree(json);

  if (taosRenameFile(tmpPath, path) != 0) {
    tscWarn("WLR: failed to rename manifest into place");
    (void)taosRemoveFile(tmpPath);
    return;
  }

  tscInfo("WLR: manifest updated %s (graceful:%d)", path, (int32_t)gracefullyClosed);
}

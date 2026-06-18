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
#include "tsdbFS2.h"
#include "vnd.h"

// ==================== Diff Entry ====================

typedef struct SMediumDiffEntry {
  SMediumSnapFileInfo fileInfo;
  int32_t            opType;  // tsdb_fop_t
} SMediumDiffEntry;

typedef TARRAY2(SMediumDiffEntry) TMediumDiffArray;

// ==================== Reader ====================

struct STsdbSnapMediumReader {
  STsdb  *tsdb;
  int64_t ever;
  int8_t  type;

  TFileSetArray   *fsetArr;
  TMediumDiffArray diffArr[1];

  // iteration state
  int32_t diffIdx;
  int64_t fileOffset;
  int64_t actualFileSize;  // actual file size on disk (from taosStatFile)
  TdFilePtr pFD;
};

// ==================== Writer ====================

struct STsdbSnapMediumWriter {
  STsdb  *tsdb;
  int64_t ever;

  TFileSetArray *fsetArr;
  TFileOpArray   fopArr[1];

  // current file being written
  TdFilePtr pFD;
  char      tmpFname[TSDB_FILENAME_LEN];
};

// ==================== Helper: convert STFile to SMediumSnapFileInfo ====================

static void tfileToMediumInfo(const STFile *f, int32_t fid, SMediumSnapFileInfo *info) {
  info->fid = fid;
  info->ftype = (int32_t)f->type;
  info->level = (f->type == TSDB_FTYPE_STT) ? f->stt[0].level : 0;
  info->minVer = f->minVer;
  info->maxVer = f->maxVer;
  info->lcn = f->lcn;
  info->mid = f->mid;
  info->cid = f->cid;
  info->diskLevel = f->did.level;
  info->diskId = f->did.id;
  info->size = f->size;
  info->missing = 0;
}

// ==================== Diff Algorithm ====================

static int32_t tsdbSnapMediumDiff(STsdb *tsdb, TFileSetArray *leaderArr,
                                  SMediumSnapFileList *followerList, int64_t beginIndex,
                                  TMediumDiffArray *diffArr) {
  int32_t code = 0;
  int32_t lino = 0;

  // Build hash of follower files keyed by (fid, ftype, level, minVer, maxVer, lcn, mid)
  SHashObj *followerHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (followerHash == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Collect fids that have missing files on follower
  SHashObj *missingFids = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (missingFids == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Track which fids are selected for transfer (used to filter delete checks)
  SHashObj *selectedFids = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (selectedFids == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Track count of follower STT files per key (for handling duplicates per spec 2.4.2)
  SHashObj *sttKeyCount = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (sttKeyCount == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Track which follower STT file (by cid+disk) was chosen for MODIFY (count > 1 case)
  SHashObj *sttModifiedKey = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (sttModifiedKey == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Log received follower file list (requirement 2: leader receives this from follower)
  tsdbInfo("vgId:%d, medium diff: received follower file list nFiles:%d beginIndex:%" PRId64,
           TD_VID(tsdb->pVnode), followerList->nFiles, beginIndex);
  for (int32_t i = 0; i < followerList->nFiles; i++) {
    SMediumSnapFileInfo *fi = &followerList->aFiles[i];
    tsdbInfo("vgId:%d, medium diff: follower file[%d]: fid:%d ftype:%d level:%d"
             " minVer:%" PRId64 " maxVer:%" PRId64 " lcn:%d mid:%d"
             " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64 " missing:%d",
             TD_VID(tsdb->pVnode), i, fi->fid, fi->ftype, fi->level,
             fi->minVer, fi->maxVer, fi->lcn, fi->mid,
             fi->diskLevel, fi->diskId, fi->cid, fi->size, fi->missing);
  }

  for (int32_t i = 0; i < followerList->nFiles; i++) {
    SMediumSnapFileInfo *fi = &followerList->aFiles[i];
    // key depends on file type per spec 2.4:
    //   non-stt: (fid, ftype)
    //   stt: (fid, ftype, minVer, maxVer)
    char key[64];
    int32_t keyLen;
    if (fi->ftype == TSDB_FTYPE_STT) {
      keyLen = snprintf(key, sizeof(key), "%d:%d:%d:%" PRId64 ":%" PRId64,
                        fi->fid, fi->ftype, fi->level, fi->minVer, fi->maxVer);
    } else {
      keyLen = snprintf(key, sizeof(key), "%d:%d", fi->fid, fi->ftype);
    }
    code = taosHashPut(followerHash, key, keyLen, fi, sizeof(SMediumSnapFileInfo));
    if (code != 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // Count STT duplicates per key
    if (fi->ftype == TSDB_FTYPE_STT) {
      int32_t *pCount = taosHashGet(sttKeyCount, key, keyLen);
      if (pCount != NULL) {
        int32_t newCount = *pCount + 1;
        code = taosHashPut(sttKeyCount, key, keyLen, &newCount, sizeof(int32_t));
        if (code != 0) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      } else {
        int32_t one = 1;
        code = taosHashPut(sttKeyCount, key, keyLen, &one, sizeof(int32_t));
        if (code != 0) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }

    if (fi->missing) {
      int32_t dummy = 1;
      code = taosHashPut(missingFids, &fi->fid, sizeof(int32_t), &dummy, sizeof(int32_t));
      if (code != 0) {
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

  // Log leader's own file list (requirement 3)
  // Iterate leader file sets
  int32_t nFsets = TARRAY2_SIZE(leaderArr);
  tsdbInfo("vgId:%d, medium diff: leader file sets count:%d", TD_VID(tsdb->pVnode), nFsets);
  for (int32_t fsIdx = 0; fsIdx < nFsets; fsIdx++) {
    STFileSet *fset = TARRAY2_GET(leaderArr, fsIdx);
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
      if (fset->farr[ftype] == NULL) continue;
      STFile *lf = fset->farr[ftype]->f;
      tsdbInfo("vgId:%d, medium diff: leader file: fid:%d ftype:%d level:0"
               " minVer:%" PRId64 " maxVer:%" PRId64 " lcn:%d mid:%d"
               " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64,
               TD_VID(tsdb->pVnode), fset->fid, ftype,
               lf->minVer, lf->maxVer, lf->lcn, lf->mid,
               lf->did.level, lf->did.id, lf->cid, lf->size);
    }
    SSttLvl *sttLvl;
    TARRAY2_FOREACH(fset->lvlArr, sttLvl) {
      STFileObj *sttObj;
      TARRAY2_FOREACH(sttLvl->fobjArr, sttObj) {
        STFile *lf = sttObj->f;
        tsdbInfo("vgId:%d, medium diff: leader file: fid:%d ftype:%d level:%d"
                 " minVer:%" PRId64 " maxVer:%" PRId64 " lcn:%d mid:%d"
                 " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64,
                 TD_VID(tsdb->pVnode), fset->fid, TSDB_FTYPE_STT, sttLvl->level,
                 lf->minVer, lf->maxVer, lf->lcn, lf->mid,
                 lf->did.level, lf->did.id, lf->cid, lf->size);
      }
    }
  }

  for (int32_t fsIdx = 0; fsIdx < nFsets; fsIdx++) {
    STFileSet *fset = TARRAY2_GET(leaderArr, fsIdx);
    int32_t    fid = fset->fid;

    // Check if this fset is selected
    bool selected = false;
    const char *selectReason = NULL;

    // Check if any follower file in this fid is missing
    if (taosHashGet(missingFids, &fid, sizeof(int32_t)) != NULL) {
      selected = true;
      selectReason = "follower has missing file";
    }

    // Check if any leader file in this fid has maxVer > beginIndex
    if (!selected) {
      for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && !selected; ftype++) {
        if (fset->farr[ftype] == NULL) continue;
        if (fset->farr[ftype]->f[0].maxVer > beginIndex) {
          selected = true;
          selectReason = "file maxVer > beginIndex";
        }
      }
      SSttLvl *lvl;
      TARRAY2_FOREACH(fset->lvlArr, lvl) {
        if (selected) break;
        STFileObj *fobj;
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          if (fobj->f[0].maxVer > beginIndex) {
            selected = true;
            selectReason = "stt file maxVer > beginIndex";
            break;
          }
        }
      }
    }

    if (!selected) continue;

    // Record this fid as selected
    int32_t dummy = 1;
    code = taosHashPut(selectedFids, &fid, sizeof(int32_t), &dummy, sizeof(int32_t));
    if (code != 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // Log fset selection reason (requirement 4)
    tsdbInfo("vgId:%d, medium diff: fset fid:%d selected, reason: %s",
             TD_VID(tsdb->pVnode), fid, selectReason);

    // Process leader files in this fset
    // Regular files (HEAD, DATA, SMA, TOMB)
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
      if (fset->farr[ftype] == NULL) continue;
      STFile *lf = fset->farr[ftype]->f;

      SMediumSnapFileInfo leaderInfo;
      tfileToMediumInfo(lf, fid, &leaderInfo);

      // Non-stt key: (fid, ftype) per spec 2.4.1
      char key[64];
      int32_t keyLen = snprintf(key, sizeof(key), "%d:%d", leaderInfo.fid, leaderInfo.ftype);

      SMediumSnapFileInfo *followerInfo = taosHashGet(followerHash, key, keyLen);
      if (followerInfo != NULL) {
        if (followerInfo->missing || followerInfo->size != leaderInfo.size) {
          // TRANSFER as MODIFY
          tsdbInfo("vgId:%d, medium diff: MODIFY fid:%d ftype:%d level:%d, reason: %s",
                   TD_VID(tsdb->pVnode), fid, ftype, 0,
                   followerInfo->missing ? "follower file missing" : "size mismatch");
          SMediumDiffEntry entry = {0};
          entry.fileInfo = leaderInfo;
          entry.fileInfo.diskLevel = followerInfo->diskLevel;
          entry.fileInfo.diskId = followerInfo->diskId;
          entry.fileInfo.cid = followerInfo->cid + 1;
          entry.opType = TSDB_FOP_MODIFY;
          code = TARRAY2_APPEND(diffArr, entry);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          // same size + not missing → SKIP
          tsdbDebug("vgId:%d, medium diff: SKIP fid:%d ftype:%d level:0, reason: same size and not missing",
                    TD_VID(tsdb->pVnode), fid, ftype);
        }
      } else {
        // Leader has, follower doesn't → TRANSFER as CREATE
        // For non-STT files: cid = follower's same ftype max cid + 1
        // For STT files: cid stays unchanged (handled in STT section below)
        int64_t newCid = leaderInfo.cid;
        // Find max cid of follower files with same fid and ftype
        for (int32_t fi = 0; fi < followerList->nFiles; fi++) {
          SMediumSnapFileInfo *ff = &followerList->aFiles[fi];
          if (ff->fid == fid && ff->ftype == ftype) {
            if (ff->cid >= newCid) {
              newCid = ff->cid + 1;
            }
          }
        }
        tsdbInfo("vgId:%d, medium diff: CREATE fid:%d ftype:%d level:%d, reason: follower does not have this file,"
                 " cid:%" PRId64 " (original:%" PRId64 ")",
                 TD_VID(tsdb->pVnode), fid, ftype, 0, newCid, leaderInfo.cid);
        SMediumDiffEntry entry = {0};
        entry.fileInfo = leaderInfo;
        entry.fileInfo.cid = newCid;
        entry.opType = TSDB_FOP_CREATE;
        code = TARRAY2_APPEND(diffArr, entry);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    // STT files
    SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      STFileObj *fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        STFile *lf = fobj->f;

        SMediumSnapFileInfo leaderInfo;
        tfileToMediumInfo(lf, fid, &leaderInfo);

        // STT key: (fid, ftype, level, minVer, maxVer) per spec 2.4.2
        char key[64];
        int32_t keyLen = snprintf(key, sizeof(key), "%d:%d:%d:%" PRId64 ":%" PRId64,
                                  leaderInfo.fid, leaderInfo.ftype, leaderInfo.level,
                                  leaderInfo.minVer, leaderInfo.maxVer);

        SMediumSnapFileInfo *followerInfo = taosHashGet(followerHash, key, keyLen);
        if (followerInfo != NULL) {
          // Per spec 2.4.2: leader and follower both have this STT key.
          // Always use CREATE for leader file; all follower duplicates will be
          // DELETE'd in the DELETE check section below.
          // This avoids MODIFY ambiguity when multiple follower files share same cid.
          int32_t *pSttCount = taosHashGet(sttKeyCount, key, keyLen);
          int32_t  sttCount = pSttCount ? *pSttCount : 1;

          // Find max cid among all follower files with this key for new cid assignment
          int64_t maxFollowerCid = followerInfo->cid;
          for (int32_t fi = 0; fi < followerList->nFiles; fi++) {
            SMediumSnapFileInfo *ff = &followerList->aFiles[fi];
            if (ff->fid == fid && ff->ftype == TSDB_FTYPE_STT &&
                ff->level == leaderInfo.level &&
                ff->minVer == leaderInfo.minVer && ff->maxVer == leaderInfo.maxVer) {
              if (ff->cid > maxFollowerCid) {
                maxFollowerCid = ff->cid;
              }
            }
          }

          tsdbInfo("vgId:%d, medium diff: STT both exist, count=%d, CREATE leader + DELETE all follower."
                   " fid:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64 " newCid:%" PRId64,
                   TD_VID(tsdb->pVnode), sttCount, fid, leaderInfo.level,
                   leaderInfo.minVer, leaderInfo.maxVer, maxFollowerCid + 1);

          // Emit CREATE for leader file
          SMediumDiffEntry createEntry = {0};
          createEntry.fileInfo = leaderInfo;
          createEntry.fileInfo.diskLevel = followerInfo->diskLevel;
          createEntry.fileInfo.diskId = followerInfo->diskId;
          createEntry.fileInfo.cid = maxFollowerCid + 1;
          createEntry.opType = TSDB_FOP_CREATE;
          code = TARRAY2_APPEND(diffArr, createEntry);
          TSDB_CHECK_CODE(code, lino, _exit);

          // Mark this key as handled — DELETE check will DELETE all follower files with this key
          int32_t handled = 1;
          code = taosHashPut(sttModifiedKey, key, keyLen, &handled, sizeof(int32_t));
          if (code != 0) {
            code = terrno;
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        } else {
          // Leader has, follower doesn't → TRANSFER as CREATE
          // For STT files: if follower has any STT in same fid, use max(follower stt cid) + 1
          //                otherwise keep leader's cid unchanged
          int64_t newCid = leaderInfo.cid;
          bool    followerHasStt = false;
          for (int32_t fi = 0; fi < followerList->nFiles; fi++) {
            SMediumSnapFileInfo *ff = &followerList->aFiles[fi];
            if (ff->fid == fid && ff->ftype == TSDB_FTYPE_STT) {
              followerHasStt = true;
              if (ff->cid >= newCid) {
                newCid = ff->cid + 1;
              }
            }
          }
          if (!followerHasStt) {
            newCid = leaderInfo.cid;  // keep unchanged
          }
          tsdbInfo(
              "vgId:%d, medium diff: CREATE fid:%d ftype:STT level:%d, reason: follower does not have this file,"
              " cid:%" PRId64 " (original:%" PRId64 " followerHasStt:%d)",
              TD_VID(tsdb->pVnode), fid, leaderInfo.level, newCid, leaderInfo.cid, followerHasStt);
          SMediumDiffEntry entry = {0};
          entry.fileInfo = leaderInfo;
          entry.fileInfo.cid = newCid;
          entry.opType = TSDB_FOP_CREATE;
          code = TARRAY2_APPEND(diffArr, entry);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

  // Check for DELETE: follower has file but leader doesn't
  // Only check files in fids that were selected for transfer
  for (int32_t i = 0; i < followerList->nFiles; i++) {
    SMediumSnapFileInfo *fi = &followerList->aFiles[i];

    // Skip files in fids that were not selected for transfer
    if (taosHashGet(selectedFids, &fi->fid, sizeof(int32_t)) == NULL) {
      tsdbDebug("vgId:%d, check delete: SKIP fid:%d ftype:%d level:%d, reason: fid not selected for transfer",
               TD_VID(tsdb->pVnode), fi->fid, fi->ftype, fi->level);
      continue;
    }

    tsdbInfo("vgId:%d, check delete: fid:%d ftype:%d level:%d"
             " minVer:%" PRId64 " maxVer:%" PRId64 " lcn:%d mid:%d"
             " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64 " missing:%d",
             TD_VID(tsdb->pVnode), fi->fid, fi->ftype, fi->level,
             fi->minVer, fi->maxVer, fi->lcn, fi->mid,
             fi->diskLevel, fi->diskId, fi->cid, fi->size, fi->missing);

    // Find corresponding leader file
    bool leaderHas = false;

    // Search leader fset array for this fid
    STFileSet  target = {.fid = fi->fid};
    STFileSet *pTarget = &target;
    STFileSet **fsetPtr = TARRAY2_SEARCH(leaderArr, &pTarget, tsdbTFileSetCmprFn, TD_EQ);
    if (fsetPtr != NULL) {
      STFileSet *fset = *fsetPtr;
      if (fi->ftype == TSDB_FTYPE_STT) {
        // STT key: (fid, ftype, level, minVer, maxVer) per spec 2.4.2
        char sttKey[64];
        int32_t sttKeyLen = snprintf(sttKey, sizeof(sttKey), "%d:%d:%d:%" PRId64 ":%" PRId64,
                                     fi->fid, fi->ftype, fi->level, fi->minVer, fi->maxVer);

        SSttLvl *lvl;
        TARRAY2_FOREACH(fset->lvlArr, lvl) {
          if (lvl->level != fi->level) continue;
          STFileObj *fobj;
          TARRAY2_FOREACH(lvl->fobjArr, fobj) {
            STFile *lf = fobj->f;
            if (lf->minVer == fi->minVer && lf->maxVer == fi->maxVer) {
              leaderHas = true;
              break;
            }
          }
          if (leaderHas) break;
        }

        if (leaderHas) {
          // Leader has same key. Check if handled (CREATE already emitted) → DELETE this follower file
          if (taosHashGet(sttModifiedKey, sttKey, sttKeyLen) != NULL) {
            // This key was handled: all follower files with this key should be DELETE'd
            leaderHas = false;
            tsdbInfo("vgId:%d, check delete: STT key handled, DELETE follower duplicate. fid:%d level:%d cid:%" PRId64
                     " disk.level:%d disk.id:%d",
                     TD_VID(tsdb->pVnode), fi->fid, fi->level, fi->cid, fi->diskLevel, fi->diskId);
          }
          // else: leader has the key but it wasn't in selected fids processing → keep file
        } else {
          tsdbInfo("vgId:%d, check delete: stt not found on leader, fid:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64,
                   TD_VID(tsdb->pVnode), fi->fid, fi->level, fi->minVer, fi->maxVer);
        }
      } else {
        // Non-stt key: (fid, ftype) per spec 2.4.1 — just check if slot exists
        int32_t ftype = fi->ftype;
        if (ftype >= 0 && ftype < TSDB_FTYPE_MAX && fset->farr[ftype] != NULL) {
          leaderHas = true;
        }
        if (!leaderHas) {
          tsdbInfo("vgId:%d, check delete: non-stt not found on leader, fid:%d ftype:%d",
                   TD_VID(tsdb->pVnode), fi->fid, ftype);
        }
      }
    }
    else{
      tsdbInfo("vgId:%d, not found", TD_VID(tsdb->pVnode));
    }

    if (!leaderHas) {
      SMediumDiffEntry entry = {0};
      entry.fileInfo = *fi;
      entry.opType = TSDB_FOP_REMOVE;
      tsdbInfo("vgId:%d, medium diff: REMOVE fid:%d ftype:STT level:%d, reason: leader does not have this file",
                   TD_VID(tsdb->pVnode), fi->fid, fi->level);
      code = TARRAY2_APPEND(diffArr, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  taosHashCleanup(followerHash);
  taosHashCleanup(missingFids);
  taosHashCleanup(selectedFids);
  taosHashCleanup(sttKeyCount);
  taosHashCleanup(sttModifiedKey);
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// ==================== Reader Implementation ====================

int32_t tsdbSnapMediumReaderOpen(STsdb *tsdb, int64_t ever, int8_t type,
                                 SMediumSnapFileList *pFollowerFileList, int64_t beginIndex,
                                 STsdbSnapMediumReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(STsdbSnapMediumReader));
  if (reader[0] == NULL) return terrno;

  reader[0]->tsdb = tsdb;
  reader[0]->ever = ever;
  reader[0]->type = type;
  reader[0]->diffIdx = 0;
  reader[0]->fileOffset = 0;
  reader[0]->pFD = NULL;

  // Take ref snapshot of leader's file state
  code = tsdbFSCreateRefSnapshot(tsdb->pFS, &reader[0]->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Run diff algorithm
  code = tsdbSnapMediumDiff(tsdb, reader[0]->fsetArr, pFollowerFileList, beginIndex, reader[0]->diffArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Log diff result summary
  int32_t nCreate = 0, nModify = 0, nRemove = 0;
  int32_t nDiffTotal = TARRAY2_SIZE(reader[0]->diffArr);
  for (int32_t i = 0; i < nDiffTotal; i++) {
    SMediumDiffEntry *e = &TARRAY2_GET(reader[0]->diffArr, i);
    if (e->opType == TSDB_FOP_CREATE) nCreate++;
    else if (e->opType == TSDB_FOP_MODIFY) nModify++;
    else if (e->opType == TSDB_FOP_REMOVE) nRemove++;
  }
  tsdbInfo("vgId:%d, tsdb snap medium reader opened. ever:%" PRId64 " type:%d diffCount:%d"
           " (create:%d modify:%d remove:%d)",
           TD_VID(tsdb->pVnode), ever, type, nDiffTotal, nCreate, nModify, nRemove);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
    if (reader[0]) {
      TARRAY2_DESTROY(reader[0]->diffArr, NULL);
      tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
      taosMemoryFree(reader[0]);
      reader[0] = NULL;
    }
  }
  return code;
}

int32_t tsdbSnapMediumReaderClose(STsdbSnapMediumReader **reader) {
  if (reader[0] == NULL) return 0;

  tsdbInfo("vgId:%d, tsdb snap medium reader closing. diffIdx:%d total:%d",
           TD_VID(reader[0]->tsdb->pVnode), reader[0]->diffIdx,
           (int32_t)TARRAY2_SIZE(reader[0]->diffArr));

  if (reader[0]->pFD) {
    taosCloseFile(&reader[0]->pFD);
  }
  TARRAY2_DESTROY(reader[0]->diffArr, NULL);
  tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return 0;
}

int32_t tsdbSnapMediumRead(STsdbSnapMediumReader *reader, uint8_t **data) {
  int32_t code = 0;
  int32_t lino = 0;

  data[0] = NULL;

  if (reader == NULL) {
    return -1;
  }

  int32_t totalDiff = TARRAY2_SIZE(reader->diffArr);
  if (reader->diffIdx >= totalDiff) {
    // All done
    return 0;
  }

  SMediumDiffEntry *entry = &TARRAY2_GET(reader->diffArr, reader->diffIdx);

  // Prepare the header
  SMediumSnapFileHdr hdr = {0};
  hdr.fileInfo = entry->fileInfo;
  hdr.opType = entry->opType;

  int32_t hdrLen = tSerializeSMediumSnapFileHdr(NULL, 0, &hdr);
  if (hdrLen < 0) {
    code = hdrLen;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (entry->opType == TSDB_FOP_REMOVE) {
    // DELETE: send header only, flag = 1 (last block)
    tsdbInfo("vgId:%d, medium reader: sending DELETE fid:%d ftype:%d level:%d [%d/%d]",
             TD_VID(reader->tsdb->pVnode), entry->fileInfo.fid, entry->fileInfo.ftype,
             entry->fileInfo.level, reader->diffIdx + 1, totalDiff);
    int64_t totalSize = hdrLen;
    void   *pBuf = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + totalSize);
    if (pBuf == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SSnapDataHdr *pHdr = pBuf;
    pHdr->type = reader->type;
    pHdr->flag = 1;  // last (and only) block
    pHdr->size = totalSize;

    tSerializeSMediumSnapFileHdr(pHdr->data, hdrLen, &hdr);

    data[0] = pBuf;
    reader->diffIdx++;
    goto _exit;
  }

  // CREATE or MODIFY: read file content
  if (reader->pFD == NULL) {
    tsdbInfo("vgId:%d, medium reader: sending %s fid:%d ftype:%d level:%d size:%" PRId64 " [%d/%d]",
             TD_VID(reader->tsdb->pVnode),
             (entry->opType == TSDB_FOP_CREATE) ? "CREATE" : "MODIFY",
             entry->fileInfo.fid, entry->fileInfo.ftype, entry->fileInfo.level,
             entry->fileInfo.size, reader->diffIdx + 1, totalDiff);
    // Open the file on leader side
    // Construct STFile from entry info to get filename
    STFile tf = {0};
    tf.type = (tsdb_ftype_t)entry->fileInfo.ftype;
    tf.did.level = entry->fileInfo.diskLevel;
    tf.did.id = entry->fileInfo.diskId;
    tf.fid = entry->fileInfo.fid;
    tf.lcn = entry->fileInfo.lcn;
    tf.mid = entry->fileInfo.mid;
    tf.cid = entry->fileInfo.cid;
    tf.size = entry->fileInfo.size;
    tf.minVer = entry->fileInfo.minVer;
    tf.maxVer = entry->fileInfo.maxVer;
    if (tf.type == TSDB_FTYPE_STT) {
      tf.stt[0].level = entry->fileInfo.level;
    }

    // For reading, use leader's actual file path (from leader's file state)
    // Find the actual file in leader's fsetArr
    char fname[TSDB_FILENAME_LEN] = {0};
    bool found = false;
    STFileSet  target = {.fid = entry->fileInfo.fid};
    STFileSet *pTarget = &target;
    STFileSet **fsetPtr = TARRAY2_SEARCH(reader->fsetArr, &pTarget, tsdbTFileSetCmprFn, TD_EQ);
    if (fsetPtr != NULL) {
      STFileSet *fset = *fsetPtr;
      if (entry->fileInfo.ftype == TSDB_FTYPE_STT) {
        SSttLvl *lvl;
        TARRAY2_FOREACH(fset->lvlArr, lvl) {
          if (lvl->level != entry->fileInfo.level) continue;
          STFileObj *fobj;
          TARRAY2_FOREACH(lvl->fobjArr, fobj) {
            STFile *lf = fobj->f;
            // STT key: (fid, ftype, level, minVer, maxVer) per spec 2.4.2
            if (lf->minVer == entry->fileInfo.minVer && lf->maxVer == entry->fileInfo.maxVer) {
              tstrncpy(fname, fobj->fname, TSDB_FILENAME_LEN);
              found = true;
              break;
            }
          }
          if (found) break;
        }
      } else {
        int32_t ftype = entry->fileInfo.ftype;
        if (ftype >= 0 && ftype < TSDB_FTYPE_MAX && fset->farr[ftype] != NULL) {
          tstrncpy(fname, fset->farr[ftype]->fname, TSDB_FILENAME_LEN);
          found = true;
        }
      }
    }

    if (!found) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    reader->pFD = taosOpenFile(fname, TD_FILE_READ);
    if (reader->pFD == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    reader->fileOffset = 0;

    // Get actual file size on disk for reading
    int64_t actualSize = 0;
    if (taosStatFile(fname, &actualSize, NULL, NULL) != 0) {
      code = terrno;
      tsdbError("vgId:%d, medium reader: failed to stat file %s", TD_VID(reader->tsdb->pVnode), fname);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    reader->actualFileSize = actualSize;
    tsdbInfo("vgId:%d, medium reader: file %s metaSize:%" PRId64 " actualSize:%" PRId64,
             TD_VID(reader->tsdb->pVnode), fname, entry->fileInfo.size, actualSize);
  }

  // Read a block (use actual file size for reading)
  int64_t remaining = reader->actualFileSize - reader->fileOffset;
  int64_t blockSize = TMIN(remaining, TSDB_SNAP_DATA_PAYLOAD_SIZE);
  bool    isLast = (reader->fileOffset + blockSize >= reader->actualFileSize);

  int64_t totalSize = hdrLen + blockSize;
  void   *pBuf = taosMemoryCalloc(1, sizeof(SSnapDataHdr) + totalSize);
  if (pBuf == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSnapDataHdr *pHdr = pBuf;
  pHdr->type = reader->type;
  pHdr->flag = isLast ? 1 : 0;
  pHdr->size = totalSize;

  // Serialize header
  tSerializeSMediumSnapFileHdr(pHdr->data, hdrLen, &hdr);

  // Read file data
  int64_t nRead = taosReadFile(reader->pFD, pHdr->data + hdrLen, blockSize);
  if (nRead < 0) {
    code = terrno;
    taosMemoryFree(pBuf);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (nRead != blockSize) {
    code = TSDB_CODE_FILE_CORRUPTED;
    taosMemoryFree(pBuf);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader->fileOffset += blockSize;
  data[0] = pBuf;

  if (isLast) {
    taosCloseFile(&reader->pFD);
    reader->pFD = NULL;
    reader->fileOffset = 0;
    reader->diffIdx++;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(reader->tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// ==================== Writer Implementation ====================

int32_t tsdbSnapMediumWriterOpen(STsdb *tsdb, int64_t ever, STsdbSnapMediumWriter **writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer[0] = taosMemoryCalloc(1, sizeof(STsdbSnapMediumWriter));
  if (writer[0] == NULL) return terrno;

  writer[0]->tsdb = tsdb;
  writer[0]->ever = ever;
  writer[0]->pFD = NULL;
  writer[0]->tmpFname[0] = '\0';

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &writer[0]->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbInfo("vgId:%d, tsdb snap medium writer opened. ever:%" PRId64, TD_VID(tsdb->pVnode), ever);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
    if (writer[0]) {
      tsdbFSDestroyCopySnapshot(&writer[0]->fsetArr);
      taosMemoryFree(writer[0]);
      writer[0] = NULL;
    }
  }
  return code;
}

int32_t tsdbSnapMediumWriterPrepareClose(STsdbSnapMediumWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSEditBegin(writer->tsdb->pFS, writer->fopArr, TSDB_FEDIT_COMMIT);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(writer->tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapMediumWriterClose(STsdbSnapMediumWriter **writer, int8_t rollback) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = writer[0]->tsdb;

  if (writer[0]->pFD) {
    taosCloseFile(&writer[0]->pFD);
    writer[0]->pFD = NULL;
  }

  if (rollback) {
    tsdbInfo("vgId:%d, tsdb snap medium writer rolling back. fops:%d",
             TD_VID(tsdb->pVnode), (int32_t)TARRAY2_SIZE(writer[0]->fopArr));
    code = tsdbFSEditAbort(writer[0]->tsdb->pFS);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    tsdbInfo("vgId:%d, tsdb snap medium writer committing. fops:%d",
             TD_VID(tsdb->pVnode), (int32_t)TARRAY2_SIZE(writer[0]->fopArr));
    code = taosThreadMutexLock(&writer[0]->tsdb->mutex);
    if (code != 0) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbFSEditCommit(writer[0]->tsdb->pFS);
    if (code) {
      int32_t unlockRet = taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
      if (unlockRet != 0) {
        tsdbError("vgId:%d, medium writer: mutex unlock failed since %s", TD_VID(tsdb->pVnode), tstrerror(unlockRet));
      }
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer[0]->tsdb->pFS->fsstate = TSDB_FS_STATE_NORMAL;

    code = taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
    if (code != 0) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  TARRAY2_DESTROY(writer[0]->fopArr, NULL);
  tsdbFSDestroyCopySnapshot(&writer[0]->fsetArr);

  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapMediumWriteDelete(STsdbSnapMediumWriter *writer, SMediumSnapFileHdr *hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  SMediumSnapFileInfo *fi = &hdr->fileInfo;

  // Build STFileOp for REMOVE
  STFileOp op = {0};
  op.optype = TSDB_FOP_REMOVE;
  op.fid = fi->fid;
  op.of.type = (tsdb_ftype_t)fi->ftype;
  op.of.did.level = fi->diskLevel;
  op.of.did.id = fi->diskId;
  op.of.fid = fi->fid;
  op.of.lcn = fi->lcn;
  op.of.mid = fi->mid;
  op.of.cid = fi->cid;
  op.of.size = fi->size;
  op.of.minVer = fi->minVer;
  op.of.maxVer = fi->maxVer;
  if (fi->ftype == TSDB_FTYPE_STT) {
    op.of.stt[0].level = fi->level;
  }

  tsdbInfo("vgId:%d, medium writer: DELETE file fid:%d ftype:%d level:%d", TD_VID(writer->tsdb->pVnode), fi->fid,
           fi->ftype, fi->level);

  code = TARRAY2_APPEND(writer->fopArr, op);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapMediumWriteData(STsdbSnapMediumWriter *writer, SSnapDataHdr *pSnapHdr,
                                       SMediumSnapFileHdr *hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  SMediumSnapFileInfo *fi = &hdr->fileInfo;

  // Compute header serialized length (same as what reader produces)
  int32_t hdrLen = tSerializeSMediumSnapFileHdr(NULL, 0, hdr);
  if (hdrLen < 0) {
    code = hdrLen;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t dataLen = pSnapHdr->size - hdrLen;
  uint8_t *fileData = pSnapHdr->data + hdrLen;

  // Open temp file if not already open
  if (writer->pFD == NULL) {
    // Build target filename (final destination)
    STFile tf = {0};
    tf.type = (tsdb_ftype_t)fi->ftype;
    tf.did.level = fi->diskLevel;
    tf.did.id = fi->diskId;
    tf.fid = fi->fid;
    tf.lcn = fi->lcn;
    tf.mid = fi->mid;
    tf.cid = fi->cid;
    tf.size = fi->size;
    tf.minVer = fi->minVer;
    tf.maxVer = fi->maxVer;
    if (tf.type == TSDB_FTYPE_STT) {
      tf.stt[0].level = fi->level;
    }

    char finalFname[TSDB_FILENAME_LEN] = {0};
    tsdbTFileName(writer->tsdb, &tf, finalFname);

    // Temp file: append ".tmp"
    snprintf(writer->tmpFname, TSDB_FILENAME_LEN, "%s.tmp", finalFname);

    tsdbInfo("vgId:%d, open file %s", TD_VID(writer->tsdb->pVnode), writer->tmpFname);
    writer->pFD = taosOpenFile(writer->tmpFname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (writer->pFD == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // Write data
  if (dataLen > 0) {
    int64_t nWrite = taosWriteFile(writer->pFD, fileData, dataLen);
    if (nWrite < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (nWrite != dataLen) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // If last block, finalize
  if (pSnapHdr->flag == 1) {
    taosCloseFile(&writer->pFD);
    writer->pFD = NULL;

    // Get actual written file size
    int64_t actualSize = 0;
    if (taosStatFile(writer->tmpFname, &actualSize, NULL, NULL) != 0) {
      tsdbError("vgId:%d, medium writer: failed to stat tmp file %s", TD_VID(writer->tsdb->pVnode), writer->tmpFname);
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    tsdbInfo("vgId:%d, medium writer: file received, metaSize:%" PRId64 " actualSize:%" PRId64 " fname:%s",
             TD_VID(writer->tsdb->pVnode), fi->size, actualSize, writer->tmpFname);

    // Build final filename
    STFile tf = {0};
    tf.type = (tsdb_ftype_t)fi->ftype;
    tf.did.level = fi->diskLevel;
    tf.did.id = fi->diskId;
    tf.fid = fi->fid;
    tf.lcn = fi->lcn;
    tf.mid = fi->mid;
    tf.cid = fi->cid;
    tf.size = fi->size;  // use metadata size for current.json
    tf.minVer = fi->minVer;
    tf.maxVer = fi->maxVer;
    if (tf.type == TSDB_FTYPE_STT) {
      tf.stt[0].level = fi->level;
    }

    char finalFname[TSDB_FILENAME_LEN] = {0};
    tsdbTFileName(writer->tsdb, &tf, finalFname);

    tsdbInfo("vgId:%d, finalize file %s, size:%" PRId64, TD_VID(writer->tsdb->pVnode), finalFname, actualSize);
    // Rename temp to final
    code = taosRenameFile(writer->tmpFname, finalFname);
    if (code) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    writer->tmpFname[0] = '\0';

    // Append file op
    STFileOp op = {0};
    op.fid = fi->fid;
    op.nf = tf;

    if (hdr->opType == TSDB_FOP_CREATE) {
      op.optype = TSDB_FOP_CREATE;
    } else {
      // MODIFY
      op.optype = TSDB_FOP_MODIFY;
      op.of = tf;
      // old file cid is one less (follower's original cid)
      op.of.cid = fi->cid - 1;
    }

    code = TARRAY2_APPEND(writer->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);

    tsdbInfo("vgId:%d, medium writer: %s file fid:%d ftype:%d level:%d size:%" PRId64,
              TD_VID(writer->tsdb->pVnode),
              (hdr->opType == TSDB_FOP_CREATE) ? "CREATE" : "MODIFY",
              fi->fid, fi->ftype, fi->level, fi->size);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbSnapMediumWrite(STsdbSnapMediumWriter *writer, SSnapDataHdr *pSnapHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  // Deserialize header from data
  SMediumSnapFileHdr hdr = {0};
  code = tDeserializeSMediumSnapFileHdr(pSnapHdr->data, pSnapHdr->size, &hdr);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Log received diff entry on follower side
  SMediumSnapFileInfo *fi = &hdr.fileInfo;
  tsdbDebug(
      "vgId:%d, medium writer recv: op:%s fid:%d ftype:%d level:%d"
      " minVer:%" PRId64 " maxVer:%" PRId64
      " lcn:%d mid:%d"
      " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64 " flag:%d",
      TD_VID(writer->tsdb->pVnode),
      (hdr.opType == TSDB_FOP_CREATE)   ? "CREATE"
      : (hdr.opType == TSDB_FOP_REMOVE) ? "REMOVE"
                                        : "MODIFY",
      fi->fid, fi->ftype, fi->level, fi->minVer, fi->maxVer, fi->lcn, fi->mid, fi->diskLevel, fi->diskId, fi->cid,
      fi->size, pSnapHdr->flag);

  if (hdr.opType == TSDB_FOP_REMOVE) {
    code = tsdbSnapMediumWriteDelete(writer, &hdr);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbSnapMediumWriteData(writer, pSnapHdr, &hdr);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, type:%d index:%" PRId64 " size:%" PRId64,
              TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code),
              pSnapHdr->type, pSnapHdr->index, pSnapHdr->size);
  } else {
    tsdbTrace("vgId:%d %s done, type:%d index:%" PRId64 " size:%" PRId64,
              TD_VID(writer->tsdb->pVnode), __func__, pSnapHdr->type, pSnapHdr->index, pSnapHdr->size);
  }
  return code;
}

// ==================== Helper: Build File List ====================

int32_t tsdbBuildMediumSnapFileList(STsdb *tsdb, SMediumSnapFileList *pList) {
  int32_t code = 0;
  int32_t lino = 0;

  TFileSetArray *fsetArr = NULL;
  code = tsdbFSCreateRefSnapshot(tsdb->pFS, &fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Count total files
  int32_t totalFiles = 0;
  int32_t nFsets = TARRAY2_SIZE(fsetArr);
  for (int32_t fsIdx = 0; fsIdx < nFsets; fsIdx++) {
    STFileSet *fset = TARRAY2_GET(fsetArr, fsIdx);
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
      if (fset->farr[ftype] != NULL) totalFiles++;
    }
    SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      totalFiles += TARRAY2_SIZE(lvl->fobjArr);
    }
  }

  if (totalFiles == 0) {
    pList->nFiles = 0;
    pList->aFiles = NULL;
    goto _exit;
  }

  pList->aFiles = taosMemoryCalloc(totalFiles, sizeof(SMediumSnapFileInfo));
  if (pList->aFiles == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int32_t idx = 0;
  for (int32_t fsIdx = 0; fsIdx < nFsets; fsIdx++) {
    STFileSet *fset = TARRAY2_GET(fsetArr, fsIdx);

    // Regular files
    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
      if (fset->farr[ftype] == NULL) continue;
      STFileObj *fobj = fset->farr[ftype];

      SMediumSnapFileInfo *info = &pList->aFiles[idx];
      tfileToMediumInfo(fobj->f, fset->fid, info);

      // Check if file exists on disk
      char fname[TSDB_FILENAME_LEN] = {0};
      tsdbTFileName(tsdb, fobj->f, fname);
      if (taosStatFile(fname, NULL, NULL, NULL) != 0) {
        info->missing = 1;
      }
      idx++;
    }

    // STT files
    SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      STFileObj *fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        SMediumSnapFileInfo *info = &pList->aFiles[idx];
        tfileToMediumInfo(fobj->f, fset->fid, info);

        char fname[TSDB_FILENAME_LEN] = {0};
        tsdbTFileName(tsdb, fobj->f, fname);
        if (taosStatFile(fname, NULL, NULL, NULL) != 0) {
          info->missing = 1;
        }
        idx++;
      }
    }
  }

  pList->nFiles = idx;

  // Log follower file list (requirement 1: follower sends this to leader)
  tsdbInfo("vgId:%d, medium snap follower file list: nFiles:%d", TD_VID(tsdb->pVnode), pList->nFiles);
  for (int32_t i = 0; i < pList->nFiles; i++) {
    SMediumSnapFileInfo *fi = &pList->aFiles[i];
    tsdbInfo("vgId:%d, medium snap follower file[%d]: fid:%d ftype:%d level:%d"
             " minVer:%" PRId64 " maxVer:%" PRId64 " lcn:%d mid:%d"
             " disk.level:%d disk.id:%d cid:%" PRId64 " size:%" PRId64 " missing:%d",
             TD_VID(tsdb->pVnode), i, fi->fid, fi->ftype, fi->level,
             fi->minVer, fi->maxVer, fi->lcn, fi->mid,
             fi->diskLevel, fi->diskId, fi->cid, fi->size, fi->missing);
  }

_exit:
  tsdbFSDestroyRefSnapshot(&fsetArr);
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pList->aFiles);
    pList->nFiles = 0;
  } else {
    tsdbInfo("vgId:%d, tsdb build medium snap file list done. nFiles:%d", TD_VID(tsdb->pVnode), pList->nFiles);
  }
  return code;
}

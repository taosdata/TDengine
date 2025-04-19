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

#include "tsdbFS2.h"
#include "cos.h"
#include "tsdbUpgrade.h"
#include "vnd.h"

#define BLOCK_COMMIT_FACTOR 3

typedef struct STFileHashEntry {
  struct STFileHashEntry *next;
  char                    fname[TSDB_FILENAME_LEN];
} STFileHashEntry;

typedef struct {
  int32_t           numFile;
  int32_t           numBucket;
  STFileHashEntry **buckets;
} STFileHash;

static const char *gCurrentFname[] = {
    [TSDB_FCURRENT] = "current.json",
    [TSDB_FCURRENT_C] = "current.c.json",
    [TSDB_FCURRENT_M] = "current.m.json",
};

static int32_t create_fs(STsdb *pTsdb, STFileSystem **fs) {
  fs[0] = taosMemoryCalloc(1, sizeof(*fs[0]));
  if (fs[0] == NULL) {
    return terrno;
  }

  fs[0]->tsdb = pTsdb;
  int32_t code = tsem_init(&fs[0]->canEdit, 0, 1);
  if (code) {
    taosMemoryFree(fs[0]);
    return code;
  }

  fs[0]->fsstate = TSDB_FS_STATE_NORMAL;
  fs[0]->neid = 0;
  TARRAY2_INIT(fs[0]->fSetArr);
  TARRAY2_INIT(fs[0]->fSetArrTmp);

  return 0;
}

static void destroy_fs(STFileSystem **fs) {
  if (fs[0] == NULL) return;

  TARRAY2_DESTROY(fs[0]->fSetArr, NULL);
  TARRAY2_DESTROY(fs[0]->fSetArrTmp, NULL);
  if (tsem_destroy(&fs[0]->canEdit) != 0) {
    tsdbError("failed to destroy semaphore");
  }
  taosMemoryFree(fs[0]);
  fs[0] = NULL;
}

void current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype) {
  int32_t offset = 0;

  vnodeGetPrimaryDir(pTsdb->path, pTsdb->pVnode->diskPrimary, pTsdb->pVnode->pTfs, fname, TSDB_FILENAME_LEN);
  offset = strlen(fname);
  snprintf(fname + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, gCurrentFname[ftype]);
}

static int32_t save_json(const cJSON *json, const char *fname) {
  int32_t   code = 0;
  int32_t   lino;
  char     *data = NULL;
  TdFilePtr fp = NULL;

  data = cJSON_PrintUnformatted(json);
  if (data == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  fp = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (fp == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  if (taosWriteFile(fp, data, strlen(data)) < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  if (taosFsyncFile(fp) < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at %s:%d since %s", __func__, fname, __LINE__, tstrerror(code));
  }
  taosMemoryFree(data);
  taosCloseFileWithLog(&fp);
  return code;
}

static int32_t load_json(const char *fname, cJSON **json) {
  int32_t code = 0;
  int32_t lino;
  char   *data = NULL;

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_READ);
  if (fp == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  int64_t size;
  code = taosFStatFile(fp, &size, NULL);
  if (code != 0) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  data = taosMemoryMalloc(size + 1);
  if (data == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  if (taosReadFile(fp, data, size) < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  data[size] = '\0';

  json[0] = cJSON_Parse(data);
  if (json[0] == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at %s:%d since %s", __func__, fname, __LINE__, tstrerror(code));
    json[0] = NULL;
  }
  taosCloseFileWithLog(&fp);
  taosMemoryFree(data);
  return code;
}

int32_t save_fs(const TFileSetArray *arr, const char *fname) {
  int32_t code = 0;
  int32_t lino = 0;

  cJSON *json = cJSON_CreateObject();
  if (json == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  // fmtv
  if (cJSON_AddNumberToObject(json, "fmtv", 1) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  // fset
  cJSON *ajson = cJSON_AddArrayToObject(json, "fset");
  if (!ajson) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  const STFileSet *fset;
  TARRAY2_FOREACH(arr, fset) {
    cJSON *item = cJSON_CreateObject();
    if (!item) {
      TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    }
    (void)cJSON_AddItemToArray(ajson, item);

    code = tsdbTFileSetToJson(fset, item);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = save_json(json, fname);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  cJSON_Delete(json);
  return code;
}

static int32_t load_fs(STsdb *pTsdb, const char *fname, TFileSetArray *arr) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(arr, tsdbTFileSetClear);

  // load json
  cJSON *json = NULL;
  code = load_json(fname, &json);
  TSDB_CHECK_CODE(code, lino, _exit);

  // parse json
  const cJSON *item1;

  /* fmtv */
  item1 = cJSON_GetObjectItem(json, "fmtv");
  if (cJSON_IsNumber(item1)) {
    if (item1->valuedouble != 1) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
    }
  } else {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  /* fset */
  item1 = cJSON_GetObjectItem(json, "fset");
  if (cJSON_IsArray(item1)) {
    const cJSON *item2;
    cJSON_ArrayForEach(item2, item1) {
      STFileSet *fset;
      code = tsdbJsonToTFileSet(pTsdb, item2, &fset);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(arr, fset);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    TARRAY2_SORT(arr, tsdbTFileSetCmprFn);
  } else {
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at %sP%d since %s, fname:%s", __func__, __FILE__, lino, tstrerror(code), fname);
  }
  if (json) {
    cJSON_Delete(json);
  }
  return code;
}

static int32_t apply_commit(STFileSystem *fs) {
  int32_t        code = 0;
  int32_t        lino;
  TFileSetArray *fsetArray1 = fs->fSetArr;
  TFileSetArray *fsetArray2 = fs->fSetArrTmp;
  int32_t        i1 = 0, i2 = 0;

  while (i1 < TARRAY2_SIZE(fsetArray1) || i2 < TARRAY2_SIZE(fsetArray2)) {
    STFileSet *fset1 = i1 < TARRAY2_SIZE(fsetArray1) ? TARRAY2_GET(fsetArray1, i1) : NULL;
    STFileSet *fset2 = i2 < TARRAY2_SIZE(fsetArray2) ? TARRAY2_GET(fsetArray2, i2) : NULL;

    if (fset1 && fset2) {
      if (fset1->fid < fset2->fid) {
        // delete fset1
        tsdbTFileSetRemove(fset1);
        i1++;
      } else if (fset1->fid > fset2->fid) {
        // create new file set with fid of fset2->fid
        code = tsdbTFileSetInitCopy(fs->tsdb, fset2, &fset1);
        TSDB_CHECK_CODE(code, lino, _exit);
        code = TARRAY2_SORT_INSERT(fsetArray1, fset1, tsdbTFileSetCmprFn);
        TSDB_CHECK_CODE(code, lino, _exit);
        i1++;
        i2++;
      } else {
        // edit
        code = tsdbTFileSetApplyEdit(fs->tsdb, fset2, fset1);
        TSDB_CHECK_CODE(code, lino, _exit);
        i1++;
        i2++;
      }
    } else if (fset1) {
      // delete fset1
      tsdbTFileSetRemove(fset1);
      i1++;
    } else {
      // create new file set with fid of fset2->fid
      code = tsdbTFileSetInitCopy(fs->tsdb, fset2, &fset1);
      TSDB_CHECK_CODE(code, lino, _exit);
      code = TARRAY2_SORT_INSERT(fsetArray1, fset1, tsdbTFileSetCmprFn);
      TSDB_CHECK_CODE(code, lino, _exit);
      i1++;
      i2++;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t commit_edit(STFileSystem *fs) {
  char current[TSDB_FILENAME_LEN];
  char current_t[TSDB_FILENAME_LEN];

  current_fname(fs->tsdb, current, TSDB_FCURRENT);
  if (fs->etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_C);
  } else {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_M);
  }

  int32_t code;
  int32_t lino;
  TSDB_CHECK_CODE(taosRenameFile(current_t, current), lino, _exit);

  code = apply_commit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(fs->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, fs->etype);
  }
  return code;
}

// static int32_t
static int32_t tsdbFSDoSanAndFix(STFileSystem *fs);
static int32_t apply_abort(STFileSystem *fs) { return tsdbFSDoSanAndFix(fs); }

static int32_t abort_edit(STFileSystem *fs) {
  char fname[TSDB_FILENAME_LEN];

  if (fs->etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->tsdb, fname, TSDB_FCURRENT_C);
  } else {
    current_fname(fs->tsdb, fname, TSDB_FCURRENT_M);
  }

  int32_t code;
  int32_t lino;
  if ((code = taosRemoveFile(fname))) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = apply_abort(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(fs->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, fs->etype);
  }
  return code;
}

static int32_t tsdbFSDoScanAndFixFile(STFileSystem *fs, const STFileObj *fobj) {
  int32_t code = 0;
  int32_t lino = 0;

  // check file existence
  if (!taosCheckExistFile(fobj->fname)) {
    bool found = false;

    if (tsS3Enabled && fobj->f->lcn > 1) {
      char fname1[TSDB_FILENAME_LEN];
      tsdbTFileLastChunkName(fs->tsdb, fobj->f, fname1);
      if (!taosCheckExistFile(fname1)) {
        code = TSDB_CODE_FILE_CORRUPTED;
        tsdbError("vgId:%d %s failed since file:%s does not exist", TD_VID(fs->tsdb->pVnode), __func__, fname1);
        return code;
      }

      found = true;
    }

    if (!found) {
      code = TSDB_CODE_FILE_CORRUPTED;
      tsdbError("vgId:%d %s failed since file:%s does not exist", TD_VID(fs->tsdb->pVnode), __func__, fobj->fname);
      return code;
    }
  }

  return 0;
}

static void tsdbFSDestroyFileObjHash(STFileHash *hash);

static int32_t tsdbFSAddEntryToFileObjHash(STFileHash *hash, const char *fname) {
  STFileHashEntry *entry = taosMemoryMalloc(sizeof(*entry));
  if (entry == NULL) return terrno;

  tstrncpy(entry->fname, fname, TSDB_FILENAME_LEN);

  uint32_t idx = MurmurHash3_32(fname, strlen(fname)) % hash->numBucket;

  entry->next = hash->buckets[idx];
  hash->buckets[idx] = entry;
  hash->numFile++;

  return 0;
}

static int32_t tsdbFSCreateFileObjHash(STFileSystem *fs, STFileHash *hash) {
  int32_t code = 0;
  int32_t lino;
  char    fname[TSDB_FILENAME_LEN];

  // init hash table
  hash->numFile = 0;
  hash->numBucket = 4096;
  hash->buckets = taosMemoryCalloc(hash->numBucket, sizeof(STFileHashEntry *));
  if (hash->buckets == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // vnode.json
  current_fname(fs->tsdb, fname, TSDB_FCURRENT);
  code = tsdbFSAddEntryToFileObjHash(hash, fname);
  TSDB_CHECK_CODE(code, lino, _exit);

  // other
  STFileSet *fset = NULL;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    // data file
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; i++) {
      if (fset->farr[i] != NULL) {
        code = tsdbFSAddEntryToFileObjHash(hash, fset->farr[i]->fname);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (TSDB_FTYPE_DATA == i && fset->farr[i]->f->lcn > 0) {
          STFileObj *fobj = fset->farr[i];
          int32_t    lcn = fobj->f->lcn;
          char       lcn_name[TSDB_FILENAME_LEN];

          snprintf(lcn_name, TSDB_FQDN_LEN, "%s", fobj->fname);
          char *dot = strrchr(lcn_name, '.');
          if (dot) {
            snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - lcn_name), "%d.data", lcn);

            code = tsdbFSAddEntryToFileObjHash(hash, lcn_name);
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        }
      }
    }

    // stt file
    SSttLvl *lvl = NULL;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      STFileObj *fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        code = tsdbFSAddEntryToFileObjHash(hash, fobj->fname);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
    tsdbFSDestroyFileObjHash(hash);
  }
  return code;
}

static const STFileHashEntry *tsdbFSGetFileObjHashEntry(STFileHash *hash, const char *fname) {
  uint32_t idx = MurmurHash3_32(fname, strlen(fname)) % hash->numBucket;

  STFileHashEntry *entry = hash->buckets[idx];
  while (entry) {
    if (strcmp(entry->fname, fname) == 0) {
      return entry;
    }
    entry = entry->next;
  }

  return NULL;
}

static void tsdbFSDestroyFileObjHash(STFileHash *hash) {
  for (int32_t i = 0; i < hash->numBucket; i++) {
    STFileHashEntry *entry = hash->buckets[i];
    while (entry) {
      STFileHashEntry *next = entry->next;
      taosMemoryFree(entry);
      entry = next;
    }
  }
  taosMemoryFree(hash->buckets);
  memset(hash, 0, sizeof(*hash));
}

static int32_t tsdbFSDoSanAndFix(STFileSystem *fs) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t corrupt = false;

  {  // scan each file
    STFileSet *fset = NULL;
    TARRAY2_FOREACH(fs->fSetArr, fset) {
      // data file
      for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
        if (fset->farr[ftype] == NULL) continue;
        STFileObj *fobj = fset->farr[ftype];
        code = tsdbFSDoScanAndFixFile(fs, fobj);
        if (code) {
          fset->maxVerValid = (fobj->f->minVer <= fobj->f->maxVer) ? TMIN(fset->maxVerValid, fobj->f->minVer - 1) : -1;
          corrupt = true;
        }
      }

      // stt file
      SSttLvl *lvl;
      TARRAY2_FOREACH(fset->lvlArr, lvl) {
        STFileObj *fobj;
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          code = tsdbFSDoScanAndFixFile(fs, fobj);
          if (code) {
            fset->maxVerValid =
                (fobj->f->minVer <= fobj->f->maxVer) ? TMIN(fset->maxVerValid, fobj->f->minVer - 1) : -1;
            corrupt = true;
          }
        }
      }
    }
  }

  if (corrupt) {
    tsdbError("vgId:%d, not to clear dangling files due to fset incompleteness", TD_VID(fs->tsdb->pVnode));
    fs->fsstate = TSDB_FS_STATE_INCOMPLETE;
    code = 0;
    goto _exit;
  }

  {  // clear unreferenced files
    STfsDir *dir = NULL;
    TAOS_CHECK_GOTO(tfsOpendir(fs->tsdb->pVnode->pTfs, fs->tsdb->path, &dir), &lino, _exit);

    STFileHash fobjHash = {0};
    code = tsdbFSCreateFileObjHash(fs, &fobjHash);
    if (code) goto _close_dir;

    for (const STfsFile *file = NULL; (file = tfsReaddir(dir)) != NULL;) {
      if (taosIsDir(file->aname)) continue;

      if (tsdbFSGetFileObjHashEntry(&fobjHash, file->aname) == NULL) {
        tsdbRemoveFile(file->aname);
      }
    }

    tsdbFSDestroyFileObjHash(&fobjHash);

  _close_dir:
    tfsClosedir(dir);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSScanAndFix(STFileSystem *fs) {
  fs->neid = 0;

  // get max commit id
  const STFileSet *fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) { fs->neid = TMAX(fs->neid, tsdbTFileSetMaxCid(fset)); }

  // scan and fix
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSDoSanAndFix(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbFSDupState(STFileSystem *fs) {
  int32_t code;

  const TFileSetArray *src = fs->fSetArr;
  TFileSetArray       *dst = fs->fSetArrTmp;

  TARRAY2_CLEAR(dst, tsdbTFileSetClear);

  const STFileSet *fset1;
  TARRAY2_FOREACH(src, fset1) {
    STFileSet *fset2;
    code = tsdbTFileSetInitCopy(fs->tsdb, fset1, &fset2);
    if (code) return code;
    code = TARRAY2_APPEND(dst, fset2);
    if (code) return code;
  }

  return 0;
}

static int32_t open_fs(STFileSystem *fs, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = fs->tsdb;

  char fCurrent[TSDB_FILENAME_LEN];
  char cCurrent[TSDB_FILENAME_LEN];
  char mCurrent[TSDB_FILENAME_LEN];

  current_fname(pTsdb, fCurrent, TSDB_FCURRENT);
  current_fname(pTsdb, cCurrent, TSDB_FCURRENT_C);
  current_fname(pTsdb, mCurrent, TSDB_FCURRENT_M);

  if (taosCheckExistFile(fCurrent)) {  // current.json exists
    code = load_fs(pTsdb, fCurrent, fs->fSetArr);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(cCurrent)) {
      // current.c.json exists

      fs->etype = TSDB_FEDIT_COMMIT;
      if (rollback) {
        code = abort_edit(fs);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = load_fs(pTsdb, cCurrent, fs->fSetArrTmp);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = commit_edit(fs);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else if (taosCheckExistFile(mCurrent)) {
      // current.m.json exists
      fs->etype = TSDB_FEDIT_MERGE;
      code = abort_edit(fs);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbFSDupState(fs);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbFSScanAndFix(fs);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = save_fs(fs->fSetArr, fCurrent);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static void close_file_system(STFileSystem *fs) {
  TARRAY2_CLEAR(fs->fSetArr, tsdbTFileSetClear);
  TARRAY2_CLEAR(fs->fSetArrTmp, tsdbTFileSetClear);
}

static int32_t fset_cmpr_fn(const struct STFileSet *pSet1, const struct STFileSet *pSet2) {
  if (pSet1->fid < pSet2->fid) {
    return -1;
  } else if (pSet1->fid > pSet2->fid) {
    return 1;
  }
  return 0;
}

static int32_t edit_fs(STFileSystem *fs, const TFileOpArray *opArray, EFEditT etype) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSDupState(fs);
  if (code) return code;

  TFileSetArray  *fsetArray = fs->fSetArrTmp;
  STFileSet      *fset = NULL;
  const STFileOp *op;
  int32_t         fid = INT32_MIN;
  TSKEY           now = taosGetTimestampMs();
  TARRAY2_FOREACH_PTR(opArray, op) {
    if (!fset || fset->fid != op->fid) {
      STFileSet tfset = {.fid = op->fid};
      fset = &tfset;
      STFileSet **fsetPtr = TARRAY2_SEARCH(fsetArray, &fset, tsdbTFileSetCmprFn, TD_EQ);
      fset = (fsetPtr == NULL) ? NULL : *fsetPtr;

      if (!fset) {
        code = tsdbTFileSetInit(op->fid, &fset);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = TARRAY2_SORT_INSERT(fsetArray, fset, tsdbTFileSetCmprFn);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    code = tsdbTFileSetEdit(fs->tsdb, fset, op);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (fid != op->fid) {
      fid = op->fid;
      if (etype == TSDB_FEDIT_COMMIT) {
        fset->lastCommit = now;
      } else if (etype == TSDB_FEDIT_COMPACT) {
        fset->lastCompact = now;
      }
    }
  }

  // remove empty empty stt level and empty file set
  int32_t i = 0;
  while (i < TARRAY2_SIZE(fsetArray)) {
    fset = TARRAY2_GET(fsetArray, i);

    SSttLvl *lvl;
    int32_t  j = 0;
    while (j < TARRAY2_SIZE(fset->lvlArr)) {
      lvl = TARRAY2_GET(fset->lvlArr, j);

      if (TARRAY2_SIZE(lvl->fobjArr) == 0) {
        TARRAY2_REMOVE(fset->lvlArr, j, tsdbSttLvlClear);
      } else {
        j++;
      }
    }

    if (tsdbTFileSetIsEmpty(fset)) {
      TARRAY2_REMOVE(fsetArray, i, tsdbTFileSetClear);
    } else {
      i++;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
  }
  return code;
}

// return error code
int32_t tsdbOpenFS(STsdb *pTsdb, STFileSystem **fs, int8_t rollback) {
  int32_t code;
  int32_t lino;

  code = tsdbCheckAndUpgradeFileSystem(pTsdb, rollback);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = create_fs(pTsdb, fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = open_fs(fs[0], rollback);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    destroy_fs(fs);
  } else {
    tsdbInfo("vgId:%d %s success", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static void tsdbFSSetBlockCommit(STFileSet *fset, bool block);
extern void tsdbStopAllCompTask(STsdb *tsdb);

int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb) {
  STFileSystem *fs = pTsdb->pFS;
  SArray       *asyncTasks = taosArrayInit(0, sizeof(SVATaskID));
  if (asyncTasks == NULL) {
    return terrno;
  }

  (void)taosThreadMutexLock(&pTsdb->mutex);

  // disable
  pTsdb->bgTaskDisabled = true;

  // collect channel
  STFileSet *fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    if (taosArrayPush(asyncTasks, &fset->mergeTask) == NULL       //
        || taosArrayPush(asyncTasks, &fset->compactTask) == NULL  //
        || taosArrayPush(asyncTasks, &fset->retentionTask) == NULL) {
      taosArrayDestroy(asyncTasks);
      (void)taosThreadMutexUnlock(&pTsdb->mutex);
      return terrno;
    }
    tsdbFSSetBlockCommit(fset, false);
  }

  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  // destroy all channels
  for (int32_t k = 0; k < 2; k++) {
    for (int32_t i = 0; i < taosArrayGetSize(asyncTasks); i++) {
      SVATaskID *task = taosArrayGet(asyncTasks, i);
      if (k == 0) {
        (void)vnodeACancel(task);
      } else {
        vnodeAWait(task);
      }
    }
  }
  taosArrayDestroy(asyncTasks);

#ifdef TD_ENTERPRISE
  tsdbStopAllCompTask(pTsdb);
#endif
  return 0;
}

void tsdbEnableBgTask(STsdb *pTsdb) {
  (void)taosThreadMutexLock(&pTsdb->mutex);
  pTsdb->bgTaskDisabled = false;
  (void)taosThreadMutexUnlock(&pTsdb->mutex);
}

void tsdbCloseFS(STFileSystem **fs) {
  if (fs[0] == NULL) return;

  int32_t code = tsdbDisableAndCancelAllBgTask((*fs)->tsdb);
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID((*fs)->tsdb->pVnode), __func__, __LINE__,
              tstrerror(code));
  }
  close_file_system(fs[0]);
  destroy_fs(fs);
  return;
}

int64_t tsdbFSAllocEid(STFileSystem *fs) {
  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  int64_t cid = ++fs->neid;
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);
  return cid;
}

void tsdbFSUpdateEid(STFileSystem *fs, int64_t cid) {
  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  fs->neid = TMAX(fs->neid, cid);
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);
}

int32_t tsdbFSEditBegin(STFileSystem *fs, const TFileOpArray *opArray, EFEditT etype) {
  int32_t code = 0;
  int32_t lino;
  char    current_t[TSDB_FILENAME_LEN];

  if (etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_C);
  } else {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_M);
  }

  if (tsem_wait(&fs->canEdit) != 0) {
    tsdbError("vgId:%d failed to wait semaphore", TD_VID(fs->tsdb->pVnode));
  }
  fs->etype = etype;

  // edit
  code = edit_fs(fs, opArray, etype);
  TSDB_CHECK_CODE(code, lino, _exit);

  // save fs
  code = save_fs(fs->fSetArrTmp, current_t);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, lino,
              tstrerror(code), etype);
  } else {
    tsdbInfo("vgId:%d %s done, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, etype);
  }
  return code;
}

static void tsdbFSSetBlockCommit(STFileSet *fset, bool block) {
  if (block) {
    fset->blockCommit = true;
  } else {
    fset->blockCommit = false;
    if (fset->numWaitCommit > 0) {
      (void)taosThreadCondSignal(&fset->canCommit);
    }
  }
}

void tsdbFSCheckCommit(STsdb *tsdb, int32_t fid) {
  (void)taosThreadMutexLock(&tsdb->mutex);
  STFileSet *fset;
  tsdbFSGetFSet(tsdb->pFS, fid, &fset);
  if (fset) {
    while (fset->blockCommit) {
      fset->numWaitCommit++;
      (void)taosThreadCondWait(&fset->canCommit, &tsdb->mutex);
      fset->numWaitCommit--;
    }
  }
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return;
}

// IMPORTANT: the caller must hold fs->tsdb->mutex
int32_t tsdbFSEditCommit(STFileSystem *fs) {
  int32_t code = 0;
  int32_t lino = 0;

  // commit
  code = commit_edit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // schedule merge
  int32_t sttTrigger = fs->tsdb->pVnode->config.sttTrigger;
  if (sttTrigger > 1 && !fs->tsdb->bgTaskDisabled) {
    STFileSet *fset;
    TARRAY2_FOREACH_REVERSE(fs->fSetArr, fset) {
      if (TARRAY2_SIZE(fset->lvlArr) == 0) {
        tsdbFSSetBlockCommit(fset, false);
        continue;
      }

      SSttLvl *lvl = TARRAY2_FIRST(fset->lvlArr);
      if (lvl->level != 0) {
        tsdbFSSetBlockCommit(fset, false);
        continue;
      }

      // bool    skipMerge = false;
      int32_t numFile = TARRAY2_SIZE(lvl->fobjArr);
      if (numFile >= sttTrigger && (!vnodeATaskValid(&fset->mergeTask))) {
        SMergeArg *arg = taosMemoryMalloc(sizeof(*arg));
        if (arg == NULL) {
          code = terrno;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        arg->tsdb = fs->tsdb;
        arg->fid = fset->fid;

        code = vnodeAsync(MERGE_TASK_ASYNC, EVA_PRIORITY_HIGH, tsdbMerge, taosAutoMemoryFree, arg, &fset->mergeTask);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (numFile >= sttTrigger * BLOCK_COMMIT_FACTOR) {
        tsdbFSSetBlockCommit(fset, true);
      } else {
        tsdbFSSetBlockCommit(fset, false);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(fs->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, fs->etype);
  }
  if (tsem_post(&fs->canEdit) != 0) {
    tsdbError("vgId:%d failed to post semaphore", TD_VID(fs->tsdb->pVnode));
  }
  return code;
}

int32_t tsdbFSEditAbort(STFileSystem *fs) {
  int32_t code = abort_edit(fs);
  if (tsem_post(&fs->canEdit) != 0) {
    tsdbError("vgId:%d failed to post semaphore", TD_VID(fs->tsdb->pVnode));
  }
  return code;
}

void tsdbFSGetFSet(STFileSystem *fs, int32_t fid, STFileSet **fset) {
  STFileSet   tfset = {.fid = fid};
  STFileSet  *pset = &tfset;
  STFileSet **fsetPtr = TARRAY2_SEARCH(fs->fSetArr, &pset, tsdbTFileSetCmprFn, TD_EQ);
  fset[0] = (fsetPtr == NULL) ? NULL : fsetPtr[0];
}

int32_t tsdbFSCreateCopySnapshot(STFileSystem *fs, TFileSetArray **fsetArr) {
  int32_t    code = 0;
  STFileSet *fset;
  STFileSet *fset1;

  fsetArr[0] = taosMemoryMalloc(sizeof(TFileSetArray));
  if (fsetArr[0] == NULL) return terrno;

  TARRAY2_INIT(fsetArr[0]);

  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    code = tsdbTFileSetInitCopy(fs->tsdb, fset, &fset1);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) break;
  }
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  return code;
}

void tsdbFSDestroyCopySnapshot(TFileSetArray **fsetArr) {
  if (fsetArr[0]) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
}

int32_t tsdbFSCreateRefSnapshot(STFileSystem *fs, TFileSetArray **fsetArr) {
  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  int32_t code = tsdbFSCreateRefSnapshotWithoutLock(fs, fsetArr);
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);
  return code;
}

int32_t tsdbFSCreateRefSnapshotWithoutLock(STFileSystem *fs, TFileSetArray **fsetArr) {
  int32_t    code = 0;
  STFileSet *fset, *fset1;

  fsetArr[0] = taosMemoryCalloc(1, sizeof(*fsetArr[0]));
  if (fsetArr[0] == NULL) return terrno;

  TARRAY2_FOREACH(fs->fSetArr, fset) {
    code = tsdbTFileSetInitRef(fs->tsdb, fset, &fset1);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) {
      tsdbTFileSetClear(&fset1);
      break;
    }
  }

  if (code) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  return code;
}

void tsdbFSDestroyRefSnapshot(TFileSetArray **fsetArr) {
  if (fsetArr[0]) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFreeClear(fsetArr[0]);
    fsetArr[0] = NULL;
  }
}

static SHashObj *tsdbFSetRangeArrayToHash(TFileSetRangeArray *pRanges) {
  int32_t   capacity = TARRAY2_SIZE(pRanges) * 2;
  SHashObj *pHash = taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (pHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < TARRAY2_SIZE(pRanges); i++) {
    STFileSetRange *u = TARRAY2_GET(pRanges, i);
    int32_t         fid = u->fid;
    int32_t         code = taosHashPut(pHash, &fid, sizeof(fid), u, sizeof(*u));
    tsdbDebug("range diff hash fid:%d, sver:%" PRId64 ", ever:%" PRId64, u->fid, u->sver, u->ever);
  }
  return pHash;
}

int32_t tsdbFSCreateCopyRangedSnapshot(STFileSystem *fs, TFileSetRangeArray *pRanges, TFileSetArray **fsetArr,
                                       TFileOpArray *fopArr) {
  int32_t    code = 0;
  STFileSet *fset;
  STFileSet *fset1;
  SHashObj  *pHash = NULL;

  fsetArr[0] = taosMemoryMalloc(sizeof(TFileSetArray));
  if (fsetArr == NULL) return terrno;
  TARRAY2_INIT(fsetArr[0]);

  if (pRanges) {
    pHash = tsdbFSetRangeArrayToHash(pRanges);
    if (pHash == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _out;
    }
  }

  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    int64_t ever = VERSION_MAX;
    if (pHash) {
      int32_t         fid = fset->fid;
      STFileSetRange *u = taosHashGet(pHash, &fid, sizeof(fid));
      if (u) {
        ever = u->sver - 1;
      }
    }

    code = tsdbTFileSetFilteredInitDup(fs->tsdb, fset, ever, &fset1, fopArr);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) break;
  }
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);

_out:
  if (code) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  if (pHash) {
    taosHashCleanup(pHash);
    pHash = NULL;
  }
  return code;
}

void tsdbFSDestroyCopyRangedSnapshot(TFileSetArray **fsetArr) { tsdbFSDestroyCopySnapshot(fsetArr); }

int32_t tsdbFSCreateRefRangedSnapshot(STFileSystem *fs, int64_t sver, int64_t ever, TFileSetRangeArray *pRanges,
                                      TFileSetRangeArray **fsrArr) {
  int32_t         code = 0;
  STFileSet      *fset;
  STFileSetRange *fsr1 = NULL;
  SHashObj       *pHash = NULL;

  fsrArr[0] = taosMemoryCalloc(1, sizeof(*fsrArr[0]));
  if (fsrArr[0] == NULL) {
    code = terrno;
    goto _out;
  }

  tsdbInfo("pRanges size:%d", (pRanges == NULL ? 0 : TARRAY2_SIZE(pRanges)));
  if (pRanges) {
    pHash = tsdbFSetRangeArrayToHash(pRanges);
    if (pHash == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _out;
    }
  }

  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    int64_t sver1 = sver;
    int64_t ever1 = ever;

    if (pHash) {
      int32_t         fid = fset->fid;
      STFileSetRange *u = taosHashGet(pHash, &fid, sizeof(fid));
      if (u) {
        sver1 = u->sver;
        tsdbDebug("range hash get fid:%d, sver:%" PRId64 ", ever:%" PRId64, u->fid, u->sver, u->ever);
      }
    }

    if (sver1 > ever1) {
      tsdbDebug("skip fid:%d, sver:%" PRId64 ", ever:%" PRId64, fset->fid, sver1, ever1);
      continue;
    }

    tsdbDebug("fsrArr:%p, fid:%d, sver:%" PRId64 ", ever:%" PRId64, fsrArr, fset->fid, sver1, ever1);

    code = tsdbTFileSetRangeInitRef(fs->tsdb, fset, sver1, ever1, &fsr1);
    if (code) break;

    code = TARRAY2_APPEND(fsrArr[0], fsr1);
    if (code) break;

    fsr1 = NULL;
  }
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    tsdbTFileSetRangeClear(&fsr1);
    TARRAY2_DESTROY(fsrArr[0], tsdbTFileSetRangeClear);
    fsrArr[0] = NULL;
  }

_out:
  if (pHash) {
    taosHashCleanup(pHash);
    pHash = NULL;
  }
  return code;
}

void tsdbFSDestroyRefRangedSnapshot(TFileSetRangeArray **fsrArr) { tsdbTFileSetRangeArrayDestroy(fsrArr); }

void tsdbBeginTaskOnFileSet(STsdb *tsdb, int32_t fid, EVATaskT task, STFileSet **fset) {
  // Here, sttTrigger is protected by tsdb->mutex, so it is safe to read it without lock
  int16_t sttTrigger = tsdb->pVnode->config.sttTrigger;

  tsdbFSGetFSet(tsdb->pFS, fid, fset);
  if (*fset == NULL) {
    return;
  }

  struct STFileSetCond *cond = NULL;
  if (sttTrigger == 1 || task == EVA_TASK_COMMIT) {
    cond = &(*fset)->conds[0];
  } else {
    cond = &(*fset)->conds[1];
  }

  while (1) {
    if (cond->running) {
      cond->numWait++;
      (void)taosThreadCondWait(&cond->cond, &tsdb->mutex);
      cond->numWait--;
    } else {
      cond->running = true;
      break;
    }
  }

  tsdbInfo("vgId:%d begin %s task on file set:%d", TD_VID(tsdb->pVnode), vnodeGetATaskName(task), fid);
  return;
}

void tsdbFinishTaskOnFileSet(STsdb *tsdb, int32_t fid, EVATaskT task) {
  // Here, sttTrigger is protected by tsdb->mutex, so it is safe to read it without lock
  int16_t sttTrigger = tsdb->pVnode->config.sttTrigger;

  STFileSet *fset = NULL;
  tsdbFSGetFSet(tsdb->pFS, fid, &fset);
  if (fset == NULL) {
    return;
  }

  struct STFileSetCond *cond = NULL;
  if (sttTrigger == 1 || task == EVA_TASK_COMMIT) {
    cond = &fset->conds[0];
  } else {
    cond = &fset->conds[1];
  }

  cond->running = false;
  if (cond->numWait > 0) {
    (void)taosThreadCondSignal(&cond->cond);
  }

  tsdbInfo("vgId:%d finish %s task on file set:%d", TD_VID(tsdb->pVnode), vnodeGetATaskName(task), fid);
  return;
}

struct SFileSetReader {
  STsdb     *pTsdb;
  STFileSet *pFileSet;
  int32_t    fid;
  int64_t    startTime;
  int64_t    endTime;
  int64_t    lastCompactTime;
  int64_t    totalSize;
};

int32_t tsdbFileSetReaderOpen(void *pVnode, struct SFileSetReader **ppReader) {
  if (pVnode == NULL || ppReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  STsdb *pTsdb = ((SVnode *)pVnode)->pTsdb;

  (*ppReader) = taosMemoryCalloc(1, sizeof(struct SFileSetReader));
  if (*ppReader == NULL) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(terrno));
    return terrno;
  }

  (*ppReader)->pTsdb = pTsdb;
  (*ppReader)->fid = INT32_MIN;
  (*ppReader)->pFileSet = NULL;

  return TSDB_CODE_SUCCESS;
}

extern bool tsdbShouldCompact(const STFileSet *pFileSet, int32_t vgId);

#ifndef TD_ENTERPRISE
bool tsdbShouldCompact(const STFileSet *pFileSet, int32_t vgId) { return false; }
#endif

static int32_t tsdbFileSetReaderNextNoLock(struct SFileSetReader *pReader) {
  STsdb  *pTsdb = pReader->pTsdb;
  int32_t code = TSDB_CODE_SUCCESS;

  tsdbTFileSetClear(&pReader->pFileSet);

  STFileSet *fset = &(STFileSet){
      .fid = pReader->fid,
  };

  STFileSet **fsetPtr = TARRAY2_SEARCH(pReader->pTsdb->pFS->fSetArr, &fset, tsdbTFileSetCmprFn, TD_GT);
  if (fsetPtr == NULL) {
    pReader->fid = INT32_MAX;
    return TSDB_CODE_NOT_FOUND;
  }

  // ref file set
  code = tsdbTFileSetInitRef(pReader->pTsdb, *fsetPtr, &pReader->pFileSet);
  if (code) return code;

  // get file set details
  pReader->fid = pReader->pFileSet->fid;
  tsdbFidKeyRange(pReader->fid, pTsdb->keepCfg.days, pTsdb->keepCfg.precision, &pReader->startTime, &pReader->endTime);
  pReader->lastCompactTime = pReader->pFileSet->lastCompact;
  pReader->totalSize = 0;
  for (int32_t i = 0; i < TSDB_FTYPE_MAX; i++) {
    STFileObj *fobj = pReader->pFileSet->farr[i];
    if (fobj) {
      pReader->totalSize += fobj->f->size;
    }
  }
  SSttLvl *lvl;
  TARRAY2_FOREACH(pReader->pFileSet->lvlArr, lvl) {
    STFileObj *fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) { pReader->totalSize += fobj->f->size; }
  }

  return code;
}

int32_t tsdbFileSetReaderNext(struct SFileSetReader *pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pReader->pTsdb->mutex);
  code = tsdbFileSetReaderNextNoLock(pReader);
  (void)taosThreadMutexUnlock(&pReader->pTsdb->mutex);
  return code;
}

int32_t tsdbFileSetGetEntryField(struct SFileSetReader *pReader, const char *field, void *value) {
  const char *fieldName;

  if (pReader->fid == INT32_MIN || pReader->fid == INT32_MAX) {
    return TSDB_CODE_INVALID_PARA;
  }

  fieldName = "fileset_id";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(int32_t *)value = pReader->fid;
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "start_time";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(int64_t *)value = pReader->startTime;
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "end_time";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(int64_t *)value = pReader->endTime;
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "total_size";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(int64_t *)value = pReader->totalSize;
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "last_compact_time";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(int64_t *)value = pReader->lastCompactTime;
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "should_compact";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    *(char *)value = tsdbShouldCompact(pReader->pFileSet, pReader->pTsdb->pVnode->config.vgId);
    return TSDB_CODE_SUCCESS;
  }

  fieldName = "details";
  if (strncmp(field, fieldName, strlen(fieldName) + 1) == 0) {
    // TODO
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_INVALID_PARA;
}

void tsdbFileSetReaderClose(struct SFileSetReader **ppReader) {
  if (ppReader == NULL || *ppReader == NULL) {
    return;
  }

  tsdbTFileSetClear(&(*ppReader)->pFileSet);
  taosMemoryFree(*ppReader);

  *ppReader = NULL;
  return;
}

static FORCE_INLINE void getLevelSize(const STFileObj *fObj, int64_t szArr[TFS_MAX_TIERS]) {
  if (fObj == NULL) return;

  int64_t sz = fObj->f->size;
  // level == 0, primary storage
  // level == 1, second storage,
  // level == 2, third storage
  int32_t level = fObj->f->did.level;
  if (level >= 0 && level < TFS_MAX_TIERS) {
    szArr[level] += sz;
  }
}

static FORCE_INLINE int32_t tsdbGetFsSizeImpl(STsdb *tsdb, SDbSizeStatisInfo *pInfo) {
  int32_t code = 0;
  int64_t levelSize[TFS_MAX_TIERS] = {0};
  int64_t s3Size = 0;

  const STFileSet *fset;
  const SSttLvl   *stt = NULL;
  const STFileObj *fObj = NULL;

  SVnodeCfg *pCfg = &tsdb->pVnode->config;
  int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;

  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    for (int32_t t = TSDB_FTYPE_MIN; t < TSDB_FTYPE_MAX; ++t) {
      getLevelSize(fset->farr[t], levelSize);
    }

    TARRAY2_FOREACH(fset->lvlArr, stt) {
      TARRAY2_FOREACH(stt->fobjArr, fObj) { getLevelSize(fObj, levelSize); }
    }

    fObj = fset->farr[TSDB_FTYPE_DATA];
    if (fObj) {
      int32_t lcn = fObj->f->lcn;
      if (lcn > 1) {
        s3Size += ((lcn - 1) * chunksize);
      }
    }
  }

  pInfo->l1Size = levelSize[0];
  pInfo->l2Size = levelSize[1];
  pInfo->l3Size = levelSize[2];
  pInfo->s3Size = s3Size;
  return code;
}
int32_t tsdbGetFsSize(STsdb *tsdb, SDbSizeStatisInfo *pInfo) {
  int32_t code = 0;

  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbGetFsSizeImpl(tsdb, pInfo);
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}
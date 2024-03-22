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

extern void remove_file(const char *fname, bool last_level);

#define TSDB_FS_EDIT_MIN TSDB_FEDIT_COMMIT
#define TSDB_FS_EDIT_MAX (TSDB_FEDIT_MERGE + 1)

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
  if (fs[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  fs[0]->tsdb = pTsdb;
  tsem_init(&fs[0]->canEdit, 0, 1);
  fs[0]->fsstate = TSDB_FS_STATE_NORMAL;
  fs[0]->neid = 0;
  TARRAY2_INIT(fs[0]->fSetArr);
  TARRAY2_INIT(fs[0]->fSetArrTmp);

  return 0;
}

static int32_t destroy_fs(STFileSystem **fs) {
  if (fs[0] == NULL) return 0;

  TARRAY2_DESTROY(fs[0]->fSetArr, NULL);
  TARRAY2_DESTROY(fs[0]->fSetArrTmp, NULL);
  tsem_destroy(&fs[0]->canEdit);
  taosMemoryFree(fs[0]);
  fs[0] = NULL;
  return 0;
}

int32_t current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype) {
  int32_t offset = 0;

  vnodeGetPrimaryDir(pTsdb->path, pTsdb->pVnode->diskPrimary, pTsdb->pVnode->pTfs, fname, TSDB_FILENAME_LEN);
  offset = strlen(fname);
  snprintf(fname + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, gCurrentFname[ftype]);

  return 0;
}

static int32_t save_json(const cJSON *json, const char *fname) {
  int32_t code = 0;

  char *data = cJSON_PrintUnformatted(json);
  if (data == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (fp == NULL) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  if (taosWriteFile(fp, data, strlen(data)) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  if (taosFsyncFile(fp) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  taosCloseFile(&fp);

_exit:
  taosMemoryFree(data);
  return code;
}

static int32_t load_json(const char *fname, cJSON **json) {
  int32_t code = 0;
  char   *data = NULL;

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_READ);
  if (fp == NULL) return TAOS_SYSTEM_ERROR(code);

  int64_t size;
  if (taosFStatFile(fp, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  data = taosMemoryMalloc(size + 1);
  if (data == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (taosReadFile(fp, data, size) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }
  data[size] = '\0';

  json[0] = cJSON_Parse(data);
  if (json[0] == NULL) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

_exit:
  taosCloseFile(&fp);
  if (data) taosMemoryFree(data);
  if (code) json[0] = NULL;
  return code;
}

int32_t save_fs(const TFileSetArray *arr, const char *fname) {
  int32_t code = 0;
  int32_t lino = 0;

  cJSON *json = cJSON_CreateObject();
  if (!json) return TSDB_CODE_OUT_OF_MEMORY;

  // fmtv
  if (cJSON_AddNumberToObject(json, "fmtv", 1) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // fset
  cJSON *ajson = cJSON_AddArrayToObject(json, "fset");
  if (!ajson) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  const STFileSet *fset;
  TARRAY2_FOREACH(arr, fset) {
    cJSON *item = cJSON_CreateObject();
    if (!item) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    cJSON_AddItemToArray(ajson, item);

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
    ASSERT(item1->valuedouble == 1);
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
    tsdbError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  if (json) cJSON_Delete(json);
  return code;
}

static int32_t apply_commit(STFileSystem *fs) {
  int32_t        code = 0;
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
        if (code) return code;
        code = TARRAY2_SORT_INSERT(fsetArray1, fset1, tsdbTFileSetCmprFn);
        if (code) return code;
        i1++;
        i2++;
      } else {
        // edit
        code = tsdbTFileSetApplyEdit(fs->tsdb, fset2, fset1);
        if (code) return code;
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
      if (code) return code;
      code = TARRAY2_SORT_INSERT(fsetArray1, fset1, tsdbTFileSetCmprFn);
      if (code) return code;
      i1++;
      i2++;
    }
  }

  return 0;
}

static int32_t commit_edit(STFileSystem *fs) {
  char current[TSDB_FILENAME_LEN];
  char current_t[TSDB_FILENAME_LEN];

  current_fname(fs->tsdb, current, TSDB_FCURRENT);
  if (fs->etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_C);
  } else if (fs->etype == TSDB_FEDIT_MERGE) {
    current_fname(fs->tsdb, current_t, TSDB_FCURRENT_M);
  } else {
    ASSERT(0);
  }

  int32_t code;
  int32_t lino;
  if ((code = taosRenameFile(current_t, current))) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = apply_commit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(fs->tsdb->pVnode), __func__, lino, tstrerror(code));
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
  } else if (fs->etype == TSDB_FEDIT_MERGE) {
    current_fname(fs->tsdb, fname, TSDB_FCURRENT_M);
  } else {
    ASSERT(0);
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
    tsdbError("vgId:%d %s failed since %s", TD_VID(fs->tsdb->pVnode), __func__, tstrerror(code));
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
    if (tsS3Enabled) {
      const char *object_name = taosDirEntryBaseName((char *)fobj->fname);
      long        s3_size = s3Size(object_name);
      if (s3_size > 0) {
        return 0;
      }
    }

    code = TSDB_CODE_FILE_CORRUPTED;
    tsdbError("vgId:%d %s failed since file:%s does not exist", TD_VID(fs->tsdb->pVnode), __func__, fobj->fname);
    return code;
  }

  {  // TODO: check file size
     // int64_t fsize;
     // if (taosStatFile(fobj->fname, &fsize, NULL, NULL) < 0) {
     //   code = TAOS_SYSTEM_ERROR(terrno);
     //   tsdbError("vgId:%d %s failed since file:%s stat failed, reason:%s", TD_VID(fs->tsdb->pVnode), __func__,
     //             fobj->fname, tstrerror(code));
     //   return code;
     // }
  }

  return 0;
}

static void tsdbFSDestroyFileObjHash(STFileHash *hash);

static int32_t tsdbFSAddEntryToFileObjHash(STFileHash *hash, const char *fname) {
  STFileHashEntry *entry = taosMemoryMalloc(sizeof(*entry));
  if (entry == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  strncpy(entry->fname, fname, TSDB_FILENAME_LEN);

  uint32_t idx = MurmurHash3_32(fname, strlen(fname)) % hash->numBucket;

  entry->next = hash->buckets[idx];
  hash->buckets[idx] = entry;
  hash->numFile++;

  return 0;
}

static int32_t tsdbFSCreateFileObjHash(STFileSystem *fs, STFileHash *hash) {
  int32_t code = 0;
  char    fname[TSDB_FILENAME_LEN];

  // init hash table
  hash->numFile = 0;
  hash->numBucket = 4096;
  hash->buckets = taosMemoryCalloc(hash->numBucket, sizeof(STFileHashEntry *));
  if (hash->buckets == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  // vnode.json
  current_fname(fs->tsdb, fname, TSDB_FCURRENT);
  code = tsdbFSAddEntryToFileObjHash(hash, fname);
  if (code) goto _exit;

  // other
  STFileSet *fset = NULL;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    // data file
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; i++) {
      if (fset->farr[i] != NULL) {
        code = tsdbFSAddEntryToFileObjHash(hash, fset->farr[i]->fname);
        if (code) goto _exit;
      }
    }

    // stt file
    SSttLvl *lvl = NULL;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      STFileObj *fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        code = tsdbFSAddEntryToFileObjHash(hash, fobj->fname);
        if (code) goto _exit;
      }
    }
  }

_exit:
  if (code) {
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
    STfsDir *dir = tfsOpendir(fs->tsdb->pVnode->pTfs, fs->tsdb->path);
    if (dir == NULL) {
      code = TAOS_SYSTEM_ERROR(terrno);
      lino = __LINE__;
      goto _exit;
    }

    STFileHash fobjHash = {0};
    code = tsdbFSCreateFileObjHash(fs, &fobjHash);
    if (code) goto _close_dir;

    for (const STfsFile *file = NULL; (file = tfsReaddir(dir)) != NULL;) {
      if (taosIsDir(file->aname)) continue;

      if (tsdbFSGetFileObjHashEntry(&fobjHash, file->aname) == NULL &&
          strncmp(file->aname + strlen(file->aname) - 3, ".cp", 3)) {
        int32_t nlevel = tfsGetLevel(fs->tsdb->pVnode->pTfs);
        remove_file(file->aname, nlevel > 1 && file->did.level == nlevel - 1);
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

static int32_t close_file_system(STFileSystem *fs) {
  TARRAY2_CLEAR(fs->fSetArr, tsdbTFileSetClear);
  TARRAY2_CLEAR(fs->fSetArrTmp, tsdbTFileSetClear);
  return 0;
}

static int32_t fset_cmpr_fn(const struct STFileSet *pSet1, const struct STFileSet *pSet2) {
  if (pSet1->fid < pSet2->fid) {
    return -1;
  } else if (pSet1->fid > pSet2->fid) {
    return 1;
  }
  return 0;
}

static int32_t edit_fs(STFileSystem *fs, const TFileOpArray *opArray) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSDupState(fs);
  if (code) return code;

  TFileSetArray  *fsetArray = fs->fSetArrTmp;
  STFileSet      *fset = NULL;
  const STFileOp *op;
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

static int32_t tsdbFSSetBlockCommit(STFileSet *fset, bool block);
extern int32_t tsdbStopAllCompTask(STsdb *tsdb);

int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb) {
  STFileSystem *fs = pTsdb->pFS;
  TARRAY2(int64_t) channelArr = {0};

  taosThreadMutexLock(&fs->tsdb->mutex);

  // disable
  pTsdb->bgTaskDisabled = true;

  // collect channel
  STFileSet *fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    if (VNODE_ASYNC_VALID_CHANNEL_ID(fset->bgTaskChannel)) {
      TARRAY2_APPEND(&channelArr, fset->bgTaskChannel);
      fset->bgTaskChannel = 0;
    }
    fset->mergeScheduled = false;
    tsdbFSSetBlockCommit(fset, false);
  }

  taosThreadMutexUnlock(&fs->tsdb->mutex);

  // destroy all channels
  int64_t channel;
  TARRAY2_FOREACH(&channelArr, channel) { vnodeAChannelDestroy(vnodeAsyncHandle[1], channel, true); }
  TARRAY2_DESTROY(&channelArr, NULL);

#ifdef TD_ENTERPRISE
  tsdbStopAllCompTask(pTsdb);
#endif
  return 0;
}

int32_t tsdbEnableBgTask(STsdb *pTsdb) {
  taosThreadMutexLock(&pTsdb->mutex);
  pTsdb->bgTaskDisabled = false;
  taosThreadMutexUnlock(&pTsdb->mutex);
  return 0;
}

int32_t tsdbCloseFS(STFileSystem **fs) {
  if (fs[0] == NULL) return 0;

  tsdbDisableAndCancelAllBgTask((*fs)->tsdb);
  close_file_system(fs[0]);
  destroy_fs(fs);
  return 0;
}

int64_t tsdbFSAllocEid(STFileSystem *fs) {
  taosThreadMutexLock(&fs->tsdb->mutex);
  int64_t cid = ++fs->neid;
  taosThreadMutexUnlock(&fs->tsdb->mutex);
  return cid;
}

void tsdbFSUpdateEid(STFileSystem *fs, int64_t cid) {
  taosThreadMutexLock(&fs->tsdb->mutex);
  fs->neid = TMAX(fs->neid, cid);
  taosThreadMutexUnlock(&fs->tsdb->mutex);
}

int32_t tsdbFSEditBegin(STFileSystem *fs, const TFileOpArray *opArray, EFEditT etype) {
  int32_t code = 0;
  int32_t lino;
  char    current_t[TSDB_FILENAME_LEN];

  switch (etype) {
    case TSDB_FEDIT_COMMIT:
      current_fname(fs->tsdb, current_t, TSDB_FCURRENT_C);
      break;
    case TSDB_FEDIT_MERGE:
      current_fname(fs->tsdb, current_t, TSDB_FCURRENT_M);
      break;
    default:
      ASSERT(0);
  }

  tsem_wait(&fs->canEdit);
  fs->etype = etype;

  // edit
  code = edit_fs(fs, opArray);
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

static int32_t tsdbFSSetBlockCommit(STFileSet *fset, bool block) {
  if (block) {
    fset->blockCommit = true;
  } else {
    fset->blockCommit = false;
    if (fset->numWaitCommit > 0) {
      taosThreadCondSignal(&fset->canCommit);
    }
  }
  return 0;
}

int32_t tsdbFSCheckCommit(STsdb *tsdb, int32_t fid) {
  taosThreadMutexLock(&tsdb->mutex);
  STFileSet *fset;
  tsdbFSGetFSet(tsdb->pFS, fid, &fset);
  if (fset) {
    while (fset->blockCommit) {
      fset->numWaitCommit++;
      taosThreadCondWait(&fset->canCommit, &tsdb->mutex);
      fset->numWaitCommit--;
    }
  }
  taosThreadMutexUnlock(&tsdb->mutex);
  return 0;
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

      bool    skipMerge = false;
      int32_t numFile = TARRAY2_SIZE(lvl->fobjArr);
      if (numFile >= sttTrigger && (!fset->mergeScheduled)) {
        // launch merge
        {
          extern int8_t  tsS3Enabled;
          extern int32_t tsS3UploadDelaySec;
          long           s3Size(const char *object_name);
          int32_t        nlevel = tfsGetLevel(fs->tsdb->pVnode->pTfs);
          if (tsS3Enabled && nlevel > 1) {
            STFileObj *fobj = fset->farr[TSDB_FTYPE_DATA];
            if (fobj && fobj->f->did.level == nlevel - 1) {
              // if exists on s3 or local mtime < committer->ctx->now - tsS3UploadDelay
              const char *object_name = taosDirEntryBaseName((char *)fobj->fname);

              if (taosCheckExistFile(fobj->fname)) {
                int32_t now = taosGetTimestampSec();
                int32_t mtime = 0;
                taosStatFile(fobj->fname, NULL, &mtime, NULL);
                if (mtime < now - tsS3UploadDelaySec) {
                  skipMerge = true;
                }
              } else /* if (s3Size(object_name) > 0) */ {
                skipMerge = true;
              }
            }
            // new fset can be written with ts data
          }
        }

        if (!skipMerge) {
          code = tsdbTFileSetOpenChannel(fset);
          TSDB_CHECK_CODE(code, lino, _exit);

          SMergeArg *arg = taosMemoryMalloc(sizeof(*arg));
          if (arg == NULL) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            TSDB_CHECK_CODE(code, lino, _exit);
          }

          arg->tsdb = fs->tsdb;
          arg->fid = fset->fid;

          code = vnodeAsyncC(vnodeAsyncHandle[1], fset->bgTaskChannel, EVA_PRIORITY_HIGH, tsdbMerge, taosMemoryFree,
                             arg, NULL);
          TSDB_CHECK_CODE(code, lino, _exit);
          fset->mergeScheduled = true;
        }
      }

      if (numFile >= sttTrigger * BLOCK_COMMIT_FACTOR && !skipMerge) {
        tsdbFSSetBlockCommit(fset, true);
      } else {
        tsdbFSSetBlockCommit(fset, false);
      }
    }
  }

  // clear empty level and fset
  int32_t i = 0;
  while (i < TARRAY2_SIZE(fs->fSetArr)) {
    STFileSet *fset = TARRAY2_GET(fs->fSetArr, i);

    int32_t j = 0;
    while (j < TARRAY2_SIZE(fset->lvlArr)) {
      SSttLvl *lvl = TARRAY2_GET(fset->lvlArr, j);

      if (TARRAY2_SIZE(lvl->fobjArr) == 0) {
        TARRAY2_REMOVE(fset->lvlArr, j, tsdbSttLvlClear);
      } else {
        j++;
      }
    }

    if (tsdbTFileSetIsEmpty(fset)) {
      if (VNODE_ASYNC_VALID_CHANNEL_ID(fset->bgTaskChannel)) {
        vnodeAChannelDestroy(vnodeAsyncHandle[1], fset->bgTaskChannel, false);
        fset->bgTaskChannel = 0;
      }
      TARRAY2_REMOVE(fs->fSetArr, i, tsdbTFileSetClear);
    } else {
      i++;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(fs->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, etype:%d", TD_VID(fs->tsdb->pVnode), __func__, fs->etype);
    tsem_post(&fs->canEdit);
  }
  return code;
}

int32_t tsdbFSEditAbort(STFileSystem *fs) {
  int32_t code = abort_edit(fs);
  tsem_post(&fs->canEdit);
  return code;
}

int32_t tsdbFSGetFSet(STFileSystem *fs, int32_t fid, STFileSet **fset) {
  STFileSet   tfset = {.fid = fid};
  STFileSet  *pset = &tfset;
  STFileSet **fsetPtr = TARRAY2_SEARCH(fs->fSetArr, &pset, tsdbTFileSetCmprFn, TD_EQ);
  fset[0] = (fsetPtr == NULL) ? NULL : fsetPtr[0];
  return 0;
}

int32_t tsdbFSCreateCopySnapshot(STFileSystem *fs, TFileSetArray **fsetArr) {
  int32_t    code = 0;
  STFileSet *fset;
  STFileSet *fset1;

  fsetArr[0] = taosMemoryMalloc(sizeof(TFileSetArray));
  if (fsetArr[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TARRAY2_INIT(fsetArr[0]);

  taosThreadMutexLock(&fs->tsdb->mutex);
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    code = tsdbTFileSetInitCopy(fs->tsdb, fset, &fset1);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) break;
  }
  taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  return code;
}

int32_t tsdbFSDestroyCopySnapshot(TFileSetArray **fsetArr) {
  if (fsetArr[0]) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFree(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  return 0;
}

int32_t tsdbFSCreateRefSnapshot(STFileSystem *fs, TFileSetArray **fsetArr) {
  taosThreadMutexLock(&fs->tsdb->mutex);
  int32_t code = tsdbFSCreateRefSnapshotWithoutLock(fs, fsetArr);
  taosThreadMutexUnlock(&fs->tsdb->mutex);
  return code;
}

int32_t tsdbFSCreateRefSnapshotWithoutLock(STFileSystem *fs, TFileSetArray **fsetArr) {
  int32_t    code = 0;
  STFileSet *fset, *fset1;

  fsetArr[0] = taosMemoryCalloc(1, sizeof(*fsetArr[0]));
  if (fsetArr[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TARRAY2_FOREACH(fs->fSetArr, fset) {
    code = tsdbTFileSetInitRef(fs->tsdb, fset, &fset1);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) break;
  }

  if (code) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    fsetArr[0] = NULL;
  }
  return code;
}

int32_t tsdbFSDestroyRefSnapshot(TFileSetArray **fsetArr) {
  if (fsetArr[0]) {
    TARRAY2_DESTROY(fsetArr[0], tsdbTFileSetClear);
    taosMemoryFreeClear(fsetArr[0]);
    fsetArr[0] = NULL;
  }
  return 0;
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
    ASSERT(code == 0);
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
  if (fsetArr == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  TARRAY2_INIT(fsetArr[0]);

  if (pRanges) {
    pHash = tsdbFSetRangeArrayToHash(pRanges);
    if (pHash == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _out;
    }
  }

  taosThreadMutexLock(&fs->tsdb->mutex);
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
  taosThreadMutexUnlock(&fs->tsdb->mutex);

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

int32_t tsdbFSDestroyCopyRangedSnapshot(TFileSetArray **fsetArr) { return tsdbFSDestroyCopySnapshot(fsetArr); }

int32_t tsdbFSCreateRefRangedSnapshot(STFileSystem *fs, int64_t sver, int64_t ever, TFileSetRangeArray *pRanges,
                                      TFileSetRangeArray **fsrArr) {
  int32_t         code = 0;
  STFileSet      *fset;
  STFileSetRange *fsr1 = NULL;
  SHashObj       *pHash = NULL;

  fsrArr[0] = taosMemoryCalloc(1, sizeof(*fsrArr[0]));
  if (fsrArr[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
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

  taosThreadMutexLock(&fs->tsdb->mutex);
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
  taosThreadMutexUnlock(&fs->tsdb->mutex);

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

int32_t tsdbFSDestroyRefRangedSnapshot(TFileSetRangeArray **fsrArr) { return tsdbTFileSetRangeArrayDestroy(fsrArr); }

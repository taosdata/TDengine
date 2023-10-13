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
#include "tsdbUpgrade.h"
#include "vnd.h"
#include "vndCos.h"

#define BLOCK_COMMIT_FACTOR 3

extern int  vnodeScheduleTask(int (*execute)(void *), void *arg);
extern int  vnodeScheduleTaskEx(int tpid, int (*execute)(void *), void *arg);
extern void remove_file(const char *fname);

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

enum {
  TSDB_FS_STATE_NONE = 0,
  TSDB_FS_STATE_OPEN,
  TSDB_FS_STATE_EDIT,
  TSDB_FS_STATE_CLOSE,
};

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
  fs[0]->state = TSDB_FS_STATE_NONE;
  fs[0]->neid = 0;
  TARRAY2_INIT(fs[0]->fSetArr);
  TARRAY2_INIT(fs[0]->fSetArrTmp);

  // background task queue
  taosThreadMutexInit(fs[0]->mutex, NULL);
  fs[0]->bgTaskQueue->next = fs[0]->bgTaskQueue;
  fs[0]->bgTaskQueue->prev = fs[0]->bgTaskQueue;

  taosThreadMutexInit(&fs[0]->commitMutex, NULL);
  taosThreadCondInit(&fs[0]->canCommit, NULL);
  fs[0]->blockCommit = false;

  return 0;
}

static int32_t destroy_fs(STFileSystem **fs) {
  if (fs[0] == NULL) return 0;
  taosThreadMutexDestroy(&fs[0]->commitMutex);
  taosThreadCondDestroy(&fs[0]->canCommit);
  taosThreadMutexDestroy(fs[0]->mutex);

  ASSERT(fs[0]->bgTaskNum == 0);

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

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
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

static bool is_same_file(const STFile *f1, const STFile f2) {
  if (f1->type != f2.type) return false;
  if (f1->did.level != f2.did.level) return false;
  if (f1->did.id != f2.did.id) return false;
  if (f1->cid != f2.cid) return false;
  return true;
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
        TARRAY2_REMOVE(fsetArray1, i1, tsdbTFileSetRemove);
      } else if (fset1->fid > fset2->fid) {
        // create new file set with fid of fset2->fid
        code = tsdbTFileSetInitDup(fs->tsdb, fset2, &fset1);
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
      TARRAY2_REMOVE(fsetArray1, i1, tsdbTFileSetRemove);
    } else {
      // create new file set with fid of fset2->fid
      code = tsdbTFileSetInitDup(fs->tsdb, fset2, &fset1);
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

  {  // scan each file
    STFileSet *fset = NULL;
    TARRAY2_FOREACH(fs->fSetArr, fset) {
      // data file
      for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
        if (fset->farr[ftype] == NULL) continue;
        code = tsdbFSDoScanAndFixFile(fs, fset->farr[ftype]);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // stt file
      SSttLvl *lvl;
      TARRAY2_FOREACH(fset->lvlArr, lvl) {
        STFileObj *fobj;
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          code = tsdbFSDoScanAndFixFile(fs, fobj);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
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

      if (tsdbFSGetFileObjHashEntry(&fobjHash, file->aname) == NULL) {
        remove_file(file->aname);
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
    code = tsdbTFileSetInitDup(fs->tsdb, fset1, &fset2);
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
  // TODO
  return 0;
}

static int32_t apply_edit(STFileSystem *pFS) {
  int32_t code = 0;
  ASSERTS(0, "TODO: Not implemented yet");
  return code;
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

  // remove empty file set
  int32_t i = 0;
  while (i < TARRAY2_SIZE(fsetArray)) {
    fset = TARRAY2_GET(fsetArray, i);
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

static void tsdbDoWaitBgTask(STFileSystem *fs, STFSBgTask *task) {
  task->numWait++;
  taosThreadCondWait(task->done, fs->mutex);
  task->numWait--;

  if (task->numWait == 0) {
    taosThreadCondDestroy(task->done);
    if (task->free) {
      task->free(task->arg);
    }
    taosMemoryFree(task);
  }
}

static void tsdbDoDoneBgTask(STFileSystem *fs, STFSBgTask *task) {
  if (task->numWait > 0) {
    taosThreadCondBroadcast(task->done);
  } else {
    taosThreadCondDestroy(task->done);
    if (task->free) {
      task->free(task->arg);
    }
    taosMemoryFree(task);
  }
}

int32_t tsdbCloseFS(STFileSystem **fs) {
  if (fs[0] == NULL) return 0;

  taosThreadMutexLock(fs[0]->mutex);
  fs[0]->stop = true;

  if (fs[0]->bgTaskRunning) {
    tsdbDoWaitBgTask(fs[0], fs[0]->bgTaskRunning);
  }
  taosThreadMutexUnlock(fs[0]->mutex);

  close_file_system(fs[0]);
  destroy_fs(fs);
  return 0;
}

int64_t tsdbFSAllocEid(STFileSystem *fs) {
  taosThreadRwlockWrlock(&fs->tsdb->rwLock);
  int64_t cid = ++fs->neid;
  taosThreadRwlockUnlock(&fs->tsdb->rwLock);
  return cid;
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

static int32_t tsdbFSSetBlockCommit(STFileSystem *fs, bool block) {
  taosThreadMutexLock(&fs->commitMutex);
  if (block) {
    fs->blockCommit = true;
  } else {
    fs->blockCommit = false;
    taosThreadCondSignal(&fs->canCommit);
  }
  taosThreadMutexUnlock(&fs->commitMutex);
  return 0;
}

int32_t tsdbFSCheckCommit(STFileSystem *fs) {
  taosThreadMutexLock(&fs->commitMutex);
  while (fs->blockCommit) {
    taosThreadCondWait(&fs->canCommit, &fs->commitMutex);
  }
  taosThreadMutexUnlock(&fs->commitMutex);
  return 0;
}

int32_t tsdbFSEditCommit(STFileSystem *fs) {
  int32_t code = 0;
  int32_t lino = 0;

  // commit
  code = commit_edit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // schedule merge
  if (fs->tsdb->pVnode->config.sttTrigger > 1) {
    STFileSet *fset;
    int32_t    sttTrigger = fs->tsdb->pVnode->config.sttTrigger;
    bool       schedMerge = false;
    bool       blockCommit = false;

    TARRAY2_FOREACH_REVERSE(fs->fSetArr, fset) {
      if (TARRAY2_SIZE(fset->lvlArr) == 0) continue;

      SSttLvl *lvl = TARRAY2_FIRST(fset->lvlArr);
      if (lvl->level != 0) continue;

      int32_t numFile = TARRAY2_SIZE(lvl->fobjArr);
      if (numFile >= sttTrigger) {
        schedMerge = true;
      }

      if (numFile >= sttTrigger * BLOCK_COMMIT_FACTOR) {
        blockCommit = true;
      }

      if (schedMerge && blockCommit) break;
    }

    if (schedMerge) {
      code = tsdbFSScheduleBgTask(fs, TSDB_BG_TASK_MERGER, tsdbMerge, NULL, fs->tsdb, NULL);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    tsdbFSSetBlockCommit(fs, blockCommit);
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

  taosThreadRwlockRdlock(&fs->tsdb->rwLock);
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    code = tsdbTFileSetInitDup(fs->tsdb, fset, &fset1);
    if (code) break;

    code = TARRAY2_APPEND(fsetArr[0], fset1);
    if (code) break;
  }
  taosThreadRwlockUnlock(&fs->tsdb->rwLock);

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

const char *gFSBgTaskName[] = {NULL, "MERGE", "RETENTION", "COMPACT"};

static int32_t tsdbFSRunBgTask(void *arg) {
  STFileSystem *fs = (STFileSystem *)arg;

  ASSERT(fs->bgTaskRunning != NULL);

  fs->bgTaskRunning->launchTime = taosGetTimestampMs();
  fs->bgTaskRunning->run(fs->bgTaskRunning->arg);
  fs->bgTaskRunning->finishTime = taosGetTimestampMs();

  tsdbDebug("vgId:%d bg task:%s task id:%" PRId64 " finished, schedule time:%" PRId64 " launch time:%" PRId64
            " finish time:%" PRId64,
            TD_VID(fs->tsdb->pVnode), gFSBgTaskName[fs->bgTaskRunning->type], fs->bgTaskRunning->taskid,
            fs->bgTaskRunning->scheduleTime, fs->bgTaskRunning->launchTime, fs->bgTaskRunning->finishTime);

  taosThreadMutexLock(fs->mutex);

  // free last
  tsdbDoDoneBgTask(fs, fs->bgTaskRunning);
  fs->bgTaskRunning = NULL;

  // schedule next
  if (fs->bgTaskNum > 0) {
    if (fs->stop) {
      while (fs->bgTaskNum > 0) {
        STFSBgTask *task = fs->bgTaskQueue->next;
        task->prev->next = task->next;
        task->next->prev = task->prev;
        fs->bgTaskNum--;
        tsdbDoDoneBgTask(fs, task);
      }
    } else {
      // pop task from head
      fs->bgTaskRunning = fs->bgTaskQueue->next;
      fs->bgTaskRunning->prev->next = fs->bgTaskRunning->next;
      fs->bgTaskRunning->next->prev = fs->bgTaskRunning->prev;
      fs->bgTaskNum--;
      vnodeScheduleTaskEx(1, tsdbFSRunBgTask, arg);
    }
  }

  taosThreadMutexUnlock(fs->mutex);
  return 0;
}

static int32_t tsdbFSScheduleBgTaskImpl(STFileSystem *fs, EFSBgTaskT   type, int32_t (*run)(void *),
                                        void (*destroy)(void *), void *arg, int64_t *taskid) {
  if (fs->stop) {
    if (destroy) {
      destroy(arg);
    }
    return 0;  // TODO: use a better error code
  }

  for (STFSBgTask *task = fs->bgTaskQueue->next; task != fs->bgTaskQueue; task = task->next) {
    if (task->type == type) {
      if (destroy) {
        destroy(arg);
      }
      return 0;
    }
  }

  // do schedule task
  STFSBgTask *task = taosMemoryCalloc(1, sizeof(STFSBgTask));
  if (task == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  taosThreadCondInit(task->done, NULL);

  task->type = type;
  task->run = run;
  task->free = destroy;
  task->arg = arg;
  task->scheduleTime = taosGetTimestampMs();
  task->taskid = ++fs->taskid;

  if (fs->bgTaskRunning == NULL && fs->bgTaskNum == 0) {
    // launch task directly
    fs->bgTaskRunning = task;
    vnodeScheduleTaskEx(1, tsdbFSRunBgTask, fs);
  } else {
    // add to the queue tail
    fs->bgTaskNum++;
    task->next = fs->bgTaskQueue;
    task->prev = fs->bgTaskQueue->prev;
    task->prev->next = task;
    task->next->prev = task;
  }

  if (taskid) *taskid = task->taskid;
  return 0;
}

int32_t tsdbFSScheduleBgTask(STFileSystem *fs, EFSBgTaskT type, int32_t (*run)(void *), void (*free)(void *), void *arg,
                             int64_t *taskid) {
  taosThreadMutexLock(fs->mutex);
  int32_t code = tsdbFSScheduleBgTaskImpl(fs, type, run, free, arg, taskid);
  taosThreadMutexUnlock(fs->mutex);
  return code;
}

int32_t tsdbFSWaitBgTask(STFileSystem *fs, int64_t taskid) {
  STFSBgTask *task = NULL;

  taosThreadMutexLock(fs->mutex);

  if (fs->bgTaskRunning && fs->bgTaskRunning->taskid == taskid) {
    task = fs->bgTaskRunning;
  } else {
    for (STFSBgTask *taskt = fs->bgTaskQueue->next; taskt != fs->bgTaskQueue; taskt = taskt->next) {
      if (taskt->taskid == taskid) {
        task = taskt;
        break;
      }
    }
  }

  if (task) {
    tsdbDoWaitBgTask(fs, task);
  }

  taosThreadMutexUnlock(fs->mutex);
  return 0;
}

int32_t tsdbFSWaitAllBgTask(STFileSystem *fs) {
  taosThreadMutexLock(fs->mutex);

  while (fs->bgTaskRunning) {
    taosThreadCondWait(fs->bgTaskRunning->done, fs->mutex);
  }

  taosThreadMutexUnlock(fs->mutex);
  return 0;
}

static int32_t tsdbFSDoDisableBgTask(STFileSystem *fs) {
  fs->stop = true;

  if (fs->bgTaskRunning) {
    tsdbDoWaitBgTask(fs, fs->bgTaskRunning);
  }
  return 0;
}

int32_t tsdbFSDisableBgTask(STFileSystem *fs) {
  taosThreadMutexLock(fs->mutex);
  int32_t code = tsdbFSDoDisableBgTask(fs);
  taosThreadMutexUnlock(fs->mutex);
  return code;
}

int32_t tsdbFSEnableBgTask(STFileSystem *fs) {
  taosThreadMutexLock(fs->mutex);
  fs->stop = false;
  taosThreadMutexUnlock(fs->mutex);
  return 0;
}
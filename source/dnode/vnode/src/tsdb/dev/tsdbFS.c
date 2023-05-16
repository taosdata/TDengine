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

#include "inc/tsdbFS.h"

#define TSDB_FS_EDIT_MIN TSDB_FEDIT_COMMIT
#define TSDB_FS_EDIT_MAX (TSDB_FEDIT_MERGE + 1)

enum {
  TSDB_FS_STATE_NONE = 0,
  TSDB_FS_STATE_OPEN,
  TSDB_FS_STATE_EDIT,
  TSDB_FS_STATE_CLOSE,
};

typedef enum {
  TSDB_FCURRENT = 1,
  TSDB_FCURRENT_C,  // for commit
  TSDB_FCURRENT_M,  // for merge
} EFCurrentT;

static const char *gCurrentFname[] = {
    [TSDB_FCURRENT] = "current.json",
    [TSDB_FCURRENT_C] = "current.c.json",
    [TSDB_FCURRENT_M] = "current.m.json",
};

static int32_t create_fs(STsdb *pTsdb, STFileSystem **fs) {
  fs[0] = taosMemoryCalloc(1, sizeof(*fs[0]));
  if (fs[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  fs[0]->cstate = taosArrayInit(16, sizeof(STFileSet));
  fs[0]->nstate = taosArrayInit(16, sizeof(STFileSet));
  if (fs[0]->cstate == NULL || fs[0]->nstate == NULL) {
    taosArrayDestroy(fs[0]->nstate);
    taosArrayDestroy(fs[0]->cstate);
    taosMemoryFree(fs[0]);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  fs[0]->pTsdb = pTsdb;
  fs[0]->state = TSDB_FS_STATE_NONE;
  tsem_init(&fs[0]->canEdit, 0, 1);
  fs[0]->neid = 0;

  return 0;
}

static int32_t destroy_fs(STFileSystem **fs) {
  if (fs[0] == NULL) return 0;
  taosArrayDestroy(fs[0]->nstate);
  taosArrayDestroy(fs[0]->cstate);
  tsem_destroy(&fs[0]->canEdit);
  taosMemoryFree(fs[0]);
  fs[0] = NULL;
  return 0;
}

static int32_t current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype) {
  if (pTsdb->pVnode->pTfs) {
    snprintf(fname,                                   //
             TSDB_FILENAME_LEN,                       //
             "%s%s%s%s%s",                            //
             tfsGetPrimaryPath(pTsdb->pVnode->pTfs),  //
             TD_DIRSEP,                               //
             pTsdb->path,                             //
             TD_DIRSEP,                               //
             gCurrentFname[ftype]);
  } else {
    snprintf(fname,              //
             TSDB_FILENAME_LEN,  //
             "%s%s%s",           //
             pTsdb->path,        //
             TD_DIRSEP,          //
             gCurrentFname[ftype]);
  }
  return 0;
}

static int32_t fs_from_json_str(const char *pData, STFileSystem *pFS) {
  int32_t code = 0;
  int32_t lino;

  ASSERTS(0, "TODO: Not implemented yet");

_exit:
  return code;
}

static int32_t save_json(const cJSON *json, const char *fname) {
  int32_t code = 0;

  char *data = cJSON_Print(json);
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

static int32_t save_fs(SArray *aTFileSet, const char *fname) {
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
  if (!ajson) TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  for (int32_t i = 0; i < taosArrayGetSize(aTFileSet); i++) {
    STFileSet *pFileSet = (STFileSet *)taosArrayGet(aTFileSet, i);
    cJSON     *item;

    item = cJSON_CreateObject();
    if (!item) TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
    cJSON_AddItemToArray(ajson, item);

    code = tsdbFileSetToJson(pFileSet, item);
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

static int32_t load_fs(const char *fname, SArray *aTFileSet) {
  int32_t code = 0;
  int32_t lino = 0;

  taosArrayClear(aTFileSet);

  // load json
  cJSON *json = NULL;
  code = load_json(fname, &json);
  TSDB_CHECK_CODE(code, lino, _exit);

  // parse json
  const cJSON *item;

  /* fmtv */
  item = cJSON_GetObjectItem(json, "fmtv");
  if (cJSON_IsNumber(item)) {
    ASSERT(item->valuedouble == 1);
  } else {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  /* fset */
  item = cJSON_GetObjectItem(json, "fset");
  if (cJSON_IsArray(item)) {
    const cJSON *titem;
    cJSON_ArrayForEach(titem, item) {
      STFileSet *fset = taosArrayReserve(aTFileSet, 1);
      if (!fset) TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);

      code = tsdbJsonToFileSet(titem, fset);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  if (json) cJSON_Delete(json);
  return code;
}

static int32_t apply_commit(STFileSystem *fs) {
  // TODO
  return 0;
}

static int32_t commit_edit(STFileSystem *fs) {
  char current[TSDB_FILENAME_LEN];
  char current_t[TSDB_FILENAME_LEN];

  current_fname(fs->pTsdb, current, TSDB_FCURRENT);
  if (fs->etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->pTsdb, current, TSDB_FCURRENT_C);
  } else if (fs->etype == TSDB_FEDIT_MERGE) {
    current_fname(fs->pTsdb, current, TSDB_FCURRENT_M);
  } else {
    ASSERT(0);
  }

  int32_t code;
  int32_t lino;
  if ((code = taosRenameFile(current_t, current))) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(code), lino, _exit);
  }

  code = apply_commit(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(fs->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success, eid:%" PRId64 " etype:%d", TD_VID(fs->pTsdb->pVnode), __func__, fs->eid, fs->etype);
  }
  return code;
}

// static int32_t
static int32_t apply_abort(STFileSystem *fs) {
  // TODO
  return 0;
}

static int32_t abort_edit(STFileSystem *fs) {
  char fname[TSDB_FILENAME_LEN];

  if (fs->etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->pTsdb, fname, TSDB_FCURRENT_C);
  } else if (fs->etype == TSDB_FEDIT_MERGE) {
    current_fname(fs->pTsdb, fname, TSDB_FCURRENT_M);
  } else {
    ASSERT(0);
  }

  int32_t code;
  int32_t lino;
  if ((code = taosRemoveFile(fname))) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(code), lino, _exit);
  }

  code = apply_abort(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed since %s", TD_VID(fs->pTsdb->pVnode), __func__, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success, eid:%" PRId64 " etype:%d", TD_VID(fs->pTsdb->pVnode), __func__, fs->eid, fs->etype);
  }
  return code;
}

static int32_t scan_and_fix_fs(STFileSystem *pFS) {
  // TODO
  return 0;
}

static int32_t update_fs_if_needed(STFileSystem *pFS) {
  // TODO
  return 0;
}

static int32_t open_fs(STFileSystem *fs, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = fs->pTsdb;

  code = update_fs_if_needed(fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  char fCurrent[TSDB_FILENAME_LEN];
  char cCurrent[TSDB_FILENAME_LEN];
  char mCurrent[TSDB_FILENAME_LEN];

  current_fname(pTsdb, fCurrent, TSDB_FCURRENT);
  current_fname(pTsdb, cCurrent, TSDB_FCURRENT_C);
  current_fname(pTsdb, mCurrent, TSDB_FCURRENT_M);

  if (taosCheckExistFile(fCurrent)) {  // current.json exists
    code = load_fs(fCurrent, fs->cstate);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(cCurrent)) {
      // current.c.json exists

      fs->etype = TSDB_FEDIT_COMMIT;
      if (rollback) {
        code = abort_edit(fs);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = load_fs(cCurrent, fs->nstate);
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

    code = scan_and_fix_fs(fs);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = save_fs(fs->cstate, fCurrent);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success", TD_VID(pTsdb->pVnode), __func__);
  }
  return 0;
}

static int32_t close_file_system(STFileSystem *pFS) {
  ASSERTS(0, "TODO: Not implemented yet");
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

static int32_t edit_fs(STFileSystem *pFS, const SArray *aFileOp) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet *pSet = NULL;
  for (int32_t iop = 0; iop < taosArrayGetSize(aFileOp); iop++) {
    struct STFileOp *op = taosArrayGet(aFileOp, iop);

    if (pSet == NULL || pSet->fid != op->fid) {
      STFileSet fset = {.fid = op->fid};
      pSet = taosArraySearch(pFS->nstate, &fset, (__compar_fn_t)tsdbFSetCmprFn, TD_EQ);
    }

    // create fset if need
    if (pSet == NULL) {
      ASSERT(op->oState.size == 0 && op->nState.size > 0);

      STFileSet fset = {.fid = op->fid};
      int32_t   idx = taosArraySearchIdx(pFS->nstate, &fset, (__compar_fn_t)tsdbFSetCmprFn, TD_GT);

      if (idx < 0) idx = taosArrayGetSize(pFS->nstate);

      pSet = taosArrayInsert(pFS->nstate, idx, &fset);
      if (pSet == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      tsdbFileSetInit(pSet);
    }

    code = tsdbFSetEdit(pSet, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  return 0;
}

int32_t tsdbOpenFS(STsdb *pTsdb, STFileSystem **fs, int8_t rollback) {
  int32_t code;
  int32_t lino;

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
  return 0;
}

int32_t tsdbCloseFS(STFileSystem **ppFS) {
  if (ppFS[0] == NULL) return 0;
  close_file_system(ppFS[0]);
  destroy_fs(ppFS);
  return 0;
}

int32_t tsdbFSAllocEid(STFileSystem *pFS, int64_t *eid) {
  eid[0] = ++pFS->neid;  // TODO: use atomic operation
  return 0;
}

int32_t tsdbFSEditBegin(STFileSystem *fs, int64_t eid, const SArray *aFileOp, EFEditT etype) {
  int32_t code = 0;
  int32_t lino;
  char    current_t[TSDB_FILENAME_LEN];

  if (etype == TSDB_FEDIT_COMMIT) {
    current_fname(fs->pTsdb, current_t, TSDB_FCURRENT_C);
  } else {
    current_fname(fs->pTsdb, current_t, TSDB_FCURRENT_M);
  }

  tsem_wait(&fs->canEdit);

  fs->etype = etype;
  fs->eid = eid;

  // edit
  code = edit_fs(fs, aFileOp);
  TSDB_CHECK_CODE(code, lino, _exit);

  // save fs
  code = save_fs(fs->nstate, current_t);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, eid:%" PRId64 " etype:%d", TD_VID(fs->pTsdb->pVnode), __func__,
              lino, tstrerror(code), fs->eid, etype);
  } else {
    tsdbInfo("vgId:%d %s done, eid:%" PRId64 " etype:%d", TD_VID(fs->pTsdb->pVnode), __func__, fs->eid, etype);
  }
  return code;
}

int32_t tsdbFSEditCommit(STFileSystem *fs) {
  int32_t code = commit_edit(fs);
  tsem_post(&fs->canEdit);
  return code;
}

int32_t tsdbFSEditAbort(STFileSystem *fs) {
  int32_t code = abort_edit(fs);
  tsem_post(&fs->canEdit);
  return code;
}

int32_t tsdbFSGetFSet(STFileSystem *fs, int32_t fid, const STFileSet **ppFSet) {
  STFileSet fset = {.fid = fid};
  ppFSet[0] = taosArraySearch(fs->cstate, &fset, (__compar_fn_t)tsdbFSetCmprFn, TD_EQ);
  return 0;
}
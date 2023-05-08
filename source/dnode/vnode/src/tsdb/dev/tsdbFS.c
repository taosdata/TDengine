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

#define TSDB_FS_EDIT_MIN TSDB_FS_EDIT_COMMIT
#define TSDB_FS_EDIT_MAX (TSDB_FS_EDIT_MERGE + 1)

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
    [TSDB_FCURRENT_C] = "current.json.0",
    [TSDB_FCURRENT_M] = "current.json.1",
};

static int32_t create_fs(STsdb *pTsdb, STFileSystem **ppFS) {
  ppFS[0] = taosMemoryCalloc(1, sizeof(*ppFS[0]));
  if (ppFS[0] == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ppFS[0]->cstate = taosArrayInit(16, sizeof(STFileSet));
  ppFS[0]->nstate = taosArrayInit(16, sizeof(STFileSet));
  if (ppFS[0]->cstate == NULL || ppFS[0]->nstate == NULL) {
    taosArrayDestroy(ppFS[0]->nstate);
    taosArrayDestroy(ppFS[0]->cstate);
    taosMemoryFree(ppFS[0]);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ppFS[0]->pTsdb = pTsdb;
  ppFS[0]->state = TSDB_FS_STATE_NONE;
  tsem_init(&ppFS[0]->canEdit, 0, 1);
  ppFS[0]->nextEditId = 0;

  return 0;
}

static int32_t destroy_fs(STFileSystem **ppFS) {
  if (ppFS[0] == NULL) return 0;
  taosArrayDestroy(ppFS[0]->nstate);
  taosArrayDestroy(ppFS[0]->cstate);
  tsem_destroy(&ppFS[0]->canEdit);
  taosMemoryFree(ppFS[0]);
  ppFS[0] = NULL;
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
  int32_t code;

  char *data = cJSON_Print(json);
  if (data == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fp == NULL) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  if (taosWriteFile(fp, data, strlen(data) + 1) < 0) {
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

static int32_t save_fs(int64_t eid, SArray *aTFileSet, const char *fname) {
  int32_t code = 0;
  int32_t lino = 0;

  cJSON *json = cJSON_CreateObject();
  if (json == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  // fmtv
  if (cJSON_AddNumberToObject(json, "fmtv", 1) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit)
  }

  // eid
  if (cJSON_AddNumberToObject(json, "eid", eid) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit)
  }

  // fset
  cJSON *ajson = cJSON_AddArrayToObject(json, "fset");
  if (ajson == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit)
  }
  for (int32_t i = 0; i < taosArrayGetSize(aTFileSet); i++) {
    STFileSet *pFileSet = (STFileSet *)taosArrayGet(aTFileSet, i);

    cJSON *tjson = cJSON_CreateObject();
    if (tjson == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit)
    }

    code = tsdbFileSetToJson(pFileSet, tjson);
    TSDB_CHECK_CODE(code, lino, _exit);

    cJSON_AddItemToArray(ajson, tjson);
  }

  code = save_json(json, fname);
  TSDB_CHECK_CODE(code, lino, _exit)

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  cJSON_Delete(json);
  return code;
}

static int32_t load_fs(const char *fname, STFileSystem *pFS) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t commit_edit(STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code;
  char    ofname[TSDB_FILENAME_LEN];
  char    nfname[TSDB_FILENAME_LEN];

  current_fname(pFS->pTsdb, nfname, TSDB_FCURRENT);
  current_fname(pFS->pTsdb, ofname, etype == TSDB_FS_EDIT_COMMIT ? TSDB_FCURRENT_C : TSDB_FCURRENT_M);

  code = taosRenameFile(ofname, nfname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    return code;
  }

  ASSERTS(0, "TODO: Do changes to pFS");

  return 0;
}

static int32_t abort_edit(STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code;
  char    fname[TSDB_FILENAME_LEN];

  current_fname(pFS->pTsdb, fname, etype == TSDB_FS_EDIT_COMMIT ? TSDB_FCURRENT_C : TSDB_FCURRENT_M);

  code = taosRemoveFile(fname);
  if (code) code = TAOS_SYSTEM_ERROR(code);

  return code;
}

static int32_t scan_file_system(STFileSystem *pFS) {
  // ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t scan_and_schedule_merge(STFileSystem *pFS) {
  // ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t update_fs_if_needed(STFileSystem *pFS) {
  // TODO
  return 0;
}

static int32_t open_fs(STFileSystem *pFS, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFS->pTsdb;

  code = update_fs_if_needed(pFS);
  TSDB_CHECK_CODE(code, lino, _exit)

  char fCurrent[TSDB_FILENAME_LEN];
  char cCurrent[TSDB_FILENAME_LEN];
  char mCurrent[TSDB_FILENAME_LEN];

  current_fname(pTsdb, fCurrent, TSDB_FCURRENT);
  current_fname(pTsdb, cCurrent, TSDB_FCURRENT_C);
  current_fname(pTsdb, mCurrent, TSDB_FCURRENT_M);

  if (taosCheckExistFile(fCurrent)) {  // current.json exists
    code = load_fs(fCurrent, pFS);
    TSDB_CHECK_CODE(code, lino, _exit);

    // check current.json.commit existence
    if (taosCheckExistFile(cCurrent)) {
      if (rollback) {
        code = commit_edit(pFS, TSDB_FS_EDIT_COMMIT);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = abort_edit(pFS, TSDB_FS_EDIT_COMMIT);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    // check current.json.t existence
    if (taosCheckExistFile(mCurrent)) {
      code = abort_edit(pFS, TSDB_FS_EDIT_MERGE);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = scan_file_system(pFS);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = scan_and_schedule_merge(pFS);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = save_fs(0, pFS->nstate, fCurrent);
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
  int32_t lino;

  taosArrayClearEx(pFS->nstate, NULL /* TODO */);

  // TODO: copy current state to new state

  for (int32_t iop = 0; iop < taosArrayGetSize(aFileOp); iop++) {
    struct SFileOp *pOp = taosArrayGet(aFileOp, iop);

    struct STFileSet tmpSet = {.fid = pOp->fid};

    int32_t idx = taosArraySearchIdx(  //
        pFS->nstate,                   //
        &tmpSet,                       //
        (__compar_fn_t)fset_cmpr_fn,   //
        TD_GE);

    struct STFileSet *pSet;
    if (idx < 0) {
      pSet = NULL;
      idx = taosArrayGetSize(pFS->nstate);
    } else {
      pSet = taosArrayGet(pFS->nstate, idx);
    }

    if (pSet == NULL || pSet->fid != pOp->fid) {
      ASSERTS(pOp->op == TSDB_FOP_CREATE, "BUG: Invalid file operation");
      TSDB_CHECK_CODE(                                //
          code = tsdbFileSetCreate(pOp->fid, &pSet),  //
          lino,                                       //
          _exit);

      if (taosArrayInsert(pFS->nstate, idx, pSet) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    // do opration on file set
    TSDB_CHECK_CODE(                        //
        code = tsdbFileSetEdit(pSet, pOp),  //
        lino,                               //
        _exit);
  }

  // TODO: write new state to file

_exit:
  return 0;
}

int32_t tsdbOpenFileSystem(STsdb *pTsdb, STFileSystem **ppFS, int8_t rollback) {
  int32_t code;
  int32_t lino;

  code = create_fs(pTsdb, ppFS);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = open_fs(ppFS[0], rollback);
  TSDB_CHECK_CODE(code, lino, _exit)

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    destroy_fs(ppFS);
  } else {
    tsdbInfo("vgId:%d %s success", TD_VID(pTsdb->pVnode), __func__);
  }
  return 0;
}

int32_t tsdbCloseFileSystem(STFileSystem **ppFS) {
  if (ppFS[0] == NULL) return 0;
  close_file_system(ppFS[0]);
  destroy_fs(ppFS);
  return 0;
}

int32_t tsdbFileSystemEditBegin(STFileSystem *pFS, const SArray *aFileOp, tsdb_fs_edit_t etype) {
  int32_t code = 0;
  int32_t lino;
  char    fname[TSDB_FILENAME_LEN];

  // current_fname(pFS->pTsdb, fname, etype == TSDB_FS_EDIT_COMMIT ? TSDB_FCURRENT_C : TSDB_FCURRENT_M);

  // tsem_wait(&pFS->canEdit);

  // TSDB_CHECK_CODE(                   //
  //     code = edit_fs(pFS, aFileOp),  //
  //     lino,                          //
  //     _exit);

  // TSDB_CHECK_CODE(                 //
  //     code = save_fs(pFS, fname),  //
  //     lino,                        //
  //     _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, etype:%d",  //
             TD_VID(pFS->pTsdb->pVnode),   //
             __func__,                     //
             etype);
  }
  return code;
}

int32_t tsdbFileSystemEditCommit(STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code = commit_edit(pFS, etype);
  tsem_post(&pFS->canEdit);
  if (code) {
    tsdbError("vgId:%d %s failed since %s",  //
              TD_VID(pFS->pTsdb->pVnode),    //
              __func__,                      //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, etype:%d",  //
             TD_VID(pFS->pTsdb->pVnode),   //
             __func__,                     //
             etype);
  }
  return code;
}

int32_t tsdbFileSystemEditAbort(STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code = abort_edit(pFS, etype);
  if (code) {
    tsdbError("vgId:%d %s failed since %s, etype:%d",  //
              TD_VID(pFS->pTsdb->pVnode),              //
              __func__,                                //
              tstrerror(code),                         //
              etype);
  } else {
  }
  tsem_post(&pFS->canEdit);
  return code;
}
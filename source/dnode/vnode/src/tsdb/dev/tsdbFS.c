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

#include "dev.h"

static int32_t create_file_system(STsdb *pTsdb, struct STFileSystem **ppFS) {
  if ((ppFS[0] = taosMemoryCalloc(1, sizeof(*ppFS[0]))) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if ((ppFS[0]->aFileSet = taosArrayInit(16, sizeof(struct SFileSet))) == NULL) {
    taosMemoryFree(ppFS[0]);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ppFS[0]->pTsdb = pTsdb;
  tsem_init(&ppFS[0]->canEdit, 0, 1);

  return 0;
}

static int32_t destroy_file_system(struct STFileSystem **ppFS) {
  if (ppFS[0]) {
    taosArrayDestroy(ppFS[0]->aFileSet);
    tsem_destroy(&ppFS[0]->canEdit);
    taosMemoryFree(ppFS[0]);
    ppFS[0] = NULL;
  }
  return 0;
}

static int32_t get_current_json(STsdb *pTsdb, char fname[]) {
  return snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", pTsdb->path, TD_DIRSEP, "current.json"), 0;
}

static int32_t get_current_temp(STsdb *pTsdb, char fname[], EFsEditType etype) {
  switch (etype) {
    case TSDB_FS_EDIT_COMMIT:
      snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", pTsdb->path, TD_DIRSEP, "current.json.commit");
      break;
    default:
      snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", pTsdb->path, TD_DIRSEP, "current.json.t");
      break;
  }

  return 0;
}

static int32_t save_fs_to_file(struct STFileSystem *pFS, const char *fname) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t load_fs_from_file(const char *fname, struct STFileSystem *pFS) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t commit_edit(struct STFileSystem *pFS, EFsEditType etype) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t abort_edit(struct STFileSystem *pFS, EFsEditType etype) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t open_file_system(struct STFileSystem *pFS, int8_t rollback) {
  int32_t code = 0;
  int32_t lino;
  STsdb  *pTsdb = pFS->pTsdb;

  if (0) {
    ASSERTS(0, "Not implemented yet");
  } else {
    char current_json[TSDB_FILENAME_LEN];
    char current_json_commit[TSDB_FILENAME_LEN];
    char current_json_t[TSDB_FILENAME_LEN];

    get_current_json(pTsdb, current_json);
    get_current_temp(pTsdb, current_json_commit, TSDB_FS_EDIT_COMMIT);
    get_current_temp(pTsdb, current_json_t, TSDB_FS_EDIT_MERGE);

    if (taosCheckExistFile(current_json)) {  // current.json exists
      code = load_fs_from_file(current_json, pFS);
      TSDB_CHECK_CODE(code, lino, _exit);

      // check current.json.commit existence
      if (taosCheckExistFile(current_json_commit)) {
        if (rollback) {
          code = commit_edit(pFS, TSDB_FS_EDIT_COMMIT);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          code = abort_edit(pFS, TSDB_FS_EDIT_COMMIT);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      } else {
        ASSERTS(1, "Do nothing");
      }

      // check current.json.t existence
      if (taosCheckExistFile(current_json_t)) {
        code = abort_edit(pFS, TSDB_FS_EDIT_MERGE);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      code = save_fs_to_file(pFS, current_json);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pTsdb->pVnode),                    //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success",   //
             TD_VID(pTsdb->pVnode),  //
             __func__);
  }
  return 0;
}

static int32_t close_file_system(struct STFileSystem *pFS) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t write_fs_to_file(struct STFileSystem *pFS, const char *fname) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t read_fs_from_file(struct STFileSystem *pFS, const char *fname) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

int32_t tsdbOpenFileSystem(STsdb *pTsdb, struct STFileSystem **ppFS, int8_t rollback) {
  int32_t code;
  int32_t lino;

  code = create_file_system(pTsdb, ppFS);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = open_file_system(ppFS[0], rollback);
  if (code) {
    destroy_file_system(ppFS);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pTsdb->pVnode),                    //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s success",   //
             TD_VID(pTsdb->pVnode),  //
             __func__);
  }
  return 0;
}

int32_t tsdbCloseFileSystem(struct STFileSystem **ppFS) {
  if (ppFS[0] == NULL) return 0;

  close_file_system(ppFS[0]);
  destroy_file_system(ppFS);
  return 0;
}

int32_t tsdbFileSystemEditBegin(struct STFileSystem *pFS, const SArray *aFileOp, EFsEditType etype) {
  int32_t code = 0;
  int32_t lino = 0;
  char    fname[TSDB_FILENAME_LEN];

  get_current_temp(pFS->pTsdb, fname, etype);

  tsem_wait(&pFS->canEdit);

  code = write_fs_to_file(pFS, fname);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  }
  return code;
}

int32_t tsdbFileSystemEditCommit(struct STFileSystem *pFS, EFsEditType etype) {
  int32_t code = 0;
  int32_t lino = 0;
  char    ofname[TSDB_FILENAME_LEN];
  char    nfname[TSDB_FILENAME_LEN];

  get_current_json(pFS->pTsdb, nfname);
  get_current_temp(pFS->pTsdb, ofname, etype);

  code = taosRenameFile(ofname, nfname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  }
  tsem_post(&pFS->canEdit);
  return code;
}

int32_t tsdbFileSystemEditAbort(struct STFileSystem *pFS, EFsEditType etype) {
  int32_t code = 0;
  int32_t lino = 0;
  char    fname[TSDB_FILENAME_LEN];

  get_current_temp(pFS->pTsdb, fname, etype);

  code = taosRemoveFile(fname);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  }
  tsem_post(&pFS->canEdit);
  return code;
}
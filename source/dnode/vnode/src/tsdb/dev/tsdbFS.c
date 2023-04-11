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
  if (pTsdb->pVnode->pTfs) {
    snprintf(fname,                                   //
             TSDB_FILENAME_LEN,                       //
             "%s%s%s%s%s",                            //
             tfsGetPrimaryPath(pTsdb->pVnode->pTfs),  //
             TD_DIRSEP,                               //
             pTsdb->path,                             //
             TD_DIRSEP,                               //
             "current.json");
  } else {
    snprintf(fname,              //
             TSDB_FILENAME_LEN,  //
             "%s%s%s",           //
             pTsdb->path,        //
             TD_DIRSEP,          //
             "current.json");
  }
  return 0;
}

static int32_t get_current_temp(STsdb *pTsdb, char fname[], tsdb_fs_edit_t etype) {
  switch (etype) {
    case TSDB_FS_EDIT_COMMIT:
      if (pTsdb->pVnode->pTfs) {
        snprintf(fname,                                   //
                 TSDB_FILENAME_LEN,                       //
                 "%s%s%s%s%s",                            //
                 tfsGetPrimaryPath(pTsdb->pVnode->pTfs),  //
                 TD_DIRSEP,                               //
                 pTsdb->path,                             //
                 TD_DIRSEP,                               //
                 "current.json.commit");
      } else {
        snprintf(fname,              //
                 TSDB_FILENAME_LEN,  //
                 "%s%s%s",           //
                 pTsdb->path,        //
                 TD_DIRSEP,          //
                 "current.json.commit");
      }
      break;
    default:
      if (pTsdb->pVnode->pTfs) {
        snprintf(fname,                                   //
                 TSDB_FILENAME_LEN,                       //
                 "%s%s%s%s%s",                            //
                 tfsGetPrimaryPath(pTsdb->pVnode->pTfs),  //
                 TD_DIRSEP,                               //
                 pTsdb->path,                             //
                 TD_DIRSEP,                               //
                 "current.json.t");
      } else {
        snprintf(fname,              //
                 TSDB_FILENAME_LEN,  //
                 "%s%s%s",           //
                 pTsdb->path,        //
                 TD_DIRSEP,          //
                 "current.json.t");
      }
      break;
  }

  return 0;
}

static int32_t fs_to_json_str(struct STFileSystem *pFS, char **ppData) {
  int32_t code = 0;
  int32_t lino;

  cJSON *pJson = cJSON_CreateObject();
  if (pJson == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* format version */
  TSDB_CHECK_NULL(                        //
      cJSON_AddNumberToObject(pJson,      //
                              "version",  //
                              1 /* TODO */),
      code,   //
      lino,   //
      _exit,  //
      TSDB_CODE_OUT_OF_MEMORY);

  /* next edit id */
  TSDB_CHECK_NULL(                        //
      cJSON_AddNumberToObject(pJson,      //
                              "edit id",  //
                              pFS->nextEditId),
      code,   //
      lino,   //
      _exit,  //
      TSDB_CODE_OUT_OF_MEMORY);

  /* file sets */
  cJSON *aFileSetJson;
  TSDB_CHECK_NULL(                                                //
      aFileSetJson = cJSON_AddArrayToObject(pJson, "file sets"),  //
      code,                                                       //
      lino,                                                       //
      _exit,                                                      //
      TSDB_CODE_OUT_OF_MEMORY);

  for (int32_t i = 0; i < taosArrayGetSize(pFS->aFileSet); i++) {
    struct SFileSet *pFileSet = taosArrayGet(pFS->aFileSet, i);

    code = tsdbFileSetToJson(aFileSetJson, pFileSet);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ppData[0] = cJSON_Print(pJson);
  if (ppData[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  cJSON_Delete(pJson);
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  }
  return code;
}

static int32_t fs_from_json_str(const char *pData, struct STFileSystem *pFS) {
  int32_t code = 0;
  int32_t lino;

  ASSERTS(0, "TODO: Not implemented yet");

_exit:
  return code;
}

static int32_t save_fs_to_file(struct STFileSystem *pFS, const char *fname) {
  int32_t code = 0;
  int32_t lino;
  char   *pData = NULL;

  // to json string
  code = fs_to_json_str(pFS, &pData);
  TSDB_CHECK_CODE(code, lino, _exit);

  TdFilePtr fd = taosOpenFile(fname,                //
                              TD_FILE_WRITE         //
                                  | TD_FILE_CREATE  //
                                  | TD_FILE_TRUNC);
  if (fd == NULL) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t n = taosWriteFile(fd, pData, strlen(pData) + 1);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    taosCloseFile(&fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosFsyncFile(fd) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    taosCloseFile(&fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCloseFile(&fd);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s",  //
              TD_VID(pFS->pTsdb->pVnode),               //
              __func__,                                 //
              lino,                                     //
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s success",        //
              TD_VID(pFS->pTsdb->pVnode),  //
              __func__);
  }
  if (pData) {
    taosMemoryFree(pData);
  }
  return code;
}

static int32_t load_fs_from_file(const char *fname, struct STFileSystem *pFS) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t commit_edit(struct STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code;
  char    ofname[TSDB_FILENAME_LEN];
  char    nfname[TSDB_FILENAME_LEN];

  get_current_json(pFS->pTsdb, nfname);
  get_current_temp(pFS->pTsdb, ofname, etype);

  code = taosRenameFile(ofname, nfname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    return code;
  }

  ASSERTS(0, "TODO: Do changes to pFS");

  return 0;
}

static int32_t abort_edit(struct STFileSystem *pFS, tsdb_fs_edit_t etype) {
  int32_t code;
  char    fname[TSDB_FILENAME_LEN];

  get_current_temp(pFS->pTsdb, fname, etype);

  code = taosRemoveFile(fname);
  if (code) code = TAOS_SYSTEM_ERROR(code);

  return code;
}

static int32_t scan_file_system(struct STFileSystem *pFS) {
  // ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t scan_and_schedule_merge(struct STFileSystem *pFS) {
  // ASSERTS(0, "TODO: Not implemented yet");
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

  code = scan_file_system(pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = scan_and_schedule_merge(pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

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

static int32_t read_fs_from_file(struct STFileSystem *pFS, const char *fname) {
  ASSERTS(0, "TODO: Not implemented yet");
  return 0;
}

static int32_t edit_fs(struct STFileSystem *pFS, const SArray *aFileOp) {
  ASSERTS(0, "TODO: Not implemented yet");

  for (int32_t iop = 0; iop < taosArrayGetSize(aFileOp); iop++) {
    struct SFileOp *pOp = taosArrayGet(aFileOp, iop);
    // if (pOp->op == TSDB_FS_OP_ADD) {
    //   ASSERTS(0, "TODO: Not implemented yet");
    // } else if (pOp->op == TSDB_FS_OP_DEL) {
    //   ASSERTS(0, "TODO: Not implemented yet");
    // } else {
    //   ASSERTS(0, "TODO: Not implemented yet");
    // }
  }

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

int32_t tsdbFileSystemEditBegin(struct STFileSystem *pFS, const SArray *aFileOp, tsdb_fs_edit_t etype) {
  int32_t code = 0;
  int32_t lino;
  char    fname[TSDB_FILENAME_LEN];

  get_current_temp(pFS->pTsdb, fname, etype);

  tsem_wait(&pFS->canEdit);

  TSDB_CHECK_CODE(                   //
      code = edit_fs(pFS, aFileOp),  //
      lino,                          //
      _exit);

  TSDB_CHECK_CODE(                         //
      code = save_fs_to_file(pFS, fname),  //
      lino,                                //
      _exit);

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

int32_t tsdbFileSystemEditCommit(struct STFileSystem *pFS, tsdb_fs_edit_t etype) {
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

int32_t tsdbFileSystemEditAbort(struct STFileSystem *pFS, tsdb_fs_edit_t etype) {
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
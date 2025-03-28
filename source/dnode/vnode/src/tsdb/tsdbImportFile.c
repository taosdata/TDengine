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

typedef struct {
  TSKEY minKey;
  TSKEY maxKey;
} SSttFileInfo;

static int32_t tsdbLoadSttFileInfo(const char *fileName, SSttFileInfo *pSttFileInfo) {
  int32_t code = 0;

  // TODO

  return code;
}

int32_t tsdbImportFile(STsdb *pTsdb, const char *fileName) {
  int32_t code = 0;

  if (pTsdb == NULL || fileName == NULL) {
    return TSDB_CODE_APP_ERROR;
  }

  tsdbDebug("Started importing data from file %s", fileName);

  // Load the infomation of the file
  SSttFileInfo info = {0};
  code = tsdbLoadSttFileInfo(fileName, &info);
  if (code) {
    tsdbError("Failed to load file info for %s, error code: %d (file: %s, func: %s, line: %d)", 
              fileName, code, __FILE__, __func__, __LINE__);
    return code;
  }

  // Create the file object
  STFile file = {
    // TODO
  };
  STFileObj *fileObj = NULL;

  code = tsdbTFileObjInit(pTsdb, &file, &STFileObj);
  if (code) {
    // TODO
  }

  TFileOpArray opArray;


  // Try to change add the stt file to the vnode
  code = tsdbFSEditBegin(pTsdb->pFS, NULL, TSDB_FEDIT_COMMIT);
  if (code) {
    tsdbError("Failed to begin edit for file %s, error code: %d (file: %s, func: %s, line: %d)", 
              fileName, code, __FILE__, __func__, __LINE__);
    return code;
  }

  // Commit the edit
  code = tsdbFSEditCommit(pTsdb->pFS);
  if (code) {
    tsdbError("Failed to commit edit for file %s, error code: %d (file: %s, func: %s, line: %d)", 
              fileName, code, __FILE__, __func__, __LINE__);
    return code;
  } else {
    // TODO
  }

  tsdbDebug("Finished importing data from file %s", fileName);
  return code;
}
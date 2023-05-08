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

#include "inc/tsdbFSet.h"

int32_t tsdbFileSetCreate(int32_t fid, struct STFileSet **ppSet) {
  int32_t code = 0;

  ppSet[0] = taosMemoryCalloc(1, sizeof(struct STFileSet));
  if (ppSet[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  ppSet[0]->fid = fid;
  ppSet[0]->nextid = 1;  // TODO

_exit:
  return code;
}

int32_t tsdbFileSetEdit(struct STFileSet *pSet, struct SFileOp *pOp) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbFileSetToJson(SJson *pJson, const struct STFileSet *pSet) {
  int32_t code = 0;

  ASSERTS(0, "TODO: Not implemented yet");

_exit:
  return code;
}

int32_t tsdbEditFileSet(struct STFileSet *pFileSet, const struct SFileOp *pOp) {
  int32_t code = 0;
  ASSERTS(0, "TODO: Not implemented yet");
  // TODO
  return code;
}
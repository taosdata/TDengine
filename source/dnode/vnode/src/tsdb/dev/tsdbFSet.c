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

int32_t tsdbFileSetCreate(int32_t fid, struct SFileSet **ppSet) {
  int32_t code = 0;

  ppSet[0] = taosMemoryCalloc(1, sizeof(struct SFileSet));
  if (ppSet[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  ppSet[0]->fid = fid;
  ppSet[0]->nextid = 1;  // TODO

_exit:
  return code;
}

int32_t tsdbFileSetEdit(struct SFileSet *pSet, struct SFileOp *pOp) {
  int32_t code = 0;
  int32_t lino;

  switch (pOp->op) {
    case TSDB_FOP_CREATE: {
      struct STFile **ppFile;
      switch (pOp->nState.type) {
        case TSDB_FTYPE_HEAD: {
          ppFile = &pSet->fHead;
        } break;
        case TSDB_FTYPE_DATA: {
          ppFile = &pSet->fData;
        } break;
        case TSDB_FTYPE_SMA: {
          ppFile = &pSet->fSma;
        } break;
        case TSDB_FTYPE_TOMB: {
          ppFile = &pSet->fTomb;
        } break;
        case TSDB_FTYPE_STT: {
          // ppFile = &pSet->lStt[0].fStt;
        } break;
        default: {
          ASSERTS(0, "Invalid file type");
        } break;
      }

      TSDB_CHECK_CODE(                                   //
          code = tsdbTFileCreate(&pOp->nState, ppFile),  //
          lino,                                          //
          _exit);
    } break;

    case TSDB_FOP_DELETE: {
      ASSERTS(0, "TODO: Not implemented yet");
    } break;
    case TSDB_FOP_TRUNCATE: {
      ASSERTS(0, "TODO: Not implemented yet");
    } break;
    case TSDB_FOP_EXTEND: {
      ASSERTS(0, "TODO: Not implemented yet");
    } break;
    default: {
      ASSERTS(0, "Invalid file operation");
    } break;
  }

_exit:
  return code;
}

int32_t tsdbFileSetToJson(SJson *pJson, const struct SFileSet *pSet) {
  int32_t code = 0;

  ASSERTS(0, "TODO: Not implemented yet");

_exit:
  return code;
}

int32_t tsdbEditFileSet(struct SFileSet *pFileSet, const struct SFileOp *pOp) {
  int32_t code = 0;
  ASSERTS(0, "TODO: Not implemented yet");
  // TODO
  return code;
}
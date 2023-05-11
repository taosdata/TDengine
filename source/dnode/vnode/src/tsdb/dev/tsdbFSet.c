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

static int32_t stt_lvl_to_json(const SSttLvl *lvl, cJSON *json) {
  if (cJSON_AddNumberToObject(json, "lvl", lvl->lvl) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  cJSON *arr = cJSON_AddArrayToObject(json, "stt");
  if (arr == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  // TODO: .stt files
  // STFile *f;
  // LISTD_FOREACH(&lvl->fstt, f, listNode) {
  //   cJSON *item = cJSON_CreateObject();
  //   if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  //   int32_t code = tsdbTFileToJson(f, item);
  //   if (code) return code;

  //   cJSON_AddItemToArray(arr, item);
  // }

  return 0;
}

static int32_t stt_lvl_from_json(const cJSON *json, SSttLvl *lvl) {
  // TODO
  return 0;
}

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

int32_t tsdbFileSetEdit(struct STFileSet *pSet, struct STFileOp *pOp) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbFileSetToJson(const STFileSet *fset, cJSON *json) {
  int32_t code = 0;

  // fid
  if (cJSON_AddNumberToObject(json, "fid", fset->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) {
      continue;
    }

    code = tsdbTFileToJson(fset->farr[ftype], json);
    if (code) return code;
  }

  // each level
  cJSON *ajson = cJSON_AddArrayToObject(json, "stt");
  if (ajson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  SSttLvl *sttLvl;
  LISTD_FOREACH(&fset->lvl0, sttLvl, listNode) {
    cJSON *ljson = cJSON_CreateObject();
    if (ljson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

    code = stt_lvl_to_json(sttLvl, ljson);
    if (code) return code;

    cJSON_AddItemToArray(ajson, ljson);
  }

  return 0;
}

int32_t tsdbFileSetFromJson(const cJSON *json, STFileSet *fset) {
  const cJSON *item;

  /* fid */
  item = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item)) {
    fset->fid = item->valueint;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    int32_t code = tsdbTFileFromJson(json, ftype, &fset->farr[ftype]);
    if (code) return code;
  }

  // each level
  item = cJSON_GetObjectItem(json, "stt");
  if (cJSON_IsArray(item)) {
    // TODO
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return 0;
}

int32_t tsdbEditFileSet(struct STFileSet *pFileSet, const struct STFileOp *pOp) {
  int32_t code = 0;
  ASSERTS(0, "TODO: Not implemented yet");
  // TODO
  return code;
}

int32_t tsdbFSetCmprFn(const STFileSet *pSet1, const STFileSet *pSet2) {
  if (pSet1->fid < pSet2->fid) return -1;
  if (pSet1->fid > pSet2->fid) return 1;
  return 0;
}
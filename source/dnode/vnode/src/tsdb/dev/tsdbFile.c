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

#include "inc/tsdbFile.h"

static int32_t head_to_json(const STFile *file, cJSON *json);
static int32_t data_to_json(const STFile *file, cJSON *json);
static int32_t sma_to_json(const STFile *file, cJSON *json);
static int32_t tomb_to_json(const STFile *file, cJSON *json);
static int32_t stt_to_json(const STFile *file, cJSON *json);

static const struct {
  const char *suffix;
  int32_t (*to_json)(const STFile *file, cJSON *json);
} g_tfile_info[] = {
    [TSDB_FTYPE_HEAD] = {"head", head_to_json},  //
    [TSDB_FTYPE_DATA] = {"data", data_to_json},  //
    [TSDB_FTYPE_SMA] = {"sma", sma_to_json},     //
    [TSDB_FTYPE_TOMB] = {"tomb", tomb_to_json},  //
    [TSDB_FTYPE_STT] = {"stt", stt_to_json},
};

static int32_t tfile_to_json(const STFile *file, cJSON *json) {
  if (cJSON_AddNumberToObject(json, "did.level", file->did.level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "did.id", file->did.id) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "fid", file->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "cid", file->cid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "size", file->size) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

static int32_t head_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }

static int32_t data_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }

static int32_t sma_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }

static int32_t tomb_to_json(const STFile *file, cJSON *json) { return tfile_to_json(file, json); }

static int32_t stt_to_json(const STFile *file, cJSON *json) {
  int32_t code = tfile_to_json(file, json);
  if (code) return code;

  if (cJSON_AddNumberToObject(json, "lvl", file->stt.lvl) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (cJSON_AddNumberToObject(json, "nseg", file->stt.nseg) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

int32_t tsdbTFileInit(STsdb *pTsdb, STFile *pFile) {
  SVnode *pVnode = pTsdb->pVnode;
  STfs   *pTfs = pVnode->pTfs;

  if (pTfs) {
    snprintf(pFile->fname,                       //
             TSDB_FILENAME_LEN,                  //
             "%s%s%s%sv%df%dver%" PRId64 ".%s",  //
             tfsGetDiskPath(pTfs, pFile->did),   //
             TD_DIRSEP,                          //
             pTsdb->path,                        //
             TD_DIRSEP,                          //
             TD_VID(pVnode),                     //
             pFile->fid,                         //
             pFile->cid,                         //
             g_tfile_info[pFile->type].suffix);
  } else {
    snprintf(pFile->fname,                   //
             TSDB_FILENAME_LEN,              //
             "%s%sv%df%dver%" PRId64 ".%s",  //
             pTsdb->path,                    //
             TD_DIRSEP,                      //
             TD_VID(pVnode),                 //
             pFile->fid,                     //
             pFile->cid,                     //
             g_tfile_info[pFile->type].suffix);
  }
  pFile->ref = 1;
  return 0;
}

int32_t tsdbTFileClear(STFile *pFile) {
  // TODO
  return 0;
}

int32_t tsdbTFileToJson(const STFile *file, cJSON *json) {
  cJSON *tjson = cJSON_AddObjectToObject(json, g_tfile_info[file->type].suffix);
  if (tjson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  return g_tfile_info[file->type].to_json(file, tjson);
}
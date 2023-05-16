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

// to_json
static int32_t head_to_json(const STFile *file, cJSON *json);
static int32_t data_to_json(const STFile *file, cJSON *json);
static int32_t sma_to_json(const STFile *file, cJSON *json);
static int32_t tomb_to_json(const STFile *file, cJSON *json);
static int32_t stt_to_json(const STFile *file, cJSON *json);

// from_json
static int32_t head_from_json(const cJSON *json, STFile *file);
static int32_t data_from_json(const cJSON *json, STFile *file);
static int32_t sma_from_json(const cJSON *json, STFile *file);
static int32_t tomb_from_json(const cJSON *json, STFile *file);
static int32_t stt_from_json(const cJSON *json, STFile *file);

static const struct {
  const char *suffix;
  int32_t (*to_json)(const STFile *file, cJSON *json);
  int32_t (*from_json)(const cJSON *json, STFile *file);
} g_tfile_info[] = {
    [TSDB_FTYPE_HEAD] = {"head", head_to_json, head_from_json},
    [TSDB_FTYPE_DATA] = {"data", data_to_json, data_from_json},
    [TSDB_FTYPE_SMA] = {"sma", sma_to_json, sma_from_json},
    [TSDB_FTYPE_TOMB] = {"tomb", tomb_to_json, tomb_from_json},
    [TSDB_FTYPE_STT] = {"stt", stt_to_json, stt_from_json},
};

static int32_t tfile_to_json(const STFile *file, cJSON *json) {
  /* did.level */
  if (cJSON_AddNumberToObject(json, "did.level", file->did.level) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* did.id */
  if (cJSON_AddNumberToObject(json, "did.id", file->did.id) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* fid */
  if (cJSON_AddNumberToObject(json, "fid", file->fid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* cid */
  if (cJSON_AddNumberToObject(json, "cid", file->cid) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* size */
  if (cJSON_AddNumberToObject(json, "size", file->size) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

static int32_t tfile_from_json(const cJSON *json, STFile *file) {
  const cJSON *item;

  /* did.level */
  item = cJSON_GetObjectItem(json, "did.level");
  if (cJSON_IsNumber(item)) {
    file->did.level = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* did.id */
  item = cJSON_GetObjectItem(json, "did.id");
  if (cJSON_IsNumber(item)) {
    file->did.id = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* fid */
  item = cJSON_GetObjectItem(json, "fid");
  if (cJSON_IsNumber(item)) {
    file->fid = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* cid */
  item = cJSON_GetObjectItem(json, "cid");
  if (cJSON_IsNumber(item)) {
    file->cid = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* size */
  item = cJSON_GetObjectItem(json, "size");
  if (cJSON_IsNumber(item)) {
    file->size = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
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

  /* lvl */
  if (cJSON_AddNumberToObject(json, "lvl", file->stt.lvl) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  /* nseg */
  if (cJSON_AddNumberToObject(json, "nseg", file->stt.nseg) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return 0;
}

static int32_t head_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t data_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t sma_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t tomb_from_json(const cJSON *json, STFile *file) { return tfile_from_json(json, file); }
static int32_t stt_from_json(const cJSON *json, STFile *file) {
  int32_t code = tfile_from_json(json, file);
  if (code) return code;

  const cJSON *item;

  /* lvl */
  item = cJSON_GetObjectItem(json, "lvl");
  if (cJSON_IsNumber(item)) {
    file->stt.lvl = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  /* nseg */
  item = cJSON_GetObjectItem(json, "nseg");
  if (cJSON_IsNumber(item)) {
    file->stt.nseg = item->valuedouble;
  } else {
    return TSDB_CODE_FILE_CORRUPTED;
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
  // pFile->ref = 1;
  return 0;
}

int32_t tsdbTFileClear(STFile *pFile) {
  // TODO
  return 0;
}

int32_t tsdbTFileToJson(const STFile *file, cJSON *json) {
  if (file->type == TSDB_FTYPE_STT) {
    return g_tfile_info[file->type].to_json(file, json);
  } else {
    cJSON *item = cJSON_AddObjectToObject(json, g_tfile_info[file->type].suffix);
    if (item == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    return g_tfile_info[file->type].to_json(file, item);
  }
}

int32_t tsdbJsonToTFile(const cJSON *json, tsdb_ftype_t ftype, STFile *f) {
  f[0] = (STFile){.type = ftype};

  if (ftype == TSDB_FTYPE_STT) {
    int32_t code = g_tfile_info[ftype].from_json(json, f);
    if (code) return code;
  } else {
    const cJSON *item = cJSON_GetObjectItem(json, g_tfile_info[ftype].suffix);
    if (cJSON_IsObject(item)) {
      int32_t code = g_tfile_info[ftype].from_json(item, f);
      if (code) return code;
    } else {
      return TSDB_CODE_NOT_FOUND;
    }
  }

  // TODO: tsdbTFileInit(NULL, f);
  return 0;
}

int32_t tsdbTFileObjCreate(STFileObj **fobj) {
  fobj[0] = taosMemoryMalloc(sizeof(STFileObj));
  if (fobj[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  fobj[0]->ref = 1;
  // TODO
  return 0;
}

int32_t tsdbTFileObjDestroy(STFileObj *fobj) {
  // TODO
  taosMemoryFree(fobj);
  return 0;
}

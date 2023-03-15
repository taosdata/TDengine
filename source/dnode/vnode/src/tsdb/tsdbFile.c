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

int32_t tPutHeadFile(uint8_t *p, SHeadFile *pHeadFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pHeadFile->commitID);
  n += tPutI64v(p ? p + n : p, pHeadFile->size);
  n += tPutI64v(p ? p + n : p, pHeadFile->offset);

  return n;
}

static int32_t tGetHeadFile(uint8_t *p, SHeadFile *pHeadFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pHeadFile->commitID);
  n += tGetI64v(p + n, &pHeadFile->size);
  n += tGetI64v(p + n, &pHeadFile->offset);

  return n;
}

int32_t tPutDataFile(uint8_t *p, SDataFile *pDataFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pDataFile->commitID);
  n += tPutI64v(p ? p + n : p, pDataFile->size);

  return n;
}

static int32_t tGetDataFile(uint8_t *p, SDataFile *pDataFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pDataFile->commitID);
  n += tGetI64v(p + n, &pDataFile->size);

  return n;
}

int32_t tPutSttFile(uint8_t *p, SSttFile *pSttFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pSttFile->commitID);
  n += tPutI64v(p ? p + n : p, pSttFile->size);
  n += tPutI64v(p ? p + n : p, pSttFile->offset);

  return n;
}

static int32_t tGetSttFile(uint8_t *p, SSttFile *pSttFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pSttFile->commitID);
  n += tGetI64v(p + n, &pSttFile->size);
  n += tGetI64v(p + n, &pSttFile->offset);

  return n;
}

int32_t tPutSmaFile(uint8_t *p, SSmaFile *pSmaFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pSmaFile->commitID);
  n += tPutI64v(p ? p + n : p, pSmaFile->size);

  return n;
}

static int32_t tGetSmaFile(uint8_t *p, SSmaFile *pSmaFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pSmaFile->commitID);
  n += tGetI64v(p + n, &pSmaFile->size);

  return n;
}

// EXPOSED APIS ==================================================
static char *getFileNamePrefix(STsdb *pTsdb, SDiskID did, int32_t fid, uint64_t commitId, char fname[]) {
  const char *p1 = tfsGetDiskPath(pTsdb->pVnode->pTfs, did);
  int32_t     len = strlen(p1);

  char *p = memcpy(fname, p1, len);
  p += len;

  *(p++) = TD_DIRSEP[0];
  len = strlen(pTsdb->path);

  memcpy(p, pTsdb->path, len);
  p += len;

  *(p++) = TD_DIRSEP[0];
  *(p++) = 'v';

  p += titoa(TD_VID(pTsdb->pVnode), 10, p);
  *(p++) = 'f';

  p += titoa(fid, 10, p);

  memcpy(p, "ver", 3);
  p += 3;

  p += titoa(commitId, 10, p);
  return p;
}

void tsdbHeadFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SHeadFile *pHeadF, char fname[]) {
  char *p = getFileNamePrefix(pTsdb, did, fid, pHeadF->commitID, fname);
  memcpy(p, ".head", 5);
  p[5] = 0;
}

void tsdbDataFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SDataFile *pDataF, char fname[]) {
  char *p = getFileNamePrefix(pTsdb, did, fid, pDataF->commitID, fname);
  memcpy(p, ".data", 5);
  p[5] = 0;
}

void tsdbSttFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SSttFile *pSttF, char fname[]) {
  char *p = getFileNamePrefix(pTsdb, did, fid, pSttF->commitID, fname);
  memcpy(p, ".stt", 4);
  p[4] = 0;
}

void tsdbSmaFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SSmaFile *pSmaF, char fname[]) {
  char *p = getFileNamePrefix(pTsdb, did, fid, pSmaF->commitID, fname);
  memcpy(p, ".sma", 4);
  p[4] = 0;
}

bool tsdbDelFileIsSame(SDelFile *pDelFile1, SDelFile *pDelFile2) { return pDelFile1->commitID == pDelFile2->commitID; }

int32_t tsdbDFileRollback(STsdb *pTsdb, SDFileSet *pSet, EDataFileT ftype) {
  int32_t   code = 0;
  int64_t   size = 0;
  int64_t   n;
  TdFilePtr pFD;
  char      fname[TSDB_FILENAME_LEN] = {0};
  char      hdr[TSDB_FHDR_SIZE] = {0};

  // truncate
  switch (ftype) {
    case TSDB_DATA_FILE:
      size = pSet->pDataF->size;
      tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
      tPutDataFile(hdr, pSet->pDataF);
      break;
    case TSDB_SMA_FILE:
      size = pSet->pSmaF->size;
      tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
      tPutSmaFile(hdr, pSet->pSmaF);
      break;
    default:
      goto _err;  // make the coverity scan happy
  }

  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  // open
  pFD = taosOpenFile(fname, TD_FILE_WRITE);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // ftruncate
  if (taosFtruncateFile(pFD, tsdbLogicToFileSize(size, pTsdb->pVnode->config.tsdbPageSize)) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update header
  n = taosLSeekFile(pFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // sync
  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // close
  taosCloseFile(&pFD);

  return code;

_err:
  tsdbError("vgId:%d, tsdb rollback file failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tPutDFileSet(uint8_t *p, SDFileSet *pSet) {
  int32_t n = 0;

  n += tPutI32v(p ? p + n : p, pSet->diskId.level);
  n += tPutI32v(p ? p + n : p, pSet->diskId.id);
  n += tPutI32v(p ? p + n : p, pSet->fid);

  // data
  n += tPutHeadFile(p ? p + n : p, pSet->pHeadF);
  n += tPutDataFile(p ? p + n : p, pSet->pDataF);
  n += tPutSmaFile(p ? p + n : p, pSet->pSmaF);

  // stt
  n += tPutU8(p ? p + n : p, pSet->nSttF);
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    n += tPutSttFile(p ? p + n : p, pSet->aSttF[iStt]);
  }

  return n;
}

int32_t tGetDFileSet(uint8_t *p, SDFileSet *pSet) {
  int32_t n = 0;

  n += tGetI32v(p + n, &pSet->diskId.level);
  n += tGetI32v(p + n, &pSet->diskId.id);
  n += tGetI32v(p + n, &pSet->fid);

  // head
  pSet->pHeadF = (SHeadFile *)taosMemoryCalloc(1, sizeof(SHeadFile));
  if (pSet->pHeadF == NULL) {
    return -1;
  }
  pSet->pHeadF->nRef = 1;
  n += tGetHeadFile(p + n, pSet->pHeadF);

  // data
  pSet->pDataF = (SDataFile *)taosMemoryCalloc(1, sizeof(SDataFile));
  if (pSet->pDataF == NULL) {
    return -1;
  }
  pSet->pDataF->nRef = 1;
  n += tGetDataFile(p + n, pSet->pDataF);

  // sma
  pSet->pSmaF = (SSmaFile *)taosMemoryCalloc(1, sizeof(SSmaFile));
  if (pSet->pSmaF == NULL) {
    return -1;
  }
  pSet->pSmaF->nRef = 1;
  n += tGetSmaFile(p + n, pSet->pSmaF);

  // stt
  n += tGetU8(p + n, &pSet->nSttF);
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    pSet->aSttF[iStt] = (SSttFile *)taosMemoryCalloc(1, sizeof(SSttFile));
    if (pSet->aSttF[iStt] == NULL) {
      return -1;
    }
    pSet->aSttF[iStt]->nRef = 1;
    n += tGetSttFile(p + n, pSet->aSttF[iStt]);
  }

  return n;
}

static int32_t tDiskIdToJson(const SDiskID *pDiskId, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "level", pDiskId->level), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "id", pDiskId->id), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}
static int32_t tJsonToDiskId(const cJSON *pJson, SDiskID *pDiskId) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;

  // level
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "level")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pDiskId->level = (int32_t)pItem->valuedouble;

  // id
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "id")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pDiskId->id = (int32_t)pItem->valuedouble;

_exit:
  return code;
}

static int32_t tHeadFileToJson(const SHeadFile *pHeadF, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "commit id", pHeadF->commitID), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "size", pHeadF->size), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "offset", pHeadF->offset), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}

static int32_t tJsonToHeadFile(const cJSON *pJson, SHeadFile *pHeadF) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;

  // commit id
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "commit id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  pHeadF->commitID = (int64_t)pItem->valuedouble;

  // size
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "size")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pHeadF->size = (int64_t)pItem->valuedouble;

  // offset
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "offset")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pHeadF->offset = (int64_t)pItem->valuedouble;

_exit:
  return code;
}

static int32_t tDataFileToJson(const SDataFile *pDataF, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "commit id", pDataF->commitID), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "size", pDataF->size), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}

static int32_t tJsonToDataFile(const cJSON *pJson, SDataFile *pDataF) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;

  // commit id
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "commit id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  pDataF->commitID = (int64_t)pItem->valuedouble;

  // size
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "size")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pDataF->size = (int64_t)pItem->valuedouble;

_exit:
  return code;
}

static int32_t tSmaFileToJson(const SSmaFile *pSmaF, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "commit id", pSmaF->commitID), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "size", pSmaF->size), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}

static int32_t tJsonToSmaFile(const cJSON *pJson, SSmaFile *pSmaF) {
  int32_t code = 0;
  int32_t lino;

  // commit id
  const cJSON *pItem;
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "commit id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  pSmaF->commitID = (int64_t)pItem->valuedouble;

  // size
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "size")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pSmaF->size = (int64_t)pItem->valuedouble;

_exit:
  return code;
}

static int32_t tSttFileToJson(const SSttFile *pSttF, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "commit id", pSttF->commitID), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "size", pSttF->size), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "offset", pSttF->offset), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}

static int32_t tJsonToSttFile(const cJSON *pJson, SSttFile *pSttF) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;

  // commit id
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "commit id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  pSttF->commitID = (int64_t)pItem->valuedouble;

  // size
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "size")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pSttF->size = (int64_t)pItem->valuedouble;

  // offset
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "offset")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pSttF->offset = (int64_t)pItem->valuedouble;

_exit:
  return code;
}

int32_t tsdbDFileSetToJson(const SDFileSet *pSet, cJSON *pJson) {
  int32_t code = 0;
  int32_t lino;

  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  code = tDiskIdToJson(&pSet->diskId, cJSON_AddObjectToObject(pJson, "disk id"));
  TSDB_CHECK_CODE(code, lino, _exit);

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "fid", pSet->fid), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

  // head
  code = tHeadFileToJson(pSet->pHeadF, cJSON_AddObjectToObject(pJson, "head"));
  TSDB_CHECK_CODE(code, lino, _exit);

  // data
  code = tDataFileToJson(pSet->pDataF, cJSON_AddObjectToObject(pJson, "data"));
  TSDB_CHECK_CODE(code, lino, _exit);

  // sma
  code = tSmaFileToJson(pSet->pSmaF, cJSON_AddObjectToObject(pJson, "sma"));
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt array
  cJSON *aSttJson = cJSON_AddArrayToObject(pJson, "stt");
  TSDB_CHECK_NULL(aSttJson, code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    cJSON *pSttJson = cJSON_CreateObject();
    TSDB_CHECK_NULL(pSttJson, code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

    cJSON_AddItemToArray(aSttJson, pSttJson);

    code = tSttFileToJson(pSet->aSttF[iStt], pSttJson);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  return code;
}

int32_t tsdbJsonToDFileSet(const cJSON *pJson, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;
  // disk id
  TSDB_CHECK(cJSON_IsObject(pItem = cJSON_GetObjectItem(pJson, "disk id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  code = tJsonToDiskId(pItem, &pSet->diskId);
  TSDB_CHECK_CODE(code, lino, _exit);

  // fid
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "fid")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pSet->fid = (int32_t)pItem->valuedouble;

  // head
  TSDB_CHECK(cJSON_IsObject(pItem = cJSON_GetObjectItem(pJson, "head")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  TSDB_CHECK_NULL(pSet->pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile)), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_CODE(code = tJsonToHeadFile(pItem, pSet->pHeadF), lino, _exit);
  pSet->pHeadF->nRef = 1;

  // data
  TSDB_CHECK(cJSON_IsObject(pItem = cJSON_GetObjectItem(pJson, "data")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  TSDB_CHECK_NULL(pSet->pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile)), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_CODE(code = tJsonToDataFile(pItem, pSet->pDataF), lino, _exit);
  pSet->pDataF->nRef = 1;

  // sma
  TSDB_CHECK(cJSON_IsObject(pItem = cJSON_GetObjectItem(pJson, "sma")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  TSDB_CHECK_NULL(pSet->pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile)), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_CODE(code = tJsonToSmaFile(pItem, pSet->pSmaF), lino, _exit);
  pSet->pSmaF->nRef = 1;

  // stt array
  const cJSON *element;
  pSet->nSttF = 0;
  TSDB_CHECK(cJSON_IsArray(pItem = cJSON_GetObjectItem(pJson, "stt")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  cJSON_ArrayForEach(element, pItem) {
    TSDB_CHECK(cJSON_IsObject(element), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);

    pSet->aSttF[pSet->nSttF] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
    TSDB_CHECK_NULL(pSet->aSttF[pSet->nSttF], code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
    TSDB_CHECK_CODE(code = tJsonToSttFile(element, pSet->aSttF[pSet->nSttF]), lino, _exit);
    pSet->aSttF[pSet->nSttF]->nRef = 1;
    pSet->nSttF++;
  }

_exit:
  if (code) tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

// SDelFile ===============================================
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]) {
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%dver%" PRId64 "%s", tfsGetPrimaryPath(pTsdb->pVnode->pTfs),
           TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pFile->commitID, ".del");
}

int32_t tPutDelFile(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pDelFile->commitID);
  n += tPutI64v(p ? p + n : p, pDelFile->size);
  n += tPutI64v(p ? p + n : p, pDelFile->offset);

  return n;
}

int32_t tGetDelFile(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pDelFile->commitID);
  n += tGetI64v(p + n, &pDelFile->size);
  n += tGetI64v(p + n, &pDelFile->offset);

  return n;
}

int32_t tsdbDelFileToJson(const SDelFile *pDelFile, cJSON *pJson) {
  if (pJson == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  int32_t code = 0;
  int32_t lino;

  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "commit id", pDelFile->commitID), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "size", pDelFile->size), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);
  TSDB_CHECK_NULL(cJSON_AddNumberToObject(pJson, "offset", pDelFile->offset), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);

_exit:
  return code;
}

int32_t tsdbJsonToDelFile(const cJSON *pJson, SDelFile *pDelFile) {
  int32_t code = 0;
  int32_t lino;

  const cJSON *pItem;

  // commit id
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "commit id")), code, lino, _exit,
             TSDB_CODE_FILE_CORRUPTED);
  pDelFile->commitID = cJSON_GetNumberValue(pItem);

  // size
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "size")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pDelFile->size = cJSON_GetNumberValue(pItem);

  // offset
  TSDB_CHECK(cJSON_IsNumber(pItem = cJSON_GetObjectItem(pJson, "offset")), code, lino, _exit, TSDB_CODE_FILE_CORRUPTED);
  pDelFile->offset = cJSON_GetNumberValue(pItem);

_exit:
  return code;
}
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

#define _DEFAULT_SOURCE
#include "tanal.h"
#include "tmsg.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef USE_ANAL
#include <curl/curl.h>
#define ANAL_ALGO_SPLIT ","

typedef struct {
  int64_t       ver;
  SHashObj     *hash;  // algoname:algotype -> SAnalUrl
  TdThreadMutex lock;
} SAlgoMgmt;

typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

static SAlgoMgmt tsAlgos = {0};
static int32_t   taosAnalBufGetCont(SAnalBuf *pBuf, char **ppCont, int64_t *pContLen);

const char *taosAnalAlgoStr(EAnalAlgoType type) {
  switch (type) {
    case ANAL_ALGO_TYPE_ANOMALY_DETECT:
      return "anomaly-detection";
    case ANAL_ALGO_TYPE_FORECAST:
      return "forecast";
    default:
      return "unknown";
  }
}

const char *taosAnalAlgoUrlStr(EAnalAlgoType type) {
  switch (type) {
    case ANAL_ALGO_TYPE_ANOMALY_DETECT:
      return "anomaly-detect";
    case ANAL_ALGO_TYPE_FORECAST:
      return "forecast";
    default:
      return "unknown";
  }
}

EAnalAlgoType taosAnalAlgoInt(const char *name) {
  for (EAnalAlgoType i = 0; i < ANAL_ALGO_TYPE_END; ++i) {
    if (strcasecmp(name, taosAnalAlgoStr(i)) == 0) {
      return i;
    }
  }

  return ANAL_ALGO_TYPE_END;
}

int32_t taosAnalInit() {
  if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
    uError("failed to init curl");
    return -1;
  }

  tsAlgos.ver = 0;
  taosThreadMutexInit(&tsAlgos.lock, NULL);
  tsAlgos.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (tsAlgos.hash == NULL) {
    uError("failed to init algo hash");
    return -1;
  }

  uInfo("analysis env is initialized");
  return 0;
}

static void taosAnalFreeHash(SHashObj *hash) {
  void *pIter = taosHashIterate(hash, NULL);
  while (pIter != NULL) {
    SAnalUrl *pUrl = (SAnalUrl *)pIter;
    taosMemoryFree(pUrl->url);
    pIter = taosHashIterate(hash, pIter);
  }
  taosHashCleanup(hash);
}

void taosAnalCleanup() {
  curl_global_cleanup();
  taosThreadMutexDestroy(&tsAlgos.lock);
  taosAnalFreeHash(tsAlgos.hash);
  tsAlgos.hash = NULL;
  uInfo("analysis env is cleaned up");
}

void taosAnalUpdate(int64_t newVer, SHashObj *pHash) {
  if (newVer > tsAlgos.ver) {
    taosThreadMutexLock(&tsAlgos.lock);
    SHashObj *hash = tsAlgos.hash;
    tsAlgos.ver = newVer;
    tsAlgos.hash = pHash;
    taosThreadMutexUnlock(&tsAlgos.lock);
    taosAnalFreeHash(hash);
  } else {
    taosAnalFreeHash(pHash);
  }
}

bool taosAnalGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen) {
  char    buf[TSDB_ANAL_ALGO_OPTION_LEN] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), "%s=", optName);

  char *pos1 = strstr(option, buf);
  char *pos2 = strstr(option, ANAL_ALGO_SPLIT);
  if (pos1 != NULL) {
    if (optMaxLen > 0) {
      int32_t copyLen = optMaxLen;
      if (pos2 != NULL) {
        copyLen = (int32_t)(pos2 - pos1 - strlen(optName) + 1);
        copyLen = MIN(copyLen, optMaxLen);
      }
      tstrncpy(optValue, pos1 + bufLen, copyLen);
    }
    return true;
  } else {
    return false;
  }
}

bool taosAnalGetOptInt(const char *option, const char *optName, int32_t *optValue) {
  char    buf[TSDB_ANAL_ALGO_OPTION_LEN] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), "%s=", optName);

  char *pos1 = strstr(option, buf);
  char *pos2 = strstr(option, ANAL_ALGO_SPLIT);
  if (pos1 != NULL) {
    *optValue = taosStr2Int32(pos1 + bufLen + 1, NULL, 10);
    return true;
  } else {
    return false;
  }
}

int32_t taosAnalGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen) {
  int32_t code = 0;
  char    name[TSDB_ANAL_ALGO_KEY_LEN] = {0};
  int32_t nameLen = 1 + snprintf(name, sizeof(name) - 1, "%d:%s", type, algoName);

  taosThreadMutexLock(&tsAlgos.lock);
  SAnalUrl *pUrl = taosHashAcquire(tsAlgos.hash, name, nameLen);
  if (pUrl != NULL) {
    tstrncpy(url, pUrl->url, urlLen);
    uDebug("algo:%s, type:%s, url:%s", algoName, taosAnalAlgoStr(type), url);
  } else {
    url[0] = 0;
    terrno = TSDB_CODE_ANAL_ALGO_NOT_FOUND;
    code = terrno;
    uError("algo:%s, type:%s, url not found", algoName, taosAnalAlgoStr(type));
  }
  taosThreadMutexUnlock(&tsAlgos.lock);

  return code;
}

int64_t taosAnalGetVersion() { return tsAlgos.ver; }

static size_t taosCurlWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SCurlResp *pRsp = userdata;
  if (contLen == 0 || nmemb == 0 || pCont == NULL) {
    pRsp->dataLen = 0;
    pRsp->data = NULL;
    uError("curl response is received, len:%" PRId64, pRsp->dataLen);
    return 0;
  }

  pRsp->dataLen = (int64_t)contLen * (int64_t)nmemb;
  pRsp->data = taosMemoryMalloc(pRsp->dataLen + 1);

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data, pCont, pRsp->dataLen);
    pRsp->data[pRsp->dataLen] = 0;
    uInfo("curl response is received, len:%" PRId64 ", content:%s", pRsp->dataLen, pRsp->data);
    // uInfo("curl response is received, len:%" PRId64, pRsp->dataLen);
    return pRsp->dataLen;
  } else {
    pRsp->dataLen = 0;
    uError("failed to malloc curl response");
    return 0;
  }
}

static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp) {
  CURL    *curl = NULL;
  CURLcode code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 100);

  uInfo("curl get request will sent, url:%s", url);
  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return code;
}

static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen) {
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  CURLcode           code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 60000);
  curl_easy_setopt(curl, CURLOPT_POST, 1);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf);

  uInfo("curl post request will sent, url:%s len:%d", url, bufLen);
  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return code;
}

SJson *taosAnalSendReqRetJson(const char *url, EAnalHttpType type, SAnalBuf *pBuf) {
  int32_t   code = -1;
  char     *pCont = NULL;
  int64_t   contentLen;
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};

  if (type == ANAL_HTTP_TYPE_GET) {
    if (taosCurlGetRequest(url, &curlRsp) != 0) {
      terrno = TSDB_CODE_ANAL_URL_CANT_ACCESS;
      goto _OVER;
    }
  } else {
    code = taosAnalBufGetCont(pBuf, &pCont, &contentLen);
    if (code != 0) {
      terrno = code;
      goto _OVER;
    }
    if (taosCurlPostRequest(url, &curlRsp, pCont, contentLen) != 0) {
      terrno = TSDB_CODE_ANAL_URL_CANT_ACCESS;
      goto _OVER;
    }
  }

  if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
    terrno = TSDB_CODE_ANAL_URL_RSP_IS_NULL;
    goto _OVER;
  }

  pJson = tjsonParse(curlRsp.data);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

_OVER:
  if (curlRsp.data != NULL) taosMemoryFreeClear(curlRsp.data);
  if (pCont != NULL) taosMemoryFree(pCont);
  return pJson;
}

static int32_t taosAnalJsonBufGetCont(const char *fileName, char **ppCont, int64_t *pContLen) {
  int32_t   code = 0;
  int64_t   contLen;
  char     *pCont = NULL;
  TdFilePtr pFile = NULL;

  pFile = taosOpenFile(fileName, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = taosFStatFile(pFile, &contLen, NULL);
  if (code != 0) goto _OVER;

  pCont = taosMemoryMalloc(contLen + 1);
  if (pCont == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pCont, contLen) != contLen) {
    code = terrno;
    goto _OVER;
  }

  pCont[contLen] = '\0';

_OVER:
  if (code == 0) {
    *ppCont = pCont;
    *pContLen = contLen;
  } else {
    if (pCont != NULL) taosMemoryFree(pCont);
  }
  if (pFile != NULL) taosCloseFile(&pFile);
  return code;
}

static int32_t taosAnalJsonBufWriteOptInt(SAnalBuf *pBuf, const char *optName, int64_t optVal) {
  char    buf[64] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), "\"%s\": %" PRId64 ",\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalJsonBufWriteOptStr(SAnalBuf *pBuf, const char *optName, const char *optVal) {
  char    buf[128] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), "\"%s\": \"%s\",\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalJsonBufWriteOptFloat(SAnalBuf *pBuf, const char *optName, float optVal) {
  char    buf[128] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), "\"%s\": %f,\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalJsonBufWriteStr(SAnalBuf *pBuf, const char *buf, int32_t bufLen) {
  if (bufLen <= 0) {
    bufLen = strlen(buf);
  }
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalJsonBufWriteStart(SAnalBuf *pBuf) { return taosAnalJsonBufWriteStr(pBuf, "{\n", 0); }

static int32_t tsosAnalJsonBufOpen(SAnalBuf *pBuf, int32_t numOfCols) {
  pBuf->filePtr = taosOpenFile(pBuf->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pBuf->filePtr == NULL) {
    return terrno;
  }

  pBuf->pCols = taosMemoryCalloc(numOfCols, sizeof(SAnalColBuf));
  if (pBuf->pCols == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  pBuf->numOfCols = numOfCols;

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON) {
    return taosAnalJsonBufWriteStart(pBuf);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SAnalColBuf *pCol = &pBuf->pCols[i];
    snprintf(pCol->fileName, sizeof(pCol->fileName), "%s-c%d", pBuf->fileName, i);
    pCol->filePtr =
        taosOpenFile(pCol->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
    if (pCol->filePtr == NULL) {
      return terrno;
    }
  }

  return taosAnalJsonBufWriteStart(pBuf);
}

static int32_t taosAnalJsonBufWriteColMeta(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) {
  char buf[128] = {0};
  bool first = (colIndex == 0);
  bool last = (colIndex == pBuf->numOfCols - 1);

  if (first) {
    if (taosAnalJsonBufWriteStr(pBuf, "\"schema\": [\n", 0) != 0) {
      return terrno;
    }
  }

  int32_t bufLen = snprintf(buf, sizeof(buf), "  [\"%s\", \"%s\", %d]%s\n", colName, tDataTypes[colType].name,
                            tDataTypes[colType].bytes, last ? "" : ",");
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }

  if (last) {
    if (taosAnalJsonBufWriteStr(pBuf, "],\n", 0) != 0) {
      return terrno;
    }
  }

  return 0;
}

static int32_t taosAnalJsonBufWriteDataBegin(SAnalBuf *pBuf) {
  return taosAnalJsonBufWriteStr(pBuf, "\"data\": [\n", 0);
}

static int32_t taosAnalJsonBufWriteStrUseCol(SAnalBuf *pBuf, const char *buf, int32_t bufLen, int32_t colIndex) {
  if (bufLen <= 0) {
    bufLen = strlen(buf);
  }

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON) {
    if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
      return terrno;
    }
  } else {
    if (taosWriteFile(pBuf->pCols[colIndex].filePtr, buf, bufLen) != bufLen) {
      return terrno;
    }
  }

  return 0;
}

static int32_t taosAnalJsonBufWriteColBegin(SAnalBuf *pBuf, int32_t colIndex) {
  return taosAnalJsonBufWriteStrUseCol(pBuf, "[\n", 0, colIndex);
}

static int32_t taosAnalJsonBufWriteColEnd(SAnalBuf *pBuf, int32_t colIndex) {
  if (colIndex == pBuf->numOfCols - 1) {
    return taosAnalJsonBufWriteStrUseCol(pBuf, "\n]\n", 0, colIndex);

  } else {
    return taosAnalJsonBufWriteStrUseCol(pBuf, "\n],\n", 0, colIndex);
  }
}

static int32_t taosAnalJsonBufWriteColData(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) {
  char    buf[64];
  int32_t bufLen = 0;

  if (pBuf->pCols[colIndex].numOfRows != 0) {
    buf[bufLen] = ',';
    buf[bufLen + 1] = '\n';
    buf[bufLen + 2] = 0;
    bufLen += 2;
  }

  switch (colType) {
    case TSDB_DATA_TYPE_BOOL:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", (*((int8_t *)colValue) == 1) ? 1 : 0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int8_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint8_t *)colValue);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int16_t *)colValue);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint16_t *)colValue);
      break;
    case TSDB_DATA_TYPE_INT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int32_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint32_t *)colValue);
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%" PRId64 "", *(int64_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%" PRIu64 "", *(uint64_t *)colValue);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%f", GET_FLOAT_VAL(colValue));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      bufLen += snprintf(buf + bufLen, sizeof(buf) - bufLen, "%f", GET_DOUBLE_VAL(colValue));
      break;
    default:
      buf[bufLen] = '\0';
  }

  pBuf->pCols[colIndex].numOfRows++;
  return taosAnalJsonBufWriteStrUseCol(pBuf, buf, bufLen, colIndex);
}

static int32_t taosAnalJsonBufWriteDataEnd(SAnalBuf *pBuf) {
  int32_t code = 0;
  char   *pCont = NULL;
  int64_t contLen = 0;

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalColBuf *pCol = &pBuf->pCols[i];

      code = taosFsyncFile(pCol->filePtr);
      if (code != 0) return code;

      code = taosCloseFile(&pCol->filePtr);
      if (code != 0) return code;

      code = taosAnalJsonBufGetCont(pBuf->pCols[i].fileName, &pCont, &contLen);
      if (code != 0) return code;

      code = taosAnalJsonBufWriteStr(pBuf, pCont, contLen);
      if (code != 0) return code;

      taosMemoryFreeClear(pCont);
      contLen = 0;
    }
  }

  return taosAnalJsonBufWriteStr(pBuf, "],\n", 0);
}

static int32_t taosAnalJsonBufWriteEnd(SAnalBuf *pBuf) {
  int32_t code = taosAnalJsonBufWriteOptInt(pBuf, "rows", pBuf->pCols[0].numOfRows);
  if (code != 0) return code;

  return taosAnalJsonBufWriteStr(pBuf, "\"protocol\": 1.0\n}", 0);
}

int32_t taosAnalJsonBufClose(SAnalBuf *pBuf) {
  int32_t code = taosAnalJsonBufWriteEnd(pBuf);
  if (code != 0) return code;

  if (pBuf->filePtr != NULL) {
    code = taosFsyncFile(pBuf->filePtr);
    if (code != 0) return code;
    code = taosCloseFile(&pBuf->filePtr);
    if (code != 0) return code;
  }

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalColBuf *pCol = &pBuf->pCols[i];
      if (pCol->filePtr != NULL) {
        code = taosFsyncFile(pCol->filePtr);
        if (code != 0) return code;
        code = taosCloseFile(&pCol->filePtr);
        if (code != 0) return code;
      }
    }
  }

  return 0;
}

void taosAnalBufDestroy(SAnalBuf *pBuf) {
  if (pBuf->fileName[0] != 0) {
    if (pBuf->filePtr != NULL) (void)taosCloseFile(&pBuf->filePtr);
    // taosRemoveFile(pBuf->fileName);
    pBuf->fileName[0] = 0;
  }

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalColBuf *pCol = &pBuf->pCols[i];
      if (pCol->fileName[0] != 0) {
        if (pCol->filePtr != NULL) (void)taosCloseFile(&pCol->filePtr);
        taosRemoveFile(pCol->fileName);
        pCol->fileName[0] = 0;
      }
    }
  }

  taosMemoryFreeClear(pBuf->pCols);
  pBuf->numOfCols = 0;
}

int32_t tsosAnalBufOpen(SAnalBuf *pBuf, int32_t numOfCols) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return tsosAnalJsonBufOpen(pBuf, numOfCols);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteOptStr(SAnalBuf *pBuf, const char *optName, const char *optVal) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteOptStr(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteOptInt(SAnalBuf *pBuf, const char *optName, int64_t optVal) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteOptInt(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteOptFloat(SAnalBuf *pBuf, const char *optName, float optVal) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteOptFloat(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteColMeta(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteColMeta(pBuf, colIndex, colType, colName);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteDataBegin(SAnalBuf *pBuf) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteDataBegin(pBuf);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteColBegin(SAnalBuf *pBuf, int32_t colIndex) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteColBegin(pBuf, colIndex);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteColData(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteColData(pBuf, colIndex, colType, colValue);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteColEnd(SAnalBuf *pBuf, int32_t colIndex) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteColEnd(pBuf, colIndex);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufWriteDataEnd(SAnalBuf *pBuf) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufWriteDataEnd(pBuf);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalBufClose(SAnalBuf *pBuf) {
  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufClose(pBuf);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

static int32_t taosAnalBufGetCont(SAnalBuf *pBuf, char **ppCont, int64_t *pContLen) {
  *ppCont = NULL;
  *pContLen = 0;

  if (pBuf->bufType == ANAL_BUF_TYPE_JSON || pBuf->bufType == ANAL_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufGetCont(pBuf->fileName, ppCont, pContLen);
  } else {
    return TSDB_CODE_ANAL_BUF_INVALID_TYPE;
  }
}

#else

int32_t taosAnalInit() { return 0; }
void    taosAnalCleanup() {}
SJson  *taosAnalSendReqRetJson(const char *url, EAnalHttpType type, SAnalBuf *pBuf) { return NULL; }

int32_t taosAnalGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen) { return 0; }
bool    taosAnalGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen) { return 0; }
bool    taosAnalGetOptInt(const char *option, const char *optName, int32_t *optValue) { return 0; }
int64_t taosAnalGetVersion() { return 0; }
void    taosAnalUpdate(int64_t newVer, SHashObj *pHash) {}

int32_t tsosAnalBufOpen(SAnalBuf *pBuf, int32_t numOfCols) { return 0; }
int32_t taosAnalBufWriteOptStr(SAnalBuf *pBuf, const char *optName, const char *optVal) { return 0; }
int32_t taosAnalBufWriteOptInt(SAnalBuf *pBuf, const char *optName, int64_t optVal) { return 0; }
int32_t taosAnalBufWriteOptFloat(SAnalBuf *pBuf, const char *optName, float optVal) { return 0; }
int32_t taosAnalBufWriteColMeta(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) { return 0; }
int32_t taosAnalBufWriteDataBegin(SAnalBuf *pBuf) { return 0; }
int32_t taosAnalBufWriteColBegin(SAnalBuf *pBuf, int32_t colIndex) { return 0; }
int32_t taosAnalBufWriteColData(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) { return 0; }
int32_t taosAnalBufWriteColEnd(SAnalBuf *pBuf, int32_t colIndex) { return 0; }
int32_t taosAnalBufWriteDataEnd(SAnalBuf *pBuf) { return 0; }
int32_t taosAnalBufClose(SAnalBuf *pBuf) { return 0; }
void    taosAnalBufDestroy(SAnalBuf *pBuf) {}

const char   *taosAnalAlgoStr(EAnalAlgoType algoType) { return 0; }
EAnalAlgoType taosAnalAlgoInt(const char *algoName) { return 0; }
const char   *taosAnalAlgoUrlStr(EAnalAlgoType algoType) { return 0; }

#endif
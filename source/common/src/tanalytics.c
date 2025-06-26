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
#include "tanalytics.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef USE_ANALYTICS
#include <curl/curl.h>

#define ANALYTICS_ALOG_SPLIT_CHAR ","

typedef struct {
  int64_t       ver;
  SHashObj     *hash;  // algoname:algotype -> SAnalyticsUrl
  TdThreadMutex lock;
} SAlgoMgmt;

typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

static SAlgoMgmt tsAlgos = {0};
static int32_t   taosAnalyBufGetCont(SAnalyticBuf *pBuf, char **ppCont, int64_t *pContLen);

const char *taosAnalysisAlgoType(EAnalAlgoType type) {
  switch (type) {
    case ANALY_ALGO_TYPE_ANOMALY_DETECT:
      return "anomaly-detection";
    case ANALY_ALGO_TYPE_FORECAST:
      return "forecast";
    default:
      return "unknown";
  }
}

const char *taosAnalyAlgoUrlStr(EAnalAlgoType type) {
  switch (type) {
    case ANALY_ALGO_TYPE_ANOMALY_DETECT:
      return "anomaly-detect";
    case ANALY_ALGO_TYPE_FORECAST:
      return "forecast";
    default:
      return "unknown";
  }
}

EAnalAlgoType taosAnalyAlgoInt(const char *name) {
  for (EAnalAlgoType i = 0; i < ANALY_ALGO_TYPE_END; ++i) {
    if (strcasecmp(name, taosAnalysisAlgoType(i)) == 0) {
      return i;
    }
  }

  return ANALY_ALGO_TYPE_END;
}

int32_t taosAnalyticsInit() {
  if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
    uError("failed to init curl");
    return -1;
  }

  tsAlgos.ver = 0;
  if (taosThreadMutexInit(&tsAlgos.lock, NULL) != 0) {
    uError("failed to init algo mutex");
    return -1;
  }

  tsAlgos.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (tsAlgos.hash == NULL) {
    uError("failed to init algo hash");
    return -1;
  }

  uInfo("analysis env is initialized");
  return 0;
}

static void taosAnalyFreeHash(SHashObj *hash) {
  void *pIter = taosHashIterate(hash, NULL);
  while (pIter != NULL) {
    SAnalyticsUrl *pUrl = (SAnalyticsUrl *)pIter;
    taosMemoryFree(pUrl->url);
    pIter = taosHashIterate(hash, pIter);
  }
  taosHashCleanup(hash);
}

void taosAnalyticsCleanup() {
  curl_global_cleanup();
  if (taosThreadMutexDestroy(&tsAlgos.lock) != 0) {
    uError("failed to destroy anal lock");
  }
  taosAnalyFreeHash(tsAlgos.hash);
  tsAlgos.hash = NULL;
  uInfo("analysis env is cleaned up");
}

void taosAnalyUpdate(int64_t newVer, SHashObj *pHash) {
  if (newVer > tsAlgos.ver) {
    if (taosThreadMutexLock(&tsAlgos.lock) == 0) {
      SHashObj *hash = tsAlgos.hash;
      tsAlgos.ver = newVer;
      tsAlgos.hash = pHash;
      if (taosThreadMutexUnlock(&tsAlgos.lock) != 0) {
        uError("failed to unlock hash")
      }
      taosAnalyFreeHash(hash);
    }
  } else {
    taosAnalyFreeHash(pHash);
  }
}

int32_t taosAnalyGetOpts(const char *pOption, SHashObj **pOptHash) {
  int32_t num = 0;
  int32_t code = 0;
  char   *pTmp = NULL;

  if (pOptHash != NULL) {
    (*pOptHash) = NULL;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  pTmp = taosStrdup(pOption);
  if (pTmp == NULL) {
    return terrno;
  }

  int32_t unused = strdequote(pTmp);
  char  **pList = strsplit(pTmp, ANALYTICS_ALOG_SPLIT_CHAR, &num);

  (*pOptHash) = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), 1, HASH_NO_LOCK);
  if ((*pOptHash) == NULL) {
    taosMemoryFree(pTmp);
    taosMemoryFree(pList);
    return terrno;
  }

  for (int32_t i = 0; i < num; ++i) {
    int32_t parts = 0;
    char  **pParts = strsplit(pList[i], "=", &parts);

    if (parts < 2) {  // invalid parameters, ignore and continue
      taosMemoryFree(pParts);
      continue;
    }

    size_t keyLen = strtrim(pParts[0]);
    size_t valLen = strtrim(pParts[1]);

    code = taosHashPut(*pOptHash, pParts[0], keyLen, pParts[1], valLen);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_DUP_KEY) {
      // return error
      taosMemoryFree(pTmp);
      taosMemoryFree(pList);
      taosMemoryFree(pParts);
      return code;
    } else {
      code = 0;
    }

    taosMemoryFree(pParts);
  }

  taosMemoryFree(pTmp);
  taosMemoryFree(pList);
  return code;
}

bool taosAnalyGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen) {
  SHashObj* p = NULL;
  int32_t code = taosAnalyGetOpts(option, &p);
  if (code != TSDB_CODE_SUCCESS) {
    if (p != NULL) {
      taosHashCleanup(p);
      p = NULL;
    }
    return false;
  }

  void* pVal = taosHashGet(p, optName, strlen(optName));
  if (pVal == NULL) {
    taosHashCleanup(p);
    return false;
  }

  int32_t valLen = taosHashGetValueSize(pVal);

  if (optValue != NULL && optMaxLen >= 1) {
    int32_t len = MIN(valLen + 1, optMaxLen);
    tstrncpy(optValue, (char *)pVal, len);
  }

  taosHashCleanup(p);
  return true;
}

int32_t taosAnalyGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen) {
  int32_t code = 0;
  char    name[TSDB_ANALYTIC_ALGO_KEY_LEN] = {0};
  int32_t nameLen = 1 + tsnprintf(name, sizeof(name) - 1, "%d:%s", type, algoName);

  char *unused = strntolower(name, name, nameLen);

  if (taosThreadMutexLock(&tsAlgos.lock) == 0) {
    SAnalyticsUrl *pUrl = taosHashAcquire(tsAlgos.hash, name, nameLen);
    if (pUrl != NULL) {
      tstrncpy(url, pUrl->url, urlLen);
      uDebug("algo:%s, type:%s, url:%s", algoName, taosAnalysisAlgoType(type), url);
    } else {
      url[0] = 0;
      code = TSDB_CODE_ANA_ALGO_NOT_FOUND;
      uError("algo:%s, type:%s, url not found", algoName, taosAnalysisAlgoType(type));
    }

    if (taosThreadMutexUnlock(&tsAlgos.lock) != 0) {
      uError("failed to unlock hash");
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return code;
}

int64_t taosAnalyGetVersion() { return tsAlgos.ver; }

static size_t taosCurlWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SCurlResp *pRsp = userdata;
  if (contLen == 0 || nmemb == 0 || pCont == NULL) {
    pRsp->dataLen = 0;
    pRsp->data = NULL;
    uError("curl response is received, len:%" PRId64, pRsp->dataLen);
    return 0;
  }

  int64_t newDataSize = (int64_t)contLen * nmemb;
  int64_t size = pRsp->dataLen + newDataSize;

  if (pRsp->data == NULL) {
    pRsp->data = taosMemoryMalloc(size + 1);
    if (pRsp->data == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }
  } else {
    char *p = taosMemoryRealloc(pRsp->data, size + 1);
    if (p == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }

    pRsp->data = p;
  }

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data + pRsp->dataLen, pCont, newDataSize);

    pRsp->dataLen = size;
    pRsp->data[size] = 0;

    uDebugL("curl response is received, len:%" PRId64 ", content:%s", size, pRsp->data);
    return newDataSize;
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

  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 100) != 0) goto _OVER;

  uDebug("curl get request will sent, url:%s", url);
  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return code;
}

static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen, int32_t timeout) {
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  CURLcode           code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  if (curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POST, 1) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L) != 0) goto _OVER;

  uDebugL("curl post request will sent, url:%s len:%d content:%s", url, bufLen, buf);
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

SJson *taosAnalySendReqRetJson(const char *url, EAnalyHttpType type, SAnalyticBuf *pBuf, int64_t timeout) {
  int32_t   code = -1;
  char     *pCont = NULL;
  int64_t   contentLen;
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};

  if (type == ANALYTICS_HTTP_TYPE_GET) {
    if (taosCurlGetRequest(url, &curlRsp) != 0) {
      terrno = TSDB_CODE_ANA_URL_CANT_ACCESS;
      goto _OVER;
    }
  } else {
    code = taosAnalyBufGetCont(pBuf, &pCont, &contentLen);
    if (code != 0) {
      terrno = code;
      goto _OVER;
    }
    if (taosCurlPostRequest(url, &curlRsp, pCont, contentLen, timeout) != 0) {
      terrno = TSDB_CODE_ANA_URL_CANT_ACCESS;
      goto _OVER;
    }
  }

  if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
    terrno = TSDB_CODE_ANA_URL_RSP_IS_NULL;
    goto _OVER;
  }

  pJson = tjsonParse(curlRsp.data);
  if (pJson == NULL) {
    if (curlRsp.data[0] == '<') {
      terrno = TSDB_CODE_ANA_ANODE_RETURN_ERROR;
    } else {
      terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    }
    goto _OVER;
  }

_OVER:
  if (curlRsp.data != NULL) taosMemoryFreeClear(curlRsp.data);
  if (pCont != NULL) taosMemoryFree(pCont);
  return pJson;
}

static int32_t taosAnalyJsonBufGetCont(const char *fileName, char **ppCont, int64_t *pContLen) {
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

static int32_t taosAnalyJsonBufWriteOptInt(SAnalyticBuf *pBuf, const char *optName, int64_t optVal) {
  char    buf[64] = {0};
  int32_t bufLen = tsnprintf(buf, sizeof(buf), "\"%s\": %" PRId64 ",\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalyJsonBufWriteOptStr(SAnalyticBuf *pBuf, const char *optName, const char *optVal) {
  int32_t code = 0;
  int32_t keyLen = strlen(optName);
  int32_t valLen = strlen(optVal);

  int32_t totalLen = keyLen + valLen + 20;
  char *  buf = taosMemoryMalloc(totalLen);
  if (buf == NULL) {
    uError("failed to prepare the buffer for serializing the key/value info for analysis, len:%d, code:%s", totalLen,
           tstrerror(terrno));
    return terrno;
  }

  int32_t bufLen = tsnprintf(buf, totalLen, "\"%s\": \"%s\",\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    code = terrno;
  }

  taosMemoryFree(buf);
  return code;
}

static int32_t taosAnalyJsonBufWriteOptFloat(SAnalyticBuf *pBuf, const char *optName, float optVal) {
  char    buf[128] = {0};
  int32_t bufLen = tsnprintf(buf, sizeof(buf), "\"%s\": %f,\n", optName, optVal);
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalyJsonBufWriteStr(SAnalyticBuf *pBuf, const char *buf, int32_t bufLen) {
  if (bufLen <= 0) {
    bufLen = strlen(buf);
  }
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }
  return 0;
}

static int32_t taosAnalyJsonBufWriteStart(SAnalyticBuf *pBuf) { return taosAnalyJsonBufWriteStr(pBuf, "{\n", 0); }

static int32_t tsosAnalyJsonBufOpen(SAnalyticBuf *pBuf, int32_t numOfCols) {
  pBuf->filePtr = taosOpenFile(pBuf->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pBuf->filePtr == NULL) {
    return terrno;
  }

  pBuf->pCols = taosMemoryCalloc(numOfCols, sizeof(SAnalyticsColBuf));
  if (pBuf->pCols == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  pBuf->numOfCols = numOfCols;

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON) {
    return taosAnalyJsonBufWriteStart(pBuf);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SAnalyticsColBuf *pCol = &pBuf->pCols[i];
    snprintf(pCol->fileName, sizeof(pCol->fileName), "%s-c%d", pBuf->fileName, i);
    pCol->filePtr =
        taosOpenFile(pCol->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
    if (pCol->filePtr == NULL) {
      return terrno;
    }
  }

  return taosAnalyJsonBufWriteStart(pBuf);
}

static int32_t taosAnalyJsonBufWriteColMeta(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) {
  char buf[128] = {0};
  bool first = (colIndex == 0);
  bool last = (colIndex == pBuf->numOfCols - 1);

  if (first) {
    if (taosAnalyJsonBufWriteStr(pBuf, "\"schema\": [\n", 0) != 0) {
      return terrno;
    }
  }

  int32_t bufLen = tsnprintf(buf, sizeof(buf), "  [\"%s\", \"%s\", %d]%s\n", colName, tDataTypes[colType].name,
                             tDataTypes[colType].bytes, last ? "" : ",");
  if (taosWriteFile(pBuf->filePtr, buf, bufLen) != bufLen) {
    return terrno;
  }

  if (last) {
    if (taosAnalyJsonBufWriteStr(pBuf, "],\n", 0) != 0) {
      return terrno;
    }
  }

  return 0;
}

static int32_t taosAnalyJsonBufWriteDataBegin(SAnalyticBuf *pBuf) {
  return taosAnalyJsonBufWriteStr(pBuf, "\"data\": [\n", 0);
}

static int32_t taosAnalyJsonBufWriteStrUseCol(SAnalyticBuf *pBuf, const char *buf, int32_t bufLen, int32_t colIndex) {
  if (bufLen <= 0) {
    bufLen = strlen(buf);
  }

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON) {
    int32_t ret = taosWriteFile(pBuf->filePtr, buf, bufLen);
    if (ret != bufLen) {
      return terrno;
    }
  } else {
    int32_t ret = taosWriteFile(pBuf->pCols[colIndex].filePtr, buf, bufLen);
    if (ret != bufLen) {
      return terrno;
    }
  }

  return 0;
}

static int32_t taosAnalyJsonBufWriteColBegin(SAnalyticBuf *pBuf, int32_t colIndex) {
  return taosAnalyJsonBufWriteStrUseCol(pBuf, "[\n", 0, colIndex);
}

static int32_t taosAnalyJsonBufWriteColEnd(SAnalyticBuf *pBuf, int32_t colIndex) {
  if (colIndex == pBuf->numOfCols - 1) {
    return taosAnalyJsonBufWriteStrUseCol(pBuf, "\n]\n", 0, colIndex);

  } else {
    return taosAnalyJsonBufWriteStrUseCol(pBuf, "\n],\n", 0, colIndex);
  }
}

static int32_t taosAnalyJsonBufWriteColData(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) {
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
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", (*((int8_t *)colValue) == 1) ? 1 : 0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int8_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint8_t *)colValue);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int16_t *)colValue);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint16_t *)colValue);
      break;
    case TSDB_DATA_TYPE_INT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%d", *(int32_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%u", *(uint32_t *)colValue);
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%" PRId64, *(int64_t *)colValue);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%" PRIu64, *(uint64_t *)colValue);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%f", GET_FLOAT_VAL(colValue));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      bufLen += tsnprintf(buf + bufLen, sizeof(buf) - bufLen, "%f", GET_DOUBLE_VAL(colValue));
      break;
    default:
      buf[bufLen] = '\0';
  }

  pBuf->pCols[colIndex].numOfRows++;
  return taosAnalyJsonBufWriteStrUseCol(pBuf, buf, bufLen, colIndex);
}

static int32_t taosAnalyJsonBufWriteDataEnd(SAnalyticBuf *pBuf) {
  int32_t code = 0;
  char   *pCont = NULL;
  int64_t contLen = 0;

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalyticsColBuf *pCol = &pBuf->pCols[i];

      code = taosFsyncFile(pCol->filePtr);
      if (code != 0) return code;

      code = taosCloseFile(&pCol->filePtr);
      if (code != 0) return code;

      code = taosAnalyJsonBufGetCont(pBuf->pCols[i].fileName, &pCont, &contLen);
      if (code != 0) return code;

      code = taosAnalyJsonBufWriteStr(pBuf, pCont, contLen);
      if (code != 0) return code;

      taosMemoryFreeClear(pCont);
      contLen = 0;
    }
  }

  return taosAnalyJsonBufWriteStr(pBuf, "],\n", 0);
}

static int32_t taosAnalyJsonBufWriteEnd(SAnalyticBuf *pBuf) {
  int32_t code = taosAnalyJsonBufWriteOptInt(pBuf, "rows", pBuf->pCols[0].numOfRows);
  if (code != 0) return code;

  return taosAnalyJsonBufWriteStr(pBuf, "\"protocol\": 1.0\n}", 0);
}

int32_t taosAnalJsonBufClose(SAnalyticBuf *pBuf) {
  int32_t code = taosAnalyJsonBufWriteEnd(pBuf);
  if (code != 0) return code;

  if (pBuf->filePtr != NULL) {
    code = taosFsyncFile(pBuf->filePtr);
    if (code != 0) return code;
    code = taosCloseFile(&pBuf->filePtr);
    if (code != 0) return code;
  }

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalyticsColBuf *pCol = &pBuf->pCols[i];
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

void taosAnalyBufDestroy(SAnalyticBuf *pBuf) {
  if (pBuf->fileName[0] != 0) {
    if (pBuf->filePtr != NULL) (void)taosCloseFile(&pBuf->filePtr);
    // taosRemoveFile(pBuf->fileName);
    pBuf->fileName[0] = 0;
  }

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    for (int32_t i = 0; i < pBuf->numOfCols; ++i) {
      SAnalyticsColBuf *pCol = &pBuf->pCols[i];
      if (pCol->fileName[0] != 0) {
        if (pCol->filePtr != NULL) (void)taosCloseFile(&pCol->filePtr);
        if (taosRemoveFile(pCol->fileName) != 0) {
          uError("failed to remove file %s", pCol->fileName);
        }
        pCol->fileName[0] = 0;
      }
    }
  }

  taosMemoryFreeClear(pBuf->pCols);
  pBuf->numOfCols = 0;
}

int32_t tsosAnalyBufOpen(SAnalyticBuf *pBuf, int32_t numOfCols) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return tsosAnalyJsonBufOpen(pBuf, numOfCols);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteOptStr(SAnalyticBuf *pBuf, const char *optName, const char *optVal) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteOptStr(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteOptInt(SAnalyticBuf *pBuf, const char *optName, int64_t optVal) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteOptInt(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteOptFloat(SAnalyticBuf *pBuf, const char *optName, float optVal) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteOptFloat(pBuf, optName, optVal);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteColMeta(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteColMeta(pBuf, colIndex, colType, colName);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteDataBegin(SAnalyticBuf *pBuf) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteDataBegin(pBuf);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteColBegin(SAnalyticBuf *pBuf, int32_t colIndex) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteColBegin(pBuf, colIndex);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteColData(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteColData(pBuf, colIndex, colType, colValue);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteColEnd(SAnalyticBuf *pBuf, int32_t colIndex) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteColEnd(pBuf, colIndex);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufWriteDataEnd(SAnalyticBuf *pBuf) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufWriteDataEnd(pBuf);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

int32_t taosAnalyBufClose(SAnalyticBuf *pBuf) {
  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalJsonBufClose(pBuf);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

static int32_t taosAnalyBufGetCont(SAnalyticBuf *pBuf, char **ppCont, int64_t *pContLen) {
  *ppCont = NULL;
  *pContLen = 0;

  if (pBuf->bufType == ANALYTICS_BUF_TYPE_JSON || pBuf->bufType == ANALYTICS_BUF_TYPE_JSON_COL) {
    return taosAnalyJsonBufGetCont(pBuf->fileName, ppCont, pContLen);
  } else {
    return TSDB_CODE_ANA_BUF_INVALID_TYPE;
  }
}

// extract the timeout parameter
int64_t taosAnalysisParseTimout(SHashObj* pHashMap, const char* id) {
  int32_t code = 0;
  char* pTimeout = taosHashGet(pHashMap, ALGO_OPT_TIMEOUT_NAME, strlen(ALGO_OPT_TIMEOUT_NAME));
  if (pTimeout == NULL) {
    uDebug("%s not set the timeout val, set default:%d", id, ANALY_DEFAULT_TIMEOUT);
    return ANALY_DEFAULT_TIMEOUT;
  } else {
    int64_t t = taosStr2Int64(pTimeout, NULL, 10);
    if (t <= 0 || t > ANALY_MAX_TIMEOUT) {
      uDebug("%s timeout val:%" PRId64 "s is invalid (greater than 10min or less than 1s), use default:%dms", id, t,
             ANALY_DEFAULT_TIMEOUT);
      return ANALY_DEFAULT_TIMEOUT;
    }

    uDebug("%s timeout val is set to: %" PRId64 "s", id, t);
    return t;
  }
}

int32_t taosAnalysisParseAlgo(const char* pOpt, char* pAlgoName, char* pUrl, int32_t type, int32_t len, SHashObj* pHashMap, const char* id) {
  char* pAlgo = taosHashGet(pHashMap, ALGO_OPT_ALGO_NAME, strlen(ALGO_OPT_ALGO_NAME));
  if (pAlgo == NULL) {
    uError("%s failed to get analysis algorithm name from %s", id, pOpt);
    return TSDB_CODE_ANA_ALGO_NOT_FOUND;
  }

  tstrncpy(pAlgoName, pAlgo, taosHashGetValueSize(pAlgo) + 1);

  if (taosAnalyGetAlgoUrl(pAlgoName, type, pUrl, len) != 0) {
    uError("%s failed to get analysis algorithm url from %s", id, pAlgoName);
    return TSDB_CODE_ANA_ALGO_NOT_LOAD;
  }

  return 0;
}

int8_t taosAnalysisParseWncheck(SHashObj* pHashMap, const char* id) {
  char* pWncheck = taosHashGet(pHashMap, ALGO_OPT_WNCHECK_NAME, strlen(ALGO_OPT_WNCHECK_NAME));
  if (pWncheck != NULL) {
    int32_t v = (int32_t) taosStr2Int64(pWncheck, NULL, 10);
    uDebug("%s analysis wncheck:%d", id, v);
    return v;
  } else {
    uDebug("%s analysis wncheck not found, use default:%d", id, ANALY_FORECAST_DEFAULT_WNCHECK);
    return ANALY_FORECAST_DEFAULT_WNCHECK;
  }
}

#else

int32_t taosAnalyticsInit() { return 0; }
void    taosAnalyticsCleanup() {}
SJson  *taosAnalySendReqRetJson(const char *url, EAnalyHttpType type, SAnalyticBuf *pBuf, int64_t timeout) {
  return NULL;
}

int32_t taosAnalyGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen) { return 0; }
bool    taosAnalyGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen) { return true; }
int64_t taosAnalyGetVersion() { return 0; }
void    taosAnalyUpdate(int64_t newVer, SHashObj *pHash) {}

int32_t tsosAnalyBufOpen(SAnalyticBuf *pBuf, int32_t numOfCols) { return 0; }
int32_t taosAnalyBufWriteOptStr(SAnalyticBuf *pBuf, const char *optName, const char *optVal) { return 0; }
int32_t taosAnalyBufWriteOptInt(SAnalyticBuf *pBuf, const char *optName, int64_t optVal) { return 0; }
int32_t taosAnalyBufWriteOptFloat(SAnalyticBuf *pBuf, const char *optName, float optVal) { return 0; }
int32_t taosAnalyBufWriteColMeta(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName) {
  return 0;
}
int32_t taosAnalyBufWriteDataBegin(SAnalyticBuf *pBuf) { return 0; }
int32_t taosAnalyBufWriteColBegin(SAnalyticBuf *pBuf, int32_t colIndex) { return 0; }
int32_t taosAnalyBufWriteColData(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue) { return 0; }
int32_t taosAnalyBufWriteColEnd(SAnalyticBuf *pBuf, int32_t colIndex) { return 0; }
int32_t taosAnalyBufWriteDataEnd(SAnalyticBuf *pBuf) { return 0; }
int32_t taosAnalyBufClose(SAnalyticBuf *pBuf) { return 0; }
void    taosAnalyBufDestroy(SAnalyticBuf *pBuf) {}

const char   *taosAnalysisAlgoType(EAnalAlgoType algoType) { return 0; }
EAnalAlgoType taosAnalAlgoInt(const char *algoName) { return 0; }
const char   *taosAnalAlgoUrlStr(EAnalAlgoType algoType) { return 0; }

int64_t taosAnalysisParseTimout(SHashObj *pHashMap, const char *id) { return 0; }

int32_t taosAnalysisParseAlgo(const char *pOpt, char *pAlgoName, char *pUrl, int32_t type, int32_t len,
                              SHashObj *pHashMap, const char *id) {
  return 0;
}

int8_t taosAnalysisParseWncheck(SHashObj* pHashMap, const char* id) { return 0;}
int32_t taosAnalyGetOpts(const char *pOption, SHashObj **pOptHash) { return 0;}

#endif
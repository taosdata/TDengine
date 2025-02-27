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

#include <curl/curl.h>

#include "executorInt.h"
#include "streamsession.h"
#include "tjson.h"

static int32_t buildSessionResultSql(SSHashObj* pRangeMap, char** ppSql, int32_t len) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  // todo(liuyao) add
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static size_t parseResult(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  qDebug("stream client response is received, contLen:%" PRId64 ", nmemb:%" PRId64 ", pCont:%p", contLen, nmemb, pCont);
  QUERY_CHECK_CONDITION(contLen > 0, code, lino, _end, TSDB_CODE_FAILED);
  QUERY_CHECK_CONDITION(nmemb > CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  QUERY_CHECK_NULL(pCont, code, lino, _end, TSDB_CODE_FAILED);

  (*(SJson**)userdata) = tjsonParse(pCont);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doProcessSql(SStreamClientParam* pParam, SJson** ppJsonResult) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  CURL* pCurl = curl_easy_init();
  QUERY_CHECK_NULL(pCurl, code, lino, _end, TSDB_CODE_FAILED);

  CURLcode curlRes = curl_easy_setopt(pCurl, CURLOPT_URL, pParam->pUrl);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  struct curl_slist* pHeaders = NULL;
  pHeaders = curl_slist_append(pHeaders, "Content-Type:application/json;charset=UTF-8");
  QUERY_CHECK_NULL(pHeaders, code, lino, _end, TSDB_CODE_FAILED);
  pHeaders = curl_slist_append(pHeaders, pParam->pAuth);
  QUERY_CHECK_NULL(pHeaders, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_HTTPHEADER, pHeaders);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_POSTFIELDS, pParam->pSql);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_FOLLOWLOCATION, 1L);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_WRITEFUNCTION, parseResult);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_WRITEDATA, ppJsonResult);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_perform(pCurl);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

_end:
  if (pHeaders != NULL) {
    curl_slist_free_all(pHeaders);
  }
  if (pCurl != NULL) {
    curl_easy_cleanup(pCurl);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTransformResult(const SJson* pJsonResult, SArray** ppRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  SJson*  jArray = tjsonGetObjectItem(pJsonResult, "data");
  QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);

  int32_t rows = tjsonGetArraySize(jArray);
  if (rows > 0) {
    *ppRangeRes = taosArrayInit(rows, POINTER_BYTES);
    QUERY_CHECK_NULL(*ppRangeRes, code, lino, _end, terrno);

    for (int32_t i = 0; i < rows; ++i) {
      SJson* pRow = tjsonGetArrayItem(jArray, i);
      QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);
      int32_t cols = tjsonGetArraySize(pRow);
      if (cols > 0) {
        SArray* pRowArray = taosArrayInit(cols, sizeof(int64_t));
        taosArrayPush((*ppRangeRes), pRowArray);
        QUERY_CHECK_NULL(*ppRangeRes, code, lino, _end, terrno);
        for (int32_t j = 0; j < cols; ++j) {
          SJson* pCell = tjsonGetArrayItem(pRow, j);
          int64_t data = 0;
          tjsonGetObjectValueBigInt(pCell, &data);
          taosArrayPush(pRowArray, &data);
        }
      }
    }
  } else {
    *ppRangeRes = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamClientGetResultRange(SStreamClientParam* pParam, SSHashObj* pRangeMap, SArray** ppRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char* pSql = NULL;
  code = buildSessionResultSql(pRangeMap, &pParam->pSql, pParam->sqlLen);
  QUERY_CHECK_CODE(code, lino, _end);

  SJson* pJsRes = NULL;
  code = doProcessSql(pParam, &pJsRes);
  QUERY_CHECK_CODE(code, lino, _end);
  code = doTransformResult(pJsRes, ppRangeRes);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
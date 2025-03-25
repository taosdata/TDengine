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

#ifndef WINDOWS

#include <curl/curl.h>

#endif

#include "executorInt.h"
#include "streamsession.h"
#include "tjson.h"

#ifndef WINDOWS

static int32_t buildSessionResultSql(SSHashObj* pRangeMap, SStreamRecParam* pParam, bool* pEnd) {
  int64_t prevLen = 0;
  int64_t len = 0;
  (void)memset(pParam->pSql, 0, pParam->sqlCapcity);
  *pEnd = true;
  while ((pParam->pIteData = tSimpleHashIterate(pRangeMap, pParam->pIteData, &pParam->iter)) != NULL) {
    SSessionKey* pKey = tSimpleHashGetKey(pParam->pIteData, NULL);
    if (prevLen > 0) {
      len += tsnprintf(pParam->pSql + len, pParam->sqlCapcity - len, " union all ");
      if (len >= pParam->sqlCapcity - 1) {
        *pEnd = false;
        break;
      }
    }
    len += tsnprintf(pParam->pSql + len, pParam->sqlCapcity - len,
                     "select %" PRId64 ", %" PRId64 ", `%s` , cast(`%s` as bigint), cast( (`%s` - %" PRId64
                     ") as bigint) from %s where `%s` == %" PRIu64 " and ( (`%s` - %" PRId64 ") >= %" PRId64
                     " and `%s` <= %" PRId64 " )",
                     pKey->win.skey, pKey->win.ekey, pParam->pGroupIdName, pParam->pWstartName, pParam->pWendName,
                     pParam->gap, pParam->pStbFullName, pParam->pGroupIdName, pKey->groupId, pParam->pWendName,
                     pParam->gap, pKey->win.skey, pParam->pWstartName, pKey->win.ekey);
    if (len >= pParam->sqlCapcity - 1) {
      *pEnd = false;
      break;
    } else {
      prevLen = len;
    }
  }
  pParam->pSql[prevLen + 1] = '\0';
  return TSDB_CODE_SUCCESS;
}

static size_t parseResult(char* pCont, size_t contLen, size_t nmemb, void* userdata) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  qDebug("stream client response is received, contLen:%d, nmemb:%d, pCont:%p", (int32_t)contLen, (int32_t)nmemb, pCont);
  QUERY_CHECK_CONDITION(contLen > 0, code, lino, _end, TSDB_CODE_FAILED);
  QUERY_CHECK_CONDITION(nmemb > CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  QUERY_CHECK_NULL(pCont, code, lino, _end, TSDB_CODE_FAILED);

  qTrace("===stream=== result:%s", pCont);
  (*(SJson**)userdata) = tjsonParse(pCont);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return 0;
  }
  return contLen * nmemb;
}

static int32_t doProcessSql(SStreamRecParam* pParam, SJson** ppJsonResult) {
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

  qDebug("===stream=== sql:%s", pParam->pSql);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_FOLLOWLOCATION, 1L);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_WRITEFUNCTION, parseResult);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_setopt(pCurl, CURLOPT_WRITEDATA, ppJsonResult);
  QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);

  curlRes = curl_easy_perform(pCurl);
  if (curlRes != CURLE_OK) {
    qError("error: unable to request data from %s.since %s. res code:%d", pParam->pUrl, curl_easy_strerror(curlRes),
           (int32_t)curlRes);
    QUERY_CHECK_CONDITION(curlRes == CURLE_OK, code, lino, _end, TSDB_CODE_FAILED);
  }

_end:
  if (pHeaders != NULL) {
    curl_slist_free_all(pHeaders);
  }
  if (pCurl != NULL) {
    curl_easy_cleanup(pCurl);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s. error code:%d", __func__, lino, tstrerror(code), curlRes);
  }
  return code;
}

static int32_t doTransformResult(const SJson* pJsonResult, SArray* pRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SJson* jArray = tjsonGetObjectItem(pJsonResult, "data");
  QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);

  int32_t rows = tjsonGetArraySize(jArray);
  if (rows > 0) {
    for (int32_t i = 0; i < rows; ++i) {
      SJson* pRow = tjsonGetArrayItem(jArray, i);
      QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);
      int32_t cols = tjsonGetArraySize(pRow);
      if (cols > 0) {
        SArray* pRowArray = taosArrayInit(cols, sizeof(int64_t));
        QUERY_CHECK_NULL(pRowArray, code, lino, _end, terrno);
        void* tmpRes = taosArrayPush(pRangeRes, &pRowArray);
        QUERY_CHECK_NULL(tmpRes, code, lino, _end, terrno);
        for (int32_t j = 0; j < cols; ++j) {
          SJson*  pCell = tjsonGetArrayItem(pRow, j);
          int64_t data = 0;
          tjsonGetObjectValueBigInt(pCell, &data);
          tmpRes = taosArrayPush(pRowArray, &data);
          QUERY_CHECK_NULL(tmpRes, code, lino, _end, terrno);
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamClientGetResultRange(SStreamRecParam* pParam, SSHashObj* pRangeMap, SArray* pRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pParam->pIteData = NULL;
  pParam->iter = 0;
  bool isEnd = false;
  while (!isEnd) {
    code = buildSessionResultSql(pRangeMap, pParam, &isEnd);
    QUERY_CHECK_CODE(code, lino, _end);

    SJson* pJsRes = NULL;
    code = doProcessSql(pParam, &pJsRes);
    QUERY_CHECK_CODE(code, lino, _end);
    code = doTransformResult(pJsRes, pRangeRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildFillResultSql(SWinKey* pKey, SStreamRecParam* pParam) {
  (void)memset(pParam->pSql, 0, pParam->sqlCapcity);
  (void)tsnprintf(
      pParam->pSql, pParam->sqlCapcity,
      "(select *, cast(`%s` as bigint) from %s where `%s` == %" PRIu64 " and `%s` < %" PRId64
      " and `%s` ==0 order by 1 desc limit 1) union all (select *, cast(`%s` as bigint) from  %s where `%s` == %" PRIu64
      " and `%s` > %" PRId64 " and `%s` ==0 order by 1 asc limit 1)",
      pParam->pWstartName, pParam->pStbFullName, pParam->pGroupIdName, pKey->groupId, pParam->pWstartName, pKey->ts,
      pParam->pIsWindowFilledName, pParam->pWstartName, pParam->pStbFullName, pParam->pGroupIdName, pKey->groupId,
      pParam->pWstartName, pKey->ts, pParam->pIsWindowFilledName);

  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToDataCell(const SJson* pJson, SResultCellData* pCell) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (IS_INTEGER_TYPE(pCell->type)) {
    int64_t data = 0;
    tjsonGetObjectValueBigInt(pJson, &data);
    SET_TYPED_DATA(pCell->pData, pCell->type, data);
  } else if (IS_FLOAT_TYPE(pCell->type)) {
    double data = 0;
    tjsonGetObjectValueDouble(pJson, &data);
    SET_TYPED_DATA(pCell->pData, pCell->type, data);
  } else if (IS_TIMESTAMP_TYPE(pCell->type)) {
  } else if (IS_BOOLEAN_TYPE(pCell->type)) {
    bool data = cJSON_IsTrue(pJson) ? true : false;
    SET_TYPED_DATA(pCell->pData, pCell->type, data);
  } else {
    char* pStr = cJSON_GetStringValue(pJson);
    STR_TO_VARSTR(pCell->pData, pStr);
  }

  return code;
}

static int32_t getColumnIndex(SSHashObj* pMap, int32_t colId) {
  void* pVal  = tSimpleHashGet(pMap, &colId, sizeof(int32_t));
  if (pVal == NULL) {
    return -1;
  }
  return *(int32_t*)pVal;
}

static int32_t doTransformFillResult(const SJson* pJsonResult, SArray* pRangeRes, void* pEmptyRow, int32_t size,
                                     int32_t* pOffsetInfo, int32_t numOfCols, SSHashObj* pMap) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SJson* jArray = tjsonGetObjectItem(pJsonResult, "data");
  QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);

  int32_t rows = tjsonGetArraySize(jArray);
  if (rows > 0) {
    for (int32_t i = 0; i < rows; ++i) {
      SJson* pRow = tjsonGetArrayItem(jArray, i);
      QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);
      int32_t        cols = tjsonGetArraySize(pRow);
      SSliceRowData* pRowData = taosMemoryCalloc(1, sizeof(TSKEY) + size);
      pRowData->key = INT64_MIN;
      memcpy(pRowData->pRowVal, pEmptyRow, size);
      int32_t colOffset = 0;
      for (int32_t j = 0; j < numOfCols; ++j) {
        SResultCellData* pDataCell = getSliceResultCell((SResultCellData*)pRowData->pRowVal, j, pOffsetInfo);
        QUERY_CHECK_NULL(pDataCell, code, lino, _end, TSDB_CODE_FAILED);

        int32_t colIndex = getColumnIndex(pMap, j);
        if (colIndex == -1 || colIndex >= cols) {
          qDebug("invalid result columm index:%d", colIndex);
          pDataCell->isNull = true;
          continue;
        }

        SJson* pJsonCell = tjsonGetArrayItem(pRow, colIndex);
        QUERY_CHECK_NULL(pJsonCell, code, lino, _end, TSDB_CODE_FAILED);

        code = jsonToDataCell(pJsonCell, pDataCell);
        QUERY_CHECK_CODE(code, lino, _end);
      }
      if (cols > 1) {
        SJson* pJsonSKey = tjsonGetArrayItem(pRow, cols - 1);
        QUERY_CHECK_NULL(pJsonSKey, code, lino, _end, TSDB_CODE_FAILED);
        tjsonGetObjectValueBigInt(pJsonSKey, &pRowData->key);
      }

      void* pTempRes = taosArrayPush(pRangeRes, &pRowData);
      QUERY_CHECK_NULL(pTempRes, code, lino, _end, terrno);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamClientGetFillRange(SStreamRecParam* pParam, SWinKey* pKey, SArray* pRangeRes, void* pEmptyRow, int32_t size,
                                 int32_t* pOffsetInfo, int32_t numOfCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  code = buildFillResultSql(pKey, pParam);
  QUERY_CHECK_CODE(code, lino, _end);

  SJson* pJsRes = NULL;
  code = doProcessSql(pParam, &pJsRes);
  QUERY_CHECK_CODE(code, lino, _end);
  code = doTransformFillResult(pJsRes, pRangeRes, pEmptyRow, size, pOffsetInfo, numOfCols, pParam->pColIdMap);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamClientCheckCfg(SStreamRecParam* pParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  const char* pTestSql = "select name, ntables, status from information_schema.ins_databases;";
  (void)memset(pParam->pSql, 0, pParam->sqlCapcity);
  tstrncpy(pParam->pSql, pTestSql, pParam->sqlCapcity);

  SJson* pJsRes = NULL;
  code = doProcessSql(pParam, &pJsRes);
  QUERY_CHECK_CODE(code, lino, _end);
  SJson* jArray = tjsonGetObjectItem(pJsRes, "data");
  QUERY_CHECK_NULL(jArray, code, lino, _end, TSDB_CODE_FAILED);

  int32_t rows = tjsonGetArraySize(jArray);
  if (rows < 2) {
    code = TSDB_CODE_INVALID_CFG_VALUE;
    qError("invalid taos adapter config value");
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

#else

int32_t streamClientGetResultRange(SStreamRecParam* pParam, SSHashObj* pRangeMap, SArray* pRangeRes) {
  return TSDB_CODE_FAILED;
}
int32_t streamClientGetFillRange(SStreamRecParam* pParam, SWinKey* pKey, SArray* pRangeRes, void* pEmptyRow, int32_t size, int32_t* pOffsetInfo, int32_t numOfCols) {
  return TSDB_CODE_FAILED;
}

int32_t streamClientCheckCfg(SStreamRecParam* pParam) {
    return TSDB_CODE_FAILED;
}

#endif
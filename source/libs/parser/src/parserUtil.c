#include "parserUtil.h"
#include "taoserror.h"
#include "tutil.h"

int32_t parserValidateIdToken(SToken* pToken) {
  if (pToken == NULL || pToken->z == NULL || pToken->type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // It is a single part token, not a complex type
    if (isNumber(pToken)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    strntolower(pToken->z, pToken->z, pToken->n);
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_SPACE) {
      pToken->n = (uint32_t)strtrim(pToken->z);
    }

    pToken->n = tGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = (uint32_t)(oldLen - (sep - pStr) - 1);
    int32_t len = tGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      uint32_t offset = (uint32_t)(pToken->z - (pStr + firstPartLen + 1));
      memset(pToken->z + pToken->n - offset, ' ', offset);
    }

    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;

    strntolower(pToken->z, pToken->z, pToken->n);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parserSetInvalidOperatorMsg(char* dst, int32_t dstBufLen, const char* msg) {
  strncpy(dst, msg, dstBufLen);
  return TSDB_CODE_TSC_INVALID_OPERATION;
}
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

#include "tunit.h"

#define UNIT_SIZE_CONVERT_FACTOR 1024LL
#define UNIT_ONE_KIBIBYTE        UNIT_SIZE_CONVERT_FACTOR
#define UNIT_ONE_MEBIBYTE        (UNIT_ONE_KIBIBYTE * UNIT_SIZE_CONVERT_FACTOR)
#define UNIT_ONE_GIBIBYTE        (UNIT_ONE_MEBIBYTE * UNIT_SIZE_CONVERT_FACTOR)
#define UNIT_ONE_TEBIBYTE        (UNIT_ONE_GIBIBYTE * UNIT_SIZE_CONVERT_FACTOR)
#define UNIT_ONE_PEBIBYTE        (UNIT_ONE_TEBIBYTE * UNIT_SIZE_CONVERT_FACTOR)
#define UNIT_ONE_EXBIBYTE        (UNIT_ONE_PEBIBYTE * UNIT_SIZE_CONVERT_FACTOR)

static int32_t parseCfgIntWithUnit(const char* str, double *res) {
  double val, temp = (double)INT64_MAX;
  char*  endPtr;
  errno = 0;
  val = taosStr2Int64(str, &endPtr, 0);
  if (*endPtr == '.' || errno == ERANGE) {
    errno = 0;
    val = taosStr2Double(str, &endPtr);
  }
  if (endPtr == str || errno == ERANGE || isnan(val)) {
    terrno = TSDB_CODE_INVALID_CFG_VALUE;
    return -1;
  }
  while (isspace((unsigned char)*endPtr)) endPtr++;
  uint64_t factor = 1;
  if (*endPtr != '\0') {
    switch (*endPtr) {
      case 'P':
      case 'p': {
        temp /= UNIT_ONE_PEBIBYTE;
        factor = UNIT_ONE_PEBIBYTE;
      } break;
      case 'T':
      case 't': {
        temp /= UNIT_ONE_TEBIBYTE;
        factor = UNIT_ONE_TEBIBYTE;
      } break;
      case 'G':
      case 'g': {
        temp /= UNIT_ONE_GIBIBYTE;
        factor = UNIT_ONE_GIBIBYTE;
      } break;
      case 'M':
      case 'm': {
        temp /= UNIT_ONE_MEBIBYTE;
        factor = UNIT_ONE_MEBIBYTE;
      } break;
      case 'K':
      case 'k': {
        temp /= UNIT_ONE_KIBIBYTE;
        factor = UNIT_ONE_KIBIBYTE;
      } break;
      default:
        terrno = TSDB_CODE_INVALID_CFG_VALUE;
        return -1;
    }
    if ((val > 0 && val > temp) || (val < 0 && val < -temp)) {
      terrno = TSDB_CODE_OUT_OF_RANGE;
      return -1;
    }
    endPtr++;
    val *= factor;
  }
  while (isspace((unsigned char)*endPtr)) endPtr++;
  if (*endPtr) {
    terrno = TSDB_CODE_INVALID_CFG_VALUE;
    return -1;
  }
  val = rint(val);
  *res = val;
  return TSDB_CODE_SUCCESS;
}

int32_t taosStrHumanToInt64(const char* str, int64_t *out) {
  double res;
  int32_t code = parseCfgIntWithUnit(str, &res);
  if (code == TSDB_CODE_SUCCESS) *out = (int64_t)res;
  return code;
}

#ifdef BUILD_NO_CALL
void taosInt64ToHumanStr(int64_t val, char* outStr) {
  if (((val >= UNIT_ONE_EXBIBYTE) || (-val >= UNIT_ONE_EXBIBYTE)) && ((val % UNIT_ONE_EXBIBYTE) == 0)) {
    sprintf(outStr, "%qdE", (long long)val / UNIT_ONE_EXBIBYTE);
  } else if (((val >= UNIT_ONE_PEBIBYTE) || (-val >= UNIT_ONE_PEBIBYTE)) && ((val % UNIT_ONE_PEBIBYTE) == 0)) {
    sprintf(outStr, "%qdP", (long long)val / UNIT_ONE_PEBIBYTE);
  } else if (((val >= UNIT_ONE_TEBIBYTE) || (-val >= UNIT_ONE_TEBIBYTE)) && ((val % UNIT_ONE_TEBIBYTE) == 0)) {
    sprintf(outStr, "%qdT", (long long)val / UNIT_ONE_TEBIBYTE);
  } else if (((val >= UNIT_ONE_GIBIBYTE) || (-val >= UNIT_ONE_GIBIBYTE)) && ((val % UNIT_ONE_GIBIBYTE) == 0)) {
    sprintf(outStr, "%qdG", (long long)val / UNIT_ONE_GIBIBYTE);
  } else if (((val >= UNIT_ONE_MEBIBYTE) || (-val >= UNIT_ONE_MEBIBYTE)) && ((val % UNIT_ONE_MEBIBYTE) == 0)) {
    sprintf(outStr, "%qdM", (long long)val / UNIT_ONE_MEBIBYTE);
  } else if (((val >= UNIT_ONE_KIBIBYTE) || (-val >= UNIT_ONE_KIBIBYTE)) && ((val % UNIT_ONE_KIBIBYTE) == 0)) {
    sprintf(outStr, "%qdK", (long long)val / UNIT_ONE_KIBIBYTE);
  } else
    sprintf(outStr, "%qd", (long long)val);
}
#endif

int32_t taosStrHumanToInt32(const char* str, int32_t* out) {
  double res;
  int32_t code = parseCfgIntWithUnit(str, &res);
  if (code == TSDB_CODE_SUCCESS) {
    if (res < INT32_MIN || res > INT32_MAX) {
      terrno = TSDB_CODE_OUT_OF_RANGE;
      return -1;
    }
    *out = (int32_t)res;
  }
  return code;
}

#ifdef BUILD_NO_CALL
void taosInt32ToHumanStr(int32_t val, char* outStr) {
  if (((val >= UNIT_ONE_GIBIBYTE) || (-val >= UNIT_ONE_GIBIBYTE)) && ((val % UNIT_ONE_GIBIBYTE) == 0)) {
    sprintf(outStr, "%qdG", (long long)val / UNIT_ONE_GIBIBYTE);
  } else if (((val >= UNIT_ONE_MEBIBYTE) || (-val >= UNIT_ONE_MEBIBYTE)) && ((val % UNIT_ONE_MEBIBYTE) == 0)) {
    sprintf(outStr, "%qdM", (long long)val / UNIT_ONE_MEBIBYTE);
  } else if (((val >= UNIT_ONE_KIBIBYTE) || (-val >= UNIT_ONE_KIBIBYTE)) && ((val % UNIT_ONE_KIBIBYTE) == 0)) {
    sprintf(outStr, "%qdK", (long long)val / UNIT_ONE_KIBIBYTE);
  } else
    sprintf(outStr, "%qd", (long long)val);
}
#endif

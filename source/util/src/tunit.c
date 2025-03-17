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

static int32_t parseCfgIntWithUnit(const char* str, int64_t* res) {
  double val = 0, temp = (double)INT64_MAX;
  char*  endPtr;
  bool   useDouble = false;
  SET_ERRNO(0);
  int64_t int64Val = taosStr2Int64(str, &endPtr, 0);
  if (*endPtr == '.' || ERRNO == ERANGE) {
    SET_ERRNO(0);
    val = taosStr2Double(str, &endPtr);
    useDouble = true;
  }
  if (endPtr == str || ERRNO == ERANGE || isnan(val)) {
    return terrno = TSDB_CODE_INVALID_CFG_VALUE;
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
        return terrno = TSDB_CODE_INVALID_CFG_VALUE;
    }
    endPtr++;

    if ((val > 0 && val > temp) || (val < 0 && val < -temp)) {
      return terrno = TSDB_CODE_OUT_OF_RANGE;
    }
    val *= factor;
    int64Val *= factor;
  }
  while (isspace((unsigned char)*endPtr)) endPtr++;
  if (*endPtr) {
    return terrno = TSDB_CODE_INVALID_CFG_VALUE;
  }
  if (useDouble) {
    val = rint(val);
    if ((val > 0 && val >= (double)INT64_MAX) || (val < 0 && val <= (double)INT64_MIN)) {
      return terrno = TSDB_CODE_OUT_OF_RANGE;
    } else {
      *res = (int64_t)val;
    }
  } else {
    *res = int64Val;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t taosStrHumanToInt64(const char* str, int64_t* out) {
  int64_t res;
  int32_t code = parseCfgIntWithUnit(str, &res);
  if (code == TSDB_CODE_SUCCESS) *out = (int64_t)res;
  return code;
}

int32_t taosStrHumanToInt32(const char* str, int32_t* out) {
  int64_t res;
  int32_t code = parseCfgIntWithUnit(str, &res);
  if (code == TSDB_CODE_SUCCESS) {
    if (res < INT32_MIN || res > INT32_MAX) {
      return terrno = TSDB_CODE_OUT_OF_RANGE;
    }
    *out = (int32_t)res;
  }
  return code;
}

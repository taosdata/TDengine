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

int64_t taosStrHumanToInt64(const char* str) {
  size_t sLen = strlen(str);
  if (sLen < 2) return atoll(str);

  int64_t val = 0;

  char* strNoUnit = NULL;
  char  unit = str[sLen - 1];
  if ((unit == 'P') || (unit == 'p')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_PEBIBYTE;
  } else if ((unit == 'T') || (unit == 't')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_TEBIBYTE;
  } else if ((unit == 'G') || (unit == 'g')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_GIBIBYTE;
  } else if ((unit == 'M') || (unit == 'm')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_MEBIBYTE;
  } else if ((unit == 'K') || (unit == 'k')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_KIBIBYTE;
  } else {
    val = atoll(str);
  }

  taosMemoryFree(strNoUnit);
  return val;
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

int32_t taosStrHumanToInt32(const char* str) {
  size_t sLen = strlen(str);
  if (sLen < 2) return atoll(str);

  int32_t val = 0;

  char* strNoUnit = NULL;
  char  unit = str[sLen - 1];
  if ((unit == 'G') || (unit == 'g')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_GIBIBYTE;
  } else if ((unit == 'M') || (unit == 'm')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_MEBIBYTE;
  } else if ((unit == 'K') || (unit == 'k')) {
    strNoUnit = taosMemoryCalloc(sLen, 1);
    memcpy(strNoUnit, str, sLen - 1);

    val = atof(strNoUnit) * UNIT_ONE_KIBIBYTE;
  } else {
    val = atoll(str);
  }

  taosMemoryFree(strNoUnit);
  return val;
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

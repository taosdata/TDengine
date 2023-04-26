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
#include "tversion.h"
#include "taoserror.h"

int32_t taosVersionStrToInt(const char *vstr, int32_t *vint) {
  if (vstr == NULL) {
    terrno = TSDB_CODE_INVALID_VERSION_STRING;
    return -1;
  }

  int32_t vnum[4] = {0};
  int32_t len = strlen(vstr);
  char    tmp[16] = {0};
  int32_t vpos = 0;

  for (int32_t spos = 0, tpos = 0; spos < len && vpos < 4; ++spos) {
    if (vstr[spos] != '.') {
      tmp[spos - tpos] = vstr[spos];
    } else {
      vnum[vpos] = atoi(tmp);
      memset(tmp, 0, sizeof(tmp));
      vpos++;
      tpos = spos + 1;
    }
  }

  if ('\0' != tmp[0] && vpos < 4) {
    vnum[vpos] = atoi(tmp);
  }

  if (vnum[0] <= 0) {
    terrno = TSDB_CODE_INVALID_VERSION_STRING;
    return -1;
  }

  *vint = vnum[0] * 1000000 + vnum[1] * 10000 + vnum[2] * 100 + vnum[3];
  return 0;
}

int32_t taosVersionIntToStr(int32_t vint, char *vstr, int32_t len) {
  int32_t s1 = (vint % 100000000) / 1000000;
  int32_t s2 = (vint % 1000000) / 10000;
  int32_t s3 = (vint % 10000) / 100;
  int32_t s4 = vint % 100;
  if (s1 <= 0) {
    terrno = TSDB_CODE_INVALID_VERSION_NUMBER;
    return -1;
  }

  snprintf(vstr, len, "%02d.%02d.%02d.%02d", s1, s2, s3, s4);
  return 0;
}

int32_t taosCheckVersionCompatible(int32_t clientVer, int32_t serverVer, int32_t comparedSegments) {
  switch (comparedSegments) {
    case 4:
      break;
    case 3:
      clientVer /= 100;
      serverVer /= 100;
      break;
    case 2:
      clientVer /= 10000;
      serverVer /= 10000;
      break;
    case 1:
      clientVer /= 1000000;
      serverVer /= 1000000;
      break;
    default:
      terrno = TSDB_CODE_INVALID_VERSION_NUMBER;
      return -1;
  }

  if (clientVer == serverVer) {
    return 0;
  } else {
    terrno = TSDB_CODE_VERSION_NOT_COMPATIBLE;
    return -1;
  }
}

int32_t taosCheckVersionCompatibleFromStr(const char *pClientVersion, const char *pServerVersion,
                                          int32_t comparedSegments) {
  int32_t clientVersion = 0;
  int32_t serverVersion = 0;
  int32_t code = taosVersionStrToInt(pClientVersion, &clientVersion);
  if (TSDB_CODE_SUCCESS == code) {
    code = taosVersionStrToInt(pServerVersion, &serverVersion);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = taosCheckVersionCompatible(clientVersion, serverVersion, comparedSegments);
  }
  if (TSDB_CODE_SUCCESS != code) {
    code = terrno;
  }
  return code;
}
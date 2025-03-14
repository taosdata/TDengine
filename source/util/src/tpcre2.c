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

#ifdef USE_PRCE2
#include "tpcre2.h"

int32_t doRegComp(pcre2_code** ppRegex, pcre2_match_data** ppMatchData, const char* pattern) {
  uint32_t   options = PCRE2_CASELESS;
  int        errorcode;
  PCRE2_SIZE erroroffset;

  pcre2_code*       pRegex = NULL;
  pcre2_match_data* pMatchData = NULL;

  pRegex = pcre2_compile((PCRE2_SPTR8)pattern, PCRE2_ZERO_TERMINATED, options, &errorcode, &erroroffset, NULL);
  if (pRegex == NULL) {
    PCRE2_UCHAR buffer[256];
    (void)pcre2_get_error_message(errorcode, buffer, sizeof(buffer));
    return -1;
  }

  pMatchData = pcre2_match_data_create_from_pattern(pRegex, NULL);
  if (pMatchData == NULL) {
    pcre2_code_free(pRegex);
    return -1;
  }

  *ppRegex = pRegex;
  *ppMatchData = pMatchData;

  return 0;
}

int32_t doRegExec(const char* pString, pcre2_code* pRegex, pcre2_match_data* pMatchData) {
  int32_t ret = 0;
  ret = pcre2_match(pRegex, (PCRE2_SPTR)pString, PCRE2_ZERO_TERMINATED, 0, 0, pMatchData, NULL);
  if (ret < 0) {
    PCRE2_UCHAR buffer[256];
    (void)pcre2_get_error_message(ret, buffer, sizeof(buffer));
    return 1;
  }

  return (ret > 0) ? 0 : 1;
}

void destroyRegexes(pcre2_code* pWktRegex, pcre2_match_data* pWktMatchData) {
  pcre2_code_free(pWktRegex);
  pcre2_match_data_free(pWktMatchData);
}
#endif  // USE_PRCE2

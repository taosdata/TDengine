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

#ifndef _TD_ULIT_PCRE2_H_
#define _TD_ULIT_PCRE2_H_

#define PCRE2_CODE_UNIT_WIDTH 8
#include "pcre2.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t doRegComp(pcre2_code** ppRegex, pcre2_match_data** ppMatchData, const char* pattern);
int32_t doRegExec(const char* pString, pcre2_code* pRegex, pcre2_match_data* pMatchData);
void    destroyRegexes(pcre2_code* pWktRegex, pcre2_match_data* pWktMatchData);

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_PAGEDBUF_H_

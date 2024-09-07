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

#ifndef _TD_UTIL_FUNC_H_
#define _TD_UTIL_FUNC_H_

#include "os.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t taosFuncInit();
void    taosFuncCleanup();
int32_t taosSendGetRequest(const char *url, char **ppCont, int32_t *pContLen);
int32_t taosSendPostRequest(const char *url, char **ppCont, int32_t *pContLen);

const char *taosFuncStr(EAFuncType type);
EAFuncType  taosFuncInt(const char *name);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_FUNC_H_*/

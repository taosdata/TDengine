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

#ifndef TDENGINE_TMISCE_H
#define TDENGINE_TMISCE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tmsg.h"

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

#define GET_ACTIVE_EP(_eps) (&((_eps)->eps[(_eps)->inUse]))

int32_t epsetToStr(const SEpSet* pEpSet, char* pBuf, int32_t len);
int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp);
int32_t addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

bool   isEpsetEqual(const SEpSet* s1, const SEpSet* s2);
void   epsetAssign(SEpSet* dst, const SEpSet* pSrc);
void   updateEpSet_s(SCorEpSet* pEpSet, SEpSet* pNewEpSet);
SEpSet getEpSet_s(SCorEpSet* pEpSet);
void   epsetSort(SEpSet* pEpSet);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TMISCE_H

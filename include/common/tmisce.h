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

#define EPSET_TO_STR(_eps, tbuf)                                                                                       \
  do {                                                                                                                 \
    int len = snprintf((tbuf), sizeof(tbuf), "epset:{");                                                               \
    for (int _i = 0; _i < (_eps)->numOfEps; _i++) {                                                                    \
      if (_i == (_eps)->numOfEps - 1) {                                                                                \
        len +=                                                                                                         \
            snprintf((tbuf) + len, sizeof(tbuf) - len, "%d. %s:%d", _i, (_eps)->eps[_i].fqdn, (_eps)->eps[_i].port);   \
      } else {                                                                                                         \
        len +=                                                                                                         \
            snprintf((tbuf) + len, sizeof(tbuf) - len, "%d. %s:%d, ", _i, (_eps)->eps[_i].fqdn, (_eps)->eps[_i].port); \
      }                                                                                                                \
    }                                                                                                                  \
    len += snprintf((tbuf) + len, sizeof(tbuf) - len, "}, inUse:%d", (_eps)->inUse);                                   \
  } while (0);

int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp);
void    addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

bool   isEpsetEqual(const SEpSet* s1, const SEpSet* s2);
void   epsetAssign(SEpSet* dst, const SEpSet* pSrc);
void   updateEpSet_s(SCorEpSet* pEpSet, SEpSet* pNewEpSet);
SEpSet getEpSet_s(SCorEpSet* pEpSet);
void   epsetSort(SEpSet* pEpSet);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TMISCE_H

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

#ifndef _TD_MND_SHOW_H_
#define _TD_MND_SHOW_H_

#include "mndInt.h"
#include "systable.h"

#ifdef __cplusplus
extern "C" {
#endif

#define COL_DATA_SET_VAL_GOTO(pData, isNull, pObj, LABEL)                      \
  do {                                                                         \
    if ((code = colDataSetVal(pColInfo, numOfRows, (pData), (isNull))) != 0) { \
      if (pObj) sdbRelease(pSdb, (pObj));                                      \
      lino = __LINE__;                                                         \
      goto LABEL;                                                              \
    }                                                                          \
  } while (0)

#define RETRIEVE_CHECK_GOTO(CMD, pObj, LINO, LABEL) \
  do {                                              \
    code = (CMD);                                   \
    if (code != TSDB_CODE_SUCCESS) {                \
      if (LINO) {                                   \
        *((int32_t *)(LINO)) = __LINE__;            \
      }                                             \
      if (pObj) sdbRelease(pSdb, (pObj));           \
      if (pObj) sdbCancelFetch(pSdb, (pObj));       \
      goto LABEL;                                   \
    }                                               \
  } while (0)

int32_t mndInitShow(SMnode *pMnode);
void    mndCleanupShow(SMnode *pMnode);
void    mndAddShowRetrieveHandle(SMnode *pMnode, EShowType showType, ShowRetrieveFp fp);
void    mndAddShowFreeIterHandle(SMnode *pMnode, EShowType msgType, ShowFreeIterFp fp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SHOW_H_*/

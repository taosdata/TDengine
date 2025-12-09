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

#define COL_DATA_SET_VAL_GOTO(pData, isNull, pObj, pIter, LABEL)                           \
  do {                                                                                     \
    if (pColInfo && (code = colDataSetVal(pColInfo, numOfRows, (pData), (isNull))) != 0) { \
      if (pObj) sdbRelease(pSdb, (pObj));                                                  \
      if (pIter) sdbCancelFetch(pSdb, (pIter));                                            \
      lino = __LINE__;                                                                     \
      goto LABEL;                                                                          \
    }                                                                                      \
  } while (0)

#define COL_DATA_SET_EMPTY_VARCHAR(pBuf, nums)                                       \
  do {                                                                               \
    for (int32_t idx = 0; idx < (nums); ++idx) {                                     \
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {                   \
        STR_WITH_MAXSIZE_TO_VARSTR((pBuf), "", 2);                                   \
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit); \
      }                                                                              \
    }                                                                                \
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

#define MND_SHOW_CHECK_OBJ_PRIVILEGE_ALL(user, privType, owner, LABEL)                \
  do {                                                                                \
    code = mndAcquireUser(pMnode, (user), &pUser);                                    \
    if (pUser == NULL) goto LABEL;                                                    \
    SPrivInfo *privInfo = privInfoGet(privType);                                      \
    if (!privInfo) {                                                                  \
      code = terrno;                                                                  \
      goto LABEL;                                                                     \
    }                                                                                 \
    char objFName[TSDB_OBJ_FNAME_LEN + 1] = {0};                                      \
    if (privInfo->objLevel == 0) {                                                    \
      (void)snprintf(objFName, sizeof(objFName), "%d.*", pUser->acctId);              \
    } else {                                                                          \
      (void)snprintf(objFName, sizeof(objFName), "%d.*.*", pUser->acctId);            \
    }                                                                                 \
    showAll = mndCheckObjPrivilege(pMnode, pUser, privType, (owner), objFName, NULL); \
  } while (0)

#define MND_SHOW_CHECK_DB_PRIVILEGE(pDb, dbFName, pSdbObj, operType, LABEL)       \
  do {                                                                            \
    if (!showAll) {                                                               \
      if (pDb) {                                                                  \
        if (dbUid != pDb->uid) {                                                  \
          if (0 != mndCheckDbPrivilege(pMnode, pUser->name, (operType), pDb)) {   \
            sdbCancelFetch(pSdb, pShow->pIter);                                   \
            sdbRelease(pSdb, (pSdbObj));                                          \
            goto LABEL;                                                           \
          }                                                                       \
          showAll = true;                                                         \
        }                                                                         \
      } else if (dbUid != (pSdbObj)->dbUid) {                                     \
        pIterDb = mndAcquireDb(pMnode, (dbFName));                                \
        if (pIterDb == NULL) {                                                    \
          sdbRelease(pSdb, (pSdbObj));                                            \
          continue;                                                               \
        }                                                                         \
        dbUid = (pSdbObj)->dbUid;                                                 \
        if (0 != mndCheckDbPrivilege(pMnode, pUser->name, (operType), pIterDb)) { \
          showIter = false;                                                       \
          mndReleaseDb(pMnode, pIterDb);                                          \
          sdbRelease(pSdb, (pSdbObj));                                            \
          continue;                                                               \
        } else {                                                                  \
          mndReleaseDb(pMnode, pIterDb);                                          \
          showIter = true;                                                        \
        }                                                                         \
      } else if (!showIter) {                                                     \
        sdbRelease(pSdb, (pSdbObj));                                              \
        continue;                                                                 \
      }                                                                           \
    }                                                                             \
  } while (0)

int32_t mndInitShow(SMnode *pMnode);
void    mndCleanupShow(SMnode *pMnode);
void    mndAddShowRetrieveHandle(SMnode *pMnode, EShowType showType, ShowRetrieveFp fp);
void    mndAddShowFreeIterHandle(SMnode *pMnode, EShowType msgType, ShowFreeIterFp fp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SHOW_H_*/

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
// clang-format off
#ifndef TD_ASTRA
#include <uv.h>
#endif
#include "crypt.h"
#include "mndRole.h"
#include "mndUser.h"
#include "audit.h"
#include "mndDb.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSync.h" 
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndToken.h"
#include "tbase64.h"
#include "totp.h"

// clang-format on

#define USER_VER_SUPPORT_WHITELIST           5
#define USER_VER_SUPPORT_WHITELIT_DUAL_STACK 7
#define USER_VER_SUPPORT_ADVANCED_SECURITY   8
#define USER_VER_NUMBER                      USER_VER_SUPPORT_ADVANCED_SECURITY 

#define USER_RESERVE_SIZE                    63

#define BIT_FLAG_MASK(n)              (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)  ((val) |= (mask))
#define BIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

#if 0
#define PRIVILEGE_TYPE_ALL       BIT_FLAG_MASK(0)
#define PRIVILEGE_TYPE_READ      BIT_FLAG_MASK(1)
#define PRIVILEGE_TYPE_WRITE     BIT_FLAG_MASK(2)
#define PRIVILEGE_TYPE_SUBSCRIBE BIT_FLAG_MASK(3)
#define PRIVILEGE_TYPE_ALTER     BIT_FLAG_MASK(4)
#endif

#define ALTER_USER_ADD_PRIVS(_type) ((_type) == TSDB_ALTER_USER_ADD_PRIVILEGES)
#define ALTER_USER_DEL_PRIVS(_type) ((_type) == TSDB_ALTER_USER_DEL_PRIVILEGES)

#if 0
#define ALTER_USER_ALL_PRIV(_priv)       (PRIV_HAS((_priv), PRIV_CM_ALL))
#define ALTER_USER_READ_PRIV(_priv)      (PRIV_HAS((_priv), PRIV_CM_READ) || PRIV_HAS((_priv), PRIV_CM_ALL))
#define ALTER_USER_WRITE_PRIV(_priv)     (PRIV_HAS((_priv), PRIV_CM_WRITE) || PRIV_HAS((_priv), PRIV_CM_ALL))
#define ALTER_USER_ALTER_PRIV(_priv)     (PRIV_HAS((_priv), PRIV_CM_ALTER) || PRIV_HAS((_priv), PRIV_CM_ALL))
#define ALTER_USER_SUBSCRIBE_PRIV(_priv) (PRIV_HAS((_priv), PRIV_TOPIC_SUBSCRIBE))

#define ALTER_USER_TARGET_DB(_tbname) (0 == (_tbname)[0])
#define ALTER_USER_TARGET_TB(_tbname) (0 != (_tbname)[0])

#define ALTER_USER_ADD_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))

#define ALTER_USER_ADD_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))

#define ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))
#define ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))
#endif

#if 1 // def TD_ENTERPRISE
extern int32_t mndAlterUserPrivInfo(SUserObj *pNew, SAlterRoleReq *pAlterReq);
extern int32_t mndAlterUserRoleInfo(SMnode *pMnode, SUserObj *pOperUser, const char *token, SUserObj *pOld, SUserObj *pNew, SAlterRoleReq *pAlterReq);
#endif

static void generateSalt(char *salt, size_t len);

static int32_t createDefaultIpWhiteList(SIpWhiteListDual **ppWhiteList);
static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteListDual **ppWhiteList, bool supportNeg);

static bool isIpWhiteListEqual(SIpWhiteListDual *a, SIpWhiteListDual *b);
static bool isIpRangeEqual(SIpRange *a, SIpRange *b);

#define MND_MAX_USER_IP_RANGE   (TSDB_PRIVILEDGE_HOST_LEN / 24)
#define MND_MAX_USER_TIME_RANGE 2048

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static int32_t  mndUpgradeUsers(SMnode *pMnode, int32_t version);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateUserReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterUserReq(SRpcMsg *pReq);
static int32_t  mndProcessDropUserReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserAuthReq(SRpcMsg *pReq);
static int32_t  mndProcessUpgradeUserReq(SRpcMsg *pReq);
static int32_t  mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);

static int32_t  mndProcessGetUserIpWhiteListReq(SRpcMsg *pReq);
static int32_t  mndProcessRetrieveIpWhiteListReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserDateTimeWhiteListReq(SRpcMsg *pReq);
static int32_t  mndProcessRetrieveDateTimeWhiteListReq(SRpcMsg *pReq);
static int32_t mndProcessCreateTotpSecretReq(SRpcMsg *pReq);
static int32_t mndProcessDropTotpSecretReq(SRpcMsg *pReq);

static int32_t createIpWhiteListFromOldVer(void *buf, int32_t len, SIpWhiteList **ppList);
static int32_t tDerializeIpWhileListFromOldVer(void *buf, int32_t len, SIpWhiteList *pList);

typedef struct {
  SHashObj *pReadDbs;
  SHashObj *pWriteDbs;
  SHashObj *pReadTbs;
  SHashObj *pWriteTbs;
  SHashObj *pTopics;
  SHashObj *pAlterTbs;
  SHashObj *pReadViews;
  SHashObj *pWriteViews;
  SHashObj *pAlterViews;
  SHashObj *pUseDbs;
} SPrivHashObjSet;

typedef struct {
  SIpWhiteListDual   *wlIp;
  SDateTimeWhiteList *wlTime;
  SLoginInfo          loginInfo;
} SCachedUserInfo;

typedef struct {
  SHashObj      *users;  // key: user, value: SCachedUserInfo*
  int64_t        verIp;
  int64_t        verTime;
  char           auditLogUser[TSDB_USER_LEN];
  TdThreadRwlock rw;
} SUserCache;

static SUserCache userCache;
static int8_t     userIdUpgraded = 0;

static int32_t userCacheInit() {
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);

  SHashObj *users = taosHashInit(8, hashFn, 1, HASH_ENTRY_LOCK);
  if (users == NULL) {
    TAOS_RETURN(terrno);
  }

  userCache.users = users;
  userCache.verIp = 0;
  userCache.verTime = 0;
  userCache.auditLogUser[0] = '\0';

  (void)taosThreadRwlockInit(&userCache.rw, NULL);
  TAOS_RETURN(0);
}



static void userCacheCleanup() {
  if (userCache.users == NULL) {
    return;
  }

  void *pIter = taosHashIterate(userCache.users, NULL);
  while (pIter) {
    SCachedUserInfo *pInfo = *(SCachedUserInfo **)pIter;
    if (pInfo != NULL) {
      taosMemoryFree(pInfo->wlIp);
      taosMemoryFree(pInfo->wlTime);
      taosMemoryFree(pInfo);
    }
    pIter = taosHashIterate(userCache.users, pIter);
  }
  taosHashCleanup(userCache.users);

  (void)taosThreadRwlockDestroy(&userCache.rw);
}



static void userCacheRemoveUser(const char *user) {
  size_t userLen = strlen(user);

  (void)taosThreadRwlockWrlock(&userCache.rw);

  SCachedUserInfo **ppInfo = taosHashGet(userCache.users, user, userLen);
  if (ppInfo != NULL) {
    if (*ppInfo != NULL) {
      taosMemoryFree((*ppInfo)->wlIp);
      taosMemoryFree((*ppInfo)->wlTime);
      taosMemoryFree(*ppInfo);
    }
    if (taosHashRemove(userCache.users, user, userLen) != 0) {
      mDebug("failed to remove user %s from user cache", user);
    }
    userCache.verIp++;
    userCache.verTime++;
  }

  (void)taosThreadRwlockUnlock(&userCache.rw);
}



static void userCacheResetLoginInfo(const char *user) {
  size_t userLen = strlen(user);

  (void)taosThreadRwlockWrlock(&userCache.rw);

  SCachedUserInfo **ppInfo = taosHashGet(userCache.users, user, userLen);
  if (ppInfo != NULL && *ppInfo != NULL) {
    (*ppInfo)->loginInfo.lastLoginTime = taosGetTimestampSec();
    (*ppInfo)->loginInfo.failedLoginCount = 0;
    (*ppInfo)->loginInfo.lastFailedLoginTime = 0;
  }

  (void)taosThreadRwlockUnlock(&userCache.rw);
}



static SCachedUserInfo* getCachedUserInfo(const char* user) {
  size_t userLen = strlen(user);
  SCachedUserInfo **ppInfo = taosHashGet(userCache.users, user, userLen);
  if (ppInfo != NULL) {
    return *ppInfo;
  }

  SCachedUserInfo  *pInfo = (SCachedUserInfo *)taosMemoryCalloc(1, sizeof(SCachedUserInfo));
  if (pInfo == NULL) {
    return NULL;
  }

  if (taosHashPut(userCache.users, user, userLen, &pInfo, sizeof(pInfo)) != 0) {
    taosMemoryFree(pInfo);
    return NULL;
  }

  return pInfo;
}



void mndGetUserLoginInfo(const char *user, SLoginInfo *pLoginInfo) {
  size_t userLen = strlen(user);

  (void)taosThreadRwlockRdlock(&userCache.rw);

  SCachedUserInfo **ppInfo = taosHashGet(userCache.users, user, userLen);
  if (ppInfo != NULL && *ppInfo != NULL) {
    pLoginInfo->lastLoginTime = (*ppInfo)->loginInfo.lastLoginTime;
    pLoginInfo->failedLoginCount = (*ppInfo)->loginInfo.failedLoginCount;
    pLoginInfo->lastFailedLoginTime = (*ppInfo)->loginInfo.lastFailedLoginTime;
  } else {
    pLoginInfo->lastLoginTime = taosGetTimestampSec();
    pLoginInfo->failedLoginCount = 0;
    pLoginInfo->lastFailedLoginTime = 0;
  }

  (void)taosThreadRwlockUnlock(&userCache.rw);

  if (pLoginInfo->lastLoginTime == 0) {
    pLoginInfo->lastLoginTime = taosGetTimestampSec();
  }
}



void mndSetUserLoginInfo(const char *user, const SLoginInfo *pLoginInfo) {
  size_t userLen = strlen(user);

  (void)taosThreadRwlockWrlock(&userCache.rw);

  SCachedUserInfo  *pInfo = getCachedUserInfo(user);
  if (pInfo != NULL) {
    pInfo->loginInfo.lastLoginTime = pLoginInfo->lastLoginTime;
    pInfo->loginInfo.failedLoginCount = pLoginInfo->failedLoginCount;
    pInfo->loginInfo.lastFailedLoginTime = pLoginInfo->lastFailedLoginTime;
  }

  (void)taosThreadRwlockUnlock(&userCache.rw);
}



static bool isDateTimeWhiteListEqual(SDateTimeWhiteList *a, SDateTimeWhiteList *b) {
  if (a == NULL && b == NULL) {
    return true;
  }

  if (a == NULL || b == NULL) {
    return false;
  }

  if (a->num != b->num) {
    return false;
  }

  for (int i = 0; i < a->num; i++) {
    if (a->ranges[i].start != b->ranges[i].start ||
        a->ranges[i].duration != b->ranges[i].duration ||
        a->ranges[i].neg != b->ranges[i].neg ||
        a->ranges[i].absolute != b->ranges[i].absolute) {
      return false;
    }
  }

  return true;
}



static int32_t userCacheUpdateWhiteList(SMnode* pMnode, SUserObj* pUser) {
  int32_t code = 0, lino = 0;

  (void)taosThreadRwlockWrlock(&userCache.rw);

  SCachedUserInfo *pInfo = getCachedUserInfo(pUser->user);
  if (pInfo == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  if (!isIpWhiteListEqual(pInfo->wlIp, pUser->pIpWhiteListDual)) {
    SIpWhiteListDual *p = cloneIpWhiteList(pUser->pIpWhiteListDual);
    if (p == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    taosMemoryFree(pInfo->wlIp);
    pInfo->wlIp = p;
    userCache.verIp++;
  }

  if (!isDateTimeWhiteListEqual(pInfo->wlTime, pUser->pTimeWhiteList)) {
    SDateTimeWhiteList *p = cloneDateTimeWhiteList(pUser->pTimeWhiteList);
    if (p == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    taosMemoryFree(pInfo->wlTime);
    pInfo->wlTime = p;
    userCache.verTime++;
  }

_OVER:
  (void)taosThreadRwlockUnlock(&userCache.rw);
  if (code < 0) {
    mError("failed to update white list for user: %s at line %d since %s", pUser->user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}



static int32_t userCacheRebuildIpWhiteList(SMnode *pMnode) {
  int32_t   code = 0, lino = 0;

  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  while (1) {
    SUserObj *pUser = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) {
      break;
    }

    SCachedUserInfo *pInfo = getCachedUserInfo(pUser->user);
    if (pInfo == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }

    SIpWhiteListDual *wl = cloneIpWhiteList(pUser->pIpWhiteListDual);
    if (wl == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }

    taosMemoryFree(pInfo->wlIp);
    pInfo->wlIp = wl;

    sdbRelease(pSdb, pUser);
  }

  userCache.verIp++;

_OVER:
  if (code < 0) {
    mError("failed to rebuild ip white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}



int64_t mndGetIpWhiteListVersion(SMnode *pMnode) {
  int64_t ver = 0;
  int32_t code = 0;

  if (mndEnableIpWhiteList(pMnode) != 0 && tsEnableWhiteList) {
    (void)taosThreadRwlockWrlock(&userCache.rw);

    if (userCache.verIp == 0) {
      // get user and dnode ip white list
      if ((code = userCacheRebuildIpWhiteList(pMnode)) != 0) {
        (void)taosThreadRwlockUnlock(&userCache.rw);
        mError("%s failed to update ip white list since %s", __func__, tstrerror(code));
        return ver;
      }
      userCache.verIp = taosGetTimestampMs();
    }
    ver = userCache.verIp;

    (void)taosThreadRwlockUnlock(&userCache.rw);
  }

  mDebug("ip-white-list on mnode ver: %" PRId64, ver);
  return ver;
}



int32_t mndRefreshUserIpWhiteList(SMnode *pMnode) {
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&userCache.rw);

  if ((code = userCacheRebuildIpWhiteList(pMnode)) != 0) {
    (void)taosThreadRwlockUnlock(&userCache.rw);
    TAOS_RETURN(code);
  }
  userCache.verIp = taosGetTimestampMs();
  (void)taosThreadRwlockUnlock(&userCache.rw);

  TAOS_RETURN(code);
}



static int32_t userCacheRebuildTimeWhiteList(SMnode *pMnode) {
  int32_t   code = 0, lino = 0;

  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  while (1) {
    SUserObj *pUser = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) {
      break;
    }

    SCachedUserInfo *pInfo = getCachedUserInfo(pUser->user);
    if (pInfo == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }

    SDateTimeWhiteList *wl = cloneDateTimeWhiteList(pUser->pTimeWhiteList);
    if (wl == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }

    taosMemoryFree(pInfo->wlTime);
    pInfo->wlTime = wl;

    sdbRelease(pSdb, pUser);
  }

  userCache.verTime++;

_OVER:
  if (code < 0) {
    mError("failed to rebuild time white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}



int32_t mndRefreshUserDateTimeWhiteList(SMnode *pMnode) {
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&userCache.rw);

  if ((code = userCacheRebuildTimeWhiteList(pMnode)) != 0) {
    (void)taosThreadRwlockUnlock(&userCache.rw);
    TAOS_RETURN(code);
  }
  userCache.verTime = taosGetTimestampMs();
  (void)taosThreadRwlockUnlock(&userCache.rw);

  TAOS_RETURN(code);
}



int64_t mndGetTimeWhiteListVersion(SMnode *pMnode) {
  int64_t ver = 0;
  int32_t code = 0;

  if (mndEnableTimeWhiteList(pMnode) != 0 && tsEnableWhiteList) {
    (void)taosThreadRwlockWrlock(&userCache.rw);

    if (userCache.verIp == 0) {
      // get user and dnode datetime white list
      if ((code = userCacheRebuildTimeWhiteList(pMnode)) != 0) {
        (void)taosThreadRwlockUnlock(&userCache.rw);
        mError("%s failed to update datetime white list since %s", __func__, tstrerror(code));
        return ver;
      }
      userCache.verTime = taosGetTimestampMs();
    }
    ver = userCache.verTime;

    (void)taosThreadRwlockUnlock(&userCache.rw);
  }

  mDebug("datetime-white-list on mnode ver: %" PRId64, ver);
  return ver;
}



int32_t mndInitUser(SMnode *pMnode) {
  TAOS_CHECK_RETURN(userCacheInit());

  SSdbTable table = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
      .upgradeFp = (SdbUpgradeFp)mndUpgradeUsers,
      .encodeFp = (SdbEncodeFp)mndUserActionEncode,
      .decodeFp = (SdbDecodeFp)mndUserActionDecode,
      .insertFp = (SdbInsertFp)mndUserActionInsert,
      .updateFp = (SdbUpdateFp)mndUserActionUpdate,
      .deleteFp = (SdbDeleteFp)mndUserActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_AUTH, mndProcessGetUserAuthReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPGRADE_USER, mndProcessUpgradeUserReq);

  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_IP_WHITELIST, mndProcessGetUserIpWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_IP_WHITELIST_DUAL, mndProcessGetUserIpWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_IP_WHITELIST, mndProcessRetrieveIpWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_IP_WHITELIST_DUAL, mndProcessRetrieveIpWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_DATETIME_WHITELIST, mndProcessGetUserDateTimeWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_DATETIME_WHITELIST, mndProcessRetrieveDateTimeWhiteListReq);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOTP_SECRET, mndProcessCreateTotpSecretReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOTP_SECRET, mndProcessDropTotpSecretReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndRetrieveUsersFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}



void mndCleanupUser(SMnode *pMnode) {
  userCacheCleanup();
}



static bool isDefaultRange(SIpRange *pRange) {
  int32_t code = 0;
  int32_t lino = 0;

  SIpRange range4 = {0};
  SIpRange range6 = {0};

  code = createDefaultIp4Range(&range4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = createDefaultIp6Range(&range6);
  TSDB_CHECK_CODE(code, lino, _error);

  if (isIpRangeEqual(pRange, &range4) || (isIpRangeEqual(pRange, &range6))) {
    return true;
  }
_error:
  return false;
};



static int32_t ipRangeListToStr(SIpRange *range, int32_t num, char *buf, int64_t bufLen) {
  int32_t len = 0;
  for (int i = 0; i < num; i++) {
    SIpRange *pRange = &range[i];
    SIpAddr   addr = {0};
    int32_t code = tIpUintToStr(pRange, &addr);
    if (code != 0) {
      mError("%s failed to convert ip range to str, code: %d", __func__, code);
    }

    len += tsnprintf(buf + len, bufLen - len, "%c%s/%d, ", pRange->neg ? '-' : '+', IP_ADDR_STR(&addr), addr.mask);
  }
  if (len > 0) buf[len - 2] = 0;
  return len;
}



static bool isIpRangeEqual(SIpRange *a, SIpRange *b) {
  if (a->type != b->type || a->neg != b->neg) {
    return false;
  }

  if (a->type == 0) {
    SIpV4Range *a4 = &a->ipV4;
    SIpV4Range *b4 = &b->ipV4;
    return (a4->ip == b4->ip && a4->mask == b4->mask);
  }
  
  SIpV6Range *a6 = &a->ipV6;
  SIpV6Range *b6 = &b->ipV6;
  return (a6->addr[0] == b6->addr[0] && a6->addr[1] == b6->addr[1] && a6->mask == b6->mask);
}



static bool isIpWhiteListEqual(SIpWhiteListDual *a, SIpWhiteListDual *b) {
  if (a == NULL && b == NULL) {
    return true;
  }
  
  if (a == NULL || b == NULL) {
    return false;
  }

  if (a->num != b->num) {
    return false;
  }
  for (int i = 0; i < a->num; i++) {
    if (!isIpRangeEqual(&a->pIpRanges[i], &b->pIpRanges[i])) {
      return false;
    }
  }
  return true;
}


static int32_t compareIpRange(const void *a, const void *b, const void* arg) {
  SIpRange *ra = (SIpRange *)a;
  SIpRange *rb = (SIpRange *)b;

  if (ra->neg != rb->neg) {
    return (ra->neg) ? -1 : 1;
  }

  if (ra->type != rb->type) {
    return (ra->type == 0) ? -1 : 1;
  }

  if (ra->type == 0) {
    if (ra->ipV4.ip != rb->ipV4.ip) {
      return (ra->ipV4.ip < rb->ipV4.ip) ? -1 : 1;
    }
    return (ra->ipV4.mask < rb->ipV4.mask) ? -1 : 1;
  }

  if (ra->ipV6.addr[0] != rb->ipV6.addr[0]) {
    return (ra->ipV6.addr[0] < rb->ipV6.addr[0]) ? -1 : 1;
  }
  if (ra->ipV6.addr[1] != rb->ipV6.addr[1]) {
    return (ra->ipV6.addr[1] < rb->ipV6.addr[1]) ? -1 : 1;
  }
  return (ra->ipV6.mask < rb->ipV6.mask) ? -1 : 1;
}

static void sortIpWhiteList(SIpWhiteListDual *pList) {
  (void)taosqsort(pList->pIpRanges, pList->num, sizeof(SIpRange), NULL, compareIpRange);
}



static int32_t convertIpWhiteListToStr(SUserObj *pUser, char **buf) {
  SIpWhiteListDual *pList = pUser->pIpWhiteListDual;

  int64_t bufLen = pList->num * 128 + 8;
  *buf = taosMemoryCalloc(1, bufLen);
  if (*buf == NULL) {
    return 0;
  }

  if (pList->num == 0) {
    return tsnprintf(*buf, bufLen, "+ALL");
  }

  int32_t len = ipRangeListToStr(pList->pIpRanges, pList->num, *buf, bufLen - 2);
  if (len == 0) {
    taosMemoryFreeClear(*buf);
    return 0;
  }
  return len;
}



static int32_t tSerializeIpWhiteList(void *buf, int32_t len, SIpWhiteListDual *pList, uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);

  TAOS_CHECK_GOTO(tStartEncode(&encoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tEncodeI32(&encoder, pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpRange *pRange = &(pList->pIpRanges[i]);
    TAOS_CHECK_GOTO(tSerializeIpRange(&encoder, pRange), &lino, _OVER);
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_OVER:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("failed to serialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  if (pLen) *pLen = tlen;
  TAOS_RETURN(code);
}

static int32_t tDerializeIpWhiteList(void *buf, int32_t len, SIpWhiteListDual *pList, bool supportNeg) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpRange *pRange = &(pList->pIpRanges[i]);
    TAOS_CHECK_GOTO(tDeserializeIpRange(&decoder, pRange, supportNeg), &lino, _OVER);
  }

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("failed to deserialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t tDerializeIpWhileListFromOldVer(void *buf, int32_t len, SIpWhiteList *pList) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pIp4 = &(pList->pIpRange[i]);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pIp4->ip), &lino, _OVER);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pIp4->mask), &lino, _OVER);
  }

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("failed to deserialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteListDual **ppList, bool supportNeg) {
  int32_t           code = 0;
  int32_t           lino = 0;
  int32_t           num = 0;
  SIpWhiteListDual *p = NULL;
  SDecoder          decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &num), &lino, _OVER);

  p = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + num * sizeof(SIpRange));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDerializeIpWhiteList(buf, len, p, supportNeg), &lino, _OVER);

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    taosMemoryFreeClear(p);
    mError("failed to create ip white list at line %d since %s", lino, tstrerror(code));
  }
  *ppList = p;
  TAOS_RETURN(code);
}

static int32_t createIpWhiteListFromOldVer(void *buf, int32_t len, SIpWhiteList **ppList) {
  int32_t       code = 0;
  int32_t       lino = 0;
  int32_t       num = 0;
  SIpWhiteList *p = NULL;
  SDecoder      decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &num), &lino, _OVER);

  p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + num * sizeof(SIpV4Range));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDerializeIpWhileListFromOldVer(buf, len, p), &lino, _OVER);

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    taosMemoryFreeClear(p);
    mError("failed to create ip white list at line %d since %s", lino, tstrerror(code));
  }
  *ppList = p;
  TAOS_RETURN(code);
}

static int32_t createDefaultIpWhiteList(SIpWhiteListDual **ppWhiteList) {
  int32_t code = 0;
  int32_t lino = 0;
  *ppWhiteList = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange) * 2);
  if (*ppWhiteList == NULL) {
    TAOS_RETURN(terrno);
  }
  (*ppWhiteList)->num = 2;

  SIpRange v4 = {0};
  SIpRange v6 = {0};

#ifndef TD_ASTRA
  code = createDefaultIp4Range(&v4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = createDefaultIp6Range(&v6);
  TSDB_CHECK_CODE(code, lino, _error);

#endif

_error:
  if (code != 0) {
    taosMemoryFree(*ppWhiteList);
    *ppWhiteList = NULL;
    mError("failed to create default ip white list at line %d since %s", __LINE__, tstrerror(code));
  } else {
    memcpy(&(*ppWhiteList)->pIpRanges[0], &v4, sizeof(SIpRange));
    memcpy(&(*ppWhiteList)->pIpRanges[1], &v6, sizeof(SIpRange));
  }
  return 0;
}


static const char* weekdays[] = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};

static int32_t convertTimeRangesToStr(SUserObj *pUser, char **buf) {
  int32_t bufLen = pUser->pTimeWhiteList->num * 32 + 8;
  *buf = taosMemoryCalloc(1, bufLen);
  if (*buf == NULL) {
    return 0;
  }

  int32_t pos = 0;
  if (pUser->pTimeWhiteList->num == 0) {
    pos += tsnprintf(*buf + pos, bufLen - pos, "+ALL");
    return pos;
  }

  for (int32_t i = 0; i < pUser->pTimeWhiteList->num; i++) {
    SDateTimeWhiteListItem *range = &pUser->pTimeWhiteList->ranges[i];
    int duration = range->duration / 60;

    if (range->absolute) {
      struct STm tm;
      (void)taosTs2Tm(range->start, TSDB_TIME_PRECISION_SECONDS, &tm, NULL);
      pos += tsnprintf(*buf + pos, bufLen - pos, "%c%04d-%02d-%02d %02d:%02d %dm, ", range->neg ? '-' : '+', tm.tm.tm_year + 1900, tm.tm.tm_mon + 1, tm.tm.tm_mday, tm.tm.tm_hour, tm.tm.tm_min, duration);
    } else {
      int day = range->start / 86400;
      int hour = (range->start % 86400) / 3600;
      int minute = (range->start % 3600) / 60;
      pos += tsnprintf(*buf + pos, bufLen - pos, "%c%s %02d:%02d %dm, ", range->neg ? '-' : '+', weekdays[day], hour, minute, duration);
    }
  }

  if (pos > 0) {
    (*buf)[pos - 2] = 0; // remove last ", "
  }

  return pos;
}


static int32_t compareDateTimeInterval(const void *a, const void *b, const void* arg) {
  SDateTimeWhiteListItem *pA = (SDateTimeWhiteListItem *)a;
  SDateTimeWhiteListItem *pB = (SDateTimeWhiteListItem *)b;

  if (pA->neg != pB->neg) {
    return pA->neg ? -1 : 1;
  }

  if (pA->absolute != pB->absolute) {
    return pA->absolute ? 1 : -1;
  }

  if (pA->start != pB->start) {
    return (pA->start < pB->start) ? -1 : 1;
  }

  if (pA->duration != pB->duration) {
    return (pA->duration < pB->duration) ? -1 : 1;
  }

  return 0;
}

static void sortTimeWhiteList(SDateTimeWhiteList *pList) {
  (void)taosqsort(pList->ranges, pList->num, sizeof(SDateTimeWhiteListItem), NULL, compareDateTimeInterval);
}




static void dropOldPasswords(SUserObj *pUser) {
  if (pUser->numOfPasswords <= pUser->passwordReuseMax) {
    return;
  }

  int32_t reuseMax = pUser->passwordReuseMax;
  if (reuseMax == 0) {
    reuseMax = 1; // keep at least one password
  }

  int32_t now = taosGetTimestampSec();
  int32_t index = reuseMax;
  while(index < pUser->numOfPasswords) {
    SUserPassword *pPass = &pUser->passwords[index];
    if (now - pPass->setTime >= pUser->passwordReuseTime) {
      break;
    }
    index++;
  }

  if (index == pUser->numOfPasswords) {
    return;
  }
  pUser->numOfPasswords = index;
  // this is a shrink operation, no need to check return value
  pUser->passwords = taosMemoryRealloc(pUser->passwords, sizeof(SUserPassword) * pUser->numOfPasswords);
}



static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SUserObj userObj = {0};

  userObj.passwords = taosMemCalloc(1, sizeof(SUserPassword));
  if (userObj.passwords == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _ERROR);
  }
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.passwords[0].pass);
  userObj.passwords[0].pass[sizeof(userObj.passwords[0].pass) - 1] = 0;
  if (tsiEncryptPassAlgorithm == DND_CA_SM4 && strlen(tsDbKey) > 0) {
    generateSalt(userObj.salt, sizeof(userObj.salt));
    TAOS_CHECK_GOTO(mndEncryptPass(userObj.passwords[0].pass, userObj.salt, &userObj.passEncryptAlgorithm), &lino, _ERROR);
  }

  userObj.passwords[0].setTime = taosGetTimestampSec();
  userObj.numOfPasswords = 1;

  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.uid = mndGenerateUid(userObj.user, strlen(userObj.user));
  userObj.sysInfo = 1;
  userObj.enable = 1;

#ifdef TD_ENTERPRISE

  // 1: force user to change password
  // 2: allow but not force user to change password
  userObj.changePass = tsAllowDefaultPassword ? 2 : 1;

  userObj.ipWhiteListVer = taosGetTimestampMs();
  userObj.timeWhiteListVer = userObj.ipWhiteListVer;
  userObj.connectTime = TSDB_USER_CONNECT_TIME_DEFAULT;
  userObj.connectIdleTime = TSDB_USER_CONNECT_IDLE_TIME_DEFAULT;
  userObj.callPerSession = TSDB_USER_CALL_PER_SESSION_DEFAULT;
  userObj.vnodePerCall = TSDB_USER_VNODE_PER_CALL_DEFAULT;
  userObj.passwordReuseTime = TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT;
  userObj.passwordReuseMax = TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT;
  userObj.passwordLockTime = TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT;
  userObj.sessionPerUser = TSDB_USER_SESSION_PER_USER_DEFAULT;
  userObj.failedLoginAttempts = TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT;
  userObj.passwordLifeTime = TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT;
  userObj.passwordGraceTime = TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT;
  userObj.inactiveAccountTime = TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT;
  userObj.allowTokenNum = TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT;
  userObj.tokenNum = 0;

#else // TD_ENTERPRISE

  userObj.ipWhiteListVer = 0;
  userObj.timeWhiteListVer = 0;
  userObj.changePass = 2; // 2: allow but not force user to change password
  userObj.connectTime = -1;
  userObj.connectIdleTime = -1;
  userObj.callPerSession = -1;
  userObj.vnodePerCall = -1;
  userObj.passwordReuseTime = 0;
  userObj.passwordReuseMax = 0;
  userObj.passwordLockTime = -1;
  userObj.sessionPerUser = -1;
  userObj.failedLoginAttempts = -1;
  userObj.passwordLifeTime = -1;
  userObj.passwordGraceTime = -1;
  userObj.inactiveAccountTime = -1;
  userObj.allowTokenNum = -1;
  userObj.tokenNum = 0;

#endif // TD_ENTERPRISE

  userObj.pTimeWhiteList = taosMemoryCalloc(1, sizeof(SDateTimeWhiteList));
  if (userObj.pTimeWhiteList == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _ERROR);
  }
  
  TAOS_CHECK_GOTO(createDefaultIpWhiteList(&userObj.pIpWhiteListDual), &lino, _ERROR);
  // if this is the root user, change the value of some fields to allow the user login without restriction
  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
    userObj.createdb = 1;
    userObj.sessionPerUser = -1;
    userObj.callPerSession = -1;
    userObj.vnodePerCall = -1;
    userObj.failedLoginAttempts = -1;
    userObj.passwordGraceTime = -1;
    userObj.inactiveAccountTime = -1;
    userObj.allowTokenNum = -1;
  }

  userObj.roles = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (userObj.roles == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _ERROR);
  }

  if ((code = taosHashPut(userObj.roles, TSDB_ROLE_SYSDBA, strlen(TSDB_ROLE_SYSDBA) + 1, NULL, 0)) ||
      (code = taosHashPut(userObj.roles, TSDB_ROLE_SYSSEC, strlen(TSDB_ROLE_SYSSEC) + 1, NULL, 0)) ||
      (code = taosHashPut(userObj.roles, TSDB_ROLE_SYSAUDIT, strlen(TSDB_ROLE_SYSAUDIT) + 1, NULL, 0))) {
    TAOS_CHECK_GOTO(code, &lino, _ERROR);
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) goto _ERROR;
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

  mInfo("user:%s, will be created when deploying, raw:%p", userObj.user, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_ROLE, NULL, "create-user");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("user:%s, failed to create since %s", userObj.user, terrstr());
    goto _ERROR;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, userObj.user);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }

  mndTransDrop(pTrans);
  mndUserFreeObj(&userObj);
  return 0;

_ERROR:
  mndUserFreeObj(&userObj);
  if (code == 0) {
    code = terrno ? terrno : TSDB_CODE_APP_ERROR;
  }
  mError("user:%s, failed to create default user since %s", user, tstrerror(code));
  TAOS_RETURN(code);
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  return mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS);
}

static int32_t mndUpgradeUserIds(SMnode *pMnode, bool *upgraded) {
//   int32_t   code = 0, lino = 0;
//   SSdb     *pSdb = pMnode->pSdb;
//   SUserObj *pUser = NULL;
//   void     *pIter = NULL;
//   while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser))) {
//     if (pUser->uid == 0) {
//       pUser->uid = mndGenerateUid(pUser->user, strlen(pUser->user));
//       TAOS_CHECK_GOTO(mndUserActionUpdate(pMnode, pUser), &lino, _exit);
//       *upgraded = true;
//       mInfo("user:%s, uid upgraded to %" PRId64, pUser->user, pUser->uid);
//     }
//     mndUserFreeObj(pUser);
//   }
// _exit:
  return 0;
}

static int32_t mndUserPrivUpgradeDbOwner(SMnode *pMnode, STrans *pTrans) {
  int32_t code = 0, lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pObj = NULL;
  void   *pIter = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pObj))) {
    if (pObj->cfg.isMount) continue;
    if (pObj->ownerId != 0) continue;
    SUserObj *pUser = NULL;
    (void)mndAcquireUser(pMnode, pObj->createUser, &pUser);
    if (pUser == NULL) {
      mWarn("db:%s, owner user:%s not found, skip upgrade owner uid", pObj->name, pObj->createUser);
      continue;
    }
    SDbObj newObj = {0};
    memcpy(&newObj, pObj, sizeof(SDbObj));
    ++newObj.cfgVersion;
    newObj.updateTime = taosGetTimestampMs();
    newObj.ownerId = pUser->uid;
    mInfo("db:%s, owner uid upgraded to %" PRId64, pObj->name, pUser->uid);
    mndReleaseUser(pMnode, pUser);
    if ((code = mndSetAlterDbCommitLogs(pMnode, pTrans, pObj, &newObj))) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pObj);
      TAOS_CHECK_EXIT(code);
    }
  }
_exit:
  if (code < 0) {
    mError("failed at line %d to upgrade db owner uid since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndUserPrivUpgradeViewOwner(SMnode *pMnode, STrans *pTrans) {
  int32_t code = 0, lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SViewObj *pObj = NULL;
  void   *pIter = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_VIEW, pIter, (void **)&pObj))) {
    if (pObj->ownerId != 0) continue;
    SUserObj *pUser = NULL;
    (void)mndAcquireUser(pMnode, pObj->createUser, &pUser);
    if (pUser == NULL) {
      mWarn("view:%s, owner user:%s not found, skip upgrade owner uid", pObj->fullname, pObj->createUser);
      continue;
    }
    SViewObj newObj = {0};
    memcpy(&newObj, pObj, sizeof(SViewObj));
    ++newObj.version;
    newObj.ownerId = pUser->uid;
    mInfo("view:%s, owner uid upgraded to %" PRId64, pObj->fullname, pUser->uid);
    mndReleaseUser(pMnode, pUser);
    // if ((code = mndSetAlterDbCommitLogs(pMnode, pTrans, pObj, &newObj))) {
    //   sdbCancelFetch(pSdb, pIter);
    //   sdbRelease(pSdb, pObj);
    //   TAOS_CHECK_EXIT(code);
    // }
  }
_exit:
  if (code < 0) {
    mError("failed at line %d to upgrade db owner uid since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessUpgradeUserReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = 0, lino = 0;
#if 0
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING , NULL, "upgrade-user");
  if (pTrans == NULL) {
    mError("failed to create upgrade-user transaction since %s", terrstr());
    TAOS_CHECK_EXIT(terrno);
  }

  TAOS_CHECK_EXIT(mndUserPrivUpgradeDbOwner(pMnode, pTrans));
#ifdef TD_ENTERPRISE
  TAOS_CHECK_EXIT(mndUserPrivUpgradeViewOwner(pMnode, pTrans));

#endif
  // update owner of consumers
  // update owner of topics
  // update owner of views
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code < 0) {
    mError("failed at line %d to upgrade users since %s", lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
#endif
  TAOS_RETURN(code);
}

static int32_t mndUpgradeUsers(SMnode *pMnode, int32_t version) {
  int32_t code = 0, lino = 0;
  if (userIdUpgraded == 0) return code;
  if (!mndIsLeader(pMnode)) return code;
#if 0
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_UPGRADE_USER, .info.ahandle = 0, .info.notFreeAhandle = 1};
  SEpSet  epSet = {0};
  mndGetMnodeEpSet(pMnode, &epSet);
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));
_exit:
  if (code < 0) {
    mError("failed at line %d to upgrade users since %s", lino, tstrerror(code));
  }
#endif
  TAOS_RETURN(code);
}

static int32_t tSerializeUserObjExt(void *buf, int32_t bufLen, SUserObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  void    *pIter = NULL;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));

  TAOS_CHECK_EXIT(tSerializePrivSysObjPolicies(&encoder, &pObj->sysPrivs, pObj->objPrivs));

  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->selectTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->insertTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->updateTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->deleteTbs));

  int32_t nRoles = taosHashGetSize(pObj->roles);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nRoles));

  while ((pIter = taosHashIterate(pObj->roles, pIter))) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(pIter, &keyLen);  // key: role name
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, key));

    uint8_t flag = *(int8_t *)pIter;
    TAOS_CHECK_EXIT(tEncodeU8(&encoder, flag));  // value: 0 reset, 1 set(default)
  }

  int32_t nOwnedDbs = taosHashGetSize(pObj->ownedDbs);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nOwnedDbs));
  pIter = NULL;
  while ((pIter = taosHashIterate(pObj->ownedDbs, pIter))) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(pIter, &keyLen);  // key: dbFName
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, key));
  }

  tEndEncode(&encoder);
  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("user:%s, %s failed at line %d since %s", pObj->user, __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeUserObjExt(void *buf, int32_t bufLen, SUserObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  TAOS_CHECK_EXIT(tDeserializePrivSysObjPolicies(&decoder, &pObj->sysPrivs, &pObj->objPrivs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->selectTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->insertTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->updateTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->deleteTbs));
  int32_t nRoles = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nRoles));
  if (nRoles > 0) {
    if (!pObj->roles &&
        !(pObj->roles = taosHashInit(nRoles, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), 1, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nRoles; i++) {
      int32_t keyLen = 0;
      char   *key = NULL;
      TAOS_CHECK_EXIT(tDecodeCStrAndLen(&decoder, &key, &keyLen));
      uint8_t flag = 0;
      TAOS_CHECK_EXIT(tDecodeU8(&decoder, &flag));
      TAOS_CHECK_EXIT(taosHashPut(pObj->roles, key, keyLen + 1, &flag, sizeof(flag)));
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    int32_t nOwnedDbs = 0;
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nOwnedDbs));
    if (nOwnedDbs > 0) {
      if (!pObj->ownedDbs &&
          !(pObj->ownedDbs =
                taosHashInit(nOwnedDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), 1, HASH_ENTRY_LOCK))) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t i = 0; i < nOwnedDbs; ++i) {
        int32_t keyLen = 0;
        char   *key = NULL;
        TAOS_CHECK_EXIT(tDecodeCStrAndLen(&decoder, &key, &keyLen));
        TAOS_CHECK_EXIT(taosHashPut(pObj->ownedDbs, key, keyLen + 1, NULL, 0));
      }
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("user, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t passReserve = (sizeof(SUserPassword) + 8) * pUser->numOfPasswords + 4;
  int32_t ipWhiteReserve = pUser->pIpWhiteListDual ? (sizeof(SIpRange) * pUser->pIpWhiteListDual->num + sizeof(SIpWhiteListDual) + 4) : 16;
  int32_t timeWhiteReserve = pUser->pTimeWhiteList ? (sizeof(SDateTimeWhiteListItem) * pUser->pTimeWhiteList->num + sizeof(SDateTimeWhiteList) + 4) : 16;
  int32_t numOfReadDbs = 0; //taosHashGetSize(pUser->readDbs);
  int32_t numOfWriteDbs = 0; //taosHashGetSize(pUser->writeDbs);
  int32_t numOfReadTbs = 0; //taosHashGetSize(pUser->readTbs);
  int32_t numOfWriteTbs = 0; //taosHashGetSize(pUser->writeTbs);
  int32_t numOfAlterTbs = 0; //taosHashGetSize(pUser->alterTbs);
  int32_t numOfReadViews = 0; //taosHashGetSize(pUser->readViews);
  int32_t numOfWriteViews = 0; //taosHashGetSize(pUser->writeViews);
  int32_t numOfAlterViews = 0; //taosHashGetSize(pUser->alterViews);
  int32_t numOfTopics = 0; // taosHashGetSize(pUser->topics);
  int32_t numOfUseDbs = 0; // taosHashGetSize(pUser->useDbs);
  int32_t numOfRoles = taosHashGetSize(pUser->roles);
  int32_t size = sizeof(SUserObj) + USER_RESERVE_SIZE + (numOfReadDbs + numOfWriteDbs) * TSDB_DB_FNAME_LEN +
                 numOfTopics * TSDB_TOPIC_FNAME_LEN + ipWhiteReserve + timeWhiteReserve + passReserve;
  char    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  char *stb = NULL;
#if 0
  stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeTbs, stb);
  }
  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  int32_t *useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;
    size += sizeof(int32_t);
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }
#endif
  int32_t sizeExt = tSerializeUserObjExt(NULL, 0, pUser);
  if (sizeExt < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  size += sizeExt;

  pRaw = sdbAllocRaw(SDB_USER, USER_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)

  dropOldPasswords(pUser);
  SDB_SET_INT32(pRaw, dataPos, pUser->numOfPasswords, _OVER)
  for (int32_t i = 0; i < pUser->numOfPasswords; i++) {
    SDB_SET_BINARY(pRaw, dataPos, pUser->passwords[i].pass, sizeof(pUser->passwords[i].pass), _OVER)
    SDB_SET_INT32(pRaw, dataPos, pUser->passwords[i].setTime, _OVER)
  }
  SDB_SET_BINARY(pRaw, dataPos, pUser->salt, sizeof(pUser->salt), _OVER)

  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->sysInfo, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->enable, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pUser->flag, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->authVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->passVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfTopics, _OVER)
#if 0
  char *db = taosHashIterate(pUser->readDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->readDbs, db);
  }

  db = taosHashIterate(pUser->writeDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->writeDbs, db);
  }
  char *topic = taosHashIterate(pUser->topics, NULL);
  while (topic != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER);
    topic = taosHashIterate(pUser->topics, topic);
  }
#endif
  SDB_SET_INT32(pRaw, dataPos, numOfReadTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfUseDbs, _OVER)

#if 0
  stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeTbs, stb);
  }
  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    SDB_SET_INT32(pRaw, dataPos, *useDb, _OVER)
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }
#endif
  // save white list
  int32_t num = pUser->pIpWhiteListDual->num;
  int32_t tlen = sizeof(SIpWhiteListDual) + num * sizeof(SIpRange) + 4;
  int32_t maxBufLen = TMAX(tlen, sizeExt);
  if ((buf = taosMemoryCalloc(1, maxBufLen)) == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  }
  int32_t len = 0;
  TAOS_CHECK_GOTO(tSerializeIpWhiteList(buf, tlen, pUser->pIpWhiteListDual, &len), &lino, _OVER);

  SDB_SET_INT32(pRaw, dataPos, len, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, len, _OVER);

  SDB_SET_INT64(pRaw, dataPos, pUser->ipWhiteListVer, _OVER);
  SDB_SET_INT8(pRaw, dataPos, pUser->passEncryptAlgorithm, _OVER);

  SDB_SET_BINARY(pRaw, dataPos, pUser->totpsecret, sizeof(pUser->totpsecret), _OVER);
  SDB_SET_INT8(pRaw, dataPos, pUser->changePass, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->sessionPerUser, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->connectTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->connectIdleTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->callPerSession, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->vnodePerCall, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->failedLoginAttempts, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->passwordLifeTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->passwordReuseTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->passwordReuseMax, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->passwordLockTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->passwordGraceTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->inactiveAccountTime, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->allowTokenNum, _OVER);
  SDB_SET_INT32(pRaw, dataPos, pUser->tokenNum, _OVER);

  SDB_SET_INT32(pRaw, dataPos, pUser->pTimeWhiteList->num, _OVER);
  for (int32_t i = 0; i < pUser->pTimeWhiteList->num; i++) {
    SDateTimeWhiteListItem *range = &pUser->pTimeWhiteList->ranges[i];
    SDB_SET_BOOL(pRaw, dataPos, range->absolute, _OVER);
    SDB_SET_BOOL(pRaw, dataPos, range->neg, _OVER);
    SDB_SET_INT64(pRaw, dataPos, range->start, _OVER);
    SDB_SET_INT32(pRaw, dataPos, range->duration, _OVER);
  }

  sizeExt = tSerializeUserObjExt(buf, sizeExt, pUser);
  if (sizeExt < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  SDB_SET_INT32(pRaw, dataPos, sizeExt, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, sizeExt, _OVER);

  SDB_SET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)

  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

_OVER:
  taosMemoryFree(buf);
  if (code < 0) {
    mError("user:%s, failed to encode user action to raw:%p at line %d since %s", pUser->user, pRaw, lino,
           tstrerror(code));
    sdbFreeRaw(pRaw);
    pRaw = NULL;
    terrno = code;
  }

  mTrace("user:%s, encode user action to raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRaw;
}

static int32_t mndUserPrivUpgradeTbViews(SUserObj *pNew, SHashObj **ppTblHash, SHashObj *pTbs, int32_t privType,
                                         uint8_t objType) {
  int32_t code = 0, lino = 0;
  void   *pIter = NULL;
  char   *key = NULL;
  char   *value = NULL;

  SAlterRoleReq alterReq = {.alterType = TSDB_ALTER_ROLE_PRIVILEGES, .add = 1, .objType = objType, .objLevel = 1};

  while ((pIter = taosHashIterate(pTbs, pIter))) {
    size_t keyLen = 0;
    key = taosHashGetKey(pIter, &keyLen);

    SName name = {0};
    TAOS_CHECK_EXIT(tNameFromString(&name, key, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));

    snprintf(alterReq.objFName, TSDB_OBJ_FNAME_LEN, "%d.%s", name.acctId, name.dbname);
    snprintf(alterReq.tblName, TSDB_TABLE_NAME_LEN, "%s", name.tname);

    privAddType(&alterReq.privileges.privSet, privType);

    if ((((char *)pIter)[0] != 0) && (((char *)pIter)[1] != 0)) {
      alterReq.privileges.cond = taosStrdup(pIter);
      if (alterReq.privileges.cond == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      alterReq.privileges.condLen = strlen(pIter) + 1;  // include '\0'
      if (ppTblHash && !*ppTblHash) {
        *ppTblHash = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
        if (!*ppTblHash) {
          TAOS_CHECK_EXIT(terrno);
        }
        taosHashSetFreeFp(*ppTblHash, privTblPoliciesFree);
      }
    }

    TAOS_CHECK_EXIT(mndAlterUserPrivInfo(pNew, &alterReq));
  }
_exit:
  tFreeSAlterRoleReq(&alterReq);
  TAOS_RETURN(code);
}

static int32_t mndUserPrivUpgradeUsedDb(SUserObj *pNew, SHashObj *pDbs) {
  int32_t code = 0, lino = 0;
  void   *pIter = NULL;
  char   *key = NULL;
  char   *value = NULL;

  SAlterRoleReq alterReq = {.alterType = TSDB_ALTER_ROLE_PRIVILEGES, .add = 1, .objType = PRIV_OBJ_DB};

  while ((pIter = taosHashIterate(pDbs, pIter))) {
    size_t keyLen = 0;
    key = taosHashGetKey(pIter, &keyLen);

    SName name = {0};
    TAOS_CHECK_EXIT(tNameFromString(&name, key, T_NAME_ACCT | T_NAME_DB));

    snprintf(alterReq.objFName, TSDB_OBJ_FNAME_LEN, "%d.%s", name.acctId, name.dbname);

    privAddType(&alterReq.privileges.privSet, PRIV_DB_USE);

    TAOS_CHECK_EXIT(mndAlterUserPrivInfo(pNew, &alterReq));
  }
_exit:
  tFreeSAlterRoleReq(&alterReq);
  TAOS_RETURN(code);
}

/**
 * @brief migrate from 3.3.x.y to 3.4.x.y
 * @return int32_t
 */
static int32_t mndUserPrivUpgrade(SSdbRaw *pRaw, SPrivHashObjSet *pPrivSet, SUserObj *pNew) {
  int32_t   code = 0, lino = 0;
  SHashObj *pReadDbs = pPrivSet->pReadDbs;
  SHashObj *pWriteDbs = pPrivSet->pWriteDbs;

  if (!pNew->objPrivs &&
      !(pNew->objPrivs = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (pNew->uid == 0) {
    pNew->uid = mndGenerateUid(pNew->name, strlen(pNew->name));
    userIdUpgraded = 1;
  }

  // read db: db.*
  // write db: db.*

  TAOS_CHECK_EXIT(mndUserPrivUpgradeTbViews(pNew, &pNew->selectTbs, pPrivSet->pReadTbs, PRIV_TBL_SELECT, PRIV_OBJ_TBL));
  TAOS_CHECK_EXIT(
      mndUserPrivUpgradeTbViews(pNew, &pNew->insertTbs, pPrivSet->pWriteTbs, PRIV_TBL_INSERT, PRIV_OBJ_TBL));
  TAOS_CHECK_EXIT(mndUserPrivUpgradeTbViews(pNew, NULL, pPrivSet->pAlterTbs, PRIV_CM_ALTER, PRIV_OBJ_TBL));
  TAOS_CHECK_EXIT(mndUserPrivUpgradeTbViews(pNew, NULL, pPrivSet->pReadViews, PRIV_VIEW_SELECT, PRIV_OBJ_VIEW));
  TAOS_CHECK_EXIT(mndUserPrivUpgradeTbViews(pNew, NULL, pPrivSet->pWriteViews, PRIV_CM_ALTER, PRIV_OBJ_VIEW));
  TAOS_CHECK_EXIT(mndUserPrivUpgradeTbViews(pNew, NULL, pPrivSet->pWriteViews, PRIV_CM_DROP, PRIV_OBJ_VIEW));

  TAOS_CHECK_EXIT(mndUserPrivUpgradeUsedDb(pNew, pPrivSet->pUseDbs));
_exit:
  TAOS_RETURN(code);
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SSdbRow        *pRow = NULL;
  SUserObj       *pUser = NULL;
  char           *key = NULL;
  char           *value = NULL;
  SPrivHashObjSet privSet = {0};

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PTR, &lino, _OVER);
  }

  if (sver < 1 || sver > USER_VER_NUMBER) {
    TAOS_CHECK_GOTO(TSDB_CODE_SDB_INVALID_DATA_VER, &lino, _OVER);
  }

  pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)

  if (sver < USER_VER_SUPPORT_ADVANCED_SECURITY) {
    pUser->passwords = taosMemoryCalloc(1, sizeof(SUserPassword));
    if (pUser->passwords == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    SDB_GET_BINARY(pRaw, dataPos, pUser->passwords[0].pass, TSDB_PASSWORD_LEN, _OVER)
    pUser->numOfPasswords = 1;
    memset(pUser->salt, 0, sizeof(pUser->salt));
  } else {
    SDB_GET_INT32(pRaw, dataPos, &pUser->numOfPasswords, _OVER)
    pUser->passwords = taosMemoryCalloc(pUser->numOfPasswords, sizeof(SUserPassword));
    if (pUser->passwords == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    for (int32_t i = 0; i < pUser->numOfPasswords; ++i) {
      SDB_GET_BINARY(pRaw, dataPos, pUser->passwords[i].pass, sizeof(pUser->passwords[i].pass), _OVER);
      SDB_GET_INT32(pRaw, dataPos, &pUser->passwords[i].setTime, _OVER);
    }
    SDB_GET_BINARY(pRaw, dataPos, pUser->salt, sizeof(pUser->salt), _OVER)
  }

  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, _OVER)
  if (sver < USER_VER_SUPPORT_ADVANCED_SECURITY) {
    pUser->passwords[0].setTime = (int32_t)(pUser->updateTime / 1000);
  }

  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->sysInfo, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->enable, _OVER)
  SDB_GET_UINT8(pRaw, dataPos, &pUser->flag, _OVER)
  if (pUser->superUser) pUser->createdb = 1;
  SDB_GET_INT32(pRaw, dataPos, &pUser->authVersion, _OVER)
  if (sver >= 4) {
    SDB_GET_INT32(pRaw, dataPos, &pUser->passVersion, _OVER)
  }

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  int32_t numOfTopics = 0;
  SDB_GET_INT32(pRaw, dataPos, &numOfReadDbs, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &numOfWriteDbs, _OVER)
  if (sver >= 2) {
    SDB_GET_INT32(pRaw, dataPos, &numOfTopics, _OVER)
  }

  if (numOfReadDbs > 0) {
    privSet.pReadDbs =
        taosHashInit(numOfReadDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (privSet.pReadDbs == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
  }
  if (numOfWriteDbs > 0) {
    privSet.pWriteDbs =
        taosHashInit(numOfWriteDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (privSet.pWriteDbs == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
  }
  if (numOfTopics > 0) {
    privSet.pTopics =
        taosHashInit(numOfTopics, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (privSet.pTopics == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
  }
  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(privSet.pReadDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(privSet.pWriteDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  if (sver >= 2) {
    for (int32_t i = 0; i < numOfTopics; ++i) {
      char topic[TSDB_TOPIC_FNAME_LEN] = {0};
      SDB_GET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER)
      int32_t len = strlen(topic) + 1;
      TAOS_CHECK_GOTO(taosHashPut(privSet.pTopics, topic, len, topic, TSDB_TOPIC_FNAME_LEN), &lino, _OVER);
    }
  }

  if (sver >= 3) {
    int32_t numOfReadTbs = 0;
    int32_t numOfWriteTbs = 0;
    int32_t numOfAlterTbs = 0;
    int32_t numOfReadViews = 0;
    int32_t numOfWriteViews = 0;
    int32_t numOfAlterViews = 0;
    int32_t numOfUseDbs = 0;
    SDB_GET_INT32(pRaw, dataPos, &numOfReadTbs, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &numOfWriteTbs, _OVER)
    if (sver >= 6) {
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterTbs, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfReadViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfWriteViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterViews, _OVER)
    }
    SDB_GET_INT32(pRaw, dataPos, &numOfUseDbs, _OVER)

    if (numOfReadTbs > 0) {
      privSet.pReadTbs =
          taosHashInit(numOfReadTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
      if (privSet.pReadTbs == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
    }
    if (numOfWriteTbs > 0) {
      privSet.pWriteTbs =
          taosHashInit(numOfWriteTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
      if (privSet.pWriteTbs == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
    }
    privSet.pAlterTbs =
        taosHashInit(numOfAlterTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    privSet.pReadViews =
        taosHashInit(numOfReadViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    privSet.pWriteViews =
        taosHashInit(numOfWriteViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    privSet.pAlterViews =
        taosHashInit(numOfAlterViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    privSet.pUseDbs =
        taosHashInit(numOfUseDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    if (privSet.pAlterTbs == NULL || privSet.pReadViews == NULL || privSet.pWriteViews == NULL ||
        privSet.pAlterViews == NULL || privSet.pUseDbs == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      goto _OVER;
    }

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(privSet.pReadTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(privSet.pWriteTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    if (sver >= 6) {
      for (int32_t i = 0; i < numOfAlterTbs; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(privSet.pAlterTbs, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfReadViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(privSet.pReadViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfWriteViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(privSet.pWriteViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfAlterViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(privSet.pAlterViews, key, keyLen, value, valuelen), &lino, _OVER);
      }
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t ref = 0;
      SDB_GET_INT32(pRaw, dataPos, &ref, _OVER);

      TAOS_CHECK_GOTO(taosHashPut(privSet.pUseDbs, key, keyLen, &ref, sizeof(ref)), &lino, _OVER);
    }
  }
  // decoder white list
  if (sver >= USER_VER_SUPPORT_WHITELIST) {
    if (sver < USER_VER_SUPPORT_WHITELIT_DUAL_STACK) {
      int32_t len = 0;
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

      TAOS_MEMORY_REALLOC(key, len);
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      SDB_GET_BINARY(pRaw, dataPos, key, len, _OVER);

      SIpWhiteList *pIpWhiteList = NULL;
      TAOS_CHECK_GOTO(createIpWhiteListFromOldVer(key, len, &pIpWhiteList), &lino, _OVER);

      SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);

      code = cvtIpWhiteListToDual(pIpWhiteList, &pUser->pIpWhiteListDual);
      if (code != 0) {
        taosMemoryFreeClear(pIpWhiteList);
      }
      TAOS_CHECK_GOTO(code, &lino, _OVER);

      taosMemoryFreeClear(pIpWhiteList);

    } else if (sver >= USER_VER_SUPPORT_WHITELIT_DUAL_STACK) {
      int32_t len = 0;
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

      TAOS_MEMORY_REALLOC(key, len);
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      SDB_GET_BINARY(pRaw, dataPos, key, len, _OVER);

      TAOS_CHECK_GOTO(createIpWhiteList(key, len, &pUser->pIpWhiteListDual, sver >= USER_VER_SUPPORT_ADVANCED_SECURITY), &lino, _OVER);
      SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);
    }
  }

  if (pUser->pIpWhiteListDual == NULL) {
    TAOS_CHECK_GOTO(createDefaultIpWhiteList(&pUser->pIpWhiteListDual), &lino, _OVER);
    pUser->ipWhiteListVer = taosGetTimestampMs();
  }

  SDB_GET_INT8(pRaw, dataPos, &pUser->passEncryptAlgorithm, _OVER);

  if (sver < USER_VER_SUPPORT_ADVANCED_SECURITY) {
    memset(pUser->totpsecret, 0, sizeof(pUser->totpsecret));
    pUser->changePass = 2;
    pUser->sessionPerUser = pUser->superUser ? -1 : TSDB_USER_SESSION_PER_USER_DEFAULT;
    pUser->connectTime = TSDB_USER_CONNECT_TIME_DEFAULT;
    pUser->connectIdleTime = TSDB_USER_CONNECT_IDLE_TIME_DEFAULT;
    pUser->callPerSession = pUser->superUser ? -1 : TSDB_USER_CALL_PER_SESSION_DEFAULT;
    pUser->vnodePerCall = pUser->superUser ? -1 : TSDB_USER_VNODE_PER_CALL_DEFAULT;
    pUser->failedLoginAttempts = pUser->superUser ? -1 : TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT;
    pUser->passwordLifeTime = TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT;
    pUser->passwordReuseTime = TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT;
    pUser->passwordReuseMax = TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT;
    pUser->passwordLockTime = TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT;
    pUser->passwordGraceTime = pUser->superUser ? -1 : TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT;
    pUser->inactiveAccountTime = pUser->superUser ? -1 : TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT;
    pUser->allowTokenNum = pUser->superUser ? -1 : TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT;
    pUser->tokenNum = 0;
    pUser->pTimeWhiteList = taosMemCalloc(1, sizeof(SDateTimeWhiteList));
    if (pUser->pTimeWhiteList == NULL) {
    }
  } else {
    SDB_GET_BINARY(pRaw, dataPos, pUser->totpsecret, sizeof(pUser->totpsecret), _OVER);
    SDB_GET_INT8(pRaw, dataPos, &pUser->changePass, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->sessionPerUser, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->connectTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->connectIdleTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->callPerSession, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->vnodePerCall, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->failedLoginAttempts, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->passwordLifeTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->passwordReuseTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->passwordReuseMax, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->passwordLockTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->passwordGraceTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->inactiveAccountTime, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->allowTokenNum, _OVER);
    SDB_GET_INT32(pRaw, dataPos, &pUser->tokenNum, _OVER);

    int32_t num = 0;
    SDB_GET_INT32(pRaw, dataPos, &num, _OVER);
    pUser->pTimeWhiteList = taosMemCalloc(1, sizeof(SDateTimeWhiteList) + num * sizeof(SDateTimeWhiteListItem));
    if (pUser->pTimeWhiteList == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    pUser->pTimeWhiteList->num = num;
    for (int32_t i = 0; i < num; i++) {
      SDateTimeWhiteListItem *range = &pUser->pTimeWhiteList->ranges[i];
      SDB_GET_BOOL(pRaw, dataPos, &range->absolute, _OVER);
      SDB_GET_BOOL(pRaw, dataPos, &range->neg, _OVER);
      SDB_GET_INT64(pRaw, dataPos, &range->start, _OVER);
      SDB_GET_INT32(pRaw, dataPos, &range->duration, _OVER);
    }

    int32_t extLen = 0;
    SDB_GET_INT32(pRaw, dataPos, &extLen, _OVER);
    TAOS_MEMORY_REALLOC(key, extLen);
    if (key == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    SDB_GET_BINARY(pRaw, dataPos, key, extLen, _OVER);
    TAOS_CHECK_GOTO(tDeserializeUserObjExt(key, extLen, pUser), &lino, _OVER);
  }

#ifndef TD_ENTERPRISE
  // community users cannot modify these fields, and the default values may prevent
  // the user from logging in, so we set them to different values here.
  pUser->sessionPerUser = -1;
  pUser->connectTime = -1;
  pUser->connectIdleTime = -1;
  pUser->callPerSession = -1;
  pUser->vnodePerCall = -1;
  pUser->failedLoginAttempts = -1;
  pUser->passwordLifeTime = -1;
  pUser->passwordReuseTime = 0;
  pUser->passwordReuseMax = 0;
  pUser->passwordLockTime = -1;
  pUser->passwordGraceTime = -1;
  pUser->inactiveAccountTime = -1;
  pUser->allowTokenNum = -1;
  pUser->tokenNum = 0;
#endif // TD_ENTERPRISE

  SDB_GET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)

  taosInitRWLatch(&pUser->lock);
  dropOldPasswords(pUser);

  if (sver < USER_VER_SUPPORT_ADVANCED_SECURITY) {
    TAOS_CHECK_GOTO(mndUserPrivUpgrade(pRaw, &privSet, pUser), &lino, _OVER);
  }

_OVER:
  taosMemoryFree(key);
  taosMemoryFree(value);
  taosHashCleanup(privSet.pReadDbs);
  taosHashCleanup(privSet.pWriteDbs);
  taosHashCleanup(privSet.pTopics);
  taosHashCleanup(privSet.pReadTbs);
  taosHashCleanup(privSet.pWriteTbs);
  taosHashCleanup(privSet.pAlterTbs);
  taosHashCleanup(privSet.pReadViews);
  taosHashCleanup(privSet.pWriteViews);
  taosHashCleanup(privSet.pAlterViews);
  taosHashCleanup(privSet.pUseDbs);
  if (code < 0) {
    terrno = code;
    mError("user:%s, failed to decode at line %d from raw:%p since %s", pUser == NULL ? "null" : pUser->user, lino,
           pRaw, tstrerror(code));
    if (pUser != NULL) {
      mndUserFreeObj(pUser);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("user:%s, decode from raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform insert action, row:%p", pUser->user, pUser);

  SAcctObj *pAcct = sdbAcquire(pSdb, SDB_ACCT, pUser->acct);
  if (pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    mError("user:%s, failed to perform insert action since %s", pUser->user, terrstr());
    TAOS_RETURN(terrno);
  }
  pUser->acctId = pAcct->acctId;
  sdbRelease(pSdb, pAcct);

  return 0;
}

int32_t mndDupTableHash(SHashObj *pOld, SHashObj **ppNew) {
  int32_t code = 0;
  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    TAOS_RETURN(terrno);
  }

  char *tb = taosHashIterate(pOld, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(tb, &keyLen);

    int32_t valueLen = strlen(tb) + 1;
    if ((code = taosHashPut(*ppNew, key, keyLen, tb, valueLen)) != 0) {
      taosHashCancelIterate(pOld, tb);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    tb = taosHashIterate(pOld, tb);
  }

  TAOS_RETURN(code);
}

int32_t mndDupRoleHash(SHashObj *pOld, SHashObj **ppNew) {
  if (!(*ppNew = taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                              HASH_ENTRY_LOCK))) {
    TAOS_RETURN(terrno);
  }

  int32_t  code = 0;
  uint8_t *flag = NULL;
  while ((flag = taosHashIterate(pOld, flag))) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(flag, &keyLen);

    if ((code = taosHashPut(*ppNew, key, keyLen, flag, sizeof(*flag))) != 0) {
      taosHashCancelIterate(pOld, flag);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndMergePrivObjHash(SHashObj *pOld, SHashObj **ppNew) {
  if (!(*ppNew)) return mndDupPrivObjHash(pOld, ppNew);

  int32_t           code = 0, lino = 0;
  SHashObj         *pNew = *ppNew;
  SPrivObjPolicies *policies = NULL;
  while ((policies = taosHashIterate(pOld, policies))) {
    size_t klen = 0;
    char  *key = taosHashGetKey(policies, &klen);
    size_t vlen = taosHashGetValueSize(policies);

    SPrivObjPolicies *pNewPolicies = taosHashGet(pNew, key, klen);
    if (pNewPolicies) {
      size_t newVlen = taosHashGetValueSize(pNewPolicies);
      if (newVlen > 0 && vlen > 0) {
        privAddSet(&pNewPolicies->policy, &policies->policy);
      }
      continue;
    }

    if ((code = taosHashPut(pNew, key, klen, vlen ? policies : NULL, vlen)) != 0) {
      taosHashCancelIterate(pOld, policies);
      taosHashCleanup(pNew);
      *ppNew = NULL;
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

/**
 * 1. Prefer to use SPrivTblPolicies from user object(the updateUs of policy in user object is INT64_MAX)
 * 2. If two or more roles have SPrivTblPolicies, the policy with latest update time take effect.
 */
int32_t mndMergePrivTblHash(SHashObj *pOld, SHashObj **ppNew, bool updateWithLatest) {
  if (!(*ppNew)) return mndDupPrivTblHash(pOld, ppNew, false);

  int32_t           code = 0, lino = 0;
  SHashObj         *pNew = *ppNew;
  SPrivTblPolicies *policies = NULL;
  while ((policies = taosHashIterate(pOld, policies))) {
    size_t klen = 0;
    char  *key = taosHashGetKey(policies, &klen);
    size_t vlen = taosHashGetValueSize(policies);

    SPrivTblPolicies *pNewPolicies = taosHashGet(pNew, key, klen);
    if (pNewPolicies) {
      size_t newVlen = taosHashGetValueSize(pNewPolicies);
      if (newVlen > 0 && vlen > 0) {
        TAOS_CHECK_EXIT(privTblPoliciesMerge(pNewPolicies, policies, updateWithLatest));
      }
      continue;
    }

    SPrivTblPolicies tmpPolicies = {0};
    if (vlen > 0) {
      if ((code = privTblPoliciesMerge(&tmpPolicies, policies, updateWithLatest))) {
        privTblPoliciesFree(&tmpPolicies);
        goto _exit;
      }
    }
    if ((code = taosHashPut(pNew, key, klen, vlen ? &tmpPolicies : NULL, vlen)) != 0) {
      privTblPoliciesFree(&tmpPolicies);
      taosHashCancelIterate(pOld, policies);
      taosHashCleanup(pNew);
      *ppNew = NULL;
      TAOS_RETURN(code);
    }
  }

_exit:
  TAOS_RETURN(code);
}

int32_t mndDupKVHash(SHashObj *pOld, SHashObj **ppNew) {
  if (!(*ppNew = taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                              HASH_ENTRY_LOCK))) {
    TAOS_RETURN(terrno);
  }
  int32_t code = 0;
  void   *val = NULL;
  while ((val = taosHashIterate(pOld, val))) {
    size_t klen = 0;
    char  *key = taosHashGetKey(val, &klen);
    size_t vlen = taosHashGetValueSize(val);
    if ((code = taosHashPut(*ppNew, key, klen, vlen > 0 ? val : NULL, vlen)) != 0) {
      taosHashCancelIterate(pOld, val);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndDupPrivObjHash(SHashObj *pOld, SHashObj **ppNew) {
  if (!(*ppNew = taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                             HASH_ENTRY_LOCK))) {
    TAOS_RETURN(terrno);
  }
  int32_t           code = 0;
  SPrivObjPolicies *policies = NULL;
  while ((policies = taosHashIterate(pOld, policies))) {
    size_t klen = 0;
    char  *key = taosHashGetKey(policies, &klen);
    size_t vlen = taosHashGetValueSize(policies);

    if ((code = taosHashPut(*ppNew, key, klen, vlen > 0 ? policies : NULL, vlen)) != 0) {
      taosHashCancelIterate(pOld, policies);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndDupPrivTblHash(SHashObj *pOld, SHashObj **ppNew, bool setUpdateTimeMax) {
  if (!(*ppNew = taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                              HASH_ENTRY_LOCK))) {
    TAOS_RETURN(terrno);
  }
  taosHashSetFreeFp(*ppNew, privTblPoliciesFree);

  int32_t           code = 0, lino = 0;
  SPrivTblPolicies *policies = NULL, *pTmpPolicies = NULL;
  SPrivTblPolicies  tmpPolicies = {0};
  while ((policies = taosHashIterate(pOld, policies))) {
    size_t klen = 0;
    char  *key = taosHashGetKey(policies, &klen);
    size_t vlen = taosHashGetValueSize(policies);
    memset(&tmpPolicies, 0, sizeof(tmpPolicies));
    pTmpPolicies = &tmpPolicies;
    if (vlen > 0) {
      TAOS_CHECK_EXIT(privTblPoliciesAdd(&tmpPolicies, policies, true, setUpdateTimeMax));
    }
    TAOS_CHECK_EXIT(taosHashPut(*ppNew, key, klen, vlen > 0 ? &tmpPolicies : NULL, vlen));
    pTmpPolicies = NULL;
  }

_exit:
  if (code != 0) {
    if (!pTmpPolicies) {
      privTblPoliciesFree(pTmpPolicies);
      pTmpPolicies = NULL;
    }
    if (policies) taosHashCancelIterate(pOld, policies);
    taosHashCleanup(*ppNew);
    *ppNew = NULL;
  }
  TAOS_RETURN(code);
}

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew) {
  int32_t code = 0;
  (void)memcpy(pNew, pUser, sizeof(SUserObj));
  pNew->authVersion++;
  pNew->updateTime = taosGetTimestampMs();
  taosInitRWLatch(&pNew->lock);

  pNew->passwords = NULL;
  pNew->pIpWhiteListDual = NULL;
  pNew->passwords = NULL;
  pNew->objPrivs = NULL;
  pNew->selectTbs = NULL;
  pNew->insertTbs = NULL;
  pNew->updateTbs = NULL;
  pNew->deleteTbs = NULL;
  pNew->ownedDbs = NULL;
  pNew->pTimeWhiteList = NULL;
  pNew->roles = NULL;

  taosRLockLatch(&pUser->lock);
  pNew->passwords = taosMemoryCalloc(pUser->numOfPasswords, sizeof(SUserPassword));
  if (pNew->passwords == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  (void)memcpy(pNew->passwords, pUser->passwords, pUser->numOfPasswords * sizeof(SUserPassword));
  TAOS_CHECK_GOTO(mndDupKVHash(pUser->ownedDbs, &pNew->ownedDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupPrivObjHash(pUser->objPrivs, &pNew->objPrivs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupPrivTblHash(pUser->selectTbs, &pNew->selectTbs, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupPrivTblHash(pUser->insertTbs, &pNew->insertTbs, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupPrivTblHash(pUser->updateTbs, &pNew->updateTbs, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupPrivTblHash(pUser->deleteTbs, &pNew->deleteTbs, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupRoleHash(pUser->roles, &pNew->roles), NULL, _OVER);
  pNew->pIpWhiteListDual = cloneIpWhiteList(pUser->pIpWhiteListDual);
  if (pNew->pIpWhiteListDual == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  pNew->pTimeWhiteList = cloneDateTimeWhiteList(pUser->pTimeWhiteList);
  if (pNew->pTimeWhiteList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

_OVER:
  taosRUnLockLatch(&pUser->lock);
  TAOS_RETURN(code);
}

void mndUserFreeObj(SUserObj *pUser) {
  taosHashCleanup(pUser->ownedDbs);
  taosHashCleanup(pUser->objPrivs);
  taosHashCleanup(pUser->selectTbs);
  taosHashCleanup(pUser->insertTbs);
  taosHashCleanup(pUser->updateTbs);
  taosHashCleanup(pUser->deleteTbs);
  taosHashCleanup(pUser->roles);
  taosMemoryFreeClear(pUser->passwords);
  taosMemoryFreeClear(pUser->pIpWhiteListDual);
  taosMemoryFreeClear(pUser->pTimeWhiteList);
  pUser->ownedDbs = NULL;
  pUser->objPrivs = NULL;
  pUser->selectTbs = NULL;
  pUser->insertTbs = NULL;
  pUser->updateTbs = NULL;
  pUser->deleteTbs = NULL;
  pUser->roles = NULL;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action, row:%p", pUser->user, pUser);
  mndUserFreeObj(pUser);
  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew) {
  mTrace("user:%s, perform update action, old row:%p new row:%p", pOld->user, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->authVersion = pNew->authVersion;
  pOld->passVersion = pNew->passVersion;
  pOld->sysInfo = pNew->sysInfo;
  pOld->enable = pNew->enable;
  pOld->flag = pNew->flag;
  pOld->changePass = pNew->changePass;
  pOld->uid = pNew->uid;

  pOld->sessionPerUser = pNew->sessionPerUser;
  pOld->connectTime = pNew->connectTime;
  pOld->connectIdleTime = pNew->connectIdleTime;
  pOld->callPerSession = pNew->callPerSession;
  pOld->vnodePerCall = pNew->vnodePerCall;
  pOld->failedLoginAttempts = pNew->failedLoginAttempts;
  pOld->passwordLifeTime = pNew->passwordLifeTime;
  pOld->passwordReuseTime = pNew->passwordReuseTime;
  pOld->passwordReuseMax = pNew->passwordReuseMax;
  pOld->passwordLockTime = pNew->passwordLockTime;
  pOld->passwordGraceTime = pNew->passwordGraceTime;
  pOld->inactiveAccountTime = pNew->inactiveAccountTime;
  pOld->allowTokenNum = pNew->allowTokenNum;
  pOld->tokenNum = pNew->tokenNum;

  pOld->numOfPasswords = pNew->numOfPasswords;
  TSWAP(pOld->passwords, pNew->passwords);
  (void)memcpy(pOld->salt, pNew->salt, sizeof(pOld->salt));
  (void)memcpy(pOld->totpsecret, pNew->totpsecret, sizeof(pOld->totpsecret));
  pOld->sysPrivs = pNew->sysPrivs;
  TSWAP(pOld->ownedDbs, pNew->ownedDbs);
  TSWAP(pOld->objPrivs, pNew->objPrivs);
  TSWAP(pOld->selectTbs, pNew->selectTbs);
  TSWAP(pOld->insertTbs, pNew->insertTbs);
  TSWAP(pOld->updateTbs, pNew->updateTbs);
  TSWAP(pOld->deleteTbs, pNew->deleteTbs);
  TSWAP(pOld->roles, pNew->roles);

  TSWAP(pOld->pIpWhiteListDual, pNew->pIpWhiteListDual);
  pOld->ipWhiteListVer = pNew->ipWhiteListVer;
  TSWAP(pOld->pTimeWhiteList, pNew->pTimeWhiteList);
  pOld->timeWhiteListVer = pNew->timeWhiteListVer;
  pOld->passEncryptAlgorithm = pNew->passEncryptAlgorithm;

  taosWUnLockLatch(&pOld->lock);

  return 0;
}

int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (*ppUser == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_USER_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

int32_t mndAcquireUserById(SMnode *pMnode, int64_t userId, SUserObj **ppUser) {
  void     *pIter = NULL;
  SUserObj *pObj;
  SSdb     *pSdb = pMnode->pSdb;
  while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj))) {
    if (pObj->uid == userId) {
      return mndAcquireUser(pMnode, pObj->user, ppUser);
    }
  }
  return 0;
}

int32_t mndBuildUidNamesHash(SMnode *pMnode, SSHashObj **ppHash) {
  int32_t    code = 0;
  void      *pIter = NULL;
  SUserObj  *pObj;
  SSHashObj *pHash = NULL;

  int32_t nUser = sdbGetSize(pMnode->pSdb, SDB_USER);

  pHash = tSimpleHashInit(nUser, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pHash == NULL) {
    TAOS_RETURN(terrno);
  }

  while ((pIter = sdbFetch(pMnode->pSdb, SDB_USER, pIter, (void **)&pObj))) {
    code = tSimpleHashPut(pHash, &pObj->uid, sizeof(pObj->uid), pObj->name, strlen(pObj->name) + 1);
    if (code != 0) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      tSimpleHashCleanup(pHash);
      TAOS_RETURN(code);
    }
    sdbRelease(pMnode->pSdb, pObj);
  }

  *ppHash = pHash;
  TAOS_RETURN(code);
}

int32_t mndEncryptPass(char *pass, const char* salt, int8_t *algo) {
  int32_t code = 0;
  if (tsMetaKey[0] == '\0') {
    return 0;
  }

  if (salt[0] != 0) {
    char passAndSalt[TSDB_PASSWORD_LEN - 1 + TSDB_PASSWORD_SALT_LEN];
    (void)memcpy(passAndSalt, pass, TSDB_PASSWORD_LEN - 1);
    (void)memcpy(passAndSalt + TSDB_PASSWORD_LEN - 1, salt, TSDB_PASSWORD_SALT_LEN);
    taosEncryptPass_c((uint8_t *)passAndSalt, sizeof(passAndSalt), pass);
  }

  unsigned char packetData[TSDB_PASSWORD_LEN] = {0};
  SCryptOpts opts = {0};
  opts.len = TSDB_PASSWORD_LEN;
  opts.source = pass;
  opts.result = packetData;
  opts.unitLen = TSDB_PASSWORD_LEN;
  tstrncpy(opts.key, tsDbKey, ENCRYPT_KEY_LEN + 1);
  int newLen = Builtin_CBC_Encrypt(&opts);
  if (newLen <= 0) return terrno;

  memcpy(pass, packetData, newLen);
  if (algo != NULL) {
    *algo = DND_CA_SM4;
  }

  return 0;
}



static void generateSalt(char *salt, size_t len) {
  const char* set = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  int32_t     setLen = 62;
  for (int32_t i = 0; i < len - 1; ++i) {
    salt[i] = set[taosSafeRand() % setLen];
  }
  salt[len - 1] = 0;
}



static int32_t addDefaultIpToTable(int8_t enableIpv6, SHashObj *pUniqueTab) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t dummpy = 0;

  SIpRange ipv4 = {0}, ipv6 = {0};
  code = createDefaultIp4Range(&ipv4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = taosHashPut(pUniqueTab, &ipv4, sizeof(ipv4), &dummpy, sizeof(dummpy));
  TSDB_CHECK_CODE(code, lino, _error);

  if (enableIpv6) {
    code = createDefaultIp6Range(&ipv6);
    TSDB_CHECK_CODE(code, lino, _error);

    code = taosHashPut(pUniqueTab, &ipv6, sizeof(ipv6), &dummpy, sizeof(dummpy));
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    mError("failed to add default ip range to table since %s", tstrerror(code));
  }
  return code;
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SUserObj userObj = {0};

  userObj.passwords = taosMemoryCalloc(1, sizeof(SUserPassword));
  if (userObj.passwords == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  userObj.numOfPasswords = 1;

  if (pCreate->isImport == 1) {
    memset(userObj.salt, 0, sizeof(userObj.salt));
    memcpy(userObj.passwords[0].pass, pCreate->pass, TSDB_PASSWORD_LEN);
  } else {
    generateSalt(userObj.salt, sizeof(userObj.salt));
    taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.passwords[0].pass);
    userObj.passwords[0].pass[sizeof(userObj.passwords[0].pass) - 1] = 0;
    TAOS_CHECK_GOTO(mndEncryptPass(userObj.passwords[0].pass, userObj.salt, &userObj.passEncryptAlgorithm), &lino, _OVER);
  }
  userObj.passwords[0].setTime = taosGetTimestampSec();

  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  if (pCreate->totpseed[0] != 0) {
    int len = taosGenerateTotpSecret(pCreate->totpseed, 0, userObj.totpsecret, sizeof(userObj.totpsecret));
    if (len < 0) {
      TAOS_CHECK_GOTO(TSDB_CODE_PAR_INVALID_OPTION_VALUE, &lino, _OVER);
    }
  }

  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;
  userObj.createdb = pCreate->createDb;
  userObj.uid = mndGenerateUid(userObj.user, strlen(userObj.user));

#ifdef TD_ENTERPRISE

  userObj.changePass = pCreate->changepass;
  userObj.sessionPerUser = pCreate->sessionPerUser;
  userObj.connectTime = pCreate->connectTime;
  userObj.connectIdleTime = pCreate->connectIdleTime;
  userObj.callPerSession = pCreate->callPerSession;
  userObj.vnodePerCall = pCreate->vnodePerCall;
  userObj.failedLoginAttempts = pCreate->failedLoginAttempts;
  userObj.passwordLifeTime = pCreate->passwordLifeTime;
  userObj.passwordReuseTime = pCreate->passwordReuseTime;
  userObj.passwordReuseMax = pCreate->passwordReuseMax;
  userObj.passwordLockTime = pCreate->passwordLockTime;
  userObj.passwordGraceTime = pCreate->passwordGraceTime;
  userObj.inactiveAccountTime = pCreate->inactiveAccountTime;
  userObj.allowTokenNum = pCreate->allowTokenNum;
  userObj.tokenNum = 0;

  if (pCreate->numIpRanges == 0) {
    TAOS_CHECK_GOTO(createDefaultIpWhiteList(&userObj.pIpWhiteListDual), &lino, _OVER);
  } else {
    SHashObj *pUniqueTab = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (pUniqueTab == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    int32_t dummpy = 0;
    
    for (int i = 0; i < pCreate->numIpRanges; i++) {
      SIpRange range = {0};
      copyIpRange(&range, pCreate->pIpDualRanges + i);
      if ((code = taosHashPut(pUniqueTab, &range, sizeof(range), &dummpy, sizeof(dummpy))) != 0) {
        taosHashCleanup(pUniqueTab);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }

    code = addDefaultIpToTable(tsEnableIpv6, pUniqueTab);
    if (code != 0) {
      taosHashCleanup(pUniqueTab);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    if (taosHashGetSize(pUniqueTab) > MND_MAX_USER_IP_RANGE) {
      taosHashCleanup(pUniqueTab);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_IP_RANGE, &lino, _OVER);
    }

    int32_t           numOfRanges = taosHashGetSize(pUniqueTab);
    SIpWhiteListDual *p = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + numOfRanges * sizeof(SIpRange));
    if (p == NULL) {
      taosHashCleanup(pUniqueTab);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    void   *pIter = taosHashIterate(pUniqueTab, NULL);
    for (int32_t i = 0; i < numOfRanges; i++) {
      size_t    len = 0;
      SIpRange *key = taosHashGetKey(pIter, &len);
      memcpy(&p->pIpRanges[i], key, sizeof(SIpRange));
      pIter = taosHashIterate(pUniqueTab, pIter);
    }

    taosHashCleanup(pUniqueTab);
    p->num = numOfRanges;
    sortIpWhiteList(p);
    userObj.pIpWhiteListDual = p;
  }

  if (pCreate->numTimeRanges == 0) {
    userObj.pTimeWhiteList = (SDateTimeWhiteList*)taosMemoryCalloc(1, sizeof(SDateTimeWhiteList));
    if (userObj.pTimeWhiteList == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
  } else {
    SHashObj *pUniqueTab = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (pUniqueTab == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    int32_t dummpy = 0;
    
    for (int i = 0; i < pCreate->numTimeRanges; i++) {
      SDateTimeRange* src = pCreate->pTimeRanges + i;
      SDateTimeWhiteListItem range = {0};
      DateTimeRangeToWhiteListItem(&range, src);

      // no need to add expired range
      if (isDateTimeWhiteListItemExpired(&range)) {
        continue;
      }

      if ((code = taosHashPut(pUniqueTab, &range, sizeof(range), &dummpy, sizeof(dummpy))) != 0) {
        taosHashCleanup(pUniqueTab);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }

    if (taosHashGetSize(pUniqueTab) > MND_MAX_USER_TIME_RANGE) {
      taosHashCleanup(pUniqueTab);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_TIME_RANGE, &lino, _OVER);
    }

    int32_t           numOfRanges = taosHashGetSize(pUniqueTab);
    SDateTimeWhiteList *p = taosMemoryCalloc(1, sizeof(SDateTimeWhiteList) + numOfRanges * sizeof(SDateTimeWhiteListItem));
    if (p == NULL) {
      taosHashCleanup(pUniqueTab);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    void   *pIter = taosHashIterate(pUniqueTab, NULL);
    for (int32_t i = 0; i < numOfRanges; i++) {
      size_t    len = 0;
      SDateTimeWhiteListItem *key = taosHashGetKey(pIter, &len);
      memcpy(p->ranges + i, key, sizeof(SDateTimeWhiteListItem));
      pIter = taosHashIterate(pUniqueTab, pIter);
    }

    taosHashCleanup(pUniqueTab);
    p->num = numOfRanges;
    sortTimeWhiteList(p);
    userObj.pTimeWhiteList = p;
  }

  userObj.ipWhiteListVer = taosGetTimestampMs();
  userObj.timeWhiteListVer = userObj.ipWhiteListVer;

#else // TD_ENTERPRISE

  userObj.changePass = 1;
  userObj.sessionPerUser = -1;
  userObj.connectTime = -1;
  userObj.connectIdleTime = -1;
  userObj.callPerSession = -1;
  userObj.vnodePerCall = -1;
  userObj.failedLoginAttempts = -1;
  userObj.passwordLifeTime = -1;
  userObj.passwordReuseTime = 0;
  userObj.passwordReuseMax = 0;
  userObj.passwordLockTime = -1;
  userObj.passwordGraceTime = -1;
  userObj.inactiveAccountTime = -1;
  userObj.allowTokenNum = -1;
  userObj.tokenNum = 0;

  TAOS_CHECK_GOTO(createDefaultIpWhiteList(&userObj.pIpWhiteListDual), &lino, _OVER);
  userObj.pTimeWhiteList = (SDateTimeWhiteList*)taosMemoryCalloc(1, sizeof(SDateTimeWhiteList));
  if (userObj.pTimeWhiteList == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  userObj.ipWhiteListVer = 0;
  userObj.timeWhiteListVer = 0;

#endif // TD_ENTERPRISE

  userObj.roles = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (userObj.roles == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  if ((code = taosHashPut(userObj.roles, TSDB_ROLE_SYSINFO_1, strlen(TSDB_ROLE_SYSINFO_1) + 1, NULL, 0)) != 0) {
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "create-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  if ((code = userCacheUpdateWhiteList(pMnode, &userObj)) != 0) {
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  if (taosHashGet(userObj.roles, TSDB_ROLE_SYSAUDIT_LOG, strlen(TSDB_ROLE_SYSAUDIT_LOG) + 1)) {
    (void)mndResetAuditLogUser(pMnode, userObj.user, true);
  }

  mndTransDrop(pTrans);

_OVER:
  mndUserFreeObj(&userObj);
  TAOS_RETURN(code);
}


static int32_t mndCheckPasswordFmt(const char *pwd) {
  if (tsEnableStrongPassword == 0) {
    for (char c = *pwd; c != 0; c = *(++pwd)) {
      if (c == ' ' || c == '\'' || c == '\"' || c == '`' || c == '\\') {
        return TSDB_CODE_MND_INVALID_PASS_FORMAT;
      }
    }
    return 0;
  }

  int32_t len = strlen(pwd);
  if (len < TSDB_PASSWORD_MIN_LEN) {
    return TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY;
  }

  if (len > TSDB_PASSWORD_MAX_LEN) {
    return TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG;
  }

  if (taosIsComplexString(pwd)) {
    return 0;
  }

  return TSDB_CODE_MND_INVALID_PASS_FORMAT;
}



static int32_t mndCheckTotpSeedFmt(const char *seed) {
  int32_t len = strlen(seed);
  if (len < TSDB_USER_TOTPSEED_MIN_LEN) {
    return TSDB_CODE_PAR_OPTION_VALUE_TOO_SHORT;
  }

  if (taosIsComplexString(seed)) {
    return 0;
  }

  return TSDB_CODE_PAR_INVALID_OPTION_VALUE;
}



static int32_t mndProcessGetUserDateTimeWhiteListReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = 0;
  int32_t              lino = 0;
  int32_t              contLen = 0;
  void                *pRsp = NULL;
  SUserObj            *pUser = NULL;
  SGetUserWhiteListReq wlReq = {0};
  SUserDateTimeWhiteList wlRsp = {0};

  if (tDeserializeSGetUserWhiteListReq(pReq->pCont, pReq->contLen, &wlReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }
  mTrace("user: %s, start to get date time whitelist", wlReq.user);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, wlReq.user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetUserDateTimeWhiteListRsp(pMnode, pUser, &wlRsp), &lino, _OVER);

  contLen = tSerializeSUserDateTimeWhiteList(NULL, 0, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  
  contLen = tSerializeSUserDateTimeWhiteList(pRsp, contLen, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  tFreeSUserDateTimeWhiteList(&wlRsp);
  if (code < 0) {
    mError("user:%s, failed to get whitelist at line %d since %s", wlReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  TAOS_RETURN(code);
  return 0;
}



static int32_t buildRetrieveDateTimeWhiteListRsp(SRetrieveDateTimeWhiteListRsp *pRsp) {
  (void)taosThreadRwlockRdlock(&userCache.rw);
  
  int32_t count = taosHashGetSize(userCache.users);
  pRsp->pUsers = taosMemoryCalloc(count, sizeof(SUserDateTimeWhiteList));
  if (pRsp->pUsers == NULL) {
    (void)taosThreadRwlockUnlock(&userCache.rw);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  count = 0;
  void   *pIter = taosHashIterate(userCache.users, NULL);
  while (pIter) {
    SDateTimeWhiteList *wl = (*(SCachedUserInfo **)pIter)->wlTime;
    if (wl == NULL || wl->num <= 0) {
      pIter = taosHashIterate(userCache.users, pIter);
      continue;
    }

    SUserDateTimeWhiteList *pUser = &pRsp->pUsers[count];
    pUser->ver = userCache.verTime;

    size_t klen;
    char  *key = taosHashGetKey(pIter, &klen);
    (void)memcpy(pUser->user, key, klen);

    pUser->numWhiteLists = wl->num;
    pUser->pWhiteLists = taosMemoryCalloc(wl->num, sizeof(SDateTimeWhiteListItem));
    if (pUser->pWhiteLists == NULL) {
      (void)taosThreadRwlockUnlock(&userCache.rw);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    (void)memcpy(pUser->pWhiteLists, wl->ranges, wl->num * sizeof(SDateTimeWhiteListItem));
    count++;
    pIter = taosHashIterate(userCache.users, pIter);
  }

  pRsp->numOfUser = count;
  pRsp->ver = userCache.verTime;
  (void)taosThreadRwlockUnlock(&userCache.rw);
  TAOS_RETURN(0);
}



static int32_t mndProcessRetrieveDateTimeWhiteListReq(SRpcMsg *pReq) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int32_t        len = 0;
  void          *pRsp = NULL;
  SRetrieveDateTimeWhiteListRsp wlRsp = {0};

  // impl later
  SRetrieveWhiteListReq req = {0};
  if (tDeserializeRetrieveWhiteListReq(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(buildRetrieveDateTimeWhiteListRsp(&wlRsp), &lino, _OVER);

  len = tSerializeSRetrieveDateTimeWhiteListRsp(NULL, 0, &wlRsp);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

  pRsp = rpcMallocCont(len);
  if (!pRsp) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  len = tSerializeSRetrieveDateTimeWhiteListRsp(pRsp, len, &wlRsp);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

_OVER:
  if (code < 0) {
    mError("failed to process retrieve ip white request at line %d since %s", lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    len = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = len;

  tFreeSRetrieveDateTimeWhiteListRsp(&wlRsp);
  TAOS_RETURN(code);
}



static int32_t mndProcessCreateUserReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0;
  int32_t        lino = 0;
  SRoleObj      *pRole = NULL;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};
  int64_t        tss = taosGetTimestampMs();

  if (tDeserializeSCreateUserReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("user:%s, start to create, createdb:%d, is_import:%d", createReq.user, createReq.createDb, createReq.isImport);

#ifndef TD_ENTERPRISE
  if (createReq.isImport == 1) {
    TAOS_CHECK_GOTO(TSDB_CODE_OPS_NOT_SUPPORT, &lino, _OVER);  // enterprise feature
  }
#endif
  code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  if (createReq.isImport != 1) {
    // TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_CREATE_USER), &lino, _OVER);
    TAOS_CHECK_GOTO(mndCheckSysObjPrivilege(pMnode, pOperUser, RPC_MSG_TOKEN(pReq), PRIV_USER_CREATE, 0, 0, NULL, NULL),
                    &lino, _OVER);
  } else if (strcmp(RPC_MSG_USER(pReq), "root") != 0) {
    mError("The operation is not permitted to create user:%s", RPC_MSG_USER(pReq));
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
  }

  if (createReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  if (createReq.isImport != 1) {
    code = mndCheckPasswordFmt(createReq.pass);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  if (createReq.totpseed[0] != 0) {
    code = mndCheckTotpSeedFmt(createReq.totpseed);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  code = mndAcquireUser(pMnode, createReq.user, &pUser);
  if (pUser != NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_ALREADY_EXIST, &lino, _OVER);
  }

  code = mndAcquireRole(pMnode, createReq.user, &pRole);
  if (pRole != NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_ROLE_ALREADY_EXIST, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_USER), &lino, _OVER);

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    char detail[1000] = {0};
    (void)tsnprintf(detail, sizeof(detail), "enable:%d, superUser:%d, sysInfo:%d, password:xxx", createReq.enable,
                    createReq.superUser, createReq.sysInfo);
    char operation[15] = {0};
    if (createReq.isImport == 1) {
      tstrncpy(operation, "importUser", sizeof(operation));
    } else {
      tstrncpy(operation, "createUser", sizeof(operation));
    }

    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, operation, "", createReq.user, detail, strlen(detail), duration, 0);
  }

_OVER:
  if (code == TSDB_CODE_MND_USER_ALREADY_EXIST && createReq.ignoreExists) {
    code = 0;
  } else if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create at line %d since %s", createReq.user, lino, tstrerror(code));
  }

  mndReleaseRole(pMnode, pRole);
  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSCreateUserReq(&createReq);

  TAOS_RETURN(code);
}



static int32_t mndProcessGetUserIpWhiteListReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = 0;
  int32_t              lino = 0;
  int32_t              contLen = 0;
  void                *pRsp = NULL;
  SUserObj            *pUser = NULL;
  SGetUserWhiteListReq wlReq = {0};
  SGetUserIpWhiteListRsp wlRsp = {0};

  int32_t (*serialFn)(void *, int32_t, SGetUserIpWhiteListRsp *) = NULL;
  int32_t (*setRspFn)(SMnode * pMnode, SUserObj * pUser, SGetUserIpWhiteListRsp * pRsp) = NULL;

  if (pReq->msgType == TDMT_MND_GET_USER_IP_WHITELIST_DUAL) {
    serialFn = tSerializeSGetUserIpWhiteListDualRsp;
    setRspFn = mndSetUserIpWhiteListDualRsp;
  } else {
    serialFn = tSerializeSGetUserIpWhiteListRsp;
    setRspFn = mndSetUserIpWhiteListRsp;
  }
  if (tDeserializeSGetUserWhiteListReq(pReq->pCont, pReq->contLen, &wlReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }
  mTrace("user: %s, start to get ip whitelist", wlReq.user);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, wlReq.user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(setRspFn(pMnode, pUser, &wlRsp), &lino, _OVER);

  contLen = serialFn(NULL, 0, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  contLen = serialFn(pRsp, contLen, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserIpWhiteListDualRsp(&wlRsp);
  if (code < 0) {
    mError("user:%s, failed to get whitelist at line %d since %s", wlReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  TAOS_RETURN(code);
}



static int32_t buildRetrieveIpWhiteListRsp(SUpdateIpWhite *pUpdate) {
  (void)taosThreadRwlockRdlock(&userCache.rw);

  int32_t count = taosHashGetSize(userCache.users);
  pUpdate->pUserIpWhite = taosMemoryCalloc(count, sizeof(SUpdateUserIpWhite));
  if (pUpdate->pUserIpWhite == NULL) {
    (void)taosThreadRwlockUnlock(&userCache.rw);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  count = 0;
  void   *pIter = taosHashIterate(userCache.users, NULL);
  while (pIter) {
    SIpWhiteListDual   *wl = (*(SCachedUserInfo**)pIter)->wlIp;
    if (wl == NULL || wl->num <= 0) {
      pIter = taosHashIterate(userCache.users, pIter);
      continue;
    }

    SUpdateUserIpWhite *pUser = &pUpdate->pUserIpWhite[count];
    pUser->ver = userCache.verIp;

    size_t klen;
    char  *key = taosHashGetKey(pIter, &klen);
    (void)memcpy(pUser->user, key, klen);

    pUser->numOfRange = wl->num;
    pUser->pIpRanges = taosMemoryCalloc(wl->num, sizeof(SIpRange));
    if (pUser->pIpRanges == NULL) {
      (void)taosThreadRwlockUnlock(&userCache.rw);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    (void)memcpy(pUser->pIpRanges, wl->pIpRanges, wl->num * sizeof(SIpRange));
    count++;
    pIter = taosHashIterate(userCache.users, pIter);
  }

  pUpdate->numOfUser = count;
  pUpdate->ver = userCache.verIp;
  (void)taosThreadRwlockUnlock(&userCache.rw);
  TAOS_RETURN(0);
}



int32_t mndProcessRetrieveIpWhiteListReq(SRpcMsg *pReq) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int32_t        len = 0;
  void          *pRsp = NULL;
  SUpdateIpWhite ipWhite = {0};

  // impl later
  SRetrieveWhiteListReq req = {0};
  if (tDeserializeRetrieveWhiteListReq(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  int32_t (*fn)(void *, int32_t, SUpdateIpWhite *) = NULL;
  if (pReq->msgType == TDMT_MND_RETRIEVE_IP_WHITELIST) {
    fn = tSerializeSUpdateIpWhite;
  } else if (pReq->msgType == TDMT_MND_RETRIEVE_IP_WHITELIST_DUAL) {
    fn = tSerializeSUpdateIpWhiteDual;
  }

  TAOS_CHECK_GOTO(buildRetrieveIpWhiteListRsp(&ipWhite), &lino, _OVER);

  len = fn(NULL, 0, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

  pRsp = rpcMallocCont(len);
  if (!pRsp) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  len = fn(pRsp, len, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

_OVER:
  if (code < 0) {
    mError("failed to process retrieve ip white request at line %d since %s", lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    len = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = len;

  tFreeSUpdateIpWhiteReq(&ipWhite);
  TAOS_RETURN(code);
}

static int32_t mndAlterUser(SMnode *pMnode, SUserObj *pNew, SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "alter-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to alter since %s", pNew->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to alter user:%s", pTrans->id, pNew->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pNew);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if ((code = userCacheUpdateWhiteList(pMnode, pNew)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter at line %d since %s", pNew->user, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndDupObjHash(SHashObj *pOld, int32_t dataLen, SHashObj **ppNew) {
  int32_t code = 0;

  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    code = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if ((code = taosHashPut(*ppNew, db, len, db, dataLen)) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    db = taosHashIterate(pOld, db);
  }

  TAOS_RETURN(code);
}

int32_t mndDupDbHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_DB_FNAME_LEN, ppNew); }

int32_t mndDupTopicHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_TOPIC_FNAME_LEN, ppNew); }

static int32_t mndTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                  SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};

  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (alterReq->tagCond != NULL && alterReq->tagCondLen != 0) {
    char *value = taosHashGet(hash, tbFName, len);
    if (value != NULL) {
      TAOS_RETURN(TSDB_CODE_MND_PRIVILEGE_EXIST);
    }

    int32_t condLen = alterReq->tagCondLen;
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->tagCond, condLen));
  } else {
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->isView ? "v" : "t", 2));
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t  ref = 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL != currRef) {
    ref = (*currRef) + 1;
  }
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

  TAOS_RETURN(0);
}

static int32_t mndRemoveTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                        SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (taosHashRemove(hash, tbFName, len) != 0) {
    TAOS_RETURN(0);  // not found
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL == currRef) {
    return 0;
  }

  if (1 == *currRef) {
    if (taosHashRemove(useDbHash, alterReq->objname, dbKeyLen) != 0) {
      TAOS_RETURN(0);  // not found
    }
    return 0;
  }
  int32_t ref = (*currRef) - 1;
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

  return 0;
}


#if 0
static int32_t mndProcessAlterUserPrivilegesReq(SRpcMsg* pReq, SAlterUserReq *pAlterReq) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0, lino = 0;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};
  int64_t   tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, pAlterReq->user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckAlterUserPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), pUser, pAlterReq), &lino, _OVER);
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);

#if 0
  if (ALTER_USER_ADD_READ_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(newUser.readDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      void   *pIter = NULL;
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(newUser.readDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_ADD_WRITE_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(newUser.writeDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      void   *pIter = NULL;
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(newUser.writeDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_DEL_READ_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      code = taosHashRemove(newUser.readDbs, pAlterReq->objname, len);
      if (code < 0) {
        mError("read db:%s, failed to remove db:%s since %s", newUser.user, pAlterReq->objname, terrstr());
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(newUser.readDbs);
    }
  }

  if (ALTER_USER_DEL_WRITE_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, &pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      code = taosHashRemove(newUser.writeDbs, pAlterReq->objname, len);
      if (code < 0) {
        mError("user:%s, failed to remove db:%s since %s", newUser.user, pAlterReq->objname, terrstr());
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(newUser.writeDbs);
    }
  }

  SHashObj *pReadTbs = newUser.readTbs;
  SHashObj *pWriteTbs = newUser.writeTbs;
  SHashObj *pAlterTbs = newUser.alterTbs;

#ifdef TD_ENTERPRISE
  if (pAlterReq->isView) {
    pReadTbs = newUser.readViews;
    pWriteTbs = newUser.writeViews;
    pAlterTbs = newUser.alterViews;
  }
#endif

  if (ALTER_USER_ADD_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pReadTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pWriteTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pAlterTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pReadTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pWriteTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pAlterTbs, newUser.useDbs, pAlterReq, pSdb), &lino, _OVER);
  }
#endif

#if 0
// #ifdef USE_TOPIC
  if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, &pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    taosRLockLatch(&pTopic->lock);
    code = taosHashPut(newUser.topics, pTopic->name, len, pTopic->name, TSDB_TOPIC_FNAME_LEN);
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseTopic(pMnode, pTopic);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, &pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    taosRLockLatch(&pTopic->lock);
    code = taosHashRemove(newUser.topics, pAlterReq->objname, len);
    if (code < 0) {
      mError("user:%s, failed to remove topic:%s since %s", newUser.user, pAlterReq->objname, tstrerror(code));
    }
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseTopic(pMnode, pTopic);
  }
#endif

  TAOS_CHECK_GOTO(mndAlterUser(pMnode, &newUser, pReq), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    if (ALTER_USER_ADD_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
              ALTER_USER_ADD_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
              ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
              ALTER_USER_ADD_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
              ALTER_USER_ADD_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
              ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
      if (strcmp(pAlterReq->objname, "1.*") != 0) {
        SName name = {0};
        TAOS_CHECK_GOTO(tNameFromString(&name, pAlterReq->objname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
        auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", name.dbname, pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
      } else {
        auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", "", pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
      }
    } else if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", pAlterReq->objname, pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
    } else if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", pAlterReq->objname, pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
    } else {
      if (strcmp(pAlterReq->objname, "1.*") != 0) {
        SName name = {0};
        TAOS_CHECK_GOTO(tNameFromString(&name, pAlterReq->objname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
        auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", name.dbname, pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
      } else {
        auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", "", pAlterReq->user, pAlterReq->sql, pAlterReq->sqlLen, duration, 0);
      }
    }
  }
  
_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter user privileges at line %d since %s", pAlterReq->user, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}
#endif

int32_t mndAlterUserFromRole(SRpcMsg *pReq, SUserObj *pOperUser, SAlterRoleReq *pAlterReq) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  int32_t   code = 0, lino = 0;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pAlterReq->principal, &pUser));

  if (pUser->enable == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_DISABLED);
  }

  if (pAlterReq->alterType == TSDB_ALTER_ROLE_PRIVILEGES) {
#ifdef TD_ENTERPRISE
    TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
    if ((code = mndAlterUserPrivInfo(&newUser, pAlterReq)) == TSDB_CODE_QRY_DUPLICATED_OPERATION) {
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(code);
    }
  } else if (pAlterReq->alterType == TSDB_ALTER_ROLE_ROLE) {
    if ((code = mndAlterUserRoleInfo(pMnode, pOperUser, RPC_MSG_TOKEN(pReq), pUser, &newUser, pAlterReq)) ==
        TSDB_CODE_QRY_DUPLICATED_OPERATION) {
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(code);
    }
#endif
  } else {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  code = mndAlterUser(pMnode, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter user at line %d since %s", pAlterReq->principal, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}


static int32_t mndProcessAlterUserBasicInfoReq(SRpcMsg *pReq, SAlterUserReq *pAlterReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = 0, lino = 0;
  SUserObj     *pUser = NULL;
  SUserObj      newUser = {0};
  char          auditLog[1000] = {0};
  int32_t       auditLen = 0;
  int64_t       tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, pAlterReq->user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckAlterUserPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), pUser, pAlterReq), &lino, _OVER);
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);

  if (pAlterReq->hasPassword) {
    auditLen += tsnprintf(auditLog, sizeof(auditLog), "password,");

    TAOS_CHECK_GOTO(mndCheckPasswordFmt(pAlterReq->pass), &lino, _OVER);
    if (newUser.salt[0] == 0) {
      generateSalt(newUser.salt, sizeof(newUser.salt));
    }
    char pass[TSDB_PASSWORD_LEN] = {0};
    taosEncryptPass_c((uint8_t *)pAlterReq->pass, strlen(pAlterReq->pass), pass);
    pass[sizeof(pass) - 1] = 0;
    TAOS_CHECK_GOTO(mndEncryptPass(pass, newUser.salt, &newUser.passEncryptAlgorithm), &lino, _OVER);

    if (newUser.passwordReuseMax > 0 || newUser.passwordReuseTime > 0) {
      for(int32_t i = 0; i < newUser.numOfPasswords; ++i) {
        if (0 == strncmp(newUser.passwords[i].pass, pass, TSDB_PASSWORD_LEN)) {
          TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_REUSE, &lino, _OVER);
        }
      }
      SUserPassword *passwords = taosMemoryCalloc(newUser.numOfPasswords + 1, sizeof(SUserPassword));
      if (passwords == NULL) {
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
      }
      memcpy(passwords + 1, newUser.passwords, newUser.numOfPasswords * sizeof(SUserPassword));
      memcpy(passwords[0].pass, pass, TSDB_PASSWORD_LEN);
      passwords[0].setTime = taosGetTimestampSec();
      taosMemoryFree(newUser.passwords);
      newUser.passwords = passwords;
      ++newUser.numOfPasswords;
      ++newUser.passVersion;
      newUser.changePass = 2;
    } else if (0 != strncmp(newUser.passwords[0].pass, pass, TSDB_PASSWORD_LEN)) {
      memcpy(newUser.passwords[0].pass, pass, TSDB_PASSWORD_LEN);
      newUser.passwords[0].setTime = taosGetTimestampSec();
      ++newUser.passVersion;
      newUser.changePass = 2;
    }
  }

  if (pAlterReq->hasTotpseed) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "totpseed,");

    if (pAlterReq->totpseed[0] == 0) { // clear totp secret
      memset(newUser.totpsecret, 0, sizeof(newUser.totpsecret));
    } else if (taosGenerateTotpSecret(pAlterReq->totpseed, 0, newUser.totpsecret, sizeof(newUser.totpsecret)) < 0) {
      TAOS_CHECK_GOTO(TSDB_CODE_PAR_INVALID_OPTION_VALUE, &lino, _OVER);
    }
  }

  if (pAlterReq->hasEnable) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "enable:%d,", pAlterReq->enable);

    newUser.enable = pAlterReq->enable; // lock or unlock user manually
    if (newUser.enable) {
      // reset login info to allow login immediately
      userCacheResetLoginInfo(newUser.user);
    }
  }

  if (pAlterReq->hasSysinfo) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "sysinfo:%d,", pAlterReq->sysinfo);
    newUser.sysInfo = pAlterReq->sysinfo;
  }

  if (pAlterReq->hasCreatedb) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "createdb:%d,", pAlterReq->createdb);
    newUser.createdb = pAlterReq->createdb;
  }

#ifdef TD_ENTERPRISE
  if (pAlterReq->hasChangepass) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "changepass:%d,", pAlterReq->changepass);
    newUser.changePass = pAlterReq->changepass;
  }

  if (pAlterReq->hasSessionPerUser) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "sessionPerUser:%d,", pAlterReq->sessionPerUser);
    newUser.sessionPerUser = pAlterReq->sessionPerUser;
  }

  if (pAlterReq->hasConnectTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "connectTime:%d,", pAlterReq->connectTime);
    newUser.connectTime = pAlterReq->connectTime;
  }
  
  if (pAlterReq->hasConnectIdleTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "connectIdleTime:%d,", pAlterReq->connectIdleTime);
    newUser.connectIdleTime = pAlterReq->connectIdleTime;
  }

  if (pAlterReq->hasCallPerSession) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "callPerSession:%d,", pAlterReq->callPerSession);
    newUser.callPerSession = pAlterReq->callPerSession;
  }

  if (pAlterReq->hasVnodePerCall) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "vnodePerCall:%d,", pAlterReq->vnodePerCall);
    newUser.vnodePerCall = pAlterReq->vnodePerCall;
  }

  if (pAlterReq->hasFailedLoginAttempts) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "failedLoginAttempts:%d,", pAlterReq->failedLoginAttempts);
    newUser.failedLoginAttempts = pAlterReq->failedLoginAttempts;
  }

  if (pAlterReq->hasPasswordLifeTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "passwordLifeTime:%d,", pAlterReq->passwordLifeTime);
    newUser.passwordLifeTime = pAlterReq->passwordLifeTime;
  }

  if (pAlterReq->hasPasswordReuseTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "passwordReuseTime:%d,", pAlterReq->passwordReuseTime);
    newUser.passwordReuseTime = pAlterReq->passwordReuseTime;
  }

  if (pAlterReq->hasPasswordReuseMax) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "passwordReuseMax:%d,", pAlterReq->passwordReuseMax);
    newUser.passwordReuseMax = pAlterReq->passwordReuseMax;
  }

  if (pAlterReq->hasPasswordLockTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "passwordLockTime:%d,", pAlterReq->passwordLockTime);
    newUser.passwordLockTime = pAlterReq->passwordLockTime;
  }

  if (pAlterReq->hasPasswordGraceTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "passwordGraceTime:%d,", pAlterReq->passwordGraceTime);
    newUser.passwordGraceTime = pAlterReq->passwordGraceTime;
  }

  if (pAlterReq->hasInactiveAccountTime) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "inactiveAccountTime:%d,", pAlterReq->inactiveAccountTime);
    newUser.inactiveAccountTime = pAlterReq->inactiveAccountTime;
  }

  if (pAlterReq->hasAllowTokenNum) {
    auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "allowTokenNum:%d,", pAlterReq->allowTokenNum);
    newUser.allowTokenNum = pAlterReq->allowTokenNum;
  }

  if (pAlterReq->numDropIpRanges > 0 || pAlterReq->numIpRanges > 0) {
    int32_t dummy = 0;

    // put previous ip whitelist into hash table
    SHashObj *m = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (m == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    for (int32_t i = 0; i < newUser.pIpWhiteListDual->num; i++) {
      SIpRange range;
      copyIpRange(&range, newUser.pIpWhiteListDual->pIpRanges + i);
      code = taosHashPut(m, &range, sizeof(range), &dummy, sizeof(dummy));
      if (code != 0) {
        taosHashCleanup(m);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }

    if (pAlterReq->numDropIpRanges > 0) {
      auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "dropIpRanges:%d,", pAlterReq->numDropIpRanges);

      for (int32_t i = 0; i < pAlterReq->numDropIpRanges; i++) {
        if (taosHashGetSize(m) == 0) {
          break;
        }

        SIpRange range;
        copyIpRange(&range, pAlterReq->pDropIpRanges + i);

        // for white list, drop default ip ranges is allowed, otherwise, we can never
        // convert white list to black list.

        code = taosHashRemove(m, &range, sizeof(range));
        if (code == TSDB_CODE_NOT_FOUND) {
          // treat not exist as success
          code = 0;
        }
        if (code != 0) {
          taosHashCleanup(m);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
      }
    }

    if (pAlterReq->numIpRanges > 0) {
      auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "addIpRanges:%d,", pAlterReq->numIpRanges);
      for (int32_t i = 0; i < pAlterReq->numIpRanges; i++) {
        SIpRange range;
        copyIpRange(&range, pAlterReq->pIpRanges + i);
        code = taosHashPut(m, &range, sizeof(range), &dummy, sizeof(dummy));
        if (code != 0) {
          taosHashCleanup(m);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
      }
    }

    int32_t numOfRanges = taosHashGetSize(m);
    if (numOfRanges > MND_MAX_USER_IP_RANGE) {
      taosHashCleanup(m);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_IP_RANGE, &lino, _OVER);
    }

    SIpWhiteListDual *p = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + numOfRanges * sizeof(SIpRange));
    if (p == NULL) {
      taosHashCleanup(m);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    void *pIter = taosHashIterate(m, NULL);
    int32_t i = 0;
    while (pIter) {
      size_t len = 0;
      SIpRange *key = taosHashGetKey(pIter, &len);
      memcpy(p->pIpRanges + i, key, sizeof(SIpRange));
      pIter = taosHashIterate(m, pIter);
      i++;
    }

    taosHashCleanup(m);
    p->num = numOfRanges;
    taosMemoryFreeClear(newUser.pIpWhiteListDual);
    sortIpWhiteList(p);
    newUser.pIpWhiteListDual = p;

    newUser.ipWhiteListVer++;
  }


  if (pAlterReq->numTimeRanges > 0 || pAlterReq->numDropTimeRanges) {
    int32_t dummy = 0;

    // put previous ip whitelist into hash table
    SHashObj *m = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (m == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    for (int32_t i = 0; i < newUser.pTimeWhiteList->num; i++) {
      SDateTimeWhiteListItem *range = &newUser.pTimeWhiteList->ranges[i];
      if (isDateTimeWhiteListItemExpired(range)) {
        continue;
      }
      code = taosHashPut(m, range, sizeof(*range), &dummy, sizeof(dummy));
      if (code != 0) {
        taosHashCleanup(m);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }

    if (pAlterReq->numDropTimeRanges > 0) {
      auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "dropTimeRanges:%d,", pAlterReq->numDropTimeRanges);
      for (int32_t i = 0; i < pAlterReq->numDropTimeRanges; i++) {
        if (taosHashGetSize(m) == 0) {
          break;
        }
        SDateTimeWhiteListItem range = { 0 };
        DateTimeRangeToWhiteListItem(&range, pAlterReq->pDropTimeRanges + i);

        code = taosHashRemove(m, &range, sizeof(range));
        if (code == TSDB_CODE_NOT_FOUND) {
          // treat not exist as success
          code = 0;
        }
        if (code != 0) {
          taosHashCleanup(m);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
      }
    }

    if (pAlterReq->numTimeRanges > 0) {
      auditLen += tsnprintf(auditLog + auditLen, sizeof(auditLog) - auditLen, "addTimeRanges:%d,", pAlterReq->numTimeRanges);
      for (int32_t i = 0; i < pAlterReq->numTimeRanges; i++) {
        SDateTimeWhiteListItem range = { 0 };
        DateTimeRangeToWhiteListItem(&range, pAlterReq->pTimeRanges + i);
        if (isDateTimeWhiteListItemExpired(&range)) {
          continue;
        }
        code = taosHashPut(m, &range, sizeof(range), &dummy, sizeof(dummy));
        if (code != 0) {
          taosHashCleanup(m);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
      }
    }

    int32_t numOfRanges = taosHashGetSize(m);
    if (numOfRanges > MND_MAX_USER_TIME_RANGE) {
      taosHashCleanup(m);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_TIME_RANGE, &lino, _OVER);
    }

    SDateTimeWhiteList *p = taosMemoryCalloc(1, sizeof(SDateTimeWhiteList) + numOfRanges * sizeof(SDateTimeWhiteListItem));
    if (p == NULL) {
      taosHashCleanup(m);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    void *pIter = taosHashIterate(m, NULL);
    int32_t i = 0;
    while (pIter) {
      size_t len = 0;
      SDateTimeWhiteListItem *key = taosHashGetKey(pIter, &len);
      memcpy(&p->ranges[i], key, sizeof(SDateTimeWhiteListItem));
      pIter = taosHashIterate(m, pIter);
      i++;
    }

    taosHashCleanup(m);
    p->num = numOfRanges;
    taosMemoryFreeClear(newUser.pTimeWhiteList);
    sortTimeWhiteList(p);
    newUser.pTimeWhiteList = p;
    newUser.timeWhiteListVer++;
  }
#endif // TD_ENTERPRISE

  TAOS_CHECK_GOTO(mndAlterUser(pMnode, &newUser, pReq), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (auditLen > 0) {
    auditLog[--auditLen] = 0; // remove last ','
  }
  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", pAlterReq->user, auditLog, auditLen, duration, 0);
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter at line %d since %s", pAlterReq->user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);
  return code;
}



static int32_t mndProcessAlterUserReq(SRpcMsg *pReq) {
  SAlterUserReq alterReq = {0};

  int32_t code = tDeserializeSAlterUserReq(pReq->pCont, pReq->contLen, &alterReq);
  if (code != 0) {
    mError("failed to deserialize alter user request at line %d since %s", __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  if (alterReq.user[0] == 0) {
    tFreeSAlterUserReq(&alterReq);
    mError("failed to alter user at line %d since invalid user format", __LINE__);
    TAOS_RETURN(TSDB_CODE_MND_INVALID_USER_FORMAT);
  }

  mInfo("user:%s, start to alter", alterReq.user);
  if (alterReq.alterType == TSDB_ALTER_USER_BASIC_INFO) {
    code = mndProcessAlterUserBasicInfoReq(pReq, &alterReq);
  } else {
    // code = mndProcessAlterUserPrivilegesReq(pReq, &alterReq); // obsolete
  }

  tFreeSAlterUserReq(&alterReq);
  TAOS_RETURN(code);
}

static int32_t mndResetAuditLogUser(SMnode *pMnode, const char* *user, bool isAdd) {
  if (user) {
    (void)taosThreadRwlockRdlock(&userCache.rw);
    if (isAdd) {
      if(userCache.auditLogUser[0] != 0) {
        (void)taosThreadRwlockUnlock(&userCache.rw);
        return 0;
      }
      (void)taosThreadRwlockUnlock(&userCache.rw);
      (void)taosThreadRwlockWrlock(&userCache.rw);
      (void)tsnprintf(userCache.auditLogUser, TSDB_USER_LEN, "%s", user);
      (void)taosThreadRwlockUnlock(&userCache.rw);
      return 0;
    } else if (strcmp(userCache.auditLogUser, user) != 0) {
      (void)taosThreadRwlockUnlock(&userCache.rw);
      return 0;
    }
    (void)taosThreadRwlockUnlock(&userCache.rw);
  }

  void     *pIter = NULL;
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  int32_t   len = strlen(TSDB_ROLE_SYSAUDIT_LOG) + 1;
  while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser))) {
    if (pUser->enable == 0) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }
    if (taosHashGet(pUser->roles, TSDB_ROLE_SYSAUDIT_LOG, len) != NULL) {
      (void)taosThreadRwlockWrlock(&userCache.rw);
      (void)tsnprintf(userCache.auditLogUser, TSDB_USER_LEN, "%s", pUser->name);
      (void)taosThreadRwlockUnlock(&userCache.rw);
      sdbCancelFetch(pSdb, pIter);
      mndReleaseUser(pMnode, pUser);
      return 0;
    }
    mndReleaseUser(pMnode, pUser);
  }
  return TSDB_CODE_MND_USER_NOT_AVAILABLE;
}

int32_t mndGetAuditUser(SMnode *pMnode, char *user) {
  (void)taosThreadRwlockRdlock(&userCache.rw);
  if (userCache.auditLogUser[0] != 0) {
    (void)tsnprintf(user, TSDB_USER_LEN, "%s", userCache.auditLogUser);
    (void)taosThreadRwlockUnlock(&userCache.rw);
    return 0;
  }
  (void)taosThreadRwlockUnlock(&userCache.rw);

  int32_t code = 0;
  if ((code = mndResetAuditLogUser(pMnode, NULL, false)) != 0) {
    return code;
  }

  (void)taosThreadRwlockRdlock(&userCache.rw);
  (void)tsnprintf(user, TSDB_USER_LEN, "%s", userCache.auditLogUser);
  (void)taosThreadRwlockUnlock(&userCache.rw);

  return 0;
}

static int32_t mndDropUser(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "drop-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) < 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  if (mndDropTokensByUser(pMnode, pTrans, pUser->user) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  userCacheRemoveUser(pUser->user);
  mndDropCachedTokensByUser(pUser->user);
  mndResetAuditLogUser(pMnode, pUser->user);

  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropUserReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = 0;
  int32_t      lino = 0;
  SUserObj    *pOperUser = NULL;
  SUserObj    *pUser = NULL;
  SDropUserReq dropReq = {0};
  int64_t      tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSDropUserReq(pReq->pCont, pReq->contLen, &dropReq), &lino, _OVER);

  mInfo("user:%s, start to drop", dropReq.user);

  if (dropReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  if (0 == strcmp(dropReq.user, TSDB_DEFAULT_USER)) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, dropReq.user, &pUser), &lino, _OVER);

  code = mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  // TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_DROP_USER), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckSysObjPrivilege(pMnode, pOperUser, RPC_MSG_TOKEN(pReq), PRIV_USER_DROP, 0, 0, NULL, NULL),
                  &lino, _OVER);

  TAOS_CHECK_GOTO(mndDropUser(pMnode, pReq, pUser), &lino, _OVER);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "dropUser", "", dropReq.user, dropReq.sql, dropReq.sqlLen, duration, 0);
  }

_OVER:
  if (dropReq.ignoreNotExists && code == TSDB_CODE_MND_USER_NOT_EXIST) {
    code = 0;
  } else if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop at line %d since %s", dropReq.user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSDropUserReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessGetUserAuthReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = 0;
  int32_t         lino = 0;
  int32_t         contLen = 0;
  void           *pRsp = NULL;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  TAOS_CHECK_EXIT(tDeserializeSGetUserAuthReq(pReq->pCont, pReq->contLen, &authReq));
  mTrace("user:%s, start to get auth", authReq.user);

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, authReq.user, &pUser));

  TAOS_CHECK_EXIT(mndSetUserAuthRsp(pMnode, pUser, &authRsp));

  contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  contLen = tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }

_exit:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserAuthRsp(&authRsp);
  if (code < 0) {
    mError("user:%s, failed to get auth at line %d since %s", authReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  pReq->code = code;

  TAOS_RETURN(code);
}



static void base32Encode(const uint8_t *in, int32_t inLen, char *out) {
  int buffer = 0, bits = 0;
  int outLen = 0;

  // process all input bytes
  for (int i = 0; i < inLen; i++) {
    buffer = (buffer << 8) | in[i];
    bits += 8;

    while (bits >= 5) {
      int v = (buffer >> (bits - 5)) & 0x1F;
      out[outLen++] = (v >= 26) ? (v - 26 + '2') : (v + 'A');
      bits -= 5;
    }
  }

  // process remaining bits
  if (bits > 0) {
    int v = (buffer << (5 - bits)) & 0x1F;
    out[outLen++] = (v >= 26) ? (v - 26 + '2') : (v + 'A');
  }

  out[outLen] = '\0';
}


static int32_t mndCreateTotpSecret(SMnode *pMnode, SUserObj *pUser, SRpcMsg *pReq) {
  SCreateTotpSecretRsp rsp = {0};

  base32Encode((uint8_t *)pUser->totpsecret, sizeof(pUser->totpsecret), rsp.totpSecret);
  tstrncpy(rsp.user, pUser->user, sizeof(rsp.user));

  int32_t len = tSerializeSCreateTotpSecretRsp(NULL, 0, &rsp);
  if (len < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  void *pData = taosMemoryMalloc(len);
  if (pData == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (tSerializeSCreateTotpSecretRsp(pData, len, &rsp) != len) {
    taosMemoryFree(pData);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "create-totp-secret");
  if (pTrans == NULL) {
    mError("user:%s, failed to create totp secret since %s", pUser->user, terrstr());
    taosMemoryFree(pData);
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to create totp secret for user:%s", pTrans->id, pUser->user);

  mndTransSetUserData(pTrans, pData, len);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) < 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}


static int32_t mndProcessCreateTotpSecretReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = 0;
  int32_t              lino = 0;
  SUserObj            *pUser = NULL;
  SUserObj             newUser = {0};
  SCreateTotpSecretReq req = {0};
  int64_t              tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSCreateTotpSecretReq(pReq->pCont, pReq->contLen, &req), &lino, _OVER);
  mTrace("user:%s, start to create/update totp secret", req.user);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, req.user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckTotpSecretPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);
  taosSafeRandBytes((uint8_t *)newUser.totpsecret, sizeof(newUser.totpsecret));
  TAOS_CHECK_GOTO(mndCreateTotpSecret(pMnode, &newUser, pReq), &lino, _OVER); 

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    double  duration = (double)(taosGetTimestampMs()- tss) / 1000.0;
    auditRecord(pReq, pMnode->clusterId, "createTotpSecret", "", req.user, req.sql, req.sqlLen, duration, 0);
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create totp secret at line %d since %s", req.user, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);
  tFreeSCreateTotpSecretReq(&req);
  TAOS_RETURN(code);
}



int32_t mndBuildSMCreateTotpSecretResp(STrans *pTrans, void **ppResp, int32_t *pRespLen) {
  // user data is the response
  *ppResp = pTrans->userData;
  *pRespLen = pTrans->userDataLen;
  pTrans->userData = NULL;
  pTrans->userDataLen = 0;
  return 0;
}



static int32_t mndProcessDropTotpSecretReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = 0;
  int32_t            lino = 0;
  SUserObj          *pUser = NULL;
  SDropTotpSecretReq req = {0};
  int64_t            tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSDropTotpSecretReq(pReq->pCont, pReq->contLen, &req), &lino, _OVER);
  mTrace("user:%s, start to drop totp secret", req.user);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, req.user, &pUser), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckTotpSecretPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), pUser), &lino, _OVER);

  SUserObj newUser = {0};
  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);
  (void)memset(newUser.totpsecret, 0, sizeof(newUser.totpsecret));
  TAOS_CHECK_GOTO(mndAlterUser(pMnode, &newUser, pReq), &lino, _OVER); 

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    double  duration = (double)(taosGetTimestampMs()- tss) / 1000.0;
    auditRecord(pReq, pMnode->clusterId, "dropTotpSecret", "", req.user, req.sql, req.sqlLen, duration, 0);
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop totp secret at line %d since %s", req.user, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);
  tFreeSDropTotpSecretReq(&req);
  TAOS_RETURN(code);
}



bool mndIsTotpEnabledUser(SUserObj *pUser) {
  for (int32_t i = 0; i < sizeof(pUser->totpsecret); i++) {
    if (pUser->totpsecret[i] != 0) {
      return true;
    }
  }
  return false;
}


static int32_t mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;
  char      tBuf[TSDB_MAX_SUBROLE * TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t   bufSize = sizeof(tBuf) - VARSTR_HEADER_SIZE;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, pShow->pIter, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->createdTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    flag = mndIsTotpEnabledUser(pUser) ? 1 : 0;
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    cols++;

    int32_t tlen = convertIpWhiteListToStr(pUser, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    cols++;
    tlen = convertTimeRangesToStr(pUser, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      void  *pIter = NULL;
      size_t klen = 0, tlen = 0;
      char  *pBuf = POINTER_SHIFT(tBuf, VARSTR_HEADER_SIZE);
      while ((pIter = taosHashIterate(pUser->roles, pIter))) {
        char *roleName = taosHashGetKey(pIter, &klen);
        tlen += snprintf(pBuf + tlen, bufSize - tlen, "%s,", roleName);
      }
      if (tlen > 0) {
        pBuf[tlen - 1] = 0;  // remove last ','
      } else {
        pBuf[0] = 0;
      }
      varDataSetLen(tBuf, tlen);
      COL_DATA_SET_VAL_GOTO((const char *)tBuf, false, pUser, pShow->pIter, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static int32_t mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
#ifdef TD_ENTERPRISE
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;
  char     *pBuf = NULL;
  char      tBuf[TSDB_MAX_SUBROLE * TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t   bufSize = sizeof(tBuf) - VARSTR_HEADER_SIZE;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, pShow->pIter, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->createdTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    flag = mndIsTotpEnabledUser(pUser) ? 1 : 0;
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->changePass, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char pass[TSDB_PASSWORD_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(pass, pUser->passwords[0].pass, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)pass, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sessionPerUser, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->connectTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->connectIdleTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->callPerSession, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->vnodePerCall, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->failedLoginAttempts, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->passwordLifeTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->passwordReuseTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->passwordReuseMax, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->passwordLockTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->passwordGraceTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->inactiveAccountTime, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->allowTokenNum, false, pUser, pShow->pIter, _exit);

    cols++;
    int32_t tlen = convertIpWhiteListToStr(pUser, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    cols++;
    tlen = convertTimeRangesToStr(pUser, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      void  *pIter = NULL;
      size_t klen = 0, tlen = 0;
      char  *pBuf = POINTER_SHIFT(tBuf, VARSTR_HEADER_SIZE);
      while ((pIter = taosHashIterate(pUser->roles, pIter))) {
        char *roleName = taosHashGetKey(pIter, &klen);
        tlen += snprintf(pBuf + tlen, bufSize - tlen, "%s,", roleName);
      }
      if (tlen > 0) {
        pBuf[tlen - 1] = 0;  // remove last ','
      } else {
        pBuf[0] = 0;
      }
      varDataSetLen(tBuf, tlen);
      COL_DATA_SET_VAL_GOTO((const char *)tBuf, false, pUser, pShow->pIter, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
#endif
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

static int32_t mndLoopHash(SHashObj *hash, char *priType, SSDataBlock *pBlock, int32_t *pNumOfRows, SSdb *pSdb,
                           SUserObj *pUser, SShowObj *pShow, char **condition, char **sql) {
  char   *value = taosHashIterate(hash, NULL);
  char   *user = pUser->user;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t cols = 0;
  int32_t numOfRows = *pNumOfRows;

  while (value != NULL) {
    cols = 0;
    char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(userName, user, pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)userName, false, NULL, NULL, _exit);

    char privilege[20] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(privilege, priType, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)privilege, false, NULL, NULL, _exit);

    size_t keyLen = 0;
    void  *key = taosHashGetKey(value, &keyLen);

    char dbName[TSDB_DB_NAME_LEN] = {0};
    (void)mndExtractShortDbNameFromStbFullName(key, dbName);
    char dbNameContent[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbNameContent, dbName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)dbNameContent, false, NULL, NULL, _exit);

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractTbNameFromStbFullName(key, tableName, TSDB_TABLE_NAME_LEN);
    char tableNameContent[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tableNameContent, tableName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)tableNameContent, false, NULL, NULL, _exit);

    if (strcmp("t", value) != 0 && strcmp("v", value) != 0) {
      SNode  *pAst = NULL;
      int32_t sqlLen = 0;
      size_t  bufSz = strlen(value) + 1;
      if (bufSz < 6) bufSz = 6;
      TAOS_MEMORY_REALLOC(*sql, bufSz);
      if (*sql == NULL) {
        code = terrno;
        goto _exit;
      }
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }

      if (nodesStringToNode(value, &pAst) == 0) {
        if (nodesNodeToSQLFormat(pAst, *sql, bufSz, &sqlLen, true) != 0) {
          sqlLen = tsnprintf(*sql, bufSz, "error");
        }
        nodesDestroyNode(pAst);
      }

      if (sqlLen == 0) {
        sqlLen = tsnprintf(*sql, bufSz, "error");
      }

      STR_WITH_MAXSIZE_TO_VARSTR((*condition), (*sql), pShow->pMeta->pSchemas[cols].bytes);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, NULL, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, NULL, _exit);
    } else {
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }
      STR_WITH_MAXSIZE_TO_VARSTR((*condition), "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, NULL, _exit);

      char notes[64 + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, value[0] == 'v' ? "view" : "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, NULL, _exit);
    }

    numOfRows++;
    value = taosHashIterate(hash, value);
  }
  *pNumOfRows = numOfRows;
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    sdbRelease(pSdb, pUser);
    sdbCancelFetch(pSdb, pShow->pIter);
  }
  TAOS_RETURN(code);
}

#if 0
static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite = NULL;
  char     *condition = NULL;
  char     *sql = NULL;

  bool fetchNextUser = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextUser) {
      pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextUser = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      pUser = sdbAcquire(pSdb, SDB_USER, pKey);
      if (!pUser) {
        continue;
      }
    }

    int32_t numOfReadDbs = 0; //taosHashGetSize(pUser->readDbs);
    int32_t numOfWriteDbs = 0; //taosHashGetSize(pUser->writeDbs);
    int32_t numOfTopics = taosHashGetSize(pUser->topics);
    int32_t numOfReadTbs = taosHashGetSize(pUser->selectTbs);
    int32_t numOfWriteTbs = taosHashGetSize(pUser->insertTbs);
    int32_t numOfAlterTbs = taosHashGetSize(pUser->alterTbs);
    int32_t numOfReadViews = taosHashGetSize(pUser->readViews);
    int32_t numOfWriteViews = taosHashGetSize(pUser->writeViews);
    int32_t numOfAlterViews = taosHashGetSize(pUser->alterViews);
    if (numOfRows + numOfReadDbs + numOfWriteDbs + numOfTopics + numOfReadTbs + numOfWriteTbs + numOfAlterTbs +
            numOfReadViews + numOfWriteViews + numOfAlterViews >=
        rows) {
      mInfo(
          "will restore. current num of rows: %d, read dbs %d, write dbs %d, topics %d, read tables %d, write tables "
          "%d, alter tables %d, select views %d, write views %d, alter views %d",
          numOfRows, numOfReadDbs, numOfWriteDbs, numOfTopics, numOfReadTbs, numOfWriteTbs, numOfAlterTbs,
          numOfReadViews, numOfWriteViews, numOfAlterViews);
      pShow->restore = true;
      sdbRelease(pSdb, pUser);
      break;
    }

    if (pUser->superUser) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      char objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(objName, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
    }
#if 0
    char *db = taosHashIterate(pUser->readDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "read", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      code = tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      if (code < 0) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->readDbs, db);
    }

    db = taosHashIterate(pUser->writeDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "write", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      code = tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      if (code < 0) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->writeDbs, db);
    }
#endif
    TAOS_CHECK_EXIT(mndLoopHash(pUser->selectTbs, "select", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->insertTbs, "insert", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterTbs, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->readViews, "read", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->writeViews, "write", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterViews, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    char *topic = taosHashIterate(pUser->topics, NULL);
    while (topic != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "subscribe", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
      tstrncpy(varDataVal(topicName), mndGetDbStr(topic), TSDB_TOPIC_NAME_LEN - 2);
      varDataSetLen(topicName, strlen(varDataVal(topicName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)topicName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      topic = taosHashIterate(pUser->topics, topic);
    }

    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(condition);
  taosMemoryFreeClear(sql);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}
#endif

static int32_t mndShowTablePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows, SUserObj *pObj,
                                      SHashObj *privTbs, EPrivType privType, char *pBuf, int32_t bufSize, int32_t *pNumOfRows) {
  int32_t     code = 0, lino = 0;
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     cols = 0, qBufSize = bufSize - VARSTR_HEADER_SIZE;
  int32_t     numOfRows = *pNumOfRows;
  char       *qBuf = NULL;
  char       *sql = NULL;
  char        roleName[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  const char *privName = privInfoGetName(privType);

  STR_WITH_MAXSIZE_TO_VARSTR(roleName, pObj->name, pShow->pMeta->pSchemas[cols].bytes);

  void *pIter = NULL;
  while ((pIter = taosHashIterate(privTbs, pIter))) {
    SPrivTblPolicies *pPolices = (SPrivTblPolicies *)pIter;
    SArray           *tblPolicies = pPolices->policy;

    char   *key = taosHashGetKey(pPolices, NULL);
    int32_t objType = PRIV_OBJ_UNKNOWN;
    char    dbName[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char    tblName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if ((code = privObjKeyParse(key, &objType, dbName, sizeof(dbName), tblName, sizeof(tblName), false))) {
      sdbRelease(pSdb, pObj);
      sdbCancelFetch(pSdb, pShow->pIter);
      TAOS_CHECK_EXIT(code);
    }

    int32_t nTbPolicies = taosArrayGetSize(tblPolicies);
    for (int32_t i = 0; i < nTbPolicies; ++i) {
      SPrivTblPolicy *tbPolicy = (SPrivTblPolicy *)TARRAY_GET_ELEM(tblPolicies, i);
      cols = 0;
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)roleName, false, pObj, pShow->pIter, _exit);

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privName, pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privObjGetName(objType), pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, dbName, pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, tblName, pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      // condition
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        SNode  *pAst = NULL;
        int32_t sqlLen = 0;
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        if (tbPolicy->condLen > 0) {
          if (nodesStringToNode(tbPolicy->cond, &pAst) == 0) {
            if (nodesNodeToSQLFormat(pAst, qBuf, qBufSize, &sqlLen, true) != 0) {
              sqlLen = tsnprintf(qBuf, qBufSize, "error");
            }
            nodesDestroyNode(pAst);
          }
          if (sqlLen == 0) {
            sqlLen = tsnprintf(qBuf, qBufSize, "error");
          }
        } else {
          sqlLen = tsnprintf(qBuf, qBufSize, "");
        }
        varDataSetLen(pBuf, sqlLen);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      // notes
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR((pBuf), "", 2);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      // columns
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        SArray *pCols = tbPolicy->cols;
        int32_t nCols = taosArrayGetSize(pCols);
        int32_t totalLen = 0;
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        for (int32_t j = 0; j < nCols; ++j) {
          SColNameFlag *pCol = (SColNameFlag *)TARRAY_GET_ELEM(pCols, j);
          char          tmpBuf[TSDB_COL_NAME_LEN + 16] = {0};
          int32_t       tmpLen = 0;
          if (IS_MASK_ON(pCol)) {
            tmpLen = snprintf(tmpBuf, sizeof(tmpBuf), "mask(%s)%s", pCol->colName, j == nCols - 1 ? "" : ",");
          } else {
            tmpLen = snprintf(tmpBuf, sizeof(tmpBuf), "%s%s", pCol->colName, j == nCols - 1 ? "" : ",");
          }
          if(totalLen + tmpLen > qBufSize) {
            break;
          }
          (void)memcpy(POINTER_SHIFT(qBuf, totalLen), tmpBuf, tmpLen);
          totalLen += tmpLen;
        }
        varDataSetLen(pBuf, totalLen);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      // update_time
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        char updateTime[40] = {0};
        (void)formatTimestampLocal(updateTime, tbPolicy->updateUs, TSDB_TIME_PRECISION_MICRO);
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, updateTime, pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      ++numOfRows;
    }
  }
  *pNumOfRows = numOfRows;
_exit:
  TAOS_RETURN(code);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0, lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  SUserObj *pObj = NULL;
  char     *pBuf = NULL, *qBuf = NULL;
  char     *sql = NULL;
  char      roleName[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t   bufSize = TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE;

  bool fetchNextInstance = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextInstance) {
      pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pObj);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextInstance = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      if (!(pObj = sdbAcquire(pSdb, SDB_USER, pKey))) {
        continue;
      }
    }

    int32_t nSysPrivileges = 0, nObjPrivileges = 0;
    if (nSysPrivileges + nObjPrivileges >= rows) {
      pShow->restore = true;
      sdbRelease(pSdb, pObj);
      break;
    }

    if (!pBuf && !(pBuf = taosMemoryMalloc(bufSize))) {
      sdbCancelFetch(pSdb, pShow->pIter);
      sdbRelease(pSdb, pObj);
      TAOS_CHECK_EXIT(terrno);
    }

    cols = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(roleName, pObj->name, pShow->pMeta->pSchemas[cols].bytes);

    // system privileges
    SPrivIter privIter = {0};
    privIterInit(&privIter, &pObj->sysPrivs);
    SPrivInfo *pPrivInfo = NULL;
    while (privIterNext(&privIter, &pPrivInfo)) {
      cols = 0;
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)roleName, false, pObj, pShow->pIter, _exit);

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, pPrivInfo->name, pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privObjGetName(PRIV_OBJ_CLUSTER), pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      // skip db, table, condition, notes, columns, update_time
      COL_DATA_SET_EMPTY_VARCHAR(pBuf, 6);
      numOfRows++;
    }

    // object privileges
    void *pIter = NULL;
    while ((pIter = taosHashIterate(pObj->objPrivs, pIter))) {
      SPrivObjPolicies *pPolices = (SPrivObjPolicies *)pIter;

      char   *key = taosHashGetKey(pPolices, NULL);
      int32_t objType = PRIV_OBJ_UNKNOWN;
      char    dbName[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char    tblName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      if ((code = privObjKeyParse(key, &objType, dbName, sizeof(dbName), tblName, sizeof(tblName), false))) {
        sdbRelease(pSdb, pObj);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_EXIT(code);
      }

      SPrivIter privIter = {0};
      privIterInit(&privIter, &pPolices->policy);
      SPrivInfo *pPrivInfo = NULL;
      while (privIterNext(&privIter, &pPrivInfo)) {
        cols = 0;
        SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
        COL_DATA_SET_VAL_GOTO((const char *)roleName, false, pObj, pShow->pIter, _exit);

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, pPrivInfo->name, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privObjGetName(objType), pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, dbName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, tblName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        // skip condition, notes, columns, update_time
        COL_DATA_SET_EMPTY_VARCHAR(pBuf, 4);

        numOfRows++;
      }
    }

    // table level privileges
    TAOS_CHECK_EXIT(mndShowTablePrivileges(pReq, pShow, pBlock, rows - numOfRows, pObj, pObj->selectTbs,
                                           PRIV_TBL_SELECT, pBuf, bufSize, &numOfRows));
    TAOS_CHECK_EXIT(mndShowTablePrivileges(pReq, pShow, pBlock, rows - numOfRows, pObj, pObj->insertTbs,
                                           PRIV_TBL_INSERT, pBuf, bufSize, &numOfRows));
    TAOS_CHECK_EXIT(mndShowTablePrivileges(pReq, pShow, pBlock, rows - numOfRows, pObj, pObj->updateTbs,
                                           PRIV_TBL_UPDATE, pBuf, bufSize, &numOfRows));
    TAOS_CHECK_EXIT(mndShowTablePrivileges(pReq, pShow, pBlock, rows - numOfRows, pObj, pObj->deleteTbs,
                                           PRIV_TBL_DELETE, pBuf, bufSize, &numOfRows));
#if 0
    while ((pIter = taosHashIterate(pObj->selectTbs, pIter))) {
      SPrivTblPolicies *pPolices = (SPrivTblPolicies *)pIter;
      SArray           *tblPolicies = pPolices->policy;

      char   *key = taosHashGetKey(pPolices, NULL);
      int32_t objType = PRIV_OBJ_UNKNOWN;
      char    dbName[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char    tblName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      if ((code = privObjKeyParse(key, &objType, dbName, sizeof(dbName), tblName, sizeof(tblName), false))) {
        sdbRelease(pSdb, pObj);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_EXIT(code);
      }

      int32_t nTbPolicies = taosArrayGetSize(tblPolicies);
      for (int32_t i = 0; i < nTbPolicies; ++i) {
        SPrivTblPolicy *tbPolicy = (SPrivTblPolicy *)TARRAY_GET_ELEM(tblPolicies, i);
        cols = 0;
        SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
        COL_DATA_SET_VAL_GOTO((const char *)roleName, false, pObj, pShow->pIter, _exit);

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privInfoGetName(PRIV_TBL_SELECT), pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, dbName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, tblName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        // skip condition, notes, columns, update_time
        COL_DATA_SET_EMPTY_VARCHAR(pBuf, 4);

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privObjGetName(objType), pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }
        numOfRows++;
      }
    }
#endif
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(pBuf);
  taosMemoryFreeClear(sql);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

int32_t mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                int32_t *pRspLen, int64_t ipWhiteListVer) {
  int32_t           code = 0;
  int32_t           lino = 0;
  int32_t           rspLen = 0;
  void             *pRsp = NULL;
  SUserAuthBatchRsp batchRsp = {0};

  batchRsp.pArray = taosArrayInit(numOfUses, sizeof(SGetUserAuthRsp));
  if (batchRsp.pArray == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  int64_t now = taosGetTimestampMs();
  for (int32_t i = 0; i < numOfUses; ++i) {
    SUserObj *pUser = NULL;
    code = mndAcquireUser(pMnode, pUsers[i].user, &pUser);
    if (pUser == NULL) {
      if (TSDB_CODE_MND_USER_NOT_EXIST == code) {
        SGetUserAuthRsp rsp = {.dropped = 1};
        (void)memcpy(rsp.user, pUsers[i].user, TSDB_USER_LEN);
        TSDB_CHECK_NULL(taosArrayPush(batchRsp.pArray, &rsp), code, lino, _OVER, TSDB_CODE_OUT_OF_MEMORY);
      }
      mError("user:%s, failed to auth user since %s", pUsers[i].user, tstrerror(code));
      code = 0;
      continue;
    }

    pUsers[i].version = ntohl(pUsers[i].version);
    if ((pUser->authVersion <= pUsers[i].version) && (ipWhiteListVer == pMnode->ipWhiteVer) &&
        !mndNeedRetrieveRole(pUser)) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }

    SGetUserAuthRsp rsp = {0};
    code = mndSetUserAuthRsp(pMnode, pUser, &rsp);
    if (code) {
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    if (!(taosArrayPush(batchRsp.pArray, &rsp))) {
      code = terrno;
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    pUser->lastRoleRetrieve = now;  // update user's last retrieve time
    mndReleaseUser(pMnode, pUser);
  }

  if (taosArrayGetSize(batchRsp.pArray) <= 0) {
    *ppRsp = NULL;
    *pRspLen = 0;

    tFreeSUserAuthBatchRsp(&batchRsp);
    return 0;
  }

  rspLen = tSerializeSUserAuthBatchRsp(NULL, 0, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
  pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  rspLen = tSerializeSUserAuthBatchRsp(pRsp, rspLen, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
_OVER:
  tFreeSUserAuthBatchRsp(&batchRsp);
  if (code < 0) {
    for (int32_t i = 0; i < numOfUses; ++i) {
      SUserObj *pUser = NULL;
      if (mndAcquireUser(pMnode, pUsers[i].user, &pUser) != 0) {
        continue;
      }
      pUser->lastRoleRetrieve = 0;  // reset last retrieve time on error
      mndReleaseUser(pMnode, pUser);
    }
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pRsp);
    rspLen = 0;
  }
  *ppRsp = pRsp;
  *pRspLen = rspLen;

  TAOS_RETURN(code);
}

static int32_t mndRemoveDbPrivileges(SHashObj *pHash, const char *dbFName, int32_t dbFNameLen, int32_t *nRemoved) {
  void *pVal = NULL;
  while ((pVal = taosHashIterate(pHash, pVal))) {
    size_t keyLen = 0;
    char  *pKey = (char *)taosHashGetKey(pVal, &keyLen);
    if (pKey == NULL || keyLen <= dbFNameLen) continue;
    if ((*(pKey + dbFNameLen) == '.') && strncmp(pKey, dbFName, dbFNameLen) == 0) {
      TAOS_CHECK_RETURN(taosHashRemove(pHash, pKey, keyLen));
      if (nRemoved) ++(*nRemoved);
    }
  }
  TAOS_RETURN(0);
}

int32_t mndUserDropRole(SMnode *pMnode, STrans *pTrans, SRoleObj *pObj) {
  int32_t   code = 0, lino = 0;
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  void     *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser))) {
    SHashObj *pRole = taosHashGet(pUser->roles, pObj->name, strlen(pObj->name) + 1);
    if (!pRole) {
      sdbRelease(pSdb, pUser);
      pUser = NULL;
      continue;
    }

    SUserObj newUser = {0};
    TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
    code = taosHashRemove(newUser.roles, pObj->name, strlen(pObj->name) + 1);
    if (code == TSDB_CODE_NOT_FOUND) {
      sdbRelease(pSdb, pUser);
      pUser = NULL;
      mndUserFreeObj(&newUser);
      continue;
    }
    if (code != 0) {
      mndUserFreeObj(&newUser);
      TAOS_CHECK_EXIT(code);
    }
    SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
    if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
      mndUserFreeObj(&newUser);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY))) {
      mndUserFreeObj(&newUser);
      TAOS_CHECK_EXIT(code);
    }
    sdbRelease(pSdb, pUser);
    pUser = NULL;
    mndUserFreeObj(&newUser);
  }
_exit:
  if (pIter) sdbCancelFetch(pSdb, pIter);
  if (pUser) sdbRelease(pSdb, pUser);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SSHashObj **ppUsers) {
  int32_t    code = 0, lino = 0;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    dbLen = strlen(pDb->name);
  void      *pIter = NULL;
  SUserObj  *pUser = NULL;
  SUserObj   newUser = {0};
  SSHashObj *pUsers = ppUsers ? *ppUsers : NULL;
  bool       output = (ppUsers != NULL);
#ifdef PRIV_TODO
  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    bool update = false;
    bool inReadDb = false; //(taosHashGet(pUser->readDbs, pDb->name, dbLen + 1) != NULL);
    bool inWriteDb = false; //(taosHashGet(pUser->writeDbs, pDb->name, dbLen + 1) != NULL);
    bool inUseDb = (taosHashGet(pUser->useDbs, pDb->name, dbLen + 1) != NULL);
    bool inReadTbs = taosHashGetSize(pUser->selectTbs) > 0;
    bool inWriteTbs = taosHashGetSize(pUser->insertTbs) > 0;
    bool inAlterTbs = taosHashGetSize(pUser->alterTbs) > 0;
    bool inReadViews = taosHashGetSize(pUser->readViews) > 0;
    bool inWriteViews = taosHashGetSize(pUser->writeViews) > 0;
    bool inAlterViews = taosHashGetSize(pUser->alterViews) > 0;
    // no need remove pUser->topics since topics must be dropped ahead of db
    if (!inReadDb && !inWriteDb && !inReadTbs && !inWriteTbs && !inAlterTbs && !inReadViews && !inWriteViews &&
        !inAlterViews) {
      sdbRelease(pSdb, pUser);
      continue;
    }
    SUserObj *pTargetUser = &newUser;
    if (output) {
      if (!pUsers) {
        TSDB_CHECK_NULL(pUsers = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)), code, lino,
                        _exit, TSDB_CODE_OUT_OF_MEMORY);
        *ppUsers = pUsers;
      }
      void   *pVal = NULL;
      int32_t userLen = strlen(pUser->user) + 1;
      if ((pVal = tSimpleHashGet(pUsers, pUser->user, userLen)) != NULL) {
        pTargetUser = (SUserObj *)pVal;
      } else {
        TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
        TAOS_CHECK_EXIT(tSimpleHashPut(pUsers, pUser->user, userLen, &newUser, sizeof(SUserObj)));
        TSDB_CHECK_NULL((pVal = tSimpleHashGet(pUsers, pUser->user, userLen)), code, lino, _exit,
                        TSDB_CODE_OUT_OF_MEMORY);
        pTargetUser = (SUserObj *)pVal;
      }
    } else {
      TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
    }
    if (inReadDb) {
      // TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->readDbs, pDb->name, dbLen + 1));
    }
    if (inWriteDb) {
      // TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->writeDbs, pDb->name, dbLen + 1));
    }
    if (inUseDb) {
      TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->useDbs, pDb->name, dbLen + 1));
    }
    update = inReadDb || inWriteDb || inUseDb;

    int32_t nRemovedReadTbs = 0;
    int32_t nRemovedWriteTbs = 0;
    int32_t nRemovedAlterTbs = 0;
    if (inReadTbs || inWriteTbs || inAlterTbs) {
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->selectTbs, pDb->name, dbLen, &nRemovedReadTbs));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->insertTbs, pDb->name, dbLen, &nRemovedWriteTbs));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->alterTbs, pDb->name, dbLen, &nRemovedAlterTbs));
      if (!update) update = nRemovedReadTbs > 0 || nRemovedWriteTbs > 0 || nRemovedAlterTbs > 0;
    }

    int32_t nRemovedReadViews = 0;
    int32_t nRemovedWriteViews = 0;
    int32_t nRemovedAlterViews = 0;
    if (inReadViews || inWriteViews || inAlterViews) {
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->readViews, pDb->name, dbLen, &nRemovedReadViews));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->writeViews, pDb->name, dbLen, &nRemovedWriteViews));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->alterViews, pDb->name, dbLen, &nRemovedAlterViews));
      if (!update) update = nRemovedReadViews > 0 || nRemovedWriteViews > 0 || nRemovedAlterViews > 0;
    }

    if (!output) {
      if (update) {
        SSdbRaw *pCommitRaw = mndUserActionEncode(pTargetUser);
        if (pCommitRaw == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pCommitRaw));
        TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
      }
      mndUserFreeObj(&newUser);
    }
    sdbRelease(pSdb, pUser);
  }
#endif
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    mndUserFreeObj(&newUser);
  }
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  if (!output) mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(stb) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};
#ifdef PRIV_TODO
  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.selectTbs, stb, len) != NULL);
    bool inWrite = (taosHashGet(newUser.insertTbs, stb, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterTbs, stb, len) != NULL);
    if (inRead || inWrite || inAlter) {
      code = taosHashRemove(newUser.selectTbs, stb, len);
      if (code < 0) {
        mError("failed to remove selectTbs:%s from user:%s", stb, pUser->user);
      }
      code = taosHashRemove(newUser.insertTbs, stb, len);
      if (code < 0) {
        mError("failed to remove insertTbs:%s from user:%s", stb, pUser->user);
      }
      code = taosHashRemove(newUser.alterTbs, stb, len);
      if (code < 0) {
        mError("failed to remove alterTbs:%s from user:%s", stb, pUser->user);
      }

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code != 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }
#endif
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveView(SMnode *pMnode, STrans *pTrans, char *view) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(view) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};
#ifdef PRIV_TODO
  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readViews, view, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeViews, view, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterViews, view, len) != NULL);
    if (inRead || inWrite || inAlter) {
      code = taosHashRemove(newUser.readViews, view, len);
      if (code < 0) {
        mError("failed to remove readViews:%s from user:%s", view, pUser->user);
      }
      code = taosHashRemove(newUser.writeViews, view, len);
      if (code < 0) {
        mError("failed to remove writeViews:%s from user:%s", view, pUser->user);
      }
      code = taosHashRemove(newUser.alterViews, view, len);
      if (code < 0) {
        mError("failed to remove alterViews:%s from user:%s", view, pUser->user);
      }

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code < 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }
#endif
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(topic) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};
#ifdef PRIV_TODO
  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) {
      break;
    }

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inTopic = (taosHashGet(newUser.topics, topic, len) != NULL);
    if (inTopic) {
      code = taosHashRemove(newUser.topics, topic, len);
      if (code < 0) {
        mError("failed to remove topic:%s from user:%s", topic, pUser->user);
      }
      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code < 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }
#endif
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser) {
  // ver = 0, disable ip white list
  // ver > 0, enable ip white list
  return tsEnableWhiteList ? pUser->ipWhiteListVer : 0;
}

int64_t mndGetUserTimeWhiteListVer(SMnode *pMnode, SUserObj *pUser) {
  // ver = 0, disable datetime white list
  // ver > 0, enable datetime white list
  return tsEnableWhiteList ? pUser->timeWhiteListVer : 0;
}

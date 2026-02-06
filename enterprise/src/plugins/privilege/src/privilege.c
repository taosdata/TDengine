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
#include "mndDb.h"
#include "mndDef.h"
#include "mndPrivilege.h"
#include "mndRole.h"
#include "mndToken.h"
#include "mndTopic.h"
#include "mndUser.h"

#define GET_PRIV_OBJ_SIZE(obj, size)               \
  do {                                             \
    if (obj) {                                     \
      (size) += taosHashGetSize((obj)->objPrivs);  \
      (size) += taosHashGetSize((obj)->selectTbs); \
      (size) += taosHashGetSize((obj)->insertTbs); \
      (size) += taosHashGetSize((obj)->deleteTbs); \
    }                                              \
  } while (0)

static TdThreadOnce operPrivInit = PTHREAD_ONCE_INIT;

typedef struct {
  EOperType operType;
  EPrivType privType;
} SOperPrivInfo;

static SOperPrivInfo operPrivInfoTable[] = {
    {MND_OPER_CONNECT, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_ACCT, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_ACCT, PRIV_TYPE_UNKNOWN},
    {MND_OPER_ALTER_ACCT, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_USER, PRIV_USER_CREATE},
    {MND_OPER_DROP_USER, PRIV_USER_DROP},
    {MND_OPER_ALTER_USER, PRIV_USER_CREATE},
    {MND_OPER_CREATE_DNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_DNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CONFIG_DNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_MNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_MNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_QNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_QNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_SNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_SNODE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_REDISTRIBUTE_VGROUP, PRIV_VG_REDISTRIBUTE},
    {MND_OPER_MERGE_VGROUP, PRIV_VG_MERGE},
    {MND_OPER_SPLIT_VGROUP, PRIV_VG_SPLIT},
    {MND_OPER_BALANCE_VGROUP, PRIV_VG_BALANCE},
    {MND_OPER_CREATE_FUNC, PRIV_FUNC_CREATE},
    {MND_OPER_DROP_FUNC, PRIV_FUNC_DROP},
    {MND_OPER_KILL_TRANS, PRIV_TRANS_KILL},
    {MND_OPER_KILL_CONN, PRIV_CONN_KILL},
    {MND_OPER_KILL_QUERY, PRIV_QUERY_KILL},
    {MND_OPER_CREATE_DB, PRIV_DB_CREATE},
    {MND_OPER_ALTER_DB, PRIV_CM_ALTER},
    {MND_OPER_DROP_DB, PRIV_CM_DROP},
    {MND_OPER_COMPACT_DB, PRIV_DB_COMPACT},
    {MND_OPER_TRIM_DB, PRIV_DB_TRIM},
    {MND_OPER_USE_DB, PRIV_DB_USE},
    {MND_OPER_WRITE_DB, PRIV_TYPE_UNKNOWN},
    {MND_OPER_READ_DB, PRIV_TYPE_UNKNOWN},
    {MND_OPER_READ_OR_WRITE_DB, PRIV_TYPE_UNKNOWN},
    {MND_OPER_SHOW_VARIABLES, PRIV_TYPE_UNKNOWN},
    {MND_OPER_SUBSCRIBE, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CREATE_TOPIC, PRIV_TOPIC_CREATE},
    {MND_OPER_DROP_TOPIC, PRIV_CM_DROP},
    {MND_OPER_CREATE_VIEW, PRIV_TYPE_UNKNOWN},
    {MND_OPER_DROP_VIEW, PRIV_TYPE_UNKNOWN},
    {MND_OPER_CONFIG_CLUSTER, PRIV_TYPE_UNKNOWN},
    {MND_OPER_BALANCE_VGROUP_LEADER, PRIV_VG_BALANCE_LEADER},
    {MND_OPER_CREATE_ANODE, PRIV_NODE_CREATE},
    {MND_OPER_UPDATE_ANODE, PRIV_NODE_CREATE},
    {MND_OPER_DROP_ANODE, PRIV_NODE_DROP},
    {MND_OPER_CREATE_BNODE, PRIV_NODE_CREATE},
    {MND_OPER_DROP_BNODE, PRIV_NODE_DROP},
    {MND_OPER_CREATE_MOUNT, PRIV_MOUNT_CREATE},
    {MND_OPER_DROP_MOUNT, PRIV_MOUNT_DROP},
    {MND_OPER_SCAN_DB, PRIV_DB_SCAN},
    {MND_OPER_CREATE_RSMA, PRIV_RSMA_CREATE},
    {MND_OPER_DROP_RSMA, PRIV_CM_DROP},
    {MND_OPER_ROLLUP_DB, PRIV_DB_ROLLUP},
    {MND_OPER_SHOW_STB, PRIV_CM_SHOW},
    {MND_OPER_ALTER_RSMA, PRIV_CM_ALTER},
    {MND_OPER_CREATE_ROLE, PRIV_ROLE_CREATE},
    {MND_OPER_DROP_ROLE, PRIV_ROLE_DROP},
    {MND_OPER_ALTER_ROLE, PRIV_GRANT_PRIVILEGE},  // TODO
    {MND_OPER_SSMIGRATE_DB, PRIV_DB_SSMIGRATE},
    {MND_OPER_SHOW_DATABASES, PRIV_CM_SHOW},
    {MND_OPER_SHOW_VGROUPS, PRIV_SHOW_VGROUPS},
    {MND_OPER_SHOW_VNODES, PRIV_SHOW_VNODES},
    {MND_OPER_SHOW_COMPACTS, PRIV_SHOW_COMPACTS},
    {MND_OPER_SHOW_RETENTIONS, PRIV_SHOW_RETENTIONS},
    {MND_OPER_SHOW_SCANS, PRIV_SHOW_SCANS},
    {MND_OPER_SHOW_SSMIGRATES, PRIV_SHOW_SSMIGRATES},
};

static SOperPrivInfo *operPrivLookUp[MND_OPER_MAX] = {0};

static bool mndHasSysObjPrivilege(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                  const char *objFName, const char *tbName);

static void initOperPrivLookup(void) {
  for (size_t i = 0; i < sizeof(operPrivInfoTable) / sizeof(operPrivInfoTable[0]); ++i) {
    if (operPrivInfoTable[i].operType < MND_OPER_MAX) {
      operPrivLookUp[operPrivInfoTable[i].operType] = &operPrivInfoTable[i];
    }
  }
}

static EPrivType getOperPrivType(EOperType operType) {
  (void)taosThreadOnce(&operPrivInit, initOperPrivLookup);
  SOperPrivInfo *result = (0 <= operType && operType < MND_OPER_MAX) ? operPrivLookUp[operType] : NULL;
  return result ? result->privType : PRIV_TYPE_UNKNOWN;
}

static bool mndMustChangePassword(SUserObj *pUser) {
  if (pUser->changePass == 1) {
    return true;
  }

  if (pUser->passwordLifeTime == -1) {
    return false;
  }

  int32_t age = taosGetTimestampSec() - pUser->passwords[0].setTime;
  return age >= pUser->passwordLifeTime;
}

int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }

void mndCleanupPrivilege(SMnode *pMnode) {}

int32_t mndCheckConnectPrivilege(SMnode *pMnode, SUserObj *pUser, const char *token, const SLoginInfo *li) {
  if ((!pUser->superUser) && (!pUser->enable)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  int64_t now = taosGetTimestampSec();

  if (token == NULL && pUser->passwordLifeTime > 0 && pUser->passwordGraceTime >= 0) {
    int32_t age = now - pUser->passwords[0].setTime;
    int32_t maxLifeTime = pUser->passwordLifeTime + pUser->passwordGraceTime;
    if (age >= maxLifeTime) {
      return TSDB_CODE_MND_USER_PASSWORD_EXPIRED;
    }
  }

  if (!isTimeInDateTimeWhiteList(pUser->pTimeWhiteList, now)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  if (pUser->inactiveAccountTime >= 0 && (now - li->lastLoginTime >= pUser->inactiveAccountTime)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  if (token == NULL && pUser->failedLoginAttempts >= 0 && li->failedLoginCount >= pUser->failedLoginAttempts) {
    if (pUser->passwordLockTime < 0 || now - li->lastFailedLoginTime < pUser->passwordLockTime) {
      return TSDB_CODE_MND_USER_DISABLED;
    }
  }

  // this function is implemented in mndProfile.c
  int32_t mndCountUserConns(SMnode * pMnode, const char *user);

  if (pUser->sessionPerUser >= 0) {
    int32_t currentSessions = mndCountUserConns(pMnode, pUser->user);
    if (currentSessions >= pUser->sessionPerUser) {
      return TSDB_CODE_MND_TOO_MANY_CONNECTIONS;
    }
  }

  return 0;
}

int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, const char *token, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  switch (operType) {
    case MND_OPER_CREATE_FUNC:
    case MND_OPER_DROP_FUNC:
    case MND_OPER_SHOW_VARIABLES:
    case MND_OPER_BALANCE_VGROUP:
    case MND_OPER_BALANCE_VGROUP_LEADER:
    case MND_OPER_MERGE_VGROUP:
    case MND_OPER_SPLIT_VGROUP:
    case MND_OPER_REDISTRIBUTE_VGROUP:
    case MND_OPER_CREATE_SNODE:
    case MND_OPER_ALTER_ROLE:
    case MND_OPER_KILL_QUERY:
    case MND_OPER_KILL_CONN:
    case MND_OPER_KILL_TRANS:
      break;
    default:
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

static bool canChangePassword(SUserObj *pOperUser, SUserObj *pUser) {
  if (!pOperUser->enable) {
    return false;
  }

  if (pOperUser->superUser) {
    return true;
  }

  if (strcmp(pUser->user, pOperUser->user) != 0) {
    return false;
  }

  if (pUser->changePass == 0) {
    return false;
  }

  if (pUser->passwordLifeTime == -1 || pUser->passwordGraceTime == -1) {
    return true;
  }

  int32_t age = taosGetTimestampSec() - pUser->passwords[0].setTime;
  int32_t maxLifeTime = pUser->passwordLifeTime + pUser->passwordGraceTime;
  return age < maxLifeTime;
}

int32_t mndCheckAlterUserPrivilege(SMnode *pMnode, const char *opUser, const char *opToken, SUserObj *pUser,
                                   SAlterUserReq *pAlter) {
  int32_t   code = 0, lino = 0;
  SUserObj *pOperUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, opUser, &pOperUser), &lino, _OVER);

  if (pAlter->alterType != TSDB_ALTER_USER_BASIC_INFO) {
    if (opToken == NULL && mndMustChangePassword(pOperUser)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
    }
    if (pOperUser->superUser && !pUser->superUser) {
      goto _OVER;
    }
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, &lino, _OVER);
  }

  if (pAlter->hasPassword) {
    if (opToken == NULL && mndMustChangePassword(pOperUser)) {
      // if operUser must change password, only allow to change its own password
      if (strcmp(pUser->user, pOperUser->user) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
      }
    }
    if (!canChangePassword(pOperUser, pUser)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  } else if (opToken == NULL && mndMustChangePassword(pOperUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
  }

  if (!pOperUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, &lino, _OVER);
  } else if (pOperUser->superUser) {
    if (!pUser->superUser) {
      // super user can alter any non-super user
      goto _OVER;
    }
  } else if (strcmp(pUser->user, pOperUser->user) == 0) {
    if (pAlter->numIpRanges > 0 || pAlter->numDropIpRanges > 0) {
      // user can not alter its own ip white list
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  }

  // now there are two cases left:
  // 1. both pOperUser and pUser are superuser
  // 2. pOperUser and pUser are same user
#if 0
  if (pAlter->hasEnable || pAlter->hasSysinfo || pAlter->hasCreatedb || pAlter->hasChangepass ||
      pAlter->hasSessionPerUser || pAlter->hasConnectTime || pAlter->hasConnectIdleTime || pAlter->hasCallPerSession ||
      pAlter->hasVnodePerCall || pAlter->hasFailedLoginAttempts || pAlter->hasPasswordLifeTime ||
      pAlter->hasPasswordReuseTime || pAlter->hasPasswordReuseMax || pAlter->hasPasswordLockTime ||
      pAlter->hasPasswordGraceTime || pAlter->hasInactiveAccountTime || pAlter->hasAllowTokenNum ||
      pAlter->numTimeRanges > 0 || pAlter->numDropTimeRanges > 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
  }
#else
  if (pUser->superUser) {
    if (pAlter->hasEnable || pAlter->hasSysinfo || pAlter->hasCreatedb || pAlter->hasFailedLoginAttempts ||
        pAlter->hasPasswordLifeTime || pAlter->hasPasswordReuseTime || pAlter->hasPasswordReuseMax ||
        pAlter->hasPasswordLockTime || pAlter->hasPasswordGraceTime || pAlter->hasInactiveAccountTime ||
        pAlter->hasAllowTokenNum || pAlter->numTimeRanges > 0 || pAlter->numDropTimeRanges > 0) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  }
#endif

  // super user can alter totp seed of any user, user can also alter its own totp seed
  // so no need to check pAlter->hasTotpseed here
_OVER:
  mndReleaseUser(pMnode, pOperUser);
  TAOS_RETURN(code);
}

// super user can modify totp secret of any non-super user, user can also modify its own totp secret
int32_t mndCheckTotpSecretPrivilege(SMnode *pMnode, const char *opUser, const char *opToken, SUserObj *pUser,
                                    EPrivType privType) {
  int32_t   code = 0, lino = 0;
  SUserObj *pOperUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, opUser, &pOperUser), &lino, _OVER);

  if (opToken == NULL && mndMustChangePassword(pOperUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
  }

  if (!pOperUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, &lino, _OVER);
  }

  // PRIV_TODO: why super user cannot modify another super user's totp secret?
  if (pOperUser->superUser && !pUser->superUser) {
    goto _OVER;
  }

  if (strcmp(pUser->user, pOperUser->user) != 0) {
    if (!mndHasSysObjPrivilege(pMnode, pOperUser, privType, 0, NULL, NULL)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
    }
  }

_OVER:
  mndReleaseUser(pMnode, pOperUser);
  TAOS_RETURN(code);
}

int32_t mndCheckTokenPrivilege(SMnode *pMnode, const char *opUser, const char *opToken, const char *user,
                               const char *token, EPrivType privType) {
  int32_t   code = 0;
  SUserObj *pOperUser = NULL;

  if (opToken != NULL && token != NULL && taosStrcasecmp(opToken, token) == 0) {
    return TSDB_CODE_MND_NO_RIGHTS;  // token cannot alter/drop itself
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, opUser, &pOperUser), NULL, _OVER);

  if (opToken == NULL && mndMustChangePassword(pOperUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

  if (!pOperUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pOperUser->superUser) {
    goto _OVER;
  }

  if (strcmp(pOperUser->user, user) != 0) {
    if (!mndHasSysObjPrivilege(pMnode, pOperUser, privType, 0, NULL, NULL)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
    }
  }

_OVER:
  mndReleaseUser(pMnode, pOperUser);
  TAOS_RETURN(code);
}

int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, const char *token, EShowType showType,
                              const char *dbname) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  // if (pUser->sysInfo) {
  //   goto _OVER;
  // }

  // switch (showType) {
  //   case TSDB_MGMT_TABLE_DB:
  //   case TSDB_MGMT_TABLE_STB:
  //   case TSDB_MGMT_TABLE_INDEX:
  //   case TSDB_MGMT_TABLE_STREAMS:
  //   case TSDB_MGMT_TABLE_CONSUMERS:
  //   case TSDB_MGMT_TABLE_TOPICS:
  //   case TSDB_MGMT_TABLE_SUBSCRIPTIONS:
  //   case TSDB_MGMT_TABLE_FUNC:
  //   case TSDB_MGMT_TABLE_QUERIES:
  //   case TSDB_MGMT_TABLE_CONNS:
  //   case TSDB_MGMT_TABLE_APPS:
  //   case TSDB_MGMT_TABLE_TRANS:
  //   case TSDB_MGMT_TABLE_COL:
  //   case TSDB_MGMT_TABLE_ANODE:
  //   case TSDB_MGMT_TABLE_ANODE_FULL:
  //     break;
  //   default:
  //     TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  // }

  // if (showType == TSDB_MGMT_TABLE_STB || showType == TSDB_MGMT_TABLE_VGROUP || showType == TSDB_MGMT_TABLE_INDEX) {
  //   code = mndCheckDbPrivilegeByName(pMnode, user, token, MND_OPER_READ_OR_WRITE_DB, dbname);
  // }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

static bool mndHasObjPrivilegeType(SHashObj *privs, const char *key, int32_t klen, EPrivType privType) {
#if 0
  SPrivObjPolicies *pp = NULL;
  while ((pp = taosHashIterate(privs, pp))) {
    char *pKey = taosHashGetKey(pp, NULL);
    printf("%s:%d key is %s\n", __func__, __LINE__, pKey);
  }
#endif
  SPrivObjPolicies *policies = taosHashGet(privs, key, klen);
  return policies && PRIV_HAS(&policies->policy, privType) ? true : false;
}

static bool mndHasSysObjPrivilege(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                  const char *objFName, const char *tbName) {
  char             objKey[TSDB_PRIV_MAX_KEY_LEN] = {0};
  const SPrivInfo *pPrivInfo = privInfoGet(privType);
  if (pPrivInfo == NULL) {
    return false;
  }

  void     *pIter = NULL;
  SRoleObj *pRole = NULL;
  if (pPrivInfo->category == PRIV_CATEGORY_SYSTEM) {
    if (PRIV_HAS(&pUser->sysPrivs, privType)) {
      return true;
    }
    while ((pIter = taosHashIterate(pUser->roles, pIter))) {
      char *key = taosHashGetKey(pIter, NULL);
      if (!key) continue;
      if (mndAcquireRole(pMnode, key, &pRole) != TSDB_CODE_SUCCESS) continue;
      if (!pRole->enable) {
        mndReleaseRole(pMnode, pRole);
        continue;
      }

      if (PRIV_HAS(&pRole->sysPrivs, privType)) {
        mndReleaseRole(pMnode, pRole);
        taosHashCancelIterate(pUser->roles, pIter);
        return true;
      }
      mndReleaseRole(pMnode, pRole);
    }
  } else {
    const SPrivInfo *privInfo = pPrivInfo;
    SPrivInfo        privInfoDup;
    // for common privilege, e.g. alter, drop, show, show create, use the real objType
    if (privInfo->objType <= 0) {
      privInfoDup = *privInfo;
      privInfoDup.objType = objType;
      privInfoDup.objLevel = privObjGetLevel(objType);
      privInfo = &privInfoDup;
    }
    int32_t klen = privObjKeyF(privInfo, objFName, tbName, objKey, sizeof(objKey));

    if (mndHasObjPrivilegeType(pUser->objPrivs, objKey, klen + 1, privType)) return true;

    while ((pIter = taosHashIterate(pUser->roles, pIter))) {
      char *key = taosHashGetKey(pIter, NULL);
      if (!key) continue;
      if (mndAcquireRole(pMnode, key, &pRole) != TSDB_CODE_SUCCESS) continue;
      if (!pRole->enable) {
        mndReleaseRole(pMnode, pRole);
        continue;
      }

      if (mndHasObjPrivilegeType(pRole->objPrivs, objKey, klen + 1, privType)) {
        mndReleaseRole(pMnode, pRole);
        taosHashCancelIterate(pUser->roles, pIter);
        return true;
      }
      mndReleaseRole(pMnode, pRole);
    }
  }

  return false;
}

/**
 * check privilege with acctId, object name and table name, up to *.* if recursive is true
 */
static bool mndHasObjPrivilege(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                               int32_t acctId, const char *objName, const char *tbName, bool recursive) {
  const SPrivInfo *pPrivInfo = privInfoGet(privType);
  if (pPrivInfo == NULL) {
    return false;
  }

  void     *pIter = NULL;
  SRoleObj *pRole = NULL;
  SPrivInfo privInfo = *(SPrivInfo *)pPrivInfo;
  if (privInfo.category == PRIV_CATEGORY_OBJECT || objType > PRIV_OBJ_CLUSTER) {
    // for common privilege, e.g. alter, drop, show, show create, use the real objType
    if (privInfo.objType <= 0) {
      privInfo.objType = objType;
      privInfo.objLevel = privObjGetLevel(objType);
    }
    if (privHasObjPrivilege(pUser->objPrivs, acctId, objName, tbName, &privInfo, true)) return true;

    while ((pIter = taosHashIterate(pUser->roles, pIter))) {
      char *key = taosHashGetKey(pIter, NULL);
      if (!key) continue;
      if (mndAcquireRole(pMnode, key, &pRole) != TSDB_CODE_SUCCESS) continue;
      if (!pRole->enable) {
        mndReleaseRole(pMnode, pRole);
        continue;
      }

      if (privHasObjPrivilege(pRole->objPrivs, acctId, objName, tbName, &privInfo, recursive)) {
        mndReleaseRole(pMnode, pRole);
        taosHashCancelIterate(pUser->roles, pIter);
        return true;
      }
      mndReleaseRole(pMnode, pRole);
    }
  } else if(privInfo.category == PRIV_CATEGORY_SYSTEM) {
    return mndHasSysObjPrivilege(pMnode, pUser, privType, objType, objName, tbName);
  }

  return false;
}

int32_t mndCheckSysObjPrivilege(SMnode *pMnode, SUserObj *pUser, const char *token, EPrivType privType,
                                EPrivObjType objType, int64_t ownerId, const char *objFName, const char *tbName) {
  if (token == NULL && mndMustChangePassword(pUser)) {
    goto _OVER;
  }
  if (!pUser->enable) {
    goto _OVER;
  }
  if (pUser->superUser) TAOS_RETURN(0);

  if (ownerId == pUser->uid) TAOS_RETURN(0);

  if (mndHasSysObjPrivilege(pMnode, pUser, privType, objType, objFName, tbName)) {
    TAOS_RETURN(0);
  }
_OVER:
  TAOS_RETURN(TSDB_CODE_MND_NO_RIGHTS);
}

/**
 * check privilege with acctId, object name and table name recursively to *.*
 */
int32_t mndCheckObjPrivilegeRec(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                int64_t ownerId, int32_t acctId, const char *objName, const char *tbName) {
  if (mndMustChangePassword(pUser)) {
    goto _OVER;
  }
  if (!pUser->enable) {
    goto _OVER;
  }
  if (pUser->superUser) TAOS_RETURN(0);

  if (ownerId == pUser->uid) TAOS_RETURN(0);

  if (mndHasObjPrivilege(pMnode, pUser, privType, objType, acctId, objName, tbName, true)) {
    TAOS_RETURN(0);
  }
_OVER:
  TAOS_RETURN(TSDB_CODE_MND_NO_RIGHTS);
}

/**
 * check privilege with object full name and table name recursively to *.*
 */
int32_t mndCheckObjPrivilegeRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                 int64_t ownerId, const char *objFName, const char *tbName) {
  int32_t code = 0;
  SName   name = {0};

  if (!objFName) {
    mError("objFName is NULL at line %d", __LINE__);
    TAOS_RETURN(TSDB_CODE_APP_ERROR);
  }

  if ((code = tNameFromString(&name, objFName, T_NAME_ACCT | T_NAME_DB))) {
    mError("failed to parse objFName %s at line %d since %s", objFName, __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  // rewrite ownerId if the user is db owner
  if (objType != PRIV_OBJ_DB && !IS_SYS_DBNAME(name.dbname) && (strncmp(name.dbname, "*", 2) != 0)) {
    SDbObj *pDb = mndAcquireDb(pMnode, objFName);
    if (pDb) {
      if ((pDb->ownerId == pUser->uid) && (pDb->ownerId != 0)) {
        ownerId = pDb->ownerId;
      }
      mndReleaseDb(pMnode, pDb);
    }
  }

  return mndCheckObjPrivilegeRec(pMnode, pUser, privType, objType, ownerId, name.acctId, name.dbname, tbName);
}

/**
 * check privilege with object full name and table name recursively to *.*
 */
int32_t mndCheckObjPrivilegeByUserNameRecF(SMnode *pMnode, const char *user, EPrivType privType, EPrivObjType objType,
                                           int64_t ownerId, const char *objFName, const char *tbName) {
  int32_t   code = 0;
  SUserObj *pOperUser = NULL;

  if ((code = mndAcquireUser(pMnode, user, &pOperUser))) {
    TAOS_RETURN(code);
  }

  code = mndCheckObjPrivilegeRecF(pMnode, pOperUser, privType, objType, ownerId, objFName, tbName);
  mndReleaseUser(pMnode, pOperUser);
  TAOS_RETURN(code);
}

int32_t mndCheckDbPrivilegeByNameRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, EPrivObjType objType,
                                      const char *dbFName, const char *tbName) {
  int32_t code = 0;
  SName   name = {0};

  if (!dbFName) {
    mError("dbFName is NULL at line %d", __LINE__);
    TAOS_RETURN(TSDB_CODE_APP_ERROR);
  }

  if ((code = tNameFromString(&name, dbFName, T_NAME_ACCT | T_NAME_DB))) {
    mError("failed to parse dbFName %s at line %d since %s", dbFName, __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  if (IS_SYS_DBNAME(name.dbname)) {
    return privType == PRIV_DB_USE ? TSDB_CODE_SUCCESS : TSDB_CODE_MND_NO_RIGHTS;
  }

  SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
  if (pDb == NULL) {
    TAOS_RETURN(terrno);
  }
  code = mndCheckObjPrivilegeRec(pMnode, pUser, privType, objType, pDb->ownerId, name.acctId, name.dbname, tbName);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

static int32_t mndCheckDbPrivilegeImpl(SMnode *pMnode, const char *user, const char *token, EOperType operType,
                                       SDbObj *pDb, const char *dbFName) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  // check these privileges in parser
  if (operType == MND_OPER_CREATE_DB) {
    if (pUser->createdb) goto _OVER;
    if (mndHasSysObjPrivilege(pMnode, pUser, PRIV_DB_CREATE, 0, NULL, NULL)) goto _OVER;
  } else if (operType == MND_OPER_SSMIGRATE_DB) {
    return TSDB_CODE_SUCCESS;
  } else if (operType == MND_OPER_ALTER_DB) {
    if (pDb != NULL) {
      if (pDb->cfg.isAudit) {
        if (mndHasSysObjPrivilege(pMnode, pUser, PRIV_AUDIT_DB_ALTER, 0, pDb->name, NULL)) goto _OVER;
      } else {
        if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_CM_ALTER, PRIV_OBJ_DB, pDb->ownerId, pDb->name, NULL)) {
          goto _OVER;
        }
      }
    }
  } else if (operType == MND_OPER_DROP_DB) {
    // if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;  // TS-7279
    if (pDb != NULL) {
      if (pDb->cfg.isAudit) {
        if (mndHasSysObjPrivilege(pMnode, pUser, PRIV_AUDIT_DB_DROP, 0, pDb->name, NULL)) goto _OVER;
      } else {
        if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_CM_DROP, PRIV_OBJ_DB, pDb->ownerId, pDb->name, NULL)) {
          goto _OVER;
        }
      }
    } else if (dbFName) {
      if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_CM_DROP, PRIV_OBJ_DB, 0, dbFName, NULL)) {
        goto _OVER;
      }
    }
  } else if (operType == MND_OPER_USE_DB || operType == MND_OPER_SHOW_DATABASES || operType == MND_OPER_SHOW_VGROUPS ||
             operType == MND_OPER_SHOW_VNODES || operType == MND_OPER_CREATE_TOPIC || operType == MND_OPER_COMPACT_DB ||
             operType == MND_OPER_TRIM_DB || operType == MND_OPER_SCAN_DB) {
    if (pDb != NULL) {
      if (pDb->cfg.isAudit) {
        if (taosHashGet(pUser->ownedDbs, pDb->name, strlen(pDb->name) + 1)) {
          goto _OVER;
        }
      } else {
        EPrivType privType = getOperPrivType(operType);
        if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, privType, PRIV_OBJ_DB, pDb->ownerId, pDb->name, NULL)) {
          goto _OVER;
        }
      }
    } else if (dbFName) {
      EPrivType privType = getOperPrivType(operType);
      if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, privType, PRIV_OBJ_DB, 0, dbFName, NULL)) {
        goto _OVER;
      }
    } else {
      goto _OVER;
    }
  } else if (operType == MND_OPER_WRITE_DB) {
    if (pDb && (pUser->uid == pDb->ownerId)) goto _OVER;
  } else if (operType == MND_OPER_READ_DB) {
    if (pDb && (pUser->uid == pDb->ownerId)) goto _OVER;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, const char *token, EOperType operType, SDbObj *pDb) {
  return mndCheckDbPrivilegeImpl(pMnode, user, token, operType, pDb, NULL);
}

int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, const char *token, EOperType operType,
                                  const char *dbname, bool skipExists) {
  int32_t code = 0;

  if (!dbname) {
    TAOS_RETURN(TSDB_CODE_APP_ERROR);
  }

  const char *realDbName = NULL;
  const char *dot = strchr(dbname, '.');
  if (dot != NULL && *(dot + 1) != '\0') {
    realDbName = dot + 1;
  }

  if (realDbName && IS_SYS_DBNAME(realDbName)) {
    if (operType == MND_OPER_USE_DB) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_MND_NO_RIGHTS;
    }
  }

  SDbObj *pDb = mndAcquireDb(pMnode, dbname);

  if (pDb == NULL) {
    if (skipExists || strncmp(realDbName, "*", 2) == 0) {
      TAOS_RETURN(mndCheckDbPrivilegeImpl(pMnode, user, token, operType, NULL, dbname));
    }
    TAOS_RETURN(terrno);
  }

  code = mndCheckDbPrivilege(pMnode, user, token, operType, pDb);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndCheckStbPrivilege(SMnode *pMnode, SUserObj *pUser, const char *token, EOperType operType, SStbObj *pStb) {
  int32_t code = 0, lino = 0;
  SDbObj *pDb = NULL;

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_PASSWORD_EXPIRED);
  }

  if (!pUser->enable) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_DISABLED);
  }

  if (pUser->superUser) goto _exit;

  if (!(pDb = mndAcquireDb(pMnode, pStb->db))) {
    code = terrno ? terrno : TSDB_CODE_MND_DB_NOT_EXIST;
    TAOS_CHECK_EXIT(code);
  }

  if (operType == MND_OPER_SHOW_STB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _exit;
    if (taosHashGet(pUser->selectTbs, pStb->name, strlen(pStb->name) + 1) != NULL) goto _exit;
    if (taosHashGet(pUser->insertTbs, pStb->name, strlen(pStb->name) + 1) != NULL) goto _exit;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_exit:
  if (pDb) mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndCheckViewPrivilege(SMnode *pMnode, const char *user, const char *token, EOperType operType,
                              const char *pViewFName) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  if (operType == MND_OPER_CREATE_VIEW || operType == MND_OPER_DROP_VIEW) {
#ifdef PRIV_TODO
    if (taosHashGet(pUser->alterViews, pViewFName, strlen(pViewFName) + 1) != NULL) goto _OVER;
#endif
    goto _OVER;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, const char *token, EOperType operType,
                               SMqTopicObj *pTopic) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  if (operType == MND_OPER_SUBSCRIBE) {
    if (pUser->uid == pTopic->ownerId) goto _OVER;
    if (0 == mndCheckDbPrivilegeByNameRecF(pMnode, pUser, PRIV_DB_USE, PRIV_OBJ_DB, pTopic->db, NULL)) {
      SName name = {0};  // 1.topic1
      TAOS_CHECK_GOTO(tNameFromString(&name, pTopic->name, T_NAME_ACCT | T_NAME_DB), NULL, _OVER);
      if (0 == mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_CM_SUBSCRIBE, PRIV_OBJ_TOPIC, 0, pTopic->db, name.dbname)) {
        goto _OVER;
      }
    }
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

static int32_t mndMergeRolePrivilges(SMnode *pMnode, SUserObj *pUser, SRoleObj *pObj, SGetUserAuthRsp *pRsp) {
  int32_t code = 0, lino = 0;

  privAddSet(&pRsp->sysPrivs, &pObj->sysPrivs);
  TAOS_CHECK_EXIT(mndMergePrivObjHash(pObj->objPrivs, &pRsp->objPrivs));
  TAOS_CHECK_EXIT(mndMergePrivTblHash(pObj->selectTbs, &pRsp->selectTbs, true));
  TAOS_CHECK_EXIT(mndMergePrivTblHash(pObj->insertTbs, &pRsp->insertTbs, true));
  TAOS_CHECK_EXIT(mndMergePrivTblHash(pObj->deleteTbs, &pRsp->deleteTbs, true));

_exit:
  TAOS_RETURN(code);
}

/**
 * merge the privileges of the user's roles and their subroles
 */
int32_t mndSetUserRolePrivileges(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  int32_t   code = 0, lino = 0;
  SRoleObj *pRole = NULL;

  void *pIter = NULL;
  while ((pIter = taosHashIterate(pUser->roles, pIter))) {
    uint8_t flag = *(uint8_t *)pIter;
    if ((flag & 0x01) == 0) {  // role is reset for current user, skip it
      continue;
    }
    char *pRoleName = taosHashGetKey(pIter, NULL);
    if ((code = mndAcquireRole(pMnode, pRoleName, &pRole)) != 0) {
      mWarn("failed to acquire role:%s for user:%s at line:%d since %s", pRoleName, pUser->user, __LINE__,
            tstrerror(code));
      continue;
    }
    if (pRole->enable == 0) {
      mndReleaseRole(pMnode, pRole);
      continue;
    }
    TAOS_CHECK_EXIT(mndMergeRolePrivilges(pMnode, pUser, pRole, pRsp));
    mndReleaseRole(pMnode, pRole);
    pRole = NULL;
  }
_exit:
  if (pRole) mndReleaseRole(pMnode, pRole);
  if (code != 0) {
    mError("failed to set role privileges for user:%s at line:%d since %s", pUser->user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static bool mndWithCondInPrivs(SHashObj *privs) {
  if (!privs) {
    return false;
  }
  void *pIter = NULL;
  while ((pIter = taosHashIterate(privs, pIter))) {
    SPrivTblPolicies *pTblPolicies = (SPrivTblPolicies *)pIter;
    int32_t           size = taosArrayGetSize(pTblPolicies->policy);
    for (int32_t i = 0; i < size; i++) {
      SPrivTblPolicy *pPolicy = (SPrivTblPolicy *)TARRAY_GET_ELEM(pTblPolicies->policy, i);
      if (pPolicy->condLen > 0) {
        return true;
      }
    }
  }
  return false;
}

int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  int32_t code = 0, lino = 0;

  (void)memcpy(pRsp->user, pUser->user, TSDB_USER_LEN);
  pRsp->userId = pUser->uid;
  pRsp->superAuth = pUser->superUser;
  pRsp->version = pUser->authVersion;
  pRsp->passVer = pUser->passVersion;
  pRsp->whiteListVer = pUser->ipWhiteListVer;
  pRsp->timeWhiteListVer = pUser->timeWhiteListVer;
  pRsp->enable = pUser->enable;
  pRsp->sysInfo = pUser->sysInfo;
  pRsp->sessCfg = (SUserSessCfg){.sessPerUser = pUser->sessionPerUser,
                                 .sessConnTime = pUser->connectTime,
                                 .sessConnIdleTime = pUser->connectIdleTime,
                                 .sessMaxConcurrency = pUser->callPerSession,
                                 .sessMaxCallVnodeNum = pUser->vnodePerCall};
  if (!pRsp->enable) {
    TAOS_RETURN(code);
  }

#if 0  // ownerId is used to identify the dbs owned by the user
  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == pRsp->createdDbs) {
    TAOS_RETURN(terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY);
  }

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;

    if (strcmp(pDb->createUser, pUser->user) == 0) {
      int32_t len = strlen(pDb->name) + 1;
      if ((code = taosHashPut(pRsp->createdDbs, pDb->name, len, pDb->name, len)) != 0) {
        sdbRelease(pSdb, pDb);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pDb);
  }
#endif

  pRsp->sysPrivs = pUser->sysPrivs;
  taosRLockLatch(&pUser->lock);
  TAOS_CHECK_EXIT(mndDupPrivObjHash(pUser->objPrivs, &pRsp->objPrivs));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pUser->selectTbs, &pRsp->selectTbs, true));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pUser->insertTbs, &pRsp->insertTbs, true));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pUser->deleteTbs, &pRsp->deleteTbs, true));
  TAOS_CHECK_EXIT(mndDupKVHash(pUser->ownedDbs, &pRsp->ownedDbs));
  taosRUnLockLatch(&pUser->lock);

  code = mndSetUserRolePrivileges(pMnode, pUser, pRsp);

  if (mndWithCondInPrivs(pRsp->insertTbs)) {
    pRsp->withInsertCond = 1;
  }

  if (code != 0) {
    TAOS_RETURN(code);
  }
  code = mndGetUserTokenStatuses(pUser->user, &pRsp->tokens);
  if (code != 0) {
    TAOS_RETURN(code);
  }
  TAOS_RETURN(0);
_exit:
  taosRUnLockLatch(&pUser->lock);
  TAOS_RETURN(code);
}

int32_t mndSetUserIpWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  if (tsEnableWhiteList) {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = pUser->pIpWhiteListDual->num;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    int32_t ipv4Count = 0;
    for (int32_t i = 0; i < pUser->pIpWhiteListDual->num; i++) {
      SIpRange *pRange = &pUser->pIpWhiteListDual->pIpRanges[i];
      if (pRange->type == 0) {
        memcpy(&pWhiteListRsp->pWhiteLists[ipv4Count], pRange, sizeof(SIpV4Range));
        ipv4Count++;
      }
    }
    pWhiteListRsp->numWhiteLists = ipv4Count;

  } else {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = 1;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memset(pWhiteListRsp->pWhiteLists, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
  }
  TAOS_RETURN(0);
}

int32_t mndSetUserIpWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);

  if (tsEnableWhiteList) {
    pWhiteListRsp->numWhiteLists = pUser->pIpWhiteListDual->num;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteListsDual == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pWhiteListRsp->pWhiteListsDual, pUser->pIpWhiteListDual->pIpRanges,
                 pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
  } else {
    pWhiteListRsp->numWhiteLists = 2;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteListsDual == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memset(pWhiteListRsp->pWhiteListsDual, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    pWhiteListRsp->pWhiteListsDual[0].type = 0;  // ipv4
    pWhiteListRsp->pWhiteListsDual[1].type = 1;  // ipv6
  }
  TAOS_RETURN(0);
}

int32_t mndEnableIpWhiteList(SMnode *pMnode) { return 1; }

int32_t mndEnableTimeWhiteList(SMnode *pMnode) { return 1; }

int32_t mndSetUserDateTimeWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SUserDateTimeWhiteList *pWhiteListRsp) {
  (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);

  if (tsEnableWhiteList) {
    pWhiteListRsp->ver = pUser->timeWhiteListVer;
    pWhiteListRsp->numWhiteLists = pUser->pTimeWhiteList->num;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SDateTimeWhiteListItem));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pWhiteListRsp->pWhiteLists, pUser->pTimeWhiteList->ranges,
                 pWhiteListRsp->numWhiteLists * sizeof(SDateTimeWhiteListItem));
  } else {
    pWhiteListRsp->ver = 0;
    pWhiteListRsp->numWhiteLists = 0;
  }

  TAOS_RETURN(0);
}

static int32_t mndGetPrivObjSize(void *pObj, ESdbType sdbType) {
  int32_t privObjSize = 0;
  if (sdbType == SDB_USER) {
    GET_PRIV_OBJ_SIZE((SUserObj *)pObj, privObjSize);
  } else {
    GET_PRIV_OBJ_SIZE((SRoleObj *)pObj, privObjSize);
  }
  return privObjSize;
}

static int32_t mndAlterObjPrivileges(SMnode *pMnode, void *pObj, SAlterRoleReq *pAlterReq, ESdbType sdbType) {
  int32_t          code = 0, lino = 0;
  SPrivSetReqArgs *pReqArgs = &pAlterReq->privileges;
  SHashObj        *objPrivs = sdbType == SDB_USER ? ((SUserObj *)pObj)->objPrivs : ((SRoleObj *)pObj)->objPrivs;
  char             key[TSDB_PRIV_MAX_KEY_LEN] = {0};
  SPrivInfo        privInfo = {.objType = pAlterReq->objType, .objLevel = pAlterReq->objLevel};
  int32_t          keyLen = privObjKeyF(&privInfo, pAlterReq->objFName, pAlterReq->tblName, key, sizeof(key));

  // conflicts check when add table privilege; remove constraint table privilege when revoke
  if (pAlterReq->objType == PRIV_OBJ_TBL) {
    if (PRIV_HAS(&pReqArgs->privSet, PRIV_TBL_SELECT)) {
      if (taosArrayGetSize(pReqArgs->selectCols) > 0) {
        mError("Cannot grant table-level and column-level select privileges simultaneously on table %s.%s to %s",
               pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
      SHashObj *selectTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->selectTbs : ((SRoleObj *)pObj)->selectTbs;
      if (taosHashGet(selectTbs, key, keyLen + 1)) {
        if (pAlterReq->add) {
          mError("select privilege on table %s.%s already exists for %s", pAlterReq->objFName, pAlterReq->tblName,
                 pAlterReq->principal);
          TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
        } else {
          TAOS_CHECK_EXIT(taosHashRemove(selectTbs, key, keyLen + 1));
        }
      }
    }
    if (PRIV_HAS(&pReqArgs->privSet, PRIV_TBL_INSERT)) {
      if (taosArrayGetSize(pReqArgs->insertCols) > 0) {
        mError("Cannot grant table-level and column-level insert privileges simultaneously on table %s.%s to %s",
               pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
      SHashObj *insertTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->insertTbs : ((SRoleObj *)pObj)->insertTbs;
      if (taosHashGet(insertTbs, key, keyLen + 1)) {
        if (pAlterReq->add) {
          mError("insert privilege on table %s.%s already exists for %s", pAlterReq->objFName, pAlterReq->tblName,
                 pAlterReq->principal);
          TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
        } else {
          TAOS_CHECK_EXIT(taosHashRemove(insertTbs, key, keyLen + 1));
        }
      }
    }
    if (PRIV_HAS(&pReqArgs->privSet, PRIV_TBL_UPDATE)) {
      if (taosArrayGetSize(pReqArgs->updateCols) > 0) {
        mError("Cannot grant table-level and column-level update privileges simultaneously on table %s.%s to %s",
               pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
      SHashObj *updateTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->updateTbs : ((SRoleObj *)pObj)->updateTbs;
      if (taosHashGet(updateTbs, key, keyLen + 1)) {
        if (pAlterReq->add) {
          mError("update privilege on table %s.%s already exists for %s", pAlterReq->objFName, pAlterReq->tblName,
                 pAlterReq->principal);
          TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
        } else {
          TAOS_CHECK_EXIT(taosHashRemove(updateTbs, key, keyLen + 1));
        }
      }
    }
    if (PRIV_HAS(&pReqArgs->privSet, PRIV_TBL_DELETE)) {
      SHashObj *deleteTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->deleteTbs : ((SRoleObj *)pObj)->deleteTbs;
      if (taosHashGet(deleteTbs, key, keyLen + 1)) {
        if (pAlterReq->add) {
          mError("delete privilege on table %s.%s already exists for %s", pAlterReq->objFName, pAlterReq->tblName,
                 pAlterReq->principal);
          TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
        } else {
          TAOS_CHECK_EXIT(taosHashRemove(deleteTbs, key, keyLen + 1));
        }
      }
    }
  } else if (pAlterReq->objType == PRIV_OBJ_TOPIC) {
    // check topic existence for topic related privileges
    if (pAlterReq->add && (pAlterReq->ignoreNotExists == 0) && (strncmp(pAlterReq->tblName, "*", 2) != 0)) {
      if (PRIV_HAS(&pReqArgs->privSet, PRIV_CM_SHOW) || PRIV_HAS(&pReqArgs->privSet, PRIV_CM_SHOW_CREATE) ||
          PRIV_HAS(&pReqArgs->privSet, PRIV_CM_SUBSCRIBE) || PRIV_HAS(&pReqArgs->privSet, PRIV_CONSUMER_SHOW) ||
          PRIV_HAS(&pReqArgs->privSet, PRIV_SUBSCRIPTION_SHOW)) {
        SName name = {0};
        TAOS_CHECK_EXIT(tNameFromString(&name, pAlterReq->objFName, T_NAME_ACCT | T_NAME_DB));
        char topicName[TSDB_TOPIC_FNAME_LEN] = {0};
        snprintf(topicName, TSDB_TOPIC_FNAME_LEN, "%d.%s", name.acctId, pAlterReq->tblName);
        SMqTopicObj *pTopic = NULL;
        TAOS_CHECK_EXIT(mndAcquireTopic(pMnode, topicName, &pTopic));
        mndReleaseTopic(pMnode, pTopic);
      }
    }
  }

  SPrivObjPolicies *policies = taosHashGet(objPrivs, key, keyLen + 1);

  if (pAlterReq->add) {
    if (policies == NULL) {
      int32_t privObjSize = mndGetPrivObjSize(pObj, sdbType);
      if (privObjSize >= TSDB_MAX_PRIV_OBJS) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_TOO_MANY_PRIV_OBJS);
      }
      SPrivObjPolicies policy = {0};
      policy.policy = pReqArgs->privSet;
      TAOS_CHECK_EXIT(taosHashPut(objPrivs, key, keyLen + 1, &policy, sizeof(SPrivObjPolicies)));
    } else {
      privAddSet(&policies->policy, &pReqArgs->privSet);
    }
  } else if (policies != NULL) {
    privRemoveSet(&policies->policy, &pReqArgs->privSet);
    if (privIsEmptySet(&policies->policy)) {
      TAOS_CHECK_EXIT(taosHashRemove(objPrivs, key, keyLen + 1));
    }
  }
_exit:
  if (code != 0) {
    mError("failed to alter object privileges on objFName:%s, tblName:%s for %s at line %d since %s",
           pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

/**
 * table privileges with row/column/tag constraints
 */
static int32_t mndAlterTableConstraintPrivileges(void *pObj, SAlterRoleReq *pAlterReq, SHashObj *privTbs,
                                                 SArray *privCols, ESdbType sdbType, EPrivType privType) {
  int32_t           code = 0, lino = 0;
  SPrivSetReqArgs  *pReqArgs = &pAlterReq->privileges;
  SHashObj         *objPrivs = sdbType == SDB_USER ? ((SUserObj *)pObj)->objPrivs : ((SRoleObj *)pObj)->objPrivs;
  SPrivObjPolicies *objPolicies = NULL;
  SPrivTblPolicies *tblPolicies = NULL;
  int32_t           keyLen = 0;
  char              key[TSDB_PRIV_MAX_KEY_LEN] = {0};
  int32_t           nPrivCols = taosArrayGetSize(privCols);

  const SPrivInfo *pPrivInfo = privInfoGet(privType);
  if (!pPrivInfo) TAOS_CHECK_EXIT(terrno);

  if (pPrivInfo->objType != pAlterReq->objType) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  if (pPrivInfo->objLevel != pAlterReq->objLevel) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  keyLen = privObjKeyF(pPrivInfo, pAlterReq->objFName, pAlterReq->tblName, key, sizeof(key));
  objPolicies = taosHashGet(objPrivs, key, keyLen + 1);
  tblPolicies = taosHashGet(privTbs, key, keyLen + 1);

  // add
  if (pAlterReq->add) {
    if ((nPrivCols <= 0) && (pReqArgs->condLen <= 0)) {
      if (!PRIV_HAS(&pReqArgs->privSet, privType)) {
        goto _exit;  // no effective privilege to add
      }
      if (tblPolicies) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
      }
      if (objPolicies == NULL) {
        int32_t privObjSize = mndGetPrivObjSize(pObj, sdbType);
        if (privObjSize >= TSDB_MAX_PRIV_OBJS) {
          TAOS_CHECK_EXIT(TSDB_CODE_MND_TOO_MANY_PRIV_OBJS);
        }
        SPrivObjPolicies policy = {0};
        policy.policy = pReqArgs->privSet;
        TAOS_CHECK_EXIT(taosHashPut(objPrivs, key, keyLen + 1, &policy, sizeof(SPrivObjPolicies)));
      } else {
        privAddType(&objPolicies->policy, privType);
      }
      goto _exit;
    }

    if (nPrivCols > 0) {
      if (objPolicies && PRIV_HAS(&objPolicies->policy, privType)) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
      }
      if (tblPolicies) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
      }
    } else if (pReqArgs->condLen > 0) {
      if (!PRIV_HAS(&pReqArgs->privSet, privType)) {
        goto _exit;  // no effective privilege to add
      }
      if (objPolicies && PRIV_HAS(&objPolicies->policy, privType)) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
      }
      if (tblPolicies) {
        TAOS_CHECK_EXIT(TSDB_CODE_MND_PRIVILEGE_EXIST);
      }
    }

    int32_t privObjSize = mndGetPrivObjSize(pObj, sdbType);
    if (privObjSize >= TSDB_MAX_PRIV_OBJS) {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_TOO_MANY_PRIV_OBJS);
    }

    SPrivTblPolicies newPolicies = {0};
    newPolicies.policy = taosArrayInit_s(sizeof(SPrivTblPolicy), 1);
    if (newPolicies.policy == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    SPrivTblPolicy *tblPolicy = TARRAY_GET_ELEM(newPolicies.policy, 0);
    tblPolicy->condLen = pReqArgs->condLen;
    if (pReqArgs->condLen > 0) {
      // the condLen is strlen(condLen) + 1
      tblPolicy->cond = taosStrdup(pReqArgs->cond);
      if (!tblPolicy->cond) {
        privTblPoliciesFree(&newPolicies);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    if (nPrivCols > 0) {
      tblPolicy->cols = taosArrayDup(privCols, NULL);
      if (!tblPolicy->cols) {
        privTblPoliciesFree(&newPolicies);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    tblPolicy->updateUs = taosGetTimestampUs();
    TAOS_CHECK_EXIT(taosHashPut(privTbs, key, keyLen + 1, &newPolicies, sizeof(SPrivTblPolicies)));
    // clear the privType alfter added to avoid duplicate add
    privRemoveType(&pReqArgs->privSet, privType);
  } else {
    // remove
    if (!PRIV_HAS(&pReqArgs->privSet, privType) && nPrivCols <= 0) {
      goto _exit;  // no effective privilege to remove
    }
    if (objPolicies) {
      privRemoveType(&objPolicies->policy, privType);
    }
    if (tblPolicies) {
      if (taosHashGet(privTbs, key, keyLen + 1)) {
        TAOS_CHECK_EXIT(taosHashRemove(privTbs, key, keyLen + 1));
      }
    }
  }

_exit:
  if (code != 0) {
    mError("failed to alter table privileges %s on objFName:%s, tblName:%s for %s at line %d since %s",
           privInfoGetName(privType), pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal, lino,
           tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndAlterTablePrivileges(SMnode *pMnode, void *pObj, SAlterRoleReq *pAlterReq, ESdbType sdbType) {
  int32_t          code = 0, lino = 0;
  SPrivSetReqArgs *pReqArgs = &pAlterReq->privileges;

  // the basic table privileges without contraints are stored in objPrivs
  if (pReqArgs->condLen <= 0 && taosArrayGetSize(pReqArgs->selectCols) <= 0 &&
      taosArrayGetSize(pReqArgs->insertCols) <= 0 && taosArrayGetSize(pReqArgs->updateCols) <= 0) {
    code = mndAlterObjPrivileges(pMnode, pObj, pAlterReq, sdbType);
    goto _exit;
  }

  SHashObj *selectTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->selectTbs : ((SRoleObj *)pObj)->selectTbs;
  SHashObj *insertTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->insertTbs : ((SRoleObj *)pObj)->insertTbs;
  SHashObj *updateTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->updateTbs : ((SRoleObj *)pObj)->updateTbs;
  SHashObj *deleteTbs = sdbType == SDB_USER ? ((SUserObj *)pObj)->deleteTbs : ((SRoleObj *)pObj)->deleteTbs;

  /**
   *  e.g. grant select on d0.stb0 with t0=0 to u1;
   *       grant select,insert(c0,c1),delete on d0.stb0 to u1;
   *       grant select,insert(c0,c1),delete on d0.stb0 with t0=0 and ts>0 to u1;
   */
  TAOS_CHECK_EXIT(
      mndAlterTableConstraintPrivileges(pObj, pAlterReq, selectTbs, pReqArgs->selectCols, sdbType, PRIV_TBL_SELECT));
  TAOS_CHECK_EXIT(
      mndAlterTableConstraintPrivileges(pObj, pAlterReq, insertTbs, pReqArgs->insertCols, sdbType, PRIV_TBL_INSERT));
  TAOS_CHECK_EXIT(
      mndAlterTableConstraintPrivileges(pObj, pAlterReq, updateTbs, pReqArgs->updateCols, sdbType, PRIV_TBL_UPDATE));
  TAOS_CHECK_EXIT(mndAlterTableConstraintPrivileges(pObj, pAlterReq, deleteTbs, NULL, sdbType, PRIV_TBL_DELETE));

  // e.g. grant drop table, select(c0,c1) on d0.stb0 to u1;
  if (pReqArgs->condLen <= 0 && !privIsEmptySet(&pReqArgs->privSet)) {
    TAOS_CHECK_EXIT(mndAlterObjPrivileges(pMnode, pObj, pAlterReq, sdbType));
  }
  // e.g. grant all on d0.stb0 with t0=1 to u1;
  if (pReqArgs->condLen > 0 && !privIsEmptySet(&pReqArgs->privSet)) {
    TAOS_CHECK_EXIT(mndAlterObjPrivileges(pMnode, pObj, pAlterReq, sdbType));
  }

_exit:
  if (code != 0) {
    mError("failed to alter table privileges on objFName:%s, tblName:%s for %s at line %d since %s",
           pAlterReq->objFName, pAlterReq->tblName, pAlterReq->principal, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndAlterObjectPrivileges(SMnode *pMnode, void *pNew, SAlterRoleReq *pAlterReq, ESdbType sdbType) {
  // remove CM_ALL privilege since it's not used in later steps currently
  if (PRIV_HAS(&pAlterReq->privileges.privSet, PRIV_CM_ALL)) {
    privRemoveType(&pAlterReq->privileges.privSet, PRIV_CM_ALL);
  }
  switch (pAlterReq->objType) {
    case PRIV_OBJ_TBL: {
      return mndAlterTablePrivileges(pMnode, pNew, pAlterReq, sdbType);
    }
    default: {
      return mndAlterObjPrivileges(pMnode, pNew, pAlterReq, sdbType);
    }
  }
  TAOS_RETURN(0);
}

static bool mndUserIsRestricedSysRole(const char *role) {
  if (role[0] == 'S' && (strcmp(role, TSDB_ROLE_SYSSEC) == 0 || strcmp(role, TSDB_ROLE_SYSAUDIT) == 0 ||
                         strcmp(role, TSDB_ROLE_SYSDBA) == 0 || strcmp(role, TSDB_ROLE_SYSAUDIT_LOG) == 0)) {
    return true;
  }
  return false;
}

#define PRIV_SYS_SUPPORT_CHECK(privType)                                                                       \
  do {                                                                                                         \
    if (PRIV_HAS(&pAlterReq->privileges.privSet, (privType))) {                                                \
      mError("role:%s, Cannot grant or revoke system privilege: %s", pNew->name, privInfoGet(privType)->name); \
      TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);                                                              \
    }                                                                                                          \
  } while (0)

int32_t mndAlterRoleInfo(SMnode *pMnode, SUserObj *pOperUser, const char *token, SRoleObj *pOld, SRoleObj *pNew,
                         SAlterRoleReq *pAlterReq) {
  int32_t code = 0, lino = 0;

  switch (pAlterReq->alterType) {
    case TSDB_ALTER_ROLE_LOCK: {
      TAOS_CHECK_EXIT(mndCheckSysObjPrivilege(pMnode, pOperUser, token,
                                              pAlterReq->lock ? PRIV_ROLE_LOCK : PRIV_ROLE_UNLOCK, 0, 0, NULL, NULL));

      if (mndUserIsRestricedSysRole(pNew->name)) {
        mError("role:%s, cannot be locked or unlocked", pNew->name);
        TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
      }
      pNew->enable = pAlterReq->lock ? 0 : 1;
      break;
    }
    case TSDB_ALTER_ROLE_PRIVILEGES: {
      TAOS_CHECK_EXIT(mndCheckSysObjPrivilege(
          pMnode, pOperUser, token, pAlterReq->lock ? PRIV_GRANT_PRIVILEGE : PRIV_REVOKE_PRIVILEGE, 0, 0, NULL, NULL));
      if (pAlterReq->sysPriv) {
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_BALANCE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_BALANCE_LEADER);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_MERGE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_REDISTRIBUTE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_SPLIT);
        if (pAlterReq->add) {
          privAddSet(&pNew->sysPrivs, &pAlterReq->privileges.privSet);
        } else {
          privRemoveSet(&pNew->sysPrivs, &pAlterReq->privileges.privSet);
        }
      } else {
        TAOS_CHECK_EXIT(mndAlterObjectPrivileges(pMnode, pNew, pAlterReq, SDB_ROLE));
      }
      break;
    }
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
_exit:
  if (code < 0) {
    mError("role:%s, failed at line %d to alter info since %s, alter type:%" PRIu8, pOld->name, lino, tstrerror(code),
           pAlterReq->alterType);
  }
  TAOS_RETURN(code);
}

int32_t mndAlterUserPrivInfo(SMnode *pMnode, SUserObj *pNew, SAlterRoleReq *pAlterReq) {
  int32_t code = 0, lino = 0;
  switch (pAlterReq->alterType) {
    case TSDB_ALTER_ROLE_PRIVILEGES: {
      if (pAlterReq->sysPriv) {
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_BALANCE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_BALANCE_LEADER);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_MERGE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_REDISTRIBUTE);
        PRIV_SYS_SUPPORT_CHECK(PRIV_VG_SPLIT);
        if (pAlterReq->add) {
          privAddSet(&pNew->sysPrivs, &pAlterReq->privileges.privSet);
        } else {
          privRemoveSet(&pNew->sysPrivs, &pAlterReq->privileges.privSet);
        }
        if (PRIV_HAS(&pAlterReq->privileges.privSet, PRIV_DB_CREATE)) {
          pNew->createdb = pAlterReq->add ? 1 : 0;
        }
      } else {
        TAOS_CHECK_EXIT(mndAlterObjectPrivileges(pMnode, pNew, pAlterReq, SDB_USER));
      }
      break;
    }
    default:
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
_exit:
  if (code < 0) {
    mError("user:%s, failed at line %d to alter info since %s, alter type:%" PRIu8, pNew->user, lino, tstrerror(code),
           pAlterReq->alterType);
  }
  TAOS_RETURN(code);
}

static bool mndUserHasRestrictedSysRole(SUserObj *pUser, char *role) {
  void *pIter = NULL;
  while ((pIter = taosHashIterate(pUser->roles, pIter))) {
    char *pRoleName = taosHashGetKey(pIter, NULL);
    if (mndUserIsRestricedSysRole(pRoleName)) {
      if (role) strncpy(role, pRoleName, TSDB_ROLE_LEN);
      return true;
    }
  }
  return false;
}

static bool mndHasOthersWithSysDBA(SMnode *pMnode, const char *exceptUser) {
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  void     *pIter = NULL;
  int32_t   klen = strlen(TSDB_ROLE_SYSDBA) + 1;

  while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser))) {
    if (pUser->enable && (strcmp(pUser->user, exceptUser) != 0)) {
      if (taosHashGet(pUser->roles, TSDB_ROLE_SYSDBA, klen)) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pIter);
        return true;
      }
    }
    sdbRelease(pSdb, pUser);
  }
  return false;
}

static int32_t mndAlterUserRoleSysRoleCheck(SMnode *pMnode, SUserObj *pUser, SAlterRoleReq *pAlterReq) {
  int32_t code = 0, lino = 0;
  if (taosStrncasecmp(pAlterReq->roleName, "sysinfo_", 8) == 0) goto _exit;
  if (pAlterReq->add) {
    char role[TSDB_ROLE_LEN] = {0};
    if (mndUserHasRestrictedSysRole(pUser, role)) {
      code = TSDB_CODE_MND_ROLE_CONFLICTS;
      mError("user:%s, cannot grant role %s since %s %s", pUser->user, pAlterReq->roleName, tstrerror(code), role);
      TAOS_CHECK_EXIT(code);
    }
  } else {
#if 0
    // prevent unintended misoperation, which may lead to audit data loss
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSAUDIT_LOG) == 0) {
      mError("user:%s, cannot revoke system role:%s", pUser->user, pAlterReq->roleName);
      TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
    }
#endif
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSDBA) == 0) {
      if (!mndHasOthersWithSysDBA(pMnode, pUser->user)) {
        code = TSDB_CODE_MND_ROLE_NO_VALID_SYSDBA;
        mError("user:%s, cannot revoke role %s since %s", pUser->user, pAlterReq->roleName, tstrerror(code));
        TAOS_CHECK_EXIT(code);
      }
    }
  }
_exit:
  TAOS_RETURN(code);
}

static int32_t mndCheckAlterRolePrivilege(SMnode *pMnode, SUserObj *pOperUser, const char *token,
                                          SAlterRoleReq *pAlterReq) {
  if (IS_SYS_PREFIX(pAlterReq->roleName)) {
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSDBA) == 0) {
      return mndCheckSysObjPrivilege(pMnode, pOperUser, token, pAlterReq->add ? PRIV_GRANT_SYSDBA : PRIV_REVOKE_SYSDBA,
                                     0, 0, NULL, NULL);
    }
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSSEC) == 0) {
      return mndCheckSysObjPrivilege(pMnode, pOperUser, token, pAlterReq->add ? PRIV_GRANT_SYSSEC : PRIV_REVOKE_SYSSEC,
                                     0, 0, NULL, NULL);
    }
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSAUDIT) == 0) {
      return mndCheckSysObjPrivilege(pMnode, pOperUser, token,
                                     pAlterReq->add ? PRIV_GRANT_SYSAUDIT : PRIV_REVOKE_SYSAUDIT, 0, 0, NULL, NULL);
    }
  }
  return mndCheckSysObjPrivilege(pMnode, pOperUser, token,
                                 pAlterReq->add ? PRIV_GRANT_PRIVILEGE : PRIV_REVOKE_PRIVILEGE, 0, 0, NULL, NULL);
}

static int32_t mndUserUpdateAuditDb(SMnode *pMnode, SUserObj *pNew, SAlterRoleReq *pAlterReq, uint8_t roleType) {
  int32_t   code = 0, lino = 0;
  char      key[TSDB_PRIV_MAX_KEY_LEN] = {0};

  if (pAlterReq->add) {
    SDbObj *pDb = NULL;
    void   *pIter = NULL;
    while ((pIter = sdbFetch(pMnode->pSdb, SDB_DB, pIter, (void **)&pDb))) {
      if (pDb->cfg.isAudit) {
        if ((code = taosHashPut(pNew->ownedDbs, pDb->name, strlen(pDb->name) + 1, NULL, 0))) {
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pDb);
          TAOS_CHECK_EXIT(code);
        }
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDb);
        break;  // only one audit db exists
      }
      sdbRelease(pMnode->pSdb, pDb);
    }
  } else {
    void *qIter = NULL;
    while ((qIter = taosHashIterate(pNew->ownedDbs, qIter))) {
      char   *dbFName = taosHashGetKey(qIter, NULL);
      SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
      if (!pDb || pDb->cfg.isAudit) {
        if ((code = taosHashRemove(pNew->ownedDbs, dbFName, strlen(dbFName) + 1))) {
          if (pDb) mndReleaseDb(pMnode, pDb);
          TAOS_CHECK_EXIT(code);
        }
      }
      mndReleaseDb(pMnode, pDb);
    }
  }
_exit:
  if (code != 0) {
    mError("user:%s, failed at line %d to update owned audit db since %s", pNew->user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndAlterUserRoleInfo(SMnode *pMnode, SUserObj *pOperUser, const char *token, SUserObj *pOld, SUserObj *pNew,
                             SAlterRoleReq *pAlterReq) {
  int32_t   code = 0, lino = 0;
  SRoleObj *pRole = NULL;
  bool      isSysRole = false;

  if (pAlterReq->roleName[0] == '\0') {
    mError("failed to alter user role since role name is empty");
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  TAOS_CHECK_EXIT(mndCheckAlterRolePrivilege(pMnode, pOperUser, token, pAlterReq));

  if (pOld->superUser && taosStrncasecmp(pAlterReq->roleName, "sys", 3) == 0) {
    mError("user:%s, is superuser, cannot grant or revoke system role:%s", pOld->user, pAlterReq->roleName);
    TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);
  }

  if (pAlterReq->add) {
    TAOS_CHECK_EXIT(mndAcquireRole(pMnode, pAlterReq->roleName, &pRole));

    if (taosHashGet(pOld->roles, pAlterReq->roleName, strlen(pAlterReq->roleName) + 1)) {
      mInfo("user:%s, no need to grant since already has role:%s", pOld->user, pAlterReq->roleName);
      TAOS_CHECK_EXIT(TSDB_CODE_QRY_DUPLICATED_OPERATION);
    }
    int32_t nSubRoles = taosHashGetSize(pOld->roles);
    if (nSubRoles >= TSDB_MAX_SUBROLE) {
      mError("user:%s, has reached max subrole number:%d", pOld->user, TSDB_MAX_SUBROLE);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_SUBROLE_EXCEEDED);
    }

    if (taosStrncasecmp(pAlterReq->roleName, "sys", 3) == 0) {
      isSysRole = true;
      TAOS_CHECK_EXIT(mndAlterUserRoleSysRoleCheck(pMnode, pOld, pAlterReq));
    }

    TAOS_CHECK_EXIT(mndUserDupObj(pOld, pNew));
    uint8_t flag = 1;  // add role with flag 1, which means the role is set(enabled) for the user.
    TAOS_CHECK_EXIT(
        taosHashPut(pNew->roles, pAlterReq->roleName, strlen(pAlterReq->roleName) + 1, &flag, sizeof(flag)));
  } else {
    if (!taosHashGet(pOld->roles, pAlterReq->roleName, strlen(pAlterReq->roleName) + 1)) {
      mInfo("user:%s, no need to revoke since not have role:%s", pOld->user, pAlterReq->roleName);
      TAOS_CHECK_EXIT(TSDB_CODE_QRY_DUPLICATED_OPERATION);
    }
    if (taosStrncasecmp(pAlterReq->roleName, "sys", 3) == 0) {
      isSysRole = true;
      TAOS_CHECK_EXIT(mndAlterUserRoleSysRoleCheck(pMnode, pOld, pAlterReq));
    }
    TAOS_CHECK_EXIT(mndUserDupObj(pOld, pNew));
    TAOS_CHECK_EXIT(taosHashRemove(pNew->roles, pAlterReq->roleName, strlen(pAlterReq->roleName) + 1));
  }

  if (isSysRole) {
    if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSAUDIT) == 0) {
      TAOS_CHECK_EXIT(mndUserUpdateAuditDb(pMnode, pNew, pAlterReq, T_ROLE_SYSAUDIT));
    } else if (strcmp(pAlterReq->roleName, TSDB_ROLE_SYSAUDIT_LOG) == 0) {
      TAOS_CHECK_EXIT(mndUserUpdateAuditDb(pMnode, pNew, pAlterReq, T_ROLE_SYSAUDIT_LOG));
      if (pAlterReq->add) {
        (void)mndResetAuditLogUser(pMnode, pNew->user, true);
      } else {
        (void)mndResetAuditLogUser(pMnode, pNew->user, false);
      }
    }
  }

_exit:
  mndReleaseRole(pMnode, pRole);
  if (code != 0 && code != TSDB_CODE_QRY_DUPLICATED_OPERATION) {
    mError("user:%s, failed at line %d to alter role:%s since %s", pOld->user, lino, pAlterReq->roleName,
           tstrerror(code));
  }
  TAOS_RETURN(code);
}

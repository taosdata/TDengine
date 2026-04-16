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
#include "mndSecurityPolicy.h"
#include "mndCluster.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

// SDB raw format version.  Bump when adding new persistent fields.
#define SEC_POLICY_VER_NUMBE    1
// Extra raw-level reserve bytes (beyond what the struct's reserve[48] already provides)
#define SEC_POLICY_RAW_RESERVE  8

static SSdbRaw *mndSecPolicyActionEncode(SSecurityPolicyObj *pObj);
static SSdbRow *mndSecPolicyActionDecode(SSdbRaw *pRaw);
static int32_t  mndSecPolicyActionInsert(SSdb *pSdb, SSecurityPolicyObj *pObj);
static int32_t  mndSecPolicyActionDelete(SSdb *pSdb, SSecurityPolicyObj *pObj);
static int32_t  mndSecPolicyActionUpdate(SSdb *pSdb, SSecurityPolicyObj *pOld, SSecurityPolicyObj *pNew);
static int32_t  mndCreateDefaultSecurityPolicy(SMnode *pMnode);
static int32_t  mndRetrieveSecurityPolicies(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextSecurityPolicy(SMnode *pMnode, void *pIter);
static int32_t  mndProcessEnforceSodImpl(SMnode *pMnode);

int32_t mndInitSecurityPolicy(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_SECURITY_POLICY,
      .keyType = SDB_KEY_INT32,
      .deployFp = (SdbDeployFp)mndCreateDefaultSecurityPolicy,
      .encodeFp = (SdbEncodeFp)mndSecPolicyActionEncode,
      .decodeFp = (SdbDecodeFp)mndSecPolicyActionDecode,
      .insertFp = (SdbInsertFp)mndSecPolicyActionInsert,
      .updateFp = (SdbUpdateFp)mndSecPolicyActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSecPolicyActionDelete,
  };

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SECURITY_POLICIES, mndRetrieveSecurityPolicies);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_SECURITY_POLICIES, mndCancelGetNextSecurityPolicy);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSecurityPolicy(SMnode *pMnode) {}

// ---- SDB encode/decode ----

static SSdbRaw *mndSecPolicyActionEncode(SSecurityPolicyObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_SECURITY_POLICY, SEC_POLICY_VER_NUMBE,
                               sizeof(SSecurityPolicyObj) + SEC_POLICY_RAW_RESERVE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->type, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->activateTime, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pObj->status, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->activator, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->reserve, sizeof(pObj->reserve), _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, SEC_POLICY_RAW_RESERVE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER);

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("secpolicy:%d, failed to encode to raw:%p since %s", pObj->type, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("secpolicy:%d, encode to raw:%p, row:%p", pObj->type, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndSecPolicyActionDecode(SSdbRaw *pRaw) {
  int32_t             code = 0;
  int32_t             lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSecurityPolicyObj *pObj = NULL;
  SSdbRow            *pRow = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != SEC_POLICY_VER_NUMBE) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SSecurityPolicyObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->type, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->activateTime, _OVER)
  SDB_GET_UINT8(pRaw, dataPos, &pObj->status, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->activator, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->reserve, sizeof(pObj->reserve), _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("secpolicy:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->type, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("secpolicy:%d, decode from raw:%p, row:%p", pObj->type, pRaw, pObj);
  return pRow;
}

static int32_t mndSecPolicyActionInsert(SSdb *pSdb, SSecurityPolicyObj *pObj) {
  mTrace("secpolicy:%d, perform insert action, row:%p", pObj->type, pObj);
  return 0;
}

static int32_t mndSecPolicyActionDelete(SSdb *pSdb, SSecurityPolicyObj *pObj) {
  mTrace("secpolicy:%d, perform delete action, row:%p", pObj->type, pObj);
  return 0;
}

static int32_t mndSecPolicyActionUpdate(SSdb *pSdb, SSecurityPolicyObj *pOld, SSecurityPolicyObj *pNew) {
  mTrace("secpolicy:%d, perform update action, old row:%p new row:%p", pOld->type, pOld, pNew);
  pOld->updateTime   = pNew->updateTime;
  pOld->activateTime = pNew->activateTime;
  pOld->status       = pNew->status;
  tstrncpy(pOld->activator, pNew->activator, sizeof(pOld->activator));
  (void)memcpy(pOld->reserve, pNew->reserve, sizeof(pOld->reserve));
  return 0;
}

// ---- Deploy helpers ----

// Append a single policy row as a commit-log entry in pTrans.
static int32_t mndAppendPolicyToTrans(STrans *pTrans, int32_t policyType) {
  SSecurityPolicyObj obj = {0};
  obj.type        = policyType;
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime  = obj.createdTime;
  obj.activateTime = obj.createdTime;
  // status defaults to 0 (SEC_POLICY_STATUS_DEFAULT) for both SOD and MAC

  SSdbRaw *pRaw = mndSecPolicyActionEncode(&obj);
  if (pRaw == NULL) return terrno;

  int32_t code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    return code;
  }

  code = mndTransAppendCommitlog(pTrans, pRaw);
  if (code != 0) {
    // pRaw ownership transferred on success; free only on failure
    sdbFreeRaw(pRaw);
    return code;
  }

  // Second sdbSetRawStatus marks it ready-for-commit (matches cluster deploy pattern)
  return sdbSetRawStatus(pRaw, SDB_STATUS_READY);
}

static int32_t mndCreateDefaultSecurityPolicy(SMnode *pMnode) {
  // One transaction creates both SOD and MAC rows.
  // key = int32 policyType, no dependency on clusterId → deploy order safe.
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-security-policy");
  if (pTrans == NULL) return terrno;

  int32_t code = 0;
  if ((code = mndAppendPolicyToTrans(pTrans, TSDB_POLICY_TYPE_SOD)) != 0) goto _OVER;
  if ((code = mndAppendPolicyToTrans(pTrans, TSDB_POLICY_TYPE_MAC)) != 0) goto _OVER;

  mInfo("trans:%d, used to create default security policies (SOD + MAC)", pTrans->id);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
  }

_OVER:
  mndTransDrop(pTrans);
  return code;
}

// ---- SDB acquire/release helpers ----

static SSecurityPolicyObj *mndAcquireSecPolicy(SMnode *pMnode, int32_t policyType) {
  return (SSecurityPolicyObj *)sdbAcquire(pMnode->pSdb, SDB_SECURITY_POLICY, &policyType);
}

static void mndReleaseSecPolicy(SMnode *pMnode, SSecurityPolicyObj *pObj) {
  sdbRelease(pMnode->pSdb, pObj);
}

// ---- Accessor functions ----

int32_t mndGetClusterSoDMode(SMnode *pMnode) {
  int32_t             sodMode = SOD_MODE_ENABLED;
  int32_t             key = TSDB_POLICY_TYPE_SOD;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, key);
  if (pObj != NULL) {
    sodMode = pObj->status;
    mndReleaseSecPolicy(pMnode, pObj);
  }
  return sodMode;
}

int32_t mndGetClusterMacActive(SMnode *pMnode) {
  int32_t             macMode = MAC_MODE_DISABLED;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, TSDB_POLICY_TYPE_MAC);
  if (pObj != NULL) {
    macMode = pObj->status;
    mndReleaseSecPolicy(pMnode, pObj);
  }
  return macMode;
}

// ---- show security_policies ----

static const char *_SoDMandatoryInfo[3][2] = {
    {"mandatory",           "system is operational, root disabled permanently"},
    {"mandatory(initial)",  "Initial phase: mandatory roles missing, only account setup operations are allowed"},
    {"mandatory(enforcing)", "Enforce phase: transitioning mode, account destructive operations are blocked"},
};

static int32_t mndRetrieveSecurityPolicies(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode             *pMnode = pMsg->info.node;
  SSdb               *pSdb = pMnode->pSdb;
  int32_t             code = 0, lino = 0;
  int32_t             numOfRows = 0;
  SSecurityPolicyObj *pObj = NULL;
  char                buf[128 + VARSTR_HEADER_SIZE] = {0};

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SECURITY_POLICY, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    int32_t cols = 0;

    if (pObj->type == TSDB_POLICY_TYPE_SOD) {
      int32_t sodPhase   = mndGetSoDPhase(pMnode);
      bool    sodEnabled = (pObj->status == SOD_MODE_ENABLED);

      STR_WITH_MAXSIZE_TO_VARSTR(buf, "SoD", pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf,
          sodEnabled ? "enabled" : _SoDMandatoryInfo[sodPhase][0],
          pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf,
          sodEnabled ? "SYSTEM" : pObj->activator,
          pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)&pObj->updateTime, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf,
          sodEnabled ? "non-mandatory, root not disabled"
                     : _SoDMandatoryInfo[sodPhase][1],
          pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    } else if (pObj->type == TSDB_POLICY_TYPE_MAC) {
      bool macActive = (pObj->status == MAC_MODE_MANDATORY);

      STR_WITH_MAXSIZE_TO_VARSTR(buf, "MAC", pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf, macActive ? "mandatory" : "disabled", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf, macActive ? pObj->activator : "",
                                 pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

      int64_t macTs = macActive ? pObj->updateTime : 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)&macTs, false, pObj, pShow->pIter, _OVER);

      STR_WITH_MAXSIZE_TO_VARSTR(buf,
                                 macActive ? "security levels 0-4; activated, irreversible"
                                           : "not activated; enable via: ALTER CLUSTER 'MAC' 'mandatory'",
                                 pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    } else {
      // Unknown policy type — skip row silently
      sdbRelease(pSdb, pObj);
      continue;
    }

    sdbRelease(pSdb, pObj);
    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;

_OVER:
  if (code != 0) {
    mError("failed to retrieve security policies at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextSecurityPolicy(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_SECURITY_POLICY);
}

// ---- SoD config handler ----

// #ifdef TD_ENTERPRISE
int32_t mndProcessConfigSoDReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t             code = 0, lino = 0;
  SSecurityPolicyObj  obj = {0};
  STrans             *pTrans = NULL;
  SUserObj           *pRootUser = NULL;
  SUserObj            newRootUser = {0};

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_CONFIG_SOD));

  if (taosStrncasecmp(pCfg->value, "mandatory", 10) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
  }

  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, TSDB_POLICY_TYPE_SOD);
  if (!pObj) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (pObj->status == SOD_MODE_MANDATORY) {
    mndReleaseSecPolicy(pMnode, pObj);
    TAOS_RETURN(0);
  }

  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseSecPolicy(pMnode, pObj);
    TAOS_CHECK_EXIT(code);
  }

  mInfo("update security policy SoD mode to mandatory by %s", RPC_MSG_USER(pReq));
  (void)memcpy(&obj, pObj, sizeof(SSecurityPolicyObj));
  obj.status       = SOD_MODE_MANDATORY;
  obj.activateTime = taosGetTimestampMs();
  obj.updateTime   = obj.activateTime;
  tstrncpy(obj.activator, RPC_MSG_USER(pReq), sizeof(obj.activator));
  mndReleaseSecPolicy(pMnode, pObj);

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, "root", &pRootUser));
  TAOS_CHECK_EXIT(mndUserDupObj(pRootUser, &newRootUser));
  newRootUser.enable = 0;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "update-sod-mode");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  SSdbRaw *pCommitRaw = mndSecPolicyActionEncode(&obj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    if (pCommitRaw) sdbFreeRaw(pCommitRaw);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  SSdbRaw *pCommitRawRoot = mndUserActionEncode(&newRootUser);
  if (pCommitRawRoot == NULL || mndTransAppendCommitlog(pTrans, pCommitRawRoot) != 0) {
    if (pCommitRawRoot) sdbFreeRaw(pCommitRawRoot);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRawRoot, SDB_STATUS_READY));

  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_ENFORCE);
  mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_SOD, NULL, 0);
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_STABLE);
    TAOS_CHECK_EXIT(code);
  }

_exit:
  if (pRootUser) mndReleaseUser(pMnode, pRootUser);
  mndUserFreeObj(&newRootUser);
  mndTransDrop(pTrans);
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
// #endif

// ---- MAC config handler ----

// #ifdef TD_ENTERPRISE
int32_t mndProcessConfigMacReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t             code = 0, lino = 0;
  SSecurityPolicyObj  obj = {0};
  STrans             *pTrans = NULL;
  SUserObj           *pUpgradeUser = NULL;
  void               *pUpgradeIter = NULL;

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_CONFIG_MAC));

  if (taosStrncasecmp(pCfg->value, "mandatory", 10) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
  }

  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, TSDB_POLICY_TYPE_MAC);
  if (!pObj) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (pObj->status == MAC_MODE_MANDATORY) {
    mInfo("cluster MAC is already mandatory, ignoring repeated activation by %s", RPC_MSG_USER(pReq));
    mndReleaseSecPolicy(pMnode, pObj);
    TAOS_RETURN(0);
  }

  mInfo("activating cluster MAC by %s", RPC_MSG_USER(pReq));
  (void)memcpy(&obj, pObj, sizeof(SSecurityPolicyObj));
  obj.status = MAC_MODE_MANDATORY;
  obj.activateTime = taosGetTimestampMs();
  obj.updateTime   = obj.activateTime;
  tstrncpy(obj.activator, RPC_MSG_USER(pReq), sizeof(obj.activator));
  mndReleaseSecPolicy(pMnode, pObj);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "activate-mac");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  SSdbRaw *pCommitRaw = mndSecPolicyActionEncode(&obj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    if (pCommitRaw) sdbFreeRaw(pCommitRaw);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  // Auto-upgrade all non-superUser users whose maxSecLevel is below their role floor.
  // This is done atomically inside the same trans so MAC activation and upgrades land together.
  {
    SSdb *pSdb = pMnode->pSdb;
    while ((pUpgradeIter = sdbFetch(pSdb, SDB_USER, pUpgradeIter, (void **)&pUpgradeUser)) != NULL) {
      if (pUpgradeUser->superUser) {
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        continue;
      }
      int8_t floorLevel = mndGetUserRoleFloorMaxLevel(pUpgradeUser->roles);
      if (pUpgradeUser->maxSecLevel >= floorLevel) {
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        continue;
      }
      SUserObj tmpUser = {0};
      if ((code = mndUserDupObj(pUpgradeUser, &tmpUser)) != 0) {
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        sdbCancelFetch(pSdb, pUpgradeIter);
        pUpgradeIter = NULL;
        TAOS_CHECK_EXIT(code);
      }
      mInfo("MAC activation: auto-upgrading user:%s maxSecLevel %d -> %d to satisfy role floor", pUpgradeUser->user,
            (int32_t)pUpgradeUser->maxSecLevel, (int32_t)floorLevel);
      tmpUser.maxSecLevel = floorLevel;
      SSdbRaw *pUserRaw = mndUserActionEncode(&tmpUser);
      mndUserFreeObj(&tmpUser);
      if (pUserRaw == NULL) {
        code = terrno;
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        sdbCancelFetch(pSdb, pUpgradeIter);
        pUpgradeIter = NULL;
        TAOS_CHECK_EXIT(code);
      }
      if ((code = mndTransAppendCommitlog(pTrans, pUserRaw)) != 0) {
        sdbFreeRaw(pUserRaw);
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        sdbCancelFetch(pSdb, pUpgradeIter);
        pUpgradeIter = NULL;
        TAOS_CHECK_EXIT(code);
      }
      if ((code = sdbSetRawStatus(pUserRaw, SDB_STATUS_READY)) != 0) {
        sdbRelease(pSdb, pUpgradeUser);
        pUpgradeUser = NULL;
        sdbCancelFetch(pSdb, pUpgradeIter);
        pUpgradeIter = NULL;
        TAOS_CHECK_EXIT(code);
      }
      sdbRelease(pSdb, pUpgradeUser);
      pUpgradeUser = NULL;
    }
    pUpgradeIter = NULL;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    TAOS_CHECK_EXIT(code);
  }

_exit:
  if (pUpgradeIter) sdbCancelFetch(pMnode->pSdb, pUpgradeIter);
  if (pUpgradeUser) sdbRelease(pMnode->pSdb, pUpgradeUser);
  mndTransDrop(pTrans);
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
// #endif

// ---- SoD enforcement ----

void mndSodTransStop(SMnode *pMnode, void *param, int32_t paramLen) {
#ifdef TD_ENTERPRISE
  mInfo("SoD trans stop, set SoD phase to %d", TSDB_SOD_PHASE_STABLE);
  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_STABLE);
#endif
}

void mndSodGrantRoleStop(SMnode *pMnode, void *param, int32_t paramLen) {
#ifdef TD_ENTERPRISE
  if (mndGetSoDPhase(pMnode) != TSDB_SOD_PHASE_INITIAL) {
    return;
  }

  if (mndCheckManagementRoleStatus(pMnode, NULL, 0) == 0) {
    mInfo("SoD role check completed, all mandatory roles satisfied, trigger enforce SoD");
    (void)mndProcessEnforceSodImpl(pMnode);
  }
#endif
}

#ifdef TD_ENTERPRISE
static int32_t mndProcessEnforceSodImpl(SMnode *pMnode) {
  int32_t code = 0, lino = 0;
  int32_t contLen = 0;
  void   *pCont = NULL;

  SMCfgClusterReq cfgReq = {0};
  tsnprintf(cfgReq.config, sizeof(cfgReq.config), "SoD");
  tsnprintf(cfgReq.value, sizeof(cfgReq.value), "mandatory");
  contLen = tSerializeSMCfgClusterReq(NULL, 0, &cfgReq);
  TAOS_CHECK_EXIT(contLen);
  if (!(pCont = rpcMallocCont(contLen))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tSerializeSMCfgClusterReq(pCont, contLen, &cfgReq)) != contLen) {
    rpcFreeCont(pCont);
    TAOS_CHECK_EXIT(code);
  }

  SRpcMsg rpcMsg = {.pCont = pCont,
                    .contLen = contLen,
                    .msgType = TDMT_MND_CONFIG_CLUSTER,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1};
  SEpSet epSet = {0};
  mndGetMnodeEpSet(pMnode, &epSet);
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));
_exit:
  if (code < 0) {
    mError("failed at line %d to enforce SoD since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndProcessEnforceSod(SMnode *pMnode) {
  int32_t code = 0, lino = 0;

  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, TSDB_POLICY_TYPE_SOD);
  if (pObj == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (pObj->status == SOD_MODE_MANDATORY) {
    mInfo("cluster is already in SoD mandatory mode");
    mndReleaseSecPolicy(pMnode, pObj);
    TAOS_RETURN(0);
  }

  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_INITIAL);
  mInfo("start to enforce SoD from initial phase, secpolicy type:%d", pObj->type);
  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseSecPolicy(pMnode, pObj);
    TAOS_CHECK_EXIT(code);
  }
  mndReleaseSecPolicy(pMnode, pObj);

  TAOS_CHECK_EXIT(mndProcessEnforceSodImpl(pMnode));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to enforce SoD at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
#endif

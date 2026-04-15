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

#define SEC_POLICY_VER_NUMBE    1
#define SEC_POLICY_RESERVE_SIZE 63

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
      .keyType = SDB_KEY_INT64,
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

  SSdbRaw *pRaw =
      sdbAllocRaw(SDB_SECURITY_POLICY, SEC_POLICY_VER_NUMBE, sizeof(SSecurityPolicyObj) + SEC_POLICY_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pObj->flag, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->sodActivator, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->sodActivateTime, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->macActivator, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->macActivateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, SEC_POLICY_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER);

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("secpolicy:%" PRId64 ", failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("secpolicy:%" PRId64 ", encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
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
  SDB_GET_INT64(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_UINT8(pRaw, dataPos, &pObj->flag, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->sodActivator, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->sodActivateTime, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->macActivator, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->macActivateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, SEC_POLICY_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("secpolicy:%" PRId64 ", failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw,
           terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("secpolicy:%" PRId64 ", decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndSecPolicyActionInsert(SSdb *pSdb, SSecurityPolicyObj *pObj) {
  mTrace("secpolicy:%" PRId64 ", perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndSecPolicyActionDelete(SSdb *pSdb, SSecurityPolicyObj *pObj) {
  mTrace("secpolicy:%" PRId64 ", perform delete action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndSecPolicyActionUpdate(SSdb *pSdb, SSecurityPolicyObj *pOld, SSecurityPolicyObj *pNew) {
  mTrace("secpolicy:%" PRId64 ", perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = taosGetTimestampMs();
  pOld->flag = pNew->flag;
  tstrncpy(pOld->sodActivator, pNew->sodActivator, sizeof(pOld->sodActivator));
  pOld->sodActivateTime = pNew->sodActivateTime;
  tstrncpy(pOld->macActivator, pNew->macActivator, sizeof(pOld->macActivator));
  pOld->macActivateTime = pNew->macActivateTime;
  return 0;
}

static int32_t mndCreateDefaultSecurityPolicy(SMnode *pMnode) {
  SSecurityPolicyObj obj = {0};
  obj.id = 1;  // fixed singleton key; deploy order: SDB_SECURITY_POLICY before SDB_CLUSTER, so clusterId unavailable
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime = obj.createdTime;
  obj.sodActivateTime = obj.createdTime;
  obj.macActivateTime = obj.createdTime;
  // macActive defaults to 0 (inactive); must be explicitly activated via ALTER CLUSTER 'MAC' 'ENABLED'

  SSdbRaw *pRaw = mndSecPolicyActionEncode(&obj);
  if (pRaw == NULL) {
    int32_t code = terrno;
    TAOS_RETURN(code);
  }
  int32_t code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    TAOS_RETURN(code);
  }

  mInfo("secpolicy:%" PRId64 ", will be created when deploying, raw:%p", obj.id, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-security-policy");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, used to create security policy:%" PRId64, pTrans->id, obj.id);

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

// ---- SDB helpers ----

static SSecurityPolicyObj *mndAcquireSecPolicy(SMnode *pMnode, void **ppIter) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SSecurityPolicyObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_SECURITY_POLICY, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    *ppIter = pIter;
    return pObj;
  }

  return NULL;
}

static void mndReleaseSecPolicy(SMnode *pMnode, SSecurityPolicyObj *pObj, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
  sdbRelease(pSdb, pObj);
}

// ---- Accessor functions ----

int32_t mndGetClusterSoDMode(SMnode *pMnode) {
  int32_t             sodMode = SOD_MODE_ENABLED;
  void               *pIter = NULL;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, &pIter);
  if (pObj != NULL) {
    sodMode = pObj->sodMode;
    mndReleaseSecPolicy(pMnode, pObj, pIter);
  }
  return sodMode;
}

int32_t mndGetClusterMacActive(SMnode *pMnode) {
  int32_t             macActive = MAC_MODE_INACTIVE;
  void               *pIter = NULL;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, &pIter);
  if (pObj) {
    macActive = pObj->macActive;
    mndReleaseSecPolicy(pMnode, pObj, pIter);
  }
  return macActive;
}

// ---- show security_policies ----

static const char *_SoDMandatoryInfo[3][2] = {
    {"mandatory", "system is operational, root disabled permanently"},
    {"mandatory(initial)", "Initial phase: mandatory roles missing, only account setup operations are allowed"},
    {"mandatory(enforcing)", "Enforce phase: transitioning mode, account destructive operations are blocked"},
};

static int32_t mndRetrieveSecurityPolicies(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode             *pMnode = pMsg->info.node;
  SSdb               *pSdb = pMnode->pSdb;
  int32_t             code = 0, lino = 0;
  int32_t             numOfRows = 0;
  int32_t             cols = 0;
  SSecurityPolicyObj *pObj = NULL;
  char                buf[128 + VARSTR_HEADER_SIZE] = {0};

  while ((numOfRows + 1) < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SECURITY_POLICY, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "SoD", pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    int32_t sodMode = pObj->sodMode;
    int32_t sodPhase = mndGetSoDPhase(pMnode);
    bool    sodEnabled = (sodMode == SOD_MODE_ENABLED && sodPhase == TSDB_SOD_PHASE_STABLE);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "enabled" : _SoDMandatoryInfo[sodPhase][0],
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "SYSTEM" : pObj->sodActivator, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pObj->sodActivateTime, false, pObj, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "non-mandatory, root not disabled" : _SoDMandatoryInfo[sodPhase][1],
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    ++numOfRows;

    cols = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "MAC", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->macActive ? "mandatory" : "inactive", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->macActive ? pObj->macActivator : "",
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int64_t macTs = pObj->macActive ? pObj->macActivateTime : 0;
    COL_DATA_SET_VAL_GOTO((const char *)&macTs, false, pObj, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf,
                               pObj->macActive ? "security levels 0-4; activated and non-configurable"
                                               : "not activated; enable via: ALTER CLUSTER 'MAC' 'ENABLED'",
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pObj, pShow->pIter, _OVER);

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

  // Only support to set SoD mode to mandatory
  if (taosStrncasecmp(pCfg->value, "mandatory", 10) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
  }

  void               *pIter = NULL;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, &pIter);
  if (!pObj) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (pObj->sodMode == SOD_MODE_MANDATORY) {
    mndReleaseSecPolicy(pMnode, pObj, pIter);
    TAOS_RETURN(0);
  }

  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseSecPolicy(pMnode, pObj, pIter);
    TAOS_CHECK_EXIT(code);
  }

  mInfo("update security policy SoD mode to mandatory by %s", RPC_MSG_USER(pReq));
  (void)memcpy(&obj, pObj, sizeof(SSecurityPolicyObj));
  obj.sodMode = SOD_MODE_MANDATORY;
  tstrncpy(obj.sodActivator, RPC_MSG_USER(pReq), sizeof(obj.sodActivator));
  obj.sodActivateTime = taosGetTimestampMs();
  obj.updateTime = obj.sodActivateTime;
  mndReleaseSecPolicy(pMnode, pObj, pIter);

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
  // disable root user
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

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_CONFIG_MAC));

  // Only support value "enabled"
  if (taosStrncasecmp(pCfg->value, "enabled", 8) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
  }

  void               *pIter = NULL;
  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, &pIter);
  if (!pObj) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  // Idempotent: already active, succeed silently
  if (pObj->macActive == MAC_MODE_ACTIVE) {
    mInfo("cluster MAC is already active, ignoring repeated activation by %s", RPC_MSG_USER(pReq));
    mndReleaseSecPolicy(pMnode, pObj, pIter);
    TAOS_RETURN(0);
  }

  mInfo("activating cluster MAC by %s", RPC_MSG_USER(pReq));
  (void)memcpy(&obj, pObj, sizeof(SSecurityPolicyObj));
  obj.macActive = MAC_MODE_ACTIVE;
  obj.macActivateTime = taosGetTimestampMs();
  obj.updateTime = obj.macActivateTime;
  tstrncpy(obj.macActivator, RPC_MSG_USER(pReq), sizeof(obj.macActivator));
  mndReleaseSecPolicy(pMnode, pObj, pIter);

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
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    TAOS_CHECK_EXIT(code);
  }
_exit:
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
  void   *pIter = NULL;

  SSecurityPolicyObj *pObj = mndAcquireSecPolicy(pMnode, &pIter);
  if (pObj == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (pObj->sodMode == SOD_MODE_MANDATORY) {
    mInfo("cluster is already in SoD mandatory mode");
    mndReleaseSecPolicy(pMnode, pObj, pIter);
    TAOS_RETURN(0);
  }

  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_INITIAL);
  mInfo("start to enforce SoD from initial phase, secpolicy:%" PRId64, pObj->id);
  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseSecPolicy(pMnode, pObj, pIter);
    TAOS_CHECK_EXIT(code);
  }

  TAOS_CHECK_EXIT(mndProcessEnforceSodImpl(pMnode));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to enforce SoD at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
#endif

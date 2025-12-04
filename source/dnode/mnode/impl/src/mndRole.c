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
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

// clang-format on

#define MND_ROLE_VER_NUMBER  1
#define MND_ROLE_SYSROLE_VER 1  // increase if system role definition updated in privInfoTable

static SRoleMgmt roleMgmt = {0};
static bool      isDeploy = false;

static int32_t  mndCreateDefaultRoles(SMnode *pMnode);
static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw);
static int32_t  mndRoleActionInsert(SSdb *pSdb, SRoleObj *pRole);
static int32_t  mndRoleActionDelete(SSdb *pSdb, SRoleObj *pRole);
static int32_t  mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew);
static int32_t  mndCreateRole(SMnode *pMnode, char *acct, SCreateRoleReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessDropRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessGetRoleAuthReq(SRpcMsg *pReq);
static int32_t  mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextRole(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);
static int32_t  mndRetrieveColPrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextColPrivileges(SMnode *pMnode, void *pIter);

int32_t mndInitRole(SMnode *pMnode) {
  // role management init
  roleMgmt.lastUpd = INT64_MAX;
  TAOS_CHECK_RETURN(taosThreadRwlockInit(&roleMgmt.rw, NULL));

  SSdbTable table = {
      .sdbType = SDB_ROLE,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultRoles,
      // .redeployFp = (SdbDeployFp)mndCreateDefaultRoles, // TODO: upgrade role table
      .encodeFp = (SdbEncodeFp)mndRoleActionEncode,
      .decodeFp = (SdbDecodeFp)mndRoleActionDecode,
      .insertFp = (SdbInsertFp)mndRoleActionInsert,
      .updateFp = (SdbUpdateFp)mndRoleActionUpdate,
      .deleteFp = (SdbDeleteFp)mndRoleActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ROLE, mndProcessCreateRoleReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ROLE, mndProcessDropRoleReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_ROLE, mndProcessAlterRoleReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ROLE, mndRetrieveRoles);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ROLE, mndCancelGetNextRole);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ROLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ROLE_PRIVILEGES, mndCancelGetNextPrivileges);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ROLE_COL_PRIVILEGES, mndRetrieveColPrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ROLE_COL_PRIVILEGES, mndCancelGetNextColPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRole(SMnode *pMnode) { (void)taosThreadRwlockDestroy(&roleMgmt.rw); }

int64_t mndGetRoleLastUpd() {
  int64_t lastUpd;
  (void)taosThreadRwlockRdlock(&roleMgmt.rw);
  lastUpd = roleMgmt.lastUpd;
  (void)taosThreadRwlockUnlock(&roleMgmt.rw);
  return lastUpd;
}

void mndSetRoleLastUpd(int64_t updateTime) {
  (void)taosThreadRwlockWrlock(&roleMgmt.rw);
  roleMgmt.lastUpd = updateTime;
  (void)taosThreadRwlockUnlock(&roleMgmt.rw);
}

bool mndNeedRetrieveRole(SUserObj *pUser) {
  bool result = false;
  if (taosHashGetSize(pUser->roles) > 0) {
    (void)taosThreadRwlockRdlock(&roleMgmt.rw);
    if (pUser->lastRoleRetrieve <= roleMgmt.lastUpd) result = true;
    (void)taosThreadRwlockUnlock(&roleMgmt.rw);
  }
  return result;
}

static int32_t mndFillSystemRoleTblPrivileges(SHashObj **ppHash) {
  int32_t code = 0, lino = 0;
  char    objKey[TSDB_PRIV_MAX_KEY_LEN] = {0};
  int32_t keyLen = privTblKey("1.*", "*", objKey, sizeof(objKey));

  if (!(*ppHash) &&
      !((*ppHash) = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
    TAOS_CHECK_EXIT(terrno);
  }

  TAOS_CHECK_EXIT(taosHashPut(*ppHash, objKey, keyLen + 1, NULL, 0));

_exit:
  TAOS_RETURN(code);
}

static int32_t mndFillSystemRolePrivileges(SMnode *pMnode, SRoleObj *pObj, uint32_t roleType) {
  int32_t       code = 0, lino = 0;
  SPrivInfoIter iter = {0};
  privInfoIterInit(&iter);

  char objKey[TSDB_PRIV_MAX_KEY_LEN] = {0};

  SPrivInfo *pPrivInfo = NULL;
  while (privInfoIterNext(&iter, &pPrivInfo)) {
    if ((pPrivInfo->sysType & roleType) == 0) continue;
    if (pPrivInfo->category == PRIV_CATEGORY_SYSTEM) {  // system privileges
      privAddType(&pObj->sysPrivs, pPrivInfo->privType);
    } else if (pPrivInfo->category == PRIV_CATEGORY_OBJECT) {  // object privileges
      switch (pPrivInfo->privType) {
        case PRIV_TBL_SELECT: {  // SELECT TABLE
          mndFillSystemRoleTblPrivileges(&pObj->selectTbs);
          break;
        }
        case PRIV_TBL_INSERT: {  // INSERT TABLE
          mndFillSystemRoleTblPrivileges(&pObj->insertTbs);
          break;
        }
        case PRIV_TBL_UPDATE: {  // UPDATE TABLE(reserved)
          mndFillSystemRoleTblPrivileges(&pObj->updateTbs);
          break;
        }
        case PRIV_TBL_DELETE: {  // DELETE TABLE:
          mndFillSystemRoleTblPrivileges(&pObj->deleteTbs);
          break;
        }
        default: {
          int32_t keyLen = privObjKey(pPrivInfo->objType, "1.*", "*", objKey, sizeof(objKey));
          if (!pObj->objPrivs && !(pObj->objPrivs = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                                                true, HASH_ENTRY_LOCK))) {
            TAOS_CHECK_EXIT(terrno);
          }
          SPrivObjPolicies *objPolicy = taosHashGet(pObj->objPrivs, objKey, keyLen + 1);
          if (objPolicy) {
            privAddType(&objPolicy->policy, pPrivInfo->privType);
          } else {
            SPrivObjPolicies policies = {0};
            privAddType(&policies.policy, pPrivInfo->privType);
            TAOS_CHECK_EXIT(taosHashPut(pObj->objPrivs, objKey, keyLen + 1, &policies, sizeof(policies)));
          }
          break;
        }
      }
    }
  }
_exit:
  if (code != 0) {
    mError("role, %s failed at line %d for %s since %s", __func__, lino, pObj->name, tstrerror(code));
  }
  TAOS_RETURN(code);
}

/**
 * system roles: SYSDBA/SYSSEC/SYSAUDIT/SYSINFO_0/SYSINFO_1
 */
static int32_t mndCreateDefaultRole(SMnode *pMnode, char *role, uint32_t roleType) {
  int32_t   code = 0, lino = 0;
  SRoleObj *pRole = NULL, *pNew = NULL;
  if (mndAcquireRole(pMnode, role, &pRole) == 0) {
    if (pRole->version < MND_ROLE_SYSROLE_VER) {
      mInfo("role:%s version:%ld upgrade to version:%d", role, pRole->version, MND_ROLE_SYSROLE_VER);
      pNew = taosMemoryCalloc(1, sizeof(SRoleObj));
      if (pNew == NULL) {
        mndReleaseRole(pMnode, pRole);
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }
      snprintf(pNew->name, TSDB_ROLE_LEN, "%s", pRole->name);
      pNew->createdTime = pRole->createdTime;
      pNew->uid = pRole->uid;
      pNew->flag = pRole->flag;
      pNew->updateTime = taosGetTimestampMs();
      pNew->version = MND_ROLE_SYSROLE_VER;
      mndReleaseRole(pMnode, pRole);
    } else {
      mndReleaseRole(pMnode, pRole);
      return 0;
    }
  } else {
    pNew = taosMemoryCalloc(1, sizeof(SRoleObj));
    if (pNew == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    tstrncpy(pNew->name, role, TSDB_ROLE_LEN);
    pNew->uid = mndGenerateUid(pNew->name, strlen(pNew->name));
    pNew->createdTime = taosGetTimestampMs();
    pNew->updateTime = pNew->createdTime;
    pNew->enable = 1;
    pNew->sys = 1;
    pNew->version = MND_ROLE_SYSROLE_VER;
  }

  TAOS_CHECK_EXIT(mndFillSystemRolePrivileges(pMnode, pNew, roleType));

  SSdbRaw *pRaw = mndRoleActionEncode(pNew);
  if (pRaw == NULL) goto _exit;
  TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));

  mInfo("role:%s, will be created when deploying, raw:%p", pNew->name, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-role");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("user:%s, failed to create since %s", pNew->name, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to create role:%s", pTrans->id, pNew->name);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }

_exit:
  if (pNew) taosMemoryFree(pNew);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndCreateDefaultRoles(SMnode *pMnode) {
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSDBA, ROLE_SYSDBA));
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSSEC, ROLE_SYSSEC));
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSAUDIT, ROLE_SYSAUDIT));
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSAUDIT_LOG, ROLE_SYSAUDIT_LOG));
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSINFO_0, ROLE_SYSINFO_0));
  TAOS_CHECK_EXIT(mndCreateDefaultRole(pMnode, TSDB_ROLE_SYSINFO_1, ROLE_SYSINFO_1));
_exit:
  TAOS_RETURN(code);
}

static int32_t tSerializeSRoleObj(void *buf, int32_t bufLen, SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->version));
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, pObj->flag));

  TAOS_CHECK_EXIT(tSerializePrivSysObjPolicies(&encoder, &pObj->sysPrivs, pObj->objPrivs));

  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->selectRows));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->insertRows));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->updateRows));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->deleteRows));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->selectTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->insertTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->updateTbs));
  TAOS_CHECK_EXIT(tSerializePrivTblPolicies(&encoder, pObj->deleteTbs));

  int32_t nParentUsers = taosHashGetSize(pObj->parentUsers);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nParentUsers));
  if (nParentUsers > 0) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(pObj->parentUsers, pIter)) {
      char *userName = taosHashGetKey(pIter, NULL);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, userName));
    }
  }
  int32_t nParentRoles = taosHashGetSize(pObj->parentRoles);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nParentRoles));
  if (nParentRoles > 0) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(pObj->parentRoles, pIter)) {
      char *roleName = taosHashGetKey(pIter, NULL);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, roleName));
    }
  }
  int32_t nSubRoles = taosHashGetSize(pObj->subRoles);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nSubRoles));
  if (nSubRoles > 0) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(pObj->subRoles, pIter)) {
      char *roleName = taosHashGetKey(pIter, NULL);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, roleName));
    }
  }

  tEndEncode(&encoder);
  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("role:%s, %s failed at line %d since %s", pObj->name, __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}


void tFreePrivTblPolicies(SHashObj **ppHash) {
  if (*ppHash) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(*ppHash, pIter)) {
      int32_t vlen = taosHashGetValueSize(pIter);  // value is NULL for key 1.db.* or 1.*.*
      if (vlen == 0) continue;
      privTblPoliciesFree((SPrivTblPolicies *)pIter);
    }
    taosHashCleanup(*ppHash);
    *ppHash = NULL;
  }
}

static int32_t tDeserializeSRoleObj(void *buf, int32_t bufLen, SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->version));
  TAOS_CHECK_EXIT(tDecodeU8(&decoder, &pObj->flag));

  TAOS_CHECK_EXIT(tDeserializePrivObjPolicies(&decoder, &pObj->sysPrivs, &pObj->objPrivs));

  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->selectRows));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->insertRows));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->updateRows));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->deleteRows));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->selectTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->insertTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->updateTbs));
  TAOS_CHECK_EXIT(tDeserializePrivTblPolicies(&decoder, &pObj->deleteTbs));

  char    userName[TSDB_USER_LEN] = {0};
  int32_t nParentUsers = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nParentUsers));
  if (nParentUsers > 0) {
    if (!(pObj->parentUsers =
              taosHashInit(nParentUsers, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nParentUsers; ++i) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, userName));
      TAOS_CHECK_EXIT(taosHashPut(pObj->parentUsers, userName, strlen(userName) + 1, NULL, 0));
    }
  }

  char    roleName[TSDB_ROLE_LEN] = {0};
  int32_t nParentRoles = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nParentRoles));
  if (nParentRoles > 0) {
    if (!(pObj->parentRoles =
              taosHashInit(nParentRoles, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nParentRoles; ++i) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, roleName));
      TAOS_CHECK_EXIT(taosHashPut(pObj->parentRoles, roleName, strlen(roleName) + 1, NULL, 0));
    }
  }
  int32_t nSubRoles = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nSubRoles));
  if (nSubRoles > 0) {
    if (!(pObj->subRoles =
              taosHashInit(nSubRoles, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nSubRoles; ++i) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, roleName));
      TAOS_CHECK_EXIT(taosHashPut(pObj->subRoles, roleName, strlen(roleName) + 1, NULL, 0));
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("role, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndRoleActionEncode(SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSRoleObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }
  int32_t size = sizeof(int32_t) + tlen;
  if (!(pRaw = sdbAllocRaw(SDB_ROLE, MND_ROLE_VER_NUMBER, size))) {
    TAOS_CHECK_EXIT(terrno);
  }
  if (!(buf = taosMemoryMalloc(tlen))) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((tlen = tSerializeSRoleObj(buf, tlen, pObj)) < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("role, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }
  mTrace("role, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0, lino = 0;
  SSdbRow  *pRow = NULL;
  SRoleObj *pObj = NULL;
  void     *buf = NULL;

  int8_t sver = 0;
  TAOS_CHECK_EXIT(sdbGetRawSoftVer(pRaw, &sver));

  if (sver != MND_ROLE_VER_NUMBER) {
    mError("role, read invalid ver, data ver: %d, curr ver: %d", sver, MND_ROLE_VER_NUMBER);
    TAOS_CHECK_EXIT(TSDB_CODE_SDB_INVALID_DATA_VER);
  }

  if (!(pRow = sdbAllocRow(sizeof(SRoleObj)))) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    TAOS_CHECK_EXIT(terrno);
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  if (!(buf = taosMemoryMalloc(tlen + 1))) {
    TAOS_CHECK_EXIT(terrno);
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  TAOS_CHECK_EXIT(tDeserializeSRoleObj(buf, tlen, pObj));
  if (pObj->version < MND_ROLE_SYSROLE_VER) {
  }
  taosInitRWLatch(&pObj->lock);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("role, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    mndRoleFreeObj(pObj);
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("role, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

void mndRoleFreeObj(SRoleObj *pObj) {
  if (pObj) {
    if (pObj->objPrivs) {
      taosHashCleanup(pObj->objPrivs);
      pObj->objPrivs = NULL;
    }
    tFreePrivTblPolicies(&pObj->selectRows);
    tFreePrivTblPolicies(&pObj->insertRows);
    tFreePrivTblPolicies(&pObj->updateRows);
    tFreePrivTblPolicies(&pObj->deleteRows);
    tFreePrivTblPolicies(&pObj->selectTbs);
    tFreePrivTblPolicies(&pObj->insertTbs);
    tFreePrivTblPolicies(&pObj->updateTbs);
    tFreePrivTblPolicies(&pObj->deleteTbs);
    if (pObj->parentUsers) {
      taosHashCleanup(pObj->parentUsers);
      pObj->parentUsers = NULL;
    }
    if (pObj->parentRoles) {
      taosHashCleanup(pObj->parentRoles);
      pObj->parentRoles = NULL;
    }
    if (pObj->subRoles) {
      taosHashCleanup(pObj->subRoles);
      pObj->subRoles = NULL;
    }
  }
}

static int32_t mndRoleActionInsert(SSdb *pSdb, SRoleObj *pObj) {
  mTrace("role:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndRoleActionDelete(SSdb *pSdb, SRoleObj *pObj) {
  mTrace("role:%s, perform delete action, row:%p", pObj->name, pObj);
  mndRoleFreeObj(pObj);
  return 0;
}

static int32_t mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew) {
  mTrace("role:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->sysPrivs = pNew->sysPrivs;
  pOld->version = pNew->version;
  pOld->flag = pNew->flag;
  TSWAP(pOld->objPrivs, pNew->objPrivs);
  TSWAP(pOld->selectRows, pNew->selectRows);
  TSWAP(pOld->insertRows, pNew->insertRows);
  TSWAP(pOld->updateRows, pNew->updateRows);
  TSWAP(pOld->deleteRows, pNew->deleteRows);
  TSWAP(pOld->selectTbs, pNew->selectTbs);
  TSWAP(pOld->insertTbs, pNew->insertTbs);
  TSWAP(pOld->updateTbs, pNew->updateTbs);
  TSWAP(pOld->deleteTbs, pNew->deleteTbs);
  TSWAP(pOld->parentUsers, pNew->parentUsers);
  TSWAP(pOld->parentRoles, pNew->parentRoles);
  TSWAP(pOld->subRoles, pNew->subRoles);
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

int32_t mndAcquireRole(SMnode *pMnode, const char *roleName, SRoleObj **ppRole) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppRole = sdbAcquire(pSdb, SDB_ROLE, roleName);
  if (*ppRole == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_ROLE_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_ROLE_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseRole(SMnode *pMnode, SRoleObj *pRole) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pRole);
}

static int32_t mndCreateRole(SMnode *pMnode, char *acct, SCreateRoleReq *pCreate, SRpcMsg *pReq) {
  int32_t  code = 0, lino = 0;
  SRoleObj obj = {0};

  tstrncpy(obj.name, pCreate->name, TSDB_ROLE_LEN);
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime = obj.createdTime;
  obj.uid = mndGenerateUid(obj.name, strlen(obj.name));
  obj.version = 1;
  obj.enable = 1;
  obj.sys = 0;
  // TODO: assign default privileges

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "create-role");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to create role:%s", pTrans->id, pCreate->name);

  SSdbRaw *pCommitRaw = mndRoleActionEncode(&obj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed at line %d to create role, since %s", obj.name, lino, tstrerror(code));
  }
  mndRoleFreeObj(&obj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateRoleReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0, lino = 0;
  SRoleObj      *pRole = NULL;
  SUserObj      *pOperUser = NULL;
  SUserObj      *pUser = NULL;
  SCreateRoleReq createReq = {0};

  if (tDeserializeSCreateRoleReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  if ((code = mndAcquireRole(pMnode, createReq.name, &pRole)) == 0) {
    if (createReq.ignoreExists) {
      mInfo("role:%s, already exist, ignore exist is set", createReq.name);
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_ALREADY_EXIST);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_ROLE_NOT_EXIST) {
      // continue
    } else {
      goto _exit;
    }
  }
  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_NO_USER_FROM_CONN);
  }

  mInfo("role:%s, start to create by %s", createReq.name, pOperUser->user);

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_ROLE));

  if (createReq.name[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_INVALID_FORMAT);
  }
  code = mndAcquireUser(pMnode, createReq.name, &pUser);
  if (pUser != NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_ALREADY_EXIST);
  }
  code = mndCreateRole(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char    detail[128] = {0};
  int32_t len = snprintf(detail, sizeof(detail), "operUser:%s", pOperUser->user);
  auditRecord(pReq, pMnode->clusterId, "createRole", "", createReq.name, detail, len);
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to create at line %d since %s", createReq.name, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  mndReleaseRole(pMnode, pRole);
  tFreeSCreateRoleReq(&createReq);
  TAOS_RETURN(code);
}

int32_t mndRoleDropParentUser(SMnode *pMnode, STrans *pTrans, SUserObj *pObj) {
  int32_t   code = 0, lino = 0;
  SSdb     *pSdb = pMnode->pSdb;
  SRoleObj *pRole = NULL;
  void     *pIter = NULL;

  while ((pIter = taosHashIterate(pObj->roles, pIter))) {
    if ((code = mndAcquireRole(pMnode, (const char *)pIter, &pRole))) {
      TAOS_CHECK_EXIT(code);
    }
    SRoleObj newRole = {0};
    TAOS_CHECK_EXIT(mndRoleDupObj(pRole, &newRole));
    code = taosHashRemove(newRole.parentUsers, pObj->user, strlen(pObj->user) + 1);
    if (code == TSDB_CODE_NOT_FOUND) {
      mndRoleFreeObj(&newRole);
      continue;
    }
    if (code != 0) {
      mndRoleFreeObj(&newRole);
      TAOS_CHECK_EXIT(code);
    }
    SSdbRaw *pCommitRaw = mndRoleActionEncode(&newRole);
    if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
      mndRoleFreeObj(&newRole);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY))) {
      mndRoleFreeObj(&newRole);
      TAOS_CHECK_EXIT(code);
    }
    mndReleaseRole(pMnode, pRole);
    pRole = NULL;
    mndRoleFreeObj(&newRole);
  }
_exit:
  if (pIter) taosHashCancelIterate(pObj->roles, pIter);
  if (pRole) mndReleaseRole(pMnode, pRole);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndRoleGrantToUser(SMnode *pMnode, STrans *pTrans, SRoleObj *pRole, SUserObj *pUser) {
  int32_t  code = 0, lino = 0;
  SRoleObj newRole = {0};
  TAOS_CHECK_EXIT(mndRoleDupObj(pRole, &newRole));
  TAOS_CHECK_EXIT(taosHashPut(newRole.parentUsers, pUser->user, strlen(pUser->user) + 1, NULL, 0));
  SSdbRaw *pCommitRaw = mndRoleActionEncode(&newRole);
  if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
_exit:
  if (code < 0) {
    mError("role:%s, failed at line %d to grant to user:%s since %s", pRole->name, lino, pUser->user, tstrerror(code));
  }
  mndRoleFreeObj(&newRole);
  TAOS_RETURN(code);
}

static int32_t mndDropParentRole(SMnode *pMnode, STrans *pTrans, SRoleObj *pObj) {  // TODO
  return 0;
}

static int32_t mndDropRole(SMnode *pMnode, SRpcMsg *pReq, SRoleObj *pObj) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "drop-role");
  if (pTrans == NULL) {
    mError("role:%s, failed to drop since %s", pObj->name, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to drop role:%s", pTrans->id, pObj->name);

  SSdbRaw *pCommitRaw = mndRoleActionEncode(pObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_CHECK_EXIT(mndDropParentRole(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndUserDropRole(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
  mndSetRoleLastUpd(taosGetTimestampMs());
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropRoleReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = 0, lino = 0;
  SRoleObj    *pObj = NULL;
  SDropRoleReq dropReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSDropRoleReq(pReq->pCont, pReq->contLen, &dropReq));

  mInfo("role:%s, start to drop", dropReq.name);
  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_ROLE));

  if (dropReq.name[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_INVALID_FORMAT);
  }

  TAOS_CHECK_EXIT(mndAcquireRole(pMnode, dropReq.name, &pObj));

  TAOS_CHECK_EXIT(mndDropRole(pMnode, pReq, pObj));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "dropRole", "", dropReq.name, dropReq.sql, dropReq.sqlLen);
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to drop at line %d since %s", dropReq.name, lino, tstrerror(code));
  }
  mndReleaseRole(pMnode, pObj);
  tFreeSDropRoleReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndAlterRole(SMnode *pMnode, SRpcMsg *pReq, SRoleObj *pObj) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "alter-role");
  if (pTrans == NULL) {
    mError("role:%s, failed to alter since %s", pObj->name, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to alter role:%s", pTrans->id, pObj->name);

  SSdbRaw *pCommitRaw = mndRoleActionEncode(pObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to alter at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

int32_t mndRoleDupObj(SRoleObj *pOld, SRoleObj *pNew) {
  int32_t code = 0, lino = 0;
  snprintf(pNew->name, TSDB_ROLE_LEN, "%s", pOld->name);
  pNew->createdTime = pOld->createdTime;
  pNew->uid = pOld->uid;
  pNew->version = pOld->version + 1;
  pNew->flag = pOld->flag;
  pNew->updateTime = taosGetTimestampMs();
  pNew->sysPrivs = pOld->sysPrivs;

  taosRLockLatch(&pOld->lock);
  TAOS_CHECK_EXIT(mndDupPrivObjHash(pOld->objPrivs, &pNew->objPrivs));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->selectRows, &pNew->selectRows));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->insertRows, &pNew->insertRows));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->updateRows, &pNew->updateRows));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->deleteRows, &pNew->deleteRows));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->selectTbs, &pNew->selectTbs));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->insertTbs, &pNew->insertTbs));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->updateTbs, &pNew->updateTbs));
  TAOS_CHECK_EXIT(mndDupPrivTblHash(pOld->deleteTbs, &pNew->deleteTbs));
  // TODO: alterTbs?
  TAOS_CHECK_EXIT(mndDupRoleHash(pOld->parentUsers, &pNew->parentUsers));
  TAOS_CHECK_EXIT(mndDupRoleHash(pOld->parentRoles, &pNew->parentRoles));
  TAOS_CHECK_EXIT(mndDupRoleHash(pOld->subRoles, &pNew->subRoles));
_exit:
  taosRUnLockLatch(&pOld->lock);
  if (code < 0) {
    mError("role:%s, failed at line %d to dup obj since %s", pOld->name, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static bool mndIsRoleChanged(SRoleObj *pOld, SAlterRoleReq *pAlterReq) {
  switch (pAlterReq->alterType) {
    case TSDB_ALTER_ROLE_LOCK: {
      if ((pAlterReq->lock && !pOld->enable) || (!pAlterReq->lock && pOld->enable)) {
        return false;
      }
      break;
    }
    default:
      break;
  }
  return true;
}

#ifdef TD_ENTERPRISE
extern int32_t mndAlterRoleInfo(SRoleObj *pOld, SRoleObj *pNew, SAlterRoleReq *pAlterReq);
#endif

static int32_t mndProcessAlterRoleReq(SRpcMsg *pReq) {
  int32_t       code = 0, lino = 0;
  SMnode       *pMnode = pReq->info.node;
  SRoleObj     *pObj = NULL;
  SRoleObj      newObj = {0};
  SAlterRoleReq alterReq = {0};
  bool          alterUser = false;

  TAOS_CHECK_EXIT(tDeserializeSAlterRoleReq(pReq->pCont, pReq->contLen, &alterReq));

  mInfo("role:%s, start to alter, flag:%u", alterReq.principal, alterReq.flag);
  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_ALTER_ROLE));

  if (alterReq.principal[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_INVALID_FORMAT);
  }

  if (mndAcquireRole(pMnode, alterReq.principal, &pObj) == 0) {
    if (alterReq.alterType == TSDB_ALTER_ROLE_ROLE) {
      TAOS_CHECK_EXIT(TSDB_CODE_OPS_NOT_SUPPORT);  // not support grant role to role yet
    }
  } else {
    if (alterReq.alterType == TSDB_ALTER_ROLE_LOCK) {
      TAOS_CHECK_EXIT(terrno);
    }
    alterUser = true;
  }

  if (alterUser) {
    mInfo("role:%s, not exist, will alter user instead", alterReq.principal);
    TAOS_CHECK_EXIT(mndAlterUserFromRole(pReq, &alterReq));
  } else if (mndIsRoleChanged(pObj, &alterReq)) {
    TAOS_CHECK_EXIT(mndRoleDupObj(pObj, &newObj));
#ifdef TD_ENTERPRISE
    TAOS_CHECK_EXIT(mndAlterRoleInfo(pObj, &newObj, &alterReq));
#endif
    TAOS_CHECK_EXIT(mndAlterRole(pMnode, pReq, &newObj));
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
    mndSetRoleLastUpd(taosGetTimestampMs());
  }
  auditRecord(pReq, pMnode->clusterId, "alterRole", "", alterReq.principal, alterReq.sql, alterReq.sqlLen);
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to alter at line %d since %s", alterReq.principal, lino, tstrerror(code));
  }
  mndReleaseRole(pMnode, pObj);
  mndRoleFreeObj(&newObj);
  tFreeSAlterRoleReq(&alterReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessGetRoleAuthReq(SRpcMsg *pReq) { TAOS_RETURN(0); }

static int32_t mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0, lino = 0;
  int32_t   numOfRows = 0;
  SRoleObj *pObj = NULL;
  int32_t   cols = 0;
  int32_t   bufSize = TSDB_MAX_SUBROLE * TSDB_ROLE_LEN + VARSTR_HEADER_SIZE;
  char      tBuf[TSDB_MAX_SUBROLE * TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ROLE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pObj->name, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pObj, pShow->pIter, _exit);

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      int8_t enable = pObj->enable ? 1 : 0;
      COL_DATA_SET_VAL_GOTO((const char *)&enable, false, pObj, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      COL_DATA_SET_VAL_GOTO((const char *)&pObj->createdTime, false, pObj, pShow->pIter, _exit);
    }
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      COL_DATA_SET_VAL_GOTO((const char *)&pObj->updateTime, false, pObj, pShow->pIter, _exit);
    }
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      char *roleType = pObj->sys ? "SYSTEM" : "USER";
      STR_WITH_MAXSIZE_TO_VARSTR(tBuf, roleType, pShow->pMeta->pSchemas[cols].bytes);
      COL_DATA_SET_VAL_GOTO((const char *)tBuf, false, pObj, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      void  *pIter = NULL;
      size_t klen = 0, tlen = 0;
      char  *pBuf = POINTER_SHIFT(tBuf, VARSTR_HEADER_SIZE);
      while (pIter = taosHashIterate(pObj->subRoles, pIter)) {
        char *roleName = taosHashGetKey(pIter, &klen);
        tlen += snprintf(pBuf + tlen, bufSize - tlen, "%s,", roleName);
      }
      if (tlen > 0) {
        pBuf[tlen - 1] = 0;  // remove last ','
      } else {
        pBuf[0] = 0;
      }
      varDataSetLen(tBuf, tlen);
      COL_DATA_SET_VAL_GOTO((const char *)tBuf, false, pObj, pShow->pIter, _exit);
    }
    numOfRows++;
    sdbRelease(pSdb, pObj);
  }
  pShow->numOfRows += numOfRows;
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextRole(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0, lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  SRoleObj *pObj = NULL;
  char     *pBuf = NULL, *qBuf = NULL;
  char     *sql = NULL;
  char      roleName[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t   bufSize = TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE;

  bool fetchNextInstance = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextInstance) {
      pShow->pIter = sdbFetch(pSdb, SDB_ROLE, pShow->pIter, (void **)&pObj);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextInstance = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      if (!(pObj = sdbAcquire(pSdb, SDB_ROLE, pKey))) {
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
      // skip db, table, condition, notes, row_span, columns
      COL_DATA_SET_EMPTY_VARCHAR(pBuf, 6);
      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        STR_WITH_MAXSIZE_TO_VARSTR(pBuf, "SYS", pShow->pMeta->pSchemas[cols].bytes);
        COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
      }
      COL_DATA_SET_EMPTY_VARCHAR(pBuf, 1);
      numOfRows++;
    }

    // object privileges
    void *pIter = NULL;
    while ((pIter = taosHashIterate(pObj->objPrivs, pIter))) {
      SPrivObjPolicies *pPolices = (SPrivObjPolicies *)pIter;

      char   *key = taosHashGetKey(pPolices, NULL);
      int32_t objType = PRIV_OBJ_UNKNOWN;
      char    dbName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char    tblName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      privObjKeyParse(key, &objType, dbName, sizeof(dbName), tblName, sizeof(tblName));

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
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, dbName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, tblName, pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        // skip condition, notes, row_span, columns
        COL_DATA_SET_EMPTY_VARCHAR(pBuf, 4);

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, "OBJ", pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }

        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, privObjTypeName(objType), pShow->pMeta->pSchemas[cols].bytes);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }
        numOfRows++;
      }
    }
    // row level privileges

    // table level privileges
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
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}

static int32_t mndRetrieveColPrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0, lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  SRoleObj *pObj = NULL;
  char     *pBuf = NULL, *qBuf = NULL;
  char     *sql = NULL;
  char      roleName[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t   bufSize = TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE;

  bool fetchNextInstance = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextInstance) {
      pShow->pIter = sdbFetch(pSdb, SDB_ROLE, pShow->pIter, (void **)&pObj);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextInstance = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      if (!(pObj = sdbAcquire(pSdb, SDB_ROLE, pKey))) {
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

      for (int32_t i = 0; i < 6; i++) {  // skip db, table, condition, notes, row_span, columns
        if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
          STR_WITH_MAXSIZE_TO_VARSTR(pBuf, "", 2);
          COL_DATA_SET_VAL_GOTO((const char *)pBuf, false, pObj, pShow->pIter, _exit);
        }
      }
      numOfRows++;
    }

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

static void mndCancelGetNextColPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}

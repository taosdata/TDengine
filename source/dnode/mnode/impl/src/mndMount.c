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
#ifdef USE_MOUNT
#define _DEFAULT_SOURCE
#include "audit.h"
#include "command.h"
#include "mndArbGroup.h"
#include "mndCluster.h"
#include "mndConfig.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndIndex.h"
#include "mndIndexComm.h"
#include "mndMnode.h"
#include "mndMount.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "mndView.h"
#include "systable.h"
#include "thttp.h"
#include "tjson.h"

#define MND_MOUNT_VER_NUMBER 1

static int32_t  mndMountActionInsert(SSdb *pSdb, SMountObj *pObj);
static int32_t  mndMountActionDelete(SSdb *pSdb, SMountObj *pObj);
static int32_t  mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew);
static int32_t  mndNewMountActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw);

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq);
static int32_t mndProcessDropMountReq(SRpcMsg *pReq);
static int32_t mndProcessExecuteMountReq(SRpcMsg *pReq);
static int32_t mndProcessRetrieveMountPathRsp(SRpcMsg *pRsp);
static int32_t mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity);
static void    mndCancelGetNextMount(SMnode *pMnode, void *pIter);

int32_t mndInitMount(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_MOUNT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndMountActionEncode,
      .decodeFp = (SdbDecodeFp)mndMountActionDecode,
      .insertFp = (SdbInsertFp)mndMountActionInsert,
      .updateFp = (SdbUpdateFp)mndMountActionUpdate,
      .deleteFp = (SdbDeleteFp)mndMountActionDelete,
      .validateFp = (SdbValidateFp)mndNewMountActionValidate,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_MOUNT, mndProcessCreateMountReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MOUNT, mndProcessDropMountReq);
  mndSetMsgHandle(pMnode, TDMT_MND_EXECUTE_MOUNT, mndProcessExecuteMountReq);
  mndSetMsgHandle(pMnode, TDMT_DND_RETRIEVE_MOUNT_PATH_RSP, mndProcessRetrieveMountPathRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_MOUNT_VNODE_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndRetrieveMounts);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndCancelGetNextMount);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMount(SMnode *pMnode) {}

void mndMountFreeObj(SMountObj *pObj) {
  if (pObj) {
    taosMemoryFreeClear(pObj->dnodeIds);
    taosMemoryFreeClear(pObj->dbObj);
    if (pObj->paths) {
      for (int32_t i = 0; i < pObj->nMounts; ++i) {
        taosMemoryFreeClear(pObj->paths[i]);
      }
      taosMemoryFreeClear(pObj->paths);
    }
  }
}

void mndMountDestroyObj(SMountObj *pObj) {
  if (pObj) {
    mndMountFreeObj(pObj);
    taosMemoryFree(pObj);
  }
}

static int32_t tSerializeSMountObj(void *buf, int32_t bufLen, const SMountObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->acct));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->createUser));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));
  TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->nMounts));
  for (int16_t i = 0; i < pObj->nMounts; ++i) {
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->dnodeIds[i]));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->paths[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->nDbs));
  for (int16_t i = 0; i < pObj->nDbs; ++i) {
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->dbObj[i].uid));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbObj[i].name));
  }
  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("mount, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeSMountObj(void *buf, int32_t bufLen, SMountObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->acct));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->createUser));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &pObj->nMounts));
  if (pObj->nMounts > 0) {
    if (!(pObj->dnodeIds = taosMemoryMalloc(sizeof(int32_t) * pObj->nMounts))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if (!(pObj->paths = taosMemoryMalloc(sizeof(char *) * pObj->nMounts))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int16_t i = 0; i < pObj->nMounts; ++i) {
      TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->dnodeIds[i]));
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pObj->paths[i]));
    }
  }
  TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &pObj->nDbs));
  if (pObj->nDbs > 0) {
    if (!(pObj->dbObj = taosMemoryMalloc(sizeof(SMountDbObj) * pObj->nDbs))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int16_t i = 0; i < pObj->nDbs; ++i) {
      TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->dbObj[i].uid));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbObj[i].name));
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mndMountDestroyObj(pObj);
    mError("mount, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndMountActionEncode(SMountObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSMountObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_MOUNT, MND_MOUNT_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  tlen = tSerializeSMountObj(buf, tlen, pObj);
  if (tlen < 0) {
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
    mError("mount, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("mount, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

SSdbRow *mndMountActionDecode(SSdbRaw *pRaw) {
  int32_t    code = 0, lino = 0;
  SSdbRow   *pRow = NULL;
  SMountObj *pObj = NULL;
  void      *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_MOUNT_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("mount read invalid ver, data ver: %d, curr ver: %d", sver, MND_MOUNT_VER_NUMBER);
    goto _exit;
  }

  if (!(pRow = sdbAllocRow(sizeof(SMountObj)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSMountObj(buf, tlen, pObj) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosInitRWLatch(&pObj->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("mount, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("mount, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

static int32_t mndNewMountActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw) {
  mTrace("mount, validate new mount action, raw:%p", pRaw);
  return 0;
}

static int32_t mndMountActionInsert(SSdb *pSdb, SMountObj *pObj) {
  mTrace("mount:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndMountActionDelete(SSdb *pSdb, SMountObj *pObj) {
  mTrace("mount:%s, perform delete action, row:%p", pObj->name, pObj);
  mndMountFreeObj(pObj);
  return 0;
}

static int32_t mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew) {
  mTrace("mount:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SMountObj *mndAcquireMount(SMnode *pMnode, const char *mountName) {
  SSdb      *pSdb = pMnode->pSdb;
  SMountObj *pObj = sdbAcquire(pSdb, SDB_MOUNT, mountName);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("mount:%s, failed to acquire mount since %s", mountName, terrstr());
    }
  }
  return pObj;
}

void mndReleaseMount(SMnode *pMnode, SMountObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

bool mndMountIsExist(SMnode *pMnode, const char *mountName) {
  SMountObj *pObj = mndAcquireMount(pMnode, mountName);
  if (pObj == NULL) {
    return false;
  }
  mndReleaseMount(pMnode, pObj);
  return true;
}

void *mndBuildRetrieveMountPathReq(SMnode *pMnode, SRpcMsg *pMsg, const char *mountName, const char *mountPath,
                                   int32_t dnodeId, int32_t *pContLen) {
  int32_t code = 0, lino = 0;
  void   *pBuf = NULL;

  SRetrieveMountPathReq req = {0};
  req.dnodeId = dnodeId;
  req.pVal = &pMsg->info;
  req.valLen = sizeof(pMsg->info);
  TAOS_UNUSED(snprintf(req.mountName, TSDB_MOUNT_NAME_LEN, "%s", mountName));
  TAOS_UNUSED(snprintf(req.mountPath, TSDB_MOUNT_PATH_LEN, "%s", mountPath));

  int32_t contLen = tSerializeSRetrieveMountPathReq(NULL, 0, &req);
  TAOS_CHECK_EXIT(contLen);
  TSDB_CHECK_NULL((pBuf = rpcMallocCont(contLen)), code, lino, _exit, terrno);
  TAOS_CHECK_EXIT(tSerializeSRetrieveMountPathReq(pBuf, contLen, &req));
_exit:
  if (code < 0) {
    rpcFreeCont(pBuf);
    terrno = code;
    return NULL;
  }
  *pContLen = contLen;
  return pBuf;
}

#if 0
static int32_t mndSetCreateMountUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t code = 0;
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, false));
    }
  }

  TAOS_RETURN(code);
}
#endif


#ifndef TD_ENTERPRISE
int32_t mndCreateMount(SMnode *pMnode, SRpcMsg *pReq, SMountInfo *pInfo, SUserObj *pUser) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}
#endif

static int32_t mndRetrieveMountInfo(SMnode *pMnode, SRpcMsg *pMsg, SCreateMountReq *pReq) {
  int32_t    code = 0, lino = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pReq->dnodeIds[0]);
  if (pDnode == NULL) TAOS_RETURN(terrno);
  if (pDnode->offlineReason != DND_REASON_ONLINE) {
    mndReleaseDnode(pMnode, pDnode);
    TAOS_RETURN(TSDB_CODE_DNODE_OFFLINE);
  }
  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t bufLen = 0;
  void   *pBuf =
      mndBuildRetrieveMountPathReq(pMnode, pMsg, pReq->mountName, pReq->mountPaths[0], pReq->dnodeIds[0], &bufLen);
  if (pBuf == NULL) TAOS_RETURN(terrno);

  SRpcMsg rpcMsg = {.msgType = TDMT_DND_RETRIEVE_MOUNT_PATH, .pCont = pBuf, .contLen = bufLen};
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));

  pMsg->info.handle = NULL;  // disable auto rsp to client
_exit:
  TAOS_RETURN(code);
}

static int32_t mndProcessRetrieveMountPathRsp(SRpcMsg *pRsp) {
  int32_t    code = 0, lino = 0;
  int32_t    rspCode = 0;
  SMnode    *pMnode = pRsp->info.node;
  SMountInfo mntInfo = {0};
  SDecoder   decoder = {0};
  void      *pBuf = NULL;
  int32_t    bufLen = 0;
  bool       rspToClient = false;

  // step 1: decode and preprocess in mnode read thread
  tDecoderInit(&decoder, pRsp->pCont, pRsp->contLen);
  TAOS_CHECK_EXIT(tDeserializeSMountInfo(&decoder, &mntInfo, false));
  const STraceId *trace = &pRsp->info.traceId;
  SRpcMsg         rsp = {
      // .code = pRsp->code,
      // .pCont = pRsp->info.rsp,
      // .contLen = pRsp->info.rspLen,
              .info = *(SRpcHandleInfo *)mntInfo.pVal,
  };
  rspToClient = true;
  if (pRsp->code != 0) {
    TAOS_CHECK_EXIT(pRsp->code);
  }

  // wait for all retrieve response received
  // TODO: ...
  // make sure the clusterId from all rsp is the same, but not with the clusterId of the host cluster
  if (mntInfo.clusterId == pMnode->clusterId) {
    mError("mount:%s, clusterId:%" PRIi64 " from dnode is identical to the host cluster's id:%" PRIi64,
           mntInfo.mountName, mntInfo.clusterId, pMnode->clusterId);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_DUP_CLUSTER_EXIST);
  }

  // step 2: collect the responses from dnodes, process and push to mnode write thread to run as transaction
  // TODO: multiple retrieve dnodes and paths supported later
  TSDB_CHECK_CONDITION((bufLen = tSerializeSMountInfo(NULL, 0, &mntInfo)) >= 0, code, lino, _exit, bufLen);
  TSDB_CHECK_CONDITION((pBuf = rpcMallocCont(bufLen)), code, lino, _exit, terrno);
  TSDB_CHECK_CONDITION((bufLen = tSerializeSMountInfo(pBuf, bufLen, &mntInfo)) >= 0, code, lino, _exit, bufLen);
  SRpcMsg rpcMsg = {.pCont = pBuf, .contLen = bufLen, .msgType = TDMT_MND_EXECUTE_MOUNT, .info.noResp = 1};
  SEpSet  mnodeEpset = {0};
  mndGetMnodeEpSet(pMnode, &mnodeEpset);

  SMountObj *pObj = NULL;
  if ((pObj = mndAcquireMount(pMnode, mntInfo.mountName))) {
    mndReleaseMount(pMnode, pObj);
    if (mntInfo.ignoreExist) {
      mInfo("mount:%s, already exist, ignore exist is set", mntInfo.mountName);
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_ALREADY_EXIST);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_MOUNT_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_MOUNT_IN_CREATING | TSDB_CODE_MND_MOUNT_IN_DROPPING | TSDB_CODE_APP_ERROR
      TAOS_CHECK_EXIT(code);
    }
  }
  TAOS_CHECK_EXIT(tmsgSendReq(&mnodeEpset, &rpcMsg));
_exit:
  if (code == 0) {
    mGInfo("mount:%s, msg:%p, retrieve mount path rsp with code:%d", mntInfo.mountName, pRsp, pRsp->code);
  } else {
    mError("mount:%s, msg:%p, failed at line %d to retrieve mount path rsp since %s", mntInfo.mountName, pRsp, lino,
           tstrerror(code));
    if (rspToClient) {
      rsp.code = code;
      tmsgSendRsp(&rsp);
    }
  }
  tDecoderClear(&decoder);
  tFreeMountInfo(&mntInfo, false);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq) {
  int32_t         code = 0, lino = 0;
  SMnode         *pMnode = pReq->info.node;
  SDbObj         *pDb = NULL;
  SMountObj      *pObj = NULL;
  SUserObj       *pUser = NULL;
  SCreateMountReq createReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSCreateMountReq(pReq->pCont, pReq->contLen, &createReq));
  mInfo("mount:%s, start to create on dnode %d from %s", createReq.mountName, *createReq.dnodeIds,
        createReq.mountPaths[0]);  // TODO: mutiple mounts

  if ((pObj = mndAcquireMount(pMnode, createReq.mountName))) {
    if (createReq.ignoreExist) {
      mInfo("mount:%s, already exist, ignore exist is set", createReq.mountName);
      code = 0;
      goto _exit;
    } else {
      code = TSDB_CODE_MND_MOUNT_ALREADY_EXIST;
      goto _exit;
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_MOUNT_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_MOUNT_IN_CREATING | TSDB_CODE_MND_MOUNT_IN_DROPPING | TSDB_CODE_APP_ERROR
      goto _exit;
    }
  }
  // mount operation share the privileges of db
  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_MOUNT, (SDbObj *)pObj));
  TAOS_CHECK_EXIT(grantCheck(TSDB_GRANT_MOUNT));
  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));
  char fullMountName[TSDB_MOUNT_NAME_LEN + 32] = {0};
  (void)snprintf(fullMountName, sizeof(fullMountName), "%d.%s", pUser->acctId, createReq.mountName);
  if ((pDb = mndAcquireDb(pMnode, fullMountName))) {
    mndReleaseDb(pMnode, pDb);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_DUP_DB_NAME_EXIST);
  }

  TAOS_CHECK_EXIT(mndRetrieveMountInfo(pMnode, pReq, &createReq));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "createMount", createReq.mountName, "", createReq.sql, createReq.sqlLen);

_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, dnode:%d, path:%s, failed to create at line:%d since %s", createReq.mountName,
           createReq.dnodeIds ? createReq.dnodeIds[0] : 0, createReq.mountPaths ? createReq.mountPaths[0] : "", lino,
           tstrerror(code));  // TODO: mutiple mounts
  }

  mndReleaseMount(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);
  tFreeSCreateMountReq(&createReq);

  TAOS_RETURN(code);
}

static int32_t mndProcessExecuteMountReq(SRpcMsg *pReq) {
  int32_t    code = 0, lino = 0;
  SMnode    *pMnode = pReq->info.node;
  SDbObj    *pDb = NULL;
  SMountObj *pObj = NULL;
  SUserObj  *pUser = NULL;
  SMountInfo mntInfo = {0};
  SDecoder   decoder = {0};
  SRpcMsg    rsp = {0};
  bool       rspToClient = false;

  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  TAOS_CHECK_EXIT(tDeserializeSMountInfo(&decoder, &mntInfo, true));
  rspToClient = true;
  mInfo("mount:%s, start to execute on mnode", mntInfo.mountName);

  if ((pDb = mndAcquireDb(pMnode, mntInfo.mountName))) {
    mndReleaseDb(pMnode, pDb);
    TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_DUP_DB_NAME_EXIST);
  }

  if ((pObj = mndAcquireMount(pMnode, mntInfo.mountName))) {
    if (mntInfo.ignoreExist) {
      mInfo("mount:%s, already exist, ignore exist is set", mntInfo.mountName);
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_MOUNT_ALREADY_EXIST);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_MOUNT_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_MOUNT_IN_CREATING | TSDB_CODE_MND_MOUNT_IN_DROPPING | TSDB_CODE_APP_ERROR
      TAOS_CHECK_EXIT(code);
    }
  }
  // mount operation share the privileges of db
  TAOS_CHECK_EXIT(grantCheck(TSDB_GRANT_MOUNT));  // TODO: implement when the plan is ready
  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));

  TAOS_CHECK_EXIT(mndCreateMount(pMnode, pReq, &mntInfo, pUser));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    // TODO: mutiple path mount
    rsp.code = code;
    mError("mount:%s, dnode:%d, path:%s, failed to create at line:%d since %s", mntInfo.mountName, mntInfo.dnodeId,
           mntInfo.mountPath, lino, tstrerror(code));
  }
  if (rspToClient) {
    rsp.info = *(SRpcHandleInfo *)mntInfo.pVal, tmsgSendRsp(&rsp);
    tmsgSendRsp(&rsp);
  }
  mndReleaseMount(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);
  tDecoderClear(&decoder);
  tFreeMountInfo(&mntInfo, true);

  TAOS_RETURN(code);
}

int32_t mndBuildDropMountRsp(SMountObj *pObj, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc) {
  int32_t       code = 0;
  SDropMountRsp dropRsp = {0};
  if (pObj != NULL) {
    (void)memcpy(dropRsp.name, pObj->name, TSDB_MOUNT_NAME_LEN);
    dropRsp.uid = pObj->uid;
  }

  int32_t rspLen = tSerializeSDropMountRsp(NULL, 0, &dropRsp);
  void   *pRsp = NULL;
  if (useRpcMalloc) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryMalloc(rspLen);
  }

  if (pRsp == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  int32_t ret = 0;
  if ((ret = tSerializeSDropMountRsp(pRsp, rspLen, &dropRsp)) < 0) return ret;
  *pRspLen = rspLen;
  *ppRsp = pRsp;
  TAOS_RETURN(code);
}

bool mndHasMountOnDnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SMountObj *pMount = NULL;
    pIter = sdbFetch(pSdb, SDB_MOUNT, pIter, (void **)&pMount);
    if (pIter == NULL) break;

    for (int32_t i = 0; i < pMount->nMounts; ++i) {
      if (pMount->dnodeIds[i] == dnodeId) {
        sdbRelease(pSdb, pMount);
        return true;
      }
    }
    sdbRelease(pSdb, pMount);
  }
  return false;
}

#ifndef TD_ENTERPRISE
int32_t mndDropMount(SMnode *pMnode, SRpcMsg *pReq, SMountObj *pObj) { return TSDB_CODE_OPS_NOT_SUPPORT; }
#endif

static int32_t mndProcessDropMountReq(SRpcMsg *pReq) {
  fprintf(stderr, "mndProcessDropMountReq\n");
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SMountObj    *pObj = NULL;
  SDropMountReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropMountReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _exit);

  mInfo("mount:%s, start to drop", dropReq.mountName);

  pObj = mndAcquireMount(pMnode, dropReq.mountName);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    if (dropReq.ignoreNotExists) {
      code = mndBuildDropMountRsp(pObj, &pReq->info.rspLen, &pReq->info.rsp, true);
    }
    goto _exit;
  }

  // mount operation share the privileges of db
  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MOUNT, (SDbObj *)pObj), NULL, _exit);

  code = mndDropMount(pMnode, pReq, pObj);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  // SName name = {0};
  // if (tNameFromString(&name, dropReq.mountName, T_NAME_ACCT | T_NAME_DB) < 0)
  //   mError("mount:%s, failed to parse db name", dropReq.mountName);

  auditRecord(pReq, pMnode->clusterId, "dropMount", dropReq.mountName, "", dropReq.sql, dropReq.sqlLen);

_exit:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, failed to drop since %s", dropReq.mountName, tstrerror(code));
  }

  mndReleaseMount(pMnode, pObj);
  tFreeSDropMountReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = 0, lino = 0;
  int32_t          numOfRows = 0;
  int32_t          cols = 0;
  char             tmp[512];
  int32_t          tmpLen = 0;
  int32_t          bufLen = 0;
  char            *pBuf = NULL;
  char            *qBuf = NULL;
  void            *pIter = NULL;
  SSdb            *pSdb = pMnode->pSdb;
  SColumnInfoData *pColInfo = NULL;

  pBuf = tmp;
  bufLen = sizeof(tmp) - VARSTR_HEADER_SIZE;
  if (pShow->numOfRows < 1) {
    SMountObj *pObj = NULL;
    int32_t    index = 0;
    while ((pIter = sdbFetch(pSdb, SDB_MOUNT, pIter, (void **)&pObj))) {
      cols = 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
      TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", pObj->name));
      varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
      COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        // TAOS_UNUSED(snprintf(pBuf, bufLen, "%d", *(int32_t *)pObj->dnodeIds));  // TODO: support mutiple dnodes
        COL_DATA_SET_VAL_GOTO((const char *)&pObj->dnodeIds[0], false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        // TAOS_UNUSED(snprintf(pBuf, bufLen, "%" PRIi64, pObj->createdTime));
        COL_DATA_SET_VAL_GOTO((const char *)&pObj->createdTime, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", pObj->paths[0]));  // TODO: support mutiple paths
        varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      sdbRelease(pSdb, pObj);
      ++numOfRows;
    }
  }

  pShow->numOfRows += numOfRows;

_exit:
  if (code < 0) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextMount(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_MOUNT);
}

#endif
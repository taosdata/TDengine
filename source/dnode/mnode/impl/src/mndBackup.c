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
#include "mndBackup.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#ifdef TD_ENTERPRISE
#include "tgrant.h"
#endif

#define MND_BACKUP_VER_NUMBER 1

static int32_t mndProcessBackupTimer(SRpcMsg *pReq);
static int32_t mndProcessBackupMetaReq(SRpcMsg *pReq);
static int32_t mndProcessBackupVgroupLeaderReq(SRpcMsg *pReq);
static int32_t mndProcessBackupProgressRsp(SRpcMsg *pReq);

static void mndGetBackupMetaSnapshotDir(const SSdb *pSdb, int64_t backupId, char *path, int32_t len) {
  (void)snprintf(path, len, "%s%sbackup-meta%s%" PRId64, pSdb->mnodePath, TD_DIRSEP, TD_DIRSEP, backupId);
}

static void mndRemoveBackupMetaSnapshot(const SSdb *pSdb, int64_t backupId) {
  char path[PATH_MAX] = {0};
  mndGetBackupMetaSnapshotDir(pSdb, backupId, path, sizeof(path));
  if (taosDirExist(path)) {
    taosRemoveDir(path);
    mInfo("backup:%" PRId64 ", removed mnode metadata snapshot:%s", backupId, path);
  }
}

#ifdef TD_ENTERPRISE
static int32_t mndCopyBackupMetaFile(const char *srcPath, const char *dstPath) {
  int64_t size = taosCopyFile(srcPath, dstPath);
  if (size < 0) {
    mError("failed to copy mnode metadata from:%s to:%s since %s", srcPath, dstPath, terrstr());
    return terrno;
  }
  return 0;
}

static int32_t mndCreateBackupMetaSnapshot(SMnode *pMnode, int64_t backupId) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;
  char    snapshotDir[PATH_MAX] = {0};
  char    dataDir[PATH_MAX] = {0};
  char    syncDir[PATH_MAX] = {0};
  char    path[PATH_MAX * 2] = {0};
  char    dstPath[PATH_MAX * 2] = {0};

  mndGetBackupMetaSnapshotDir(pSdb, backupId, snapshotDir, sizeof(snapshotDir));
  if (taosDirExist(snapshotDir)) {
    mError("backup:%" PRId64 ", metadata snapshot already exists:%s", backupId, snapshotDir);
    return TSDB_CODE_BACKUP_ALREADY_EXIST;
  }

  if ((code = sdbWriteFile(pSdb, 0)) != 0) goto _OVER;
  if ((code = taosMulMkDir(snapshotDir)) != 0) goto _OVER;

  (void)snprintf(dataDir, sizeof(dataDir), "%s%sdata", snapshotDir, TD_DIRSEP);
  (void)snprintf(syncDir, sizeof(syncDir), "%s%ssync", snapshotDir, TD_DIRSEP);
  if ((code = taosMulMkDir(dataDir)) != 0) goto _OVER;
  if ((code = taosMulMkDir(syncDir)) != 0) goto _OVER;

  (void)snprintf(path, sizeof(path), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);
  (void)snprintf(dstPath, sizeof(dstPath), "%s%ssdb.data", dataDir, TD_DIRSEP);
  if ((code = mndCopyBackupMetaFile(path, dstPath)) != 0) goto _OVER;

  (void)snprintf(path, sizeof(path), "%s%smnode.json", pSdb->mnodePath, TD_DIRSEP);
  (void)snprintf(dstPath, sizeof(dstPath), "%s%smnode.json", snapshotDir, TD_DIRSEP);
  if ((code = mndCopyBackupMetaFile(path, dstPath)) != 0) goto _OVER;

  (void)snprintf(path, sizeof(path), "%s%ssync%sraft_config.json", pSdb->mnodePath, TD_DIRSEP, TD_DIRSEP);
  (void)snprintf(dstPath, sizeof(dstPath), "%s%sraft_config.json", syncDir, TD_DIRSEP);
  if ((code = mndCopyBackupMetaFile(path, dstPath)) != 0) goto _OVER;

  (void)snprintf(path, sizeof(path), "%s%ssync%sraft_store.json", pSdb->mnodePath, TD_DIRSEP, TD_DIRSEP);
  (void)snprintf(dstPath, sizeof(dstPath), "%s%sraft_store.json", syncDir, TD_DIRSEP);
  if ((code = mndCopyBackupMetaFile(path, dstPath)) != 0) goto _OVER;

  mInfo("backup:%" PRId64 ", created mnode metadata snapshot:%s", backupId, snapshotDir);

_OVER:
  if (code != 0) mndRemoveBackupMetaSnapshot(pSdb, backupId);
  return code;
}
#endif

int32_t mndInitBackup(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_BACKUP, mndRetrieveBackups);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP, mndProcessBackupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_BACKUP, mndProcessKillBackupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP_TIMER, mndProcessBackupTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP_META, mndProcessBackupMetaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP_VGROUP_LEADER, mndProcessBackupVgroupLeaderReq);
  mndSetMsgHandle(pMnode, TDMT_DND_BACKUP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_BACKUP_PROGRESS_RSP, mndProcessBackupProgressRsp);

  SSdbTable table = {
      .sdbType = SDB_BACKUP,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndBackupActionEncode,
      .decodeFp = (SdbDecodeFp)mndBackupActionDecode,
      .insertFp = (SdbInsertFp)mndBackupActionInsert,
      .updateFp = (SdbUpdateFp)mndBackupActionUpdate,
      .deleteFp = (SdbDeleteFp)mndBackupActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupBackup(SMnode *pMnode) { mDebug("mnd backup cleanup"); }

static void tFreeBackupObj(SBackupObj *pBackup) {}

static int32_t tSerializeSBackupObj(void *buf, int32_t bufLen, const SBackupObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->chainId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->backupId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->dnodeId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->backupRoot));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbName));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->lastUpdateTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->endTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->totalSize));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->isFull));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->encrypted));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->code));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->totalVgroups));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->finishedVgroups));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->currentVgId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t tDeserializeSBackupObj(void *buf, int32_t bufLen, SBackupObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->chainId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->backupId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->dnodeId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->backupRoot));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbName));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->lastUpdateTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->endTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->totalSize));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->isFull));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->encrypted));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->code));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->totalVgroups));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->finishedVgroups));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->currentVgId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

SSdbRaw *mndBackupActionEncode(SBackupObj *pBackup) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSBackupObj(NULL, 0, pBackup);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int64_t) + sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_BACKUP, MND_BACKUP_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSBackupObj(buf, tlen, pBackup);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pBackup->backupId, OVER);
  SDB_SET_INT32(pRaw, dataPos, tlen, OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OVER);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("backup:%" PRId64 ", failed to encode to raw:%p since %s", pBackup->chainId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("backup:%" PRId64 ", encode to raw:%p, row:%p", pBackup->chainId, pRaw, pBackup);
  return pRaw;
}

SSdbRow *mndBackupActionDecode(SSdbRaw *pRaw) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSdbRow    *pRow = NULL;
  SBackupObj *pBackup = NULL;
  void       *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver < 1 || sver > MND_BACKUP_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("backup read invalid ver, data ver: %d, curr ver: %d", sver, MND_BACKUP_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SBackupObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pBackup = sdbGetRowObj(pRow);
  if (pBackup == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int64_t backupId;
  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, dataPos, &backupId, OVER);

  int32_t tlen;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OVER);

  if ((terrno = tDeserializeSBackupObj(buf, tlen, pBackup)) < 0) {
    goto OVER;
  }
  if (pBackup->backupId != backupId) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("backup, failed to decode from raw:%p since %s", pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("backup:%" PRId64 ", decode from raw:%p, row:%p", pBackup->chainId, pRaw, pBackup);
  return pRow;
}

int32_t mndBackupActionInsert(SSdb *pSdb, SBackupObj *pBackup) {
  mTrace("backup:%" PRId64 ", perform insert action", pBackup->chainId);
  return 0;
}

int32_t mndBackupActionDelete(SSdb *pSdb, SBackupObj *pBackup) {
  mTrace("backup:%" PRId64 ", perform delete action", pBackup->chainId);
  tFreeBackupObj(pBackup);
  return 0;
}

int32_t mndBackupActionUpdate(SSdb *pSdb, SBackupObj *pOldBackup, SBackupObj *pNewBackup) {
  mTrace("backup:%" PRId64 ", perform update action, old row:%p new row:%p", pOldBackup->chainId, pOldBackup, pNewBackup);
  pOldBackup->totalSize = pNewBackup->totalSize;
  pOldBackup->code = pNewBackup->code;
  pOldBackup->lastUpdateTime = pNewBackup->lastUpdateTime;
  pOldBackup->endTime = pNewBackup->endTime;
  pOldBackup->totalVgroups = pNewBackup->totalVgroups;
  pOldBackup->finishedVgroups = pNewBackup->finishedVgroups;
  pOldBackup->currentVgId = pNewBackup->currentVgId;
  return 0;
}

SBackupObj *mndAcquireBackup(SMnode *pMnode, int64_t backupId) {
  SSdb       *pSdb = pMnode->pSdb;
  SBackupObj *pBackup = sdbAcquire(pSdb, SDB_BACKUP, &backupId);
  if (pBackup == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pBackup;
}

void mndReleaseBackup(SMnode *pMnode, SBackupObj *pBackup) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pBackup);
}

// retrieve backups (SHOW BACKUPS): iterate SDB, includes finished/running history
int32_t mndRetrieveBackups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SBackupObj *pBackup = NULL;
  int32_t     code = 0;
  int32_t     lino = 0;
  SUserObj   *pUser = NULL;
  char        objFName[TSDB_OBJ_FNAME_LEN + 1] = {0};
  bool        showAll = false;

  MND_SHOW_CHECK_OBJ_PRIVILEGE_ALL(RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), PRIV_SHOW_BACKUPS, PRIV_OBJ_DB, 0, _OVER);
  (void)showAll;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_BACKUP, pShow->pIter, (void **)&pBackup);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    int32_t          cols = 0;
    char             tmpBuf[TSDB_MOUNT_PATH_LEN + VARSTR_HEADER_SIZE] = {0};

    // backup_id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->backupId, false), pBackup, &lino, _OVER);

    // status (running / succeeded / failed)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    const char *status = pBackup->endTime == 0 ? "running" : (pBackup->code == 0 ? "succeeded" : "failed");
    char statusBuf[16 + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(statusBuf, status, sizeof(statusBuf));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)statusBuf, false), pBackup, &lino, _OVER);

    // chain_id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->chainId, false), pBackup, &lino, _OVER);

    // backup_root
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    STR_WITH_MAXSIZE_TO_VARSTR(tmpBuf, pBackup->backupRoot, sizeof(tmpBuf));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pBackup, &lino, _OVER);

    // db_name ("*" means whole cluster)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    char dbBuf[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if ('\0' == pBackup->dbName[0]) {
      STR_WITH_MAXSIZE_TO_VARSTR(dbBuf, "*", sizeof(dbBuf));
    } else {
      SName name = {0};
      if (tNameFromString(&name, pBackup->dbName, T_NAME_ACCT | T_NAME_DB) == 0) {
        STR_WITH_MAXSIZE_TO_VARSTR(dbBuf, name.dbname, sizeof(dbBuf));
      } else {
        STR_WITH_MAXSIZE_TO_VARSTR(dbBuf, pBackup->dbName, sizeof(dbBuf));
      }
    }
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)dbBuf, false), pBackup, &lino, _OVER);

    // full
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->isFull, false), pBackup, &lino, _OVER);

    // encrypted
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->encrypted, false), pBackup, &lino, _OVER);

    // total_size
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->totalSize, false), pBackup, &lino, _OVER);

    // total_vgroups
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->totalVgroups, false), pBackup, &lino, _OVER);

    // finished_vgroups
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->finishedVgroups, false), pBackup, &lino, _OVER);

    // current_vgroup_id (0 means no vgroup is currently being processed)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->currentVgId, false), pBackup, &lino, _OVER);

    // start_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->startTime, false), pBackup, &lino, _OVER);

    // last_update_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->lastUpdateTime, false), pBackup, &lino, _OVER);

    // end_time (0 while the backup is running)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pBackup->endTime, false), pBackup, &lino, _OVER);

    numOfRows++;
    sdbRelease(pSdb, pBackup);
  }

_OVER:
  if (pUser) mndReleaseUser(pMnode, pUser);
  if (code != 0) {
    mError("failed to retrieve backups at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

// Read an on-disk file into a single SBackupFile slot. On success pFile->data
// owns the newly allocated blob (freed by tFreeSBackupMetaRsp). Reusable across
// mnode/vnode/snode backup units. The caller manages the array and its counter.
static int32_t mndBackupPackFile(SBackupFile *pDst, const char *absPath, const char *relPath) {
  int32_t   code = 0;
  TdFilePtr pFile = NULL;
  char     *pData = NULL;

  pFile = taosOpenFile(absPath, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    goto _exit;
  }

  int64_t fileSize = 0;
  if (taosFStatFile(pFile, &fileSize, NULL) != 0) {
    code = terrno;
    goto _exit;
  }

  if (fileSize > 0) {
    pData = taosMemoryMalloc(fileSize);
    if (pData == NULL) {
      code = terrno;
      goto _exit;
    }
    if (taosReadFile(pFile, pData, fileSize) != fileSize) {
      code = terrno;
      goto _exit;
    }
  }

  tstrncpy(pDst->relPath, relPath, sizeof(pDst->relPath));
  pDst->dataLen = (int32_t)fileSize;
  pDst->data = pData;
  pData = NULL;  // ownership transferred to pDst

_exit:
  if (pFile != NULL) taosCloseFile(&pFile);
  taosMemoryFreeClear(pData);
  return code;
}

static int32_t mndProcessBackupMetaReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  int32_t        code = 0;
  int32_t        lino = 0;
  SBackupMetaReq req = {0};
  SBackupMetaRsp rsp = {0};
  char           snapshotDir[PATH_MAX] = {0};
  char           path[PATH_MAX * 2] = {0};

  TAOS_CHECK_GOTO(tDeserializeSBackupMetaReq(pReq->pCont, pReq->contLen, &req), &lino, _OVER);

  rsp.sdbEndVersion = pMnode->applied;
  mndGetBackupMetaSnapshotDir(pSdb, req.backupId, snapshotDir, sizeof(snapshotDir));

  // The snapshot was frozen before the backup transaction was created, so it
  // deliberately excludes the SDB_BACKUP record for this backup itself.
  (void)snprintf(path, sizeof(path), "%s%sdata%ssdb.data", snapshotDir, TD_DIRSEP, TD_DIRSEP);
  TAOS_CHECK_GOTO(mndBackupPackFile(&rsp.files[rsp.numOfFiles], path, "data/sdb.data"), &lino, _OVER);
  rsp.numOfFiles++;

  (void)snprintf(path, sizeof(path), "%s%smnode.json", snapshotDir, TD_DIRSEP);
  TAOS_CHECK_GOTO(mndBackupPackFile(&rsp.files[rsp.numOfFiles], path, "mnode.json"), &lino, _OVER);
  rsp.numOfFiles++;

  (void)snprintf(path, sizeof(path), "%s%ssync%sraft_config.json", snapshotDir, TD_DIRSEP, TD_DIRSEP);
  TAOS_CHECK_GOTO(mndBackupPackFile(&rsp.files[rsp.numOfFiles], path, "sync/raft_config.json"), &lino, _OVER);
  rsp.numOfFiles++;

  (void)snprintf(path, sizeof(path), "%s%ssync%sraft_store.json", snapshotDir, TD_DIRSEP, TD_DIRSEP);
  TAOS_CHECK_GOTO(mndBackupPackFile(&rsp.files[rsp.numOfFiles], path, "sync/raft_store.json"), &lino, _OVER);
  rsp.numOfFiles++;

  int32_t contLen = tSerializeSBackupMetaRsp(NULL, 0, &rsp);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  void *pRspBuf = rpcMallocCont(contLen);
  if (pRspBuf == NULL) {
    code = terrno;
    goto _OVER;
  }
  contLen = tSerializeSBackupMetaRsp(pRspBuf, contLen, &rsp);
  if (contLen < 0) {
    rpcFreeCont(pRspBuf);
    code = contLen;
    goto _OVER;
  }
  pReq->info.rsp = pRspBuf;
  pReq->info.rspLen = contLen;

_OVER:
  tFreeSBackupMetaRsp(&rsp);
  if (code != 0) {
    mError("failed to process backup meta req at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

static int32_t mndProcessBackupVgroupLeaderReq(SRpcMsg *pReq) {
  SMnode                 *pMnode = pReq->info.node;
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SBackupVgroupLeaderReq  req = {0};
  SBackupVgroupLeaderRsp  rsp = {0};
  SBackupObj              *pBackup = NULL;
  SVgObj                  *pVgroup = NULL;
  SDnodeObj               *pLeader = NULL;

  TAOS_CHECK_GOTO(tDeserializeSBackupVgroupLeaderReq(pReq->pCont, pReq->contLen, &req), &lino, _OVER);

  pBackup = mndAcquireBackup(pMnode, req.backupId);
  if (pBackup == NULL || pBackup->chainId != req.chainId || pBackup->endTime != 0) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, req.vgId);
  if (pVgroup == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_MND_VGROUP_NOT_EXIST;
    goto _OVER;
  }

  for (int32_t i = 0; i < pVgroup->replica; ++i) {
    if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER) {
      rsp.leaderDnodeId = pVgroup->vnodeGid[i].dnodeId;
      break;
    }
  }
  if (rsp.leaderDnodeId <= 0) {
    code = TSDB_CODE_SYN_NOT_LEADER;
    goto _OVER;
  }

  pLeader = mndAcquireDnode(pMnode, rsp.leaderDnodeId);
  if (pLeader == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }
  rsp.vgId = req.vgId;
  rsp.leaderEpSet = mndGetDnodeEpset(pLeader);
  if (rsp.leaderEpSet.numOfEps != 1) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  int32_t contLen = tSerializeSBackupVgroupLeaderRsp(NULL, 0, &rsp);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }
  contLen = tSerializeSBackupVgroupLeaderRsp(pRsp, contLen, &rsp);
  if (contLen < 0) {
    rpcFreeCont(pRsp);
    code = contLen;
    goto _OVER;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

_OVER:
  if (pLeader != NULL) mndReleaseDnode(pMnode, pLeader);
  if (pVgroup != NULL) mndReleaseVgroup(pMnode, pVgroup);
  if (pBackup != NULL) mndReleaseBackup(pMnode, pBackup);
  if (code != 0) {
    mError("backup:%" PRId64 ", failed to resolve vgroup:%d leader at line %d since %s", req.backupId, req.vgId,
           lino, tstrerror(code));
  }
  return code;
}

static int32_t mndSaveBackupProgress(SMnode *pMnode, const SBackupProgressRsp *pRsp) {
  int32_t     code = 0;
  SBackupObj *pBackup = mndAcquireBackup(pMnode, pRsp->backupId);
  if (pBackup == NULL) return terrno != 0 ? terrno : TSDB_CODE_SDB_OBJ_NOT_THERE;

  if (pBackup->backupId != pRsp->backupId) {
    mndReleaseBackup(pMnode, pBackup);
    return TSDB_CODE_SDB_OBJ_NOT_THERE;
  }

  bool finished = pRsp->finished != 0;
  if (pBackup->endTime != 0) {
    mndReleaseBackup(pMnode, pBackup);
    return 0;
  }

  SBackupObj backup = *pBackup;
  mndReleaseBackup(pMnode, pBackup);
  backup.code = pRsp->code;
  backup.totalSize = pRsp->totalSize;
  backup.totalVgroups = pRsp->totalVgroups;
  backup.finishedVgroups = pRsp->finishedVgroups;
  backup.currentVgId = pRsp->currentVgId;
  backup.lastUpdateTime = taosGetTimestampMs();
  if (finished) backup.endTime = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-backup-progress");
  if (pTrans == NULL) return terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;

  SSdbRaw *pRaw = mndBackupActionEncode(&backup);
  if (pRaw == NULL) {
    mndTransDrop(pTrans);
    return terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
  }
  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    sdbFreeRaw(pRaw);
    mndTransDrop(pTrans);
    return code;
  }
  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_READY)) != 0 || (code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mndTransDrop(pTrans);
    return code;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessBackupProgressRsp(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  if (pReq->code != 0) {
    mDebug("backup progress request failed since %s", tstrerror(pReq->code));
    return 0;
  }

  SBackupProgressRsp rsp = {0};
  int32_t code = tDeserializeSBackupProgressRsp(pReq->pCont, pReq->contLen, &rsp);
  if (code != 0) {
    mError("failed to deserialize backup progress response since %s", tstrerror(code));
    return code;
  }
  if (rsp.code != 0) {
    mError("backup chain:%" PRId64 " failed on dnode since %s", rsp.chainId, tstrerror(rsp.code));
  }

  code = mndSaveBackupProgress(pMnode, &rsp);
#ifdef TD_ENTERPRISE
  if (code == 0 && rsp.finished != 0) {
    mndRemoveBackupMetaSnapshot(pMnode->pSdb, rsp.backupId);
  }
#endif
  return code;
}

static int32_t mndProcessBackupTimer(SRpcMsg *pReq) {
#ifdef TD_ENTERPRISE
  mTrace("start to process backup timer");
  mndBackupPullup(pReq->info.node);
#endif
  return 0;
}

#ifdef TD_ENTERPRISE
static int64_t mndBackupGenId(void) {
  time_t    tt = (time_t)(taosGetTimestampMs() / 1000);
  struct tm tm = {0};
  if (taosLocalTime(&tt, &tm, NULL, 0, NULL) == NULL) {
    return (int64_t)tt;
  }
  return (int64_t)(tm.tm_year + 1900) * 10000000000LL + (int64_t)(tm.tm_mon + 1) * 100000000LL +
         (int64_t)tm.tm_mday * 1000000LL + (int64_t)tm.tm_hour * 10000LL + (int64_t)tm.tm_min * 100LL +
         (int64_t)tm.tm_sec;
}

static int32_t mndBackupDnodeCompare(const void *left, const void *right) {
  const SBackupDnodeInfo *pLeft = left;
  const SBackupDnodeInfo *pRight = right;

  return pLeft->id < pRight->id ? -1 : (pLeft->id > pRight->id ? 1 : 0);
}

static int32_t mndBuildBackupDnodeReq(SMnode *pMnode, SBackupObj *pBackup, int8_t hasPassword, const char password[TSDB_PASSWORD_LEN], SBackupDnodeReq *pReq) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  pReq->chainId = pBackup->chainId;
  pReq->backupId = pBackup->backupId;
  pReq->dnodeId = pBackup->dnodeId;
  pReq->isFull = pBackup->isFull;
  tstrncpy(pReq->backupRoot, pBackup->backupRoot, sizeof(pReq->backupRoot));
  tstrncpy(pReq->dbName, pBackup->dbName, sizeof(pReq->dbName));
  pReq->hasPassword = hasPassword;
  memcpy(pReq->password, password, sizeof(pReq->password));

  pReq->dnodes = taosArrayInit(sdbGetSize(pSdb, SDB_DNODE), sizeof(SBackupDnodeInfo));
  pReq->streamSnodes = taosArrayInit(sdbGetSize(pSdb, SDB_STREAM), sizeof(SBackupStreamSnodeInfo));
  pReq->vgroups = taosArrayInit(sdbGetSize(pSdb, SDB_VGROUP), sizeof(SBackupVgroupInfo));
  if (pReq->dnodes == NULL || pReq->streamSnodes == NULL || pReq->vgroups == NULL) {
    return terrno;
  }

  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    SBackupDnodeInfo info = {
      .id = pObj->id,
      .isSnode = sdbCheckExists(pSdb, SDB_SNODE, &pObj->id),
      .isMnode = sdbCheckExists(pSdb, SDB_MNODE, &pObj->id),
    };
    if (taosArrayPush(pReq->dnodes, &info) == NULL) {
      sdbRelease(pSdb, pObj);
      return terrno;
    }
    sdbRelease(pSdb, pObj);
  }
  taosArraySort(pReq->dnodes, mndBackupDnodeCompare);

  pIter = NULL;
  while (1) {
    SStreamObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    int32_t snodeDnodeId = atomic_load_32(&pObj->mainSnodeId);
    if (snodeDnodeId > 0 && pObj->pCreate != NULL) {
      SBackupStreamSnodeInfo info = {.streamId = pObj->pCreate->streamId, .snodeDnodeId = snodeDnodeId};
      if (taosArrayPush(pReq->streamSnodes, &info) != NULL) {
        sdbRelease(pSdb, pObj);
        continue;
      }
      sdbRelease(pSdb, pObj);
      return terrno;
    }
    sdbRelease(pSdb, pObj);
  }

  pIter = NULL;
  while (1) {
    SVgObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SBackupVgroupInfo info = {.vgId = pObj->vgId, .dbUid = pObj->dbUid, .numOfDnodes = pObj->replica};
    tstrncpy(info.dbName, pObj->dbName, sizeof(info.dbName));
    for (int32_t i = 0; i < info.numOfDnodes; ++i) {
      info.dnodeIds[i] = pObj->vnodeGid[i].dnodeId;
    }
    if (taosArrayPush(pReq->vgroups, &info) == NULL) {
      sdbRelease(pSdb, pObj);
      return terrno;
    }
    sdbRelease(pSdb, pObj);
  }

  return 0;
}

static void *mndSerializeBackupDnodeReq(SBackupDnodeReq *pBackupReq, int32_t *pContLen) {
  int32_t contLen = tSerializeSBackupDnodeReq(NULL, 0, pBackupReq);
  if (contLen < 0) {
    terrno = contLen;
    return NULL;
  }
  void *pCont = taosMemoryMalloc(contLen);
  if (pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  if (tSerializeSBackupDnodeReq(pCont, contLen, pBackupReq) < 0) {
    taosMemoryFreeClear(pCont);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pCont;
}

static int32_t mndAddBackupRedoAction(SMnode *pMnode, STrans *pTrans, SBackupObj *pBackup, int8_t hasPassword, const char password[TSDB_PASSWORD_LEN]) {
  int32_t      code = 0;
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pBackup->dnodeId);
  if (pDnode == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
    TAOS_RETURN(code);
  }
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  SBackupDnodeReq req = {0};
  if ((code = mndBuildBackupDnodeReq(pMnode, pBackup, hasPassword, password, &req)) != 0) {
    tFreeSBackupDnodeReq(&req);
    TAOS_RETURN(code);
  }

  void *pReq = mndSerializeBackupDnodeReq(&req, &contLen);
  tFreeSBackupDnodeReq(&req);
  if (pReq == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_BACKUP;
  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
  }
  return code;
}

static int32_t mndAddKillBackupRedoAction(SMnode *pMnode, STrans *pTrans, const SBackupObj *pBackup) {
  int32_t      code = 0;
  STransAction action = {0};
  SDnodeObj   *pDnode = mndAcquireDnode(pMnode, pBackup->dnodeId);
  if (pDnode == NULL) return terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;

  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  SBackupProgressReq req = {.chainId = pBackup->chainId, .backupId = pBackup->backupId};
  int32_t contLen = tSerializeSBackupProgressReq(NULL, 0, &req);
  if (contLen < 0) return contLen;
  void *pCont = taosMemoryMalloc(contLen);
  if (pCont == NULL) return terrno;
  if (tSerializeSBackupProgressReq(pCont, contLen, &req) < 0) {
    taosMemoryFree(pCont);
    return TSDB_CODE_INVALID_MSG;
  }

  action.pCont = pCont;
  action.contLen = contLen;
  action.msgType = TDMT_DND_KILL_BACKUP;
  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) taosMemoryFree(pCont);
  return code;
}

static int32_t mndSetBackupCommitLogs(SMnode *pMnode, STrans *pTrans, SBackupObj *pBackup) {
  SSdbRaw *pRaw = mndBackupActionEncode(pBackup);
  if (pRaw == NULL) return terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;

  int32_t code = mndTransAppendCommitlog(pTrans, pRaw);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    return code;
  }
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRaw, SDB_STATUS_READY));
  return 0;
}

static SBackupObj *mndGetRunningBackup(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SBackupObj *pBackup = NULL;
    pIter = sdbFetch(pSdb, SDB_BACKUP, pIter, (void **)&pBackup);
    if (pIter == NULL) break;
    if (pBackup->endTime == 0) return pBackup;
    sdbRelease(pSdb, pBackup);
  }
  return NULL;
}

int32_t mndProcessBackupReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  int32_t    code = -1;
  int32_t    lino = 0;
  SBackupReq backupReq = {0};
  STrans    *pTrans = NULL;
  bool       metaSnapshotCreated = false;

  if (tDeserializeSBackupReq(pReq->pCont, pReq->contLen, &backupReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("start to create backup on dnode:%d, root:%s", backupReq.dnodeId, backupReq.backupRoot);
  TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_BACKUP_RESTORE), &lino, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_BACKUP), &lino,
                  _OVER);

  if (!backupReq.isFull || backupReq.backupChainId != 0 || backupReq.dbName[0] != '\0') {
    TAOS_CHECK_GOTO(TSDB_CODE_OPS_NOT_SUPPORT, &lino, _OVER);
  }

  SBackupObj *pRunningBackup = mndGetRunningBackup(pMnode);
  if (pRunningBackup != NULL) {
    mError("backup:%" PRId64 " is still in progress", pRunningBackup->backupId);
    sdbRelease(pMnode->pSdb, pRunningBackup);
    TAOS_CHECK_GOTO(TSDB_CODE_BACKUP_IN_PROGRESS, &lino, _OVER);
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, backupReq.dnodeId);
  if (pDnode == NULL) {
    TAOS_CHECK_GOTO(terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _OVER);
  }
  mndReleaseDnode(pMnode, pDnode);

  int64_t now = taosGetTimestampMs();
  int64_t id = mndBackupGenId();
  SBackupObj backup = {0};
  backup.chainId = id;
  backup.backupId = id;
  backup.dnodeId = backupReq.dnodeId;
  backup.isFull = backupReq.isFull;
  backup.encrypted = backupReq.hasPassword != 0;
  backup.startTime = now;
  backup.lastUpdateTime = now;
  tstrncpy(backup.backupRoot, backupReq.backupRoot, sizeof(backup.backupRoot));
  tstrncpy(backup.dbName, backupReq.dbName, sizeof(backup.dbName));

  TAOS_CHECK_GOTO(mndCreateBackupMetaSnapshot(pMnode, backup.backupId), &lino, _OVER);
  metaSnapshotCreated = true;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-backup");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _OVER);
  }
  mInfo("trans:%d, used to create backup chain:%" PRId64, pTrans->id, backup.chainId);

  TAOS_CHECK_GOTO(mndSetBackupCommitLogs(pMnode, pTrans, &backup), &lino, _OVER);
  TAOS_CHECK_GOTO(mndAddBackupRedoAction(pMnode, pTrans, &backup, backupReq.hasPassword, backupReq.password), &lino, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to process backup req at line %d since %s", lino, tstrerror(code));
  }
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS && metaSnapshotCreated) {
    mndRemoveBackupMetaSnapshot(pMnode->pSdb, backup.backupId);
  }
  mndTransDrop(pTrans);
  tFreeSBackupReq(&backupReq);
  TAOS_RETURN(code);
}

int32_t mndProcessKillBackupReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  int32_t        lino = 0;
  SKillBackupReq killReq = {0};
  SBackupObj    *pBackup = NULL;
  STrans        *pTrans = NULL;

  if (tDeserializeSKillBackupReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("start to kill backup:%" PRId64, killReq.backupId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_BACKUP), &lino,
                  _OVER);

  pBackup = mndAcquireBackup(pMnode, killReq.backupId);
  if (pBackup == NULL) {
    TAOS_CHECK_GOTO(terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _OVER);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "kill-backup");
  if (pTrans == NULL) {
    TAOS_CHECK_GOTO(terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _OVER);
  }
  mInfo("trans:%d, used to kill backup:%" PRId64, pTrans->id, pBackup->backupId);

  TAOS_CHECK_GOTO(mndAddKillBackupRedoAction(pMnode, pTrans, pBackup), &lino, _OVER);
  SSdbRaw *pRaw = mndBackupActionEncode(pBackup);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL, &lino, _OVER);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    lino = __LINE__;
    sdbFreeRaw(pRaw);
    goto _OVER;
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED), &lino, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to process kill backup req at line %d since %s", lino, tstrerror(code));
  }
  mndReleaseBackup(pMnode, pBackup);
  mndTransDrop(pTrans);
  tFreeSKillBackupReq(&killReq);
  TAOS_RETURN(code);
}

static void mndBackupSendProgressReq(SMnode *pMnode, const SBackupObj *pBackup) {
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pBackup->dnodeId);
  if (pDnode == NULL) {
    mDebug("backup chain:%" PRId64 ", dnode:%d is unavailable for progress query", pBackup->chainId,
           pBackup->dnodeId);
    return;
  }

  SBackupProgressReq req = {.chainId = pBackup->chainId, .backupId = pBackup->backupId};
  int32_t contLen = tSerializeSBackupProgressReq(NULL, 0, &req);
  if (contLen < 0) {
    mndReleaseDnode(pMnode, pDnode);
    return;
  }
  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    mndReleaseDnode(pMnode, pDnode);
    return;
  }
  if (tSerializeSBackupProgressReq(pCont, contLen, &req) < 0) {
    rpcFreeCont(pCont);
    mndReleaseDnode(pMnode, pDnode);
    return;
  }

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);
  SRpcMsg rpcMsg = {.msgType = TDMT_DND_BACKUP_PROGRESS, .pCont = pCont, .contLen = contLen};
  if (tmsgSendReq(&epSet, &rpcMsg) < 0) {
    mDebug("backup chain:%" PRId64 ", failed to send progress query", pBackup->chainId);
  }
}

void mndBackupPullup(SMnode *pMnode) {
  SBackupObj *pBackup = mndGetRunningBackup(pMnode);
  if (pBackup != NULL) {
    mndBackupSendProgressReq(pMnode, pBackup);
    sdbRelease(pMnode->pSdb, pBackup);
  }
}

#else // TD_ENTERPRISE

int32_t mndProcessBackupReq(SRpcMsg *pReq) {
  mError("failed to process backup req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t mndProcessKillBackupReq(SRpcMsg *pReq) {
  mError("failed to process kill backup req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

#endif // TD_ENTERPRISE

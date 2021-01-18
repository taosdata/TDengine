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

#include "tsdbint.h"

static int  tsdbSyncSendMeta(STsdbRepo *pRepo, int socketFd, SMFile *pmf);
static int  tsdbSyncRecvMeta(STsdbRepo *pRepo, int socketFd);
static int  tsdbSyncSendDFileSet(STsdbRepo *pRepo, int socketFd, SDFileSet *pOSet);
static int  tsdbSyncRecvDFileSet(STsdbRepo *pRepo, int socketFd);
static bool tsdbIsFSetSame(SDFileSet *pSet1, SDFileSet *pSet2);

int tsdbSyncSend(STsdbRepo *pRepo, int socketFd) {
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSIter    fsiter;
  SDFileSet *pSet;

  // Disable commit while syncing TSDB files
  sem_wait(&(pRepo->readyToCommit));

  // Sync send meta file
  if (tsdbSyncSendMeta(pRepo, socketFd, pfs->cstatus->pmf) < 0) {
    tsdbError("vgId:%d failed to sync send meta file since %s", REPO_ID(pRepo), tstrerror(terrno));
    sem_post(&(pRepo->readyToCommit));
    return -1;
  }

  // Sync send SDFileSet
  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);

  while ((pSet = tsdbFSIterNext(&fsiter)) != NULL) {
    if (tsdbSyncSendDFileSet(pRepo, socketFd, pSet) < 0) {
      sem_post(&(pRepo->readyToCommit));
      return -1;
    }
  }

  // Enable commit
  sem_post(&(pRepo->readyToCommit));
  return 0;
}

int tsdbSyncRecv(STsdbRepo *pRepo, int socketFd) {
  SFSIter    fsiter;
  SDFileSet *pSet;
  SDFileSet  dset;
  SDFileSet *pRecvSet = &dset;
  uint32_t   tlen;
  char       buf[128];
  void *     pBuf = NULL;

  tsdbStartFSTxn(pRepo, 0, 0);

  // Sync recv meta file from remote
  if (tsdbSyncRecvMeta(pRepo, socketFd) < 0) {
    // TODO
    goto _err;
  }

  // Sync recv SDFileSet
  tsdbFSIterInit(&fsiter, REPO_FS(pRepo), TSDB_FS_ITER_FORWARD);
  pSet = tsdbFSIterNext(&fsiter);

  if (taosReadMsg(socketFd, buf, sizeof(uint32_t)) < 0) {
    // TODO
    goto _err;
  }

  taosDecodeFixedU32(buf, &tlen);
  if (tlen == 0) {
    // No more remote files
    pRecvSet = NULL;
  } else {
    // Has remote files
    if (tsdbMakeRoom(&pBuf, tlen) < 0) {
      // TODO
      goto _err;
    }

    if (taosReadMsg(socketFd, pBuf, tlen) < tlen) {
      // TODO
      goto _err;
    }

    pRecvSet = &dset;
    tsdbDecodeDFileSet(pBuf, pRecvSet);
  }

  while (true) {
    if (pSet == NULL && pRecvSet == NULL) break;

    if (pSet == NULL) {
      // TODO: local not has, copy from remote
      // Process the next remote fset(next pRecvSet)
    } else {
      if (pRecvSet == NULL) {
        // Remote not has, just remove this file
        pSet = tsdbFSIterNext(&fsiter);
      } else {
        if (pSet->fid == pRecvSet->fid) {
          if (tsdbIsFSetSame(pSet, pRecvSet)) {
            tsdbUpdateDFileSet(REPO_FS(pRepo), pSet);
          } else {
            // Copy from remote
          }
          pSet = tsdbFSIterNext(&fsiter);
          // Process the next remote fset
        } else if (pSet->fid < pRecvSet->fid) {
          // Remote has not, just remove this file
          pSet = tsdbFSIterNext(&fsiter);
        } else {
          // not has, copy pRecvSet from remote
          // Process the next remote fset
        }
      }

    }
  }
  
  tsdbEndFSTxn(pRepo);
  return 0;

_err:
  taosTZfree(pBuf);
  tsdbEndFSTxnWithError(REPO_FS(pRepo));
  return -1;
}

static int tsdbSyncSendMeta(STsdbRepo *pRepo, int socketFd, SMFile *pmf) {
  void *   pBuf = NULL;
  uint32_t tlen = 0;
  void *   ptr;
  SMFile   mf;
  SMFile * pMFile = NULL;

  if (pmf) {
    // copy out
    mf = *pmf;
    pMFile = &mf;
  }

  if (pMFile) {
    tlen = tsdbEncodeMFInfo(NULL, TSDB_FILE_INFO(pMFile)) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom(&pBuf, sizeof(tlen) + tlen) < 0) {
    return -1;
  }

  ptr = pBuf;
  taosEncodeFixedU32(&ptr, tlen);
  if (pMFile) {
    tsdbEncodeMFInfo(&ptr, TSDB_FILE_INFO(pMFile));
    taosCalcChecksumAppend(0, (uint8_t *)pBuf, POINTER_DISTANCE(ptr, pBuf));
    ptr = POINTER_SHIFT(ptr, sizeof(TSCKSUM));
  }

  if (taosWriteMsg(socketFd, pBuf, POINTER_DISTANCE(ptr, pBuf)) < POINTER_DISTANCE(ptr, pBuf)) {
    tsdbError("vgId:%d failed to sync meta file since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (pMFile == NULL) {
    // No meta file, no need to send
    return 0;
  }

  bool shouldSend = false;
  {
    // TODO: Recv command to know if need to send file
  }

  if (shouldSend) {
    if (tsdbOpenMFile(pMFile, O_RDONLY) < 0) {
      tsdbError("vgId:%d failed to open meta file since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    if (taosSendFile(socketFd, TSDB_FILE_FD(pMFile), 0, pMFile->info.size) < pMFile->info.size) {
      tsdbError("vgId:%d failed to send meta file since %s", REPO_ID(pRepo), strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbCloseMFile(pMFile);
      goto _err;
    }

    tsdbCloseMFile(pMFile);
  }

  return 0;

_err:
  taosTZfree(pBuf);
  return -1;
}

static int tsdbSyncRecvMeta(STsdbRepo *pRepo, int socketFd) {
  uint32_t tlen;
  char     buf[128];
  void *   pBuf = NULL;
  SMFInfo  mfInfo;
  SMFile * pMFile = pRepo->fs->cstatus->pmf;
  SMFile   mf;

  if (taosReadMsg(socketFd, (void *)buf, sizeof(int32_t)) < sizeof(int32_t)) {
    tsdbError("vgId:%d failed to sync recv meta file since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  taosDecodeFixedU32(buf, &tlen);

  // Remote not has meta file, just remove meta file (do nothing)
  if (tlen == 0) {
    // TODO: need to notify remote?
    return 0;
  }

  if (tsdbMakeRoom(&pBuf, tlen) < 0) {
    tsdbError("vgId:%d failed to sync recv meta file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (taosReadMsg(socketFd, pBuf, tlen) < tlen) {
    tsdbError("vgId:%d failed to sync recv meta file since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (!taosCheckChecksumWhole((uint8_t *)pBuf, tlen)) {
    tsdbError("vgId:%d failed to sync recv meta file since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TSDB_CODE_TDB_MESSED_MSG;
    goto _err;
  }

  void *ptr = pBuf;
  ptr = tsdbDecodeMFInfo(ptr, &mfInfo);

  if (pMFile != NULL && memcmp(&(pMFile->info), &mfInfo, sizeof(SMInfo)) == 0) {
    // has file and same as remote, just keep the old one
    tsdbUpdateMFile(REPO_FS(pRepo), pMFile);
    // Notify remote that no need to send meta file
    {
      // TODO
    }
  } else {
    // Need to copy meta file from remote
    SDiskID did = {.level = TFS_PRIMARY_LEVEL, .id = TFS_PRIMARY_ID};
    tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));
    mf.info = mfInfo;

    // Create new file
    if (tsdbCreateMFile(&mf, false) < 0) {
      tsdbError("vgId:%d failed to create meta file since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    // Notify remote to send meta file
    {
      // TODO
    }

    if (taosCopyFds(socketFd, mf.fd, mfInfo.size) < 0) {
      tsdbError("vgId:%d failed to sync recv meta file since %s", REPO_ID(pRepo), strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbCloseMFile(&mf);
      tsdbRemoveMFile(&mf);
      goto _err;
    }

    TSDB_FILE_FSYNC(&mf);
    tsdbCloseMFile(&mf);
    tsdbUpdateMFile(REPO_FS(pRepo), &mf);
  }

  return 0;

_err:
  taosTZfree(pBuf);
  return -1;
}

static int tsdbSyncSendDFileSet(STsdbRepo *pRepo, int socketFd, SDFileSet *pOSet) {
  void *    pBuf = NULL;
  uint32_t  tlen = 0;
  void *    ptr;
  SDFileSet dset;
  SDFileSet *pSet = NULL;

  if (pOSet) {
    dset = *pOSet;
    pSet = &dset;
  }

  if (pSet) {
    tlen = tsdbEncodeDFileSet(NULL, pSet) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom(&pBuf, sizeof(tlen) + tlen) < 0) {
    return -1;
  }

  ptr = pBuf;
  taosEncodeFixedU32(&ptr, tlen);
  if (pSet) {
    tsdbEncodeDFileSet(&ptr, pSet);
    taosCalcChecksumAppend(0, (uint8_t *)pBuf, tlen);
    ptr = POINTER_SHIFT(ptr, sizeof(TSCKSUM));
  }

  if (taosWriteMsg(socketFd, pBuf, POINTER_DISTANCE(ptr, pBuf)) < POINTER_DISTANCE(ptr, pBuf)) {
    // TODO
    goto _err;
  }

  if (pSet == NULL) {
    // No need to wait
    return 0;
  }

  bool shouldSend = false;
  {
    // TODO: Recv command to know if need to send file
  }

  if (shouldSend) {
    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);

      if (tsdbOpenDFile(pDFile, O_RDONLY) < 0) {
        // TODO
        goto _err;
      }

      if (taosSendFile(socketFd, TSDB_FILE_FD(pDFile), 0, pDFile->info.size) < pDFile->info.size) {
        // TODO
        tsdbCloseDFile(pDFile);
        goto _err;
      }

      tsdbCloseDFile(pDFile);
    }
  }

  taosTZfree(pBuf);
  return 0;

_err:
  taosTZfree(pBuf);
  return -1;
}

static UNUSED_FUNC int tsdbSyncRecvDFileSet(STsdbRepo *pRepo, int socketFd) {
  // TODO
  return 0;
}

static bool tsdbIsFSetSame(SDFileSet *pSet1, SDFileSet *pSet2) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (memcmp(TSDB_FILE_INFO(TSDB_DFILE_IN_SET(pSet1, ftype)), TSDB_FILE_INFO(TSDB_DFILE_IN_SET(pSet2, ftype)),
               sizeof(SDFInfo)) != 0) {
      return false;
    }
  }

  return true;
}
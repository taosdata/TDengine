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

// Sync handle
typedef struct {
  STsdbRepo *pRepo;
  SRtn       rtn;
  int        socketFd;
  void *     pBuf;
  SMFile *   pmf;
  SMFile     mf;
  SDFileSet  df;
  SDFileSet *pdf;
} SSyncH;

#define SYNC_BUFFER(sh) ((sh)->pBuf)

static void tsdbInitSyncH(SSyncH *pSyncH, STsdbRepo *pRepo, int socketFd);
static void tsdbDestroySyncH(SSyncH *pSyncH);
static int  tsdbSyncSendMeta(SSyncH *pSynch);
static int  tsdbSyncRecvMeta(SSyncH *pSynch);
static int  tsdbSendMetaInfo(SSyncH *pSynch);
static int  tsdbRecvMetaInfo(SSyncH *pSynch);
static int  tsdbSendDecision(SSyncH *pSynch, bool toSend);
static int  tsdbRecvDecision(SSyncH *pSynch, bool *toSend);
static int  tsdbSyncSendDFileSetArray(SSyncH *pSynch);
static int  tsdbSyncRecvDFileSetArray(SSyncH *pSynch);
static bool tsdbIsTowFSetSame(SDFileSet *pSet1, SDFileSet *pSet2);
static int  tsdbSyncSendDFileSet(SSyncH *pSynch, SDFileSet *pSet);
static int  tsdbSendDFileSetInfo(SSyncH *pSynch, SDFileSet *pSet);
static int  tsdbRecvDFileSetInfo(SSyncH *pSynch);

int tsdbSyncSend(STsdbRepo *pRepo, int socketFd) {
  SSyncH synch = {0};

  tsdbInitSyncH(&synch, pRepo, socketFd);
  // Disable TSDB commit
  sem_wait(&(pRepo->readyToCommit));

  if (tsdbSyncSendMeta(&synch) < 0) {
    tsdbError("vgId:%d failed to send meta file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbSyncSendDFileSetArray(&synch) < 0) {
    tsdbError("vgId:%d failed to send data file set array since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Enable TSDB commit
  sem_post(&(pRepo->readyToCommit));
  tsdbDestroySyncH(&synch);
  return 0;

_err:
  sem_post(&(pRepo->readyToCommit));
  tsdbDestroySyncH(&synch);
  return -1;
}

int tsdbSyncRecv(STsdbRepo *pRepo, int socketFd) {
  SSyncH synch;

  tsdbInitSyncH(&synch, pRepo, socketFd);
  tsdbStartFSTxn(pRepo, 0, 0);

  if (tsdbSyncRecvMeta(&synch) < 0) {
    tsdbError("vgId:%d failed to sync recv meta file from remote since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbSyncRecvDFileSetArray(&synch) < 0) {
    tsdbError("vgId:%d failed to sync recv data file set from remote since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // TODO: need to restart TSDB or reload TSDB here

  tsdbEndFSTxn(pRepo);
  tsdbDestroySyncH(&synch);
  return 0;

_err:
  tsdbEndFSTxnWithError(REPO_FS(pRepo));
  tsdbDestroySyncH(&synch);
  return -1;
}

static void tsdbInitSyncH(SSyncH *pSyncH, STsdbRepo *pRepo, int socketFd) {
  pSyncH->pRepo = pRepo;
  pSyncH->socketFd = socketFd;
  tsdbGetRtnSnap(pRepo, &(pSyncH->rtn));
}

static void tsdbDestroySyncH(SSyncH *pSyncH) { taosTZfree(pSyncH->pBuf); }

// ============ SYNC META API
static int tsdbSyncSendMeta(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  bool       toSendMeta = false;
  SMFile     mf;

  // Send meta info to remote
  if (tsdbSendMetaInfo(pSynch) < 0) {
    tsdbError("vgId:%d failed to send meta file info since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (pRepo->fs->cstatus->pmf == NULL) {
    // No meta file, not need to wait to retrieve meta file
    return 0;
  }

  if (tsdbRecvDecision(pSynch, &toSendMeta) < 0) {
    tsdbError("vgId:%d failed to recv send file decision since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (toSendMeta) {
    tsdbInitMFileEx(&mf, pRepo->fs->cstatus->pmf);
    if (tsdbOpenMFile(&mf, O_RDONLY) < 0) {
      tsdbError("vgId:%d failed to open meta file while sync send meta since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    if (taosSendFile(pSynch->socketFd, TSDB_FILE_FD(&mf), 0, mf.info.size) < mf.info.size) {
      tsdbError("vgId:%d failed to copy meta file to remote since %s", REPO_ID(pRepo), tstrerror(terrno));
      tsdbCloseMFile(&mf);
      return -1;
    }

    tsdbCloseMFile(&mf);
  }

  return 0;
}

static int tsdbSyncRecvMeta(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  SMFile *   pLMFile = pRepo->fs->cstatus->pmf;

  // Recv meta info from remote
  if (tsdbRecvMetaInfo(pSynch) < 0) {
    tsdbError("vgId:%d failed to recv meta info from remote since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // No meta file, do nothing (rm local meta file)
  if (pSynch->pmf == NULL) {
    return 0;
  }

  if (pLMFile == NULL || memcmp(&(pSynch->pmf->info), &(pLMFile->info), sizeof(SMFInfo)) != 0) {
    // Local has no meta file or has a different meta file, need to copy from remote
    if (tsdbSendDecision(pSynch, true) < 0) {
      // TODO
      return -1;
    }

    // Recv from remote
    SMFile  mf;
    SDiskID did = {.level = TFS_PRIMARY_LEVEL, .id = TFS_PRIMARY_ID};
    tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));
    if (tsdbCreateMFile(&mf, false) < 0) {
      // TODO
      return -1;
    }

    if (taosCopyFds(pSynch->socketFd, TSDB_FILE_FD(&mf), pSynch->pmf->info.size) < pSynch->pmf->info.size) {
      // TODO
      tsdbCloseMFile(&mf);
      tsdbRemoveMFile(&mf);
      return -1;
    }

    tsdbCloseMFile(&mf);
    tsdbUpdateMFile(REPO_FS(pRepo), &mf);
  } else {
    if (tsdbSendDecision(pSynch, false) < 0) {
      // TODO
      return -1;
    }
  }

  return 0;
}

static int tsdbSendMetaInfo(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint32_t   tlen = 0;
  SMFile *   pMFile = pRepo->fs->cstatus->pmf;

  if (pMFile) {
    tlen = tlen + tsdbEncodeSMFileEx(NULL, pMFile) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen) < 0) {
    return -1;
  }

  void *ptr = SYNC_BUFFER(pSynch);
  taosEncodeFixedU32(&ptr, tlen);
  void *tptr = ptr;
  if (pMFile) {
    tsdbEncodeSMFileEx(&ptr, pMFile);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, tlen);
  }

  if (taosWriteMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen + sizeof(uint32_t)) < tlen + sizeof(uint32_t)) {
    tsdbError("vgId:%d failed to send sync meta file info to remote since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tsdbRecvMetaInfo(SSyncH *pSynch) {
  uint32_t tlen;
  char     buf[64] = "\0";

  if (taosReadMsg(pSynch->socketFd, buf, sizeof(tlen)) < sizeof(tlen)) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  taosDecodeFixedU32(buf, &tlen);

  if (tlen == 0) {
    pSynch->pmf = NULL;
    return 0;
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen) < 0) {
    return -1;
  }

  if (taosReadMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen) < tlen) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)SYNC_BUFFER(pSynch), tlen)) {
    // TODO
    terrno = TSDB_CODE_TDB_MESSED_MSG;
    return -1;
  }

  pSynch->pmf = &(pSynch->mf);
  tsdbDecodeSMFileEx(SYNC_BUFFER(pSynch), pSynch->pmf);

  return 0;
}

static int tsdbSendDecision(SSyncH *pSynch, bool toSend) {
  uint8_t decision = toSend;

  if (taosWriteMsg(pSynch->socketFd, (void *)(&decision), sizeof(decision)) < sizeof(decision)) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tsdbRecvDecision(SSyncH *pSynch, bool *toSend) {
  uint8_t decision;

  if (taosReadMsg(pSynch->socketFd, (void *)(&decision), sizeof(decision)) < sizeof(decision)) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  *toSend = decision;

  return 0;
}

// ========== SYNC DATA FILE SET ARRAY API
static int tsdbSyncSendDFileSetArray(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSIter    fsiter;
  SDFileSet *pSet;

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);

  do {
    pSet = tsdbFSIterNext(&fsiter);
    if (tsdbSyncSendDFileSet(pSynch, pSet) < 0) {
      // TODO
      return -1;
    }

    // No more file set to send, jut break
    if (pSet == NULL) {
      break;
    }
  } while (true);

  return 0;
}

static int tsdbSyncRecvDFileSetArray(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSIter    fsiter;
  SDFileSet *pLSet;  // Local file set

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);

  pLSet = tsdbFSIterNext(&fsiter);
  if (tsdbRecvDFileSetInfo(pSynch) < 0) {
    // TODO
    return -1;
  }

  while (true) {
    if (pLSet == NULL && pSynch->pdf == NULL) break;

    if (pLSet && (pSynch->pdf == NULL || pLSet->fid < pSynch->pdf->fid)) {
      // remote not has pLSet->fid set, just remove local (do nothing to remote the fset)
      pLSet = tsdbFSIterNext(&fsiter);
    } else {
      if (pLSet && pSynch->pdf && pLSet->fid == pSynch->pdf->fid && tsdbIsTowFSetSame(pLSet, pSynch->pdf)) {
        // Just keep local files and notify remote not to send
        if (tsdbUpdateDFileSet(pfs, pLSet) < 0) {
          // TODO
          return -1;
        }

        if (tsdbSendDecision(pSynch, false) < 0) {
          // TODO
          return -1;
        }
      } else {
        // Need to copy from remote

        // Notify remote to send there file here
        if (tsdbSendDecision(pSynch, true) < 0) {
          // TODO
          return -1;
        }

        // Create local files and copy from remote
        SDiskID   did;
        SDFileSet fset;

        tfsAllocDisk(tsdbGetFidLevel(pSynch->pdf->fid, &(pSynch->rtn)), &(did.level), &(did.id));
        if (did.level == TFS_UNDECIDED_LEVEL) {
          terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
          return -1;
        }

        tsdbInitDFileSet(&fset, did, REPO_ID(pRepo), pSynch->pdf->fid, FS_TXN_VERSION(pfs));

        // Create new FSET
        if (tsdbCreateDFileSet(&fset, false) < 0) {
          // TODO
          return -1;
        }

        for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
          SDFile *pDFile = TSDB_DFILE_IN_SET(&fset, ftype);         // local file
          SDFile *pRDFile = TSDB_DFILE_IN_SET(pSynch->pdf, ftype);  // remote file
          if (taosCopyFds(pSynch->socketFd, pDFile->fd, pRDFile->info.size) < pRDFile->info.size) {
            // TODO
            terrno = TAOS_SYSTEM_ERROR(errno);
            tsdbCloseDFileSet(&fset);
            tsdbRemoveDFileSet(&fset);
            return -1;
          }
          // Update new file info
          pDFile->info = pRDFile->info;
        }

        tsdbCloseDFileSet(&fset);
        if (tsdbUpdateDFileSet(pfs, &fset) < 0) {
          // TODO
          return -1;
        }
      }

      // Move forward
      if (tsdbRecvDFileSetInfo(pSynch) < 0) {
        // TODO
        return -1;
      }

      if (pLSet) {
        pLSet = tsdbFSIterNext(&fsiter);
      }
    }

#if 0
    if (pLSet == NULL) {
      // Copy from remote >>>>>>>>>>>
    } else {
      if (pSynch->pdf == NULL) {
        // Remove local file, just ignore ++++++++++++++
        pLSet = tsdbFSIterNext(&fsiter);
      } else {
        if (pLSet->fid < pSynch->pdf->fid) {
          // Remove local file, just ignore ++++++++++++
          pLSet = tsdbFSIterNext(&fsiter);
        } else if (pLSet->fid > pSynch->pdf->fid){
          // Copy from remote >>>>>>>>>>>>>>
          if (tsdbRecvDFileSetInfo(pSynch) < 0) {
            // TODO
            return -1;
          }
        } else {
          if (true/*TODO: is same fset*/) {
            // No need to copy ---------------------
          } else {
            // copy from remote >>>>>>>>>>>>>.
          }
        }
      }
    }
#endif
  }

  return 0;
}

static bool tsdbIsTowFSetSame(SDFileSet *pSet1, SDFileSet *pSet2) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFile1 = TSDB_DFILE_IN_SET(pSet1, ftype);
    SDFile *pDFile2 = TSDB_DFILE_IN_SET(pSet2, ftype);

    if (memcmp((void *)(TSDB_FILE_INFO(pDFile1)), (void *)(TSDB_FILE_INFO(pDFile2)), sizeof(SDFInfo)) != 0) {
      return false;
    }
  }

  return true;
}

static int tsdbSyncSendDFileSet(SSyncH *pSynch, SDFileSet *pSet) {
  bool toSend = false;

  if (tsdbSendDFileSetInfo(pSynch, pSet) < 0) {
    // TODO
    return -1;
  }

  // No file any more, no need to send file, just return
  if (pSet == NULL) {
    return 0;
  }

  if (tsdbRecvDecision(pSynch, &toSend) < 0) {
    return -1;
  }

  if (toSend) {
    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile df = *TSDB_DFILE_IN_SET(pSet, ftype);

      if (tsdbOpenDFile(&df, O_RDONLY) < 0) {
        return -1;
      }

      if (taosSendFile(pSynch->socketFd, TSDB_FILE_FD(&df), 0, df.info.size) < df.info.size) {
        tsdbCloseDFile(&df);
        return -1;
      }

      tsdbCloseDFile(&df);
    }
  }

  return 0;
}

static int tsdbSendDFileSetInfo(SSyncH *pSynch, SDFileSet *pSet) {
  uint32_t tlen = 0;

  if (pSet) {
    tlen = tsdbEncodeDFileSetEx(NULL, pSet) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen + sizeof(tlen)) < 0) {
    return -1;
  }

  void *ptr = SYNC_BUFFER(pSynch);
  taosEncodeFixedU32(&ptr, tlen);
  void *tptr = ptr;
  if (pSet) {
    tsdbEncodeDFileSetEx(&ptr, pSet);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, tlen);
  }

  if (taosWriteMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen + sizeof(tlen)) < tlen + sizeof(tlen)) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tsdbRecvDFileSetInfo(SSyncH *pSynch) {
  uint32_t tlen;
  char     buf[64] = "\0";

  if (taosReadMsg(pSynch->socketFd, buf, sizeof(tlen)) < sizeof(tlen)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  taosDecodeFixedU32(buf, &tlen);

  if (tlen == 0) {
    pSynch->pdf = NULL;
    return 0;
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen) < 0) {
    return -1;
  }

  if (taosReadMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen) < tlen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)SYNC_BUFFER(pSynch), tlen)) {
    terrno = TSDB_CODE_TDB_MESSED_MSG;
    return -1;
  }

  pSynch->pdf = &(pSynch->df);
  tsdbDecodeDFileSetEx(SYNC_BUFFER(pSynch), pSynch->pdf);

  return 0;
}
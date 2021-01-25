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
#include "os.h"
#include "taoserror.h"
#include "tsdbint.h"

// Sync handle
typedef struct {
  STsdbRepo *pRepo;
  SRtn       rtn;
  int32_t    socketFd;
  void *     pBuf;
  bool       mfChanged;
  SMFile *   pmf;
  SMFile     mf;
  SDFileSet  df;
  SDFileSet *pdf;
} SSyncH;

#define SYNC_BUFFER(sh) ((sh)->pBuf)

static void    tsdbInitSyncH(SSyncH *pSyncH, STsdbRepo *pRepo, int32_t socketFd);
static void    tsdbDestroySyncH(SSyncH *pSyncH);
static int32_t tsdbSyncSendMeta(SSyncH *pSynch);
static int32_t tsdbSyncRecvMeta(SSyncH *pSynch);
static int32_t tsdbSendMetaInfo(SSyncH *pSynch);
static int32_t tsdbRecvMetaInfo(SSyncH *pSynch);
static int32_t tsdbSendDecision(SSyncH *pSynch, bool toSend);
static int32_t tsdbRecvDecision(SSyncH *pSynch, bool *toSend);
static int32_t tsdbSyncSendDFileSetArray(SSyncH *pSynch);
static int32_t tsdbSyncRecvDFileSetArray(SSyncH *pSynch);
static bool    tsdbIsTowFSetSame(SDFileSet *pSet1, SDFileSet *pSet2);
static int32_t tsdbSyncSendDFileSet(SSyncH *pSynch, SDFileSet *pSet);
static int32_t tsdbSendDFileSetInfo(SSyncH *pSynch, SDFileSet *pSet);
static int32_t tsdbRecvDFileSetInfo(SSyncH *pSynch);
static int     tsdbReload(STsdbRepo *pRepo, bool isMfChanged);

int32_t tsdbSyncSend(void *tsdb, int32_t socketFd) {
  STsdbRepo *pRepo = (STsdbRepo *)tsdb;
  SSyncH     synch = {0};

  tsdbInitSyncH(&synch, pRepo, socketFd);
  // Disable TSDB commit
  sem_wait(&(pRepo->readyToCommit));

  if (tsdbSyncSendMeta(&synch) < 0) {
    tsdbError("vgId:%d, failed to send metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbSyncSendDFileSetArray(&synch) < 0) {
    tsdbError("vgId:%d, failed to send filesets since %s", REPO_ID(pRepo), tstrerror(terrno));
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

int32_t tsdbSyncRecv(void *tsdb, int32_t socketFd) {
  STsdbRepo *pRepo = (STsdbRepo *)tsdb;
  SSyncH synch = {0};

  pRepo->state = TSDB_STATE_OK;

  tsdbInitSyncH(&synch, pRepo, socketFd);
  tsdbStartFSTxn(pRepo, 0, 0);

  if (tsdbSyncRecvMeta(&synch) < 0) {
    tsdbError("vgId:%d, failed to recv metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbSyncRecvDFileSetArray(&synch) < 0) {
    tsdbError("vgId:%d, failed to recv filesets since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbEndFSTxn(pRepo);
  tsdbDestroySyncH(&synch);

  // Reload file change
  tsdbReload(pRepo, synch.mfChanged);

  return 0;

_err:
  tsdbEndFSTxnWithError(REPO_FS(pRepo));
  tsdbDestroySyncH(&synch);
  return -1;
}

static void tsdbInitSyncH(SSyncH *pSyncH, STsdbRepo *pRepo, int32_t socketFd) {
  pSyncH->pRepo = pRepo;
  pSyncH->socketFd = socketFd;
  tsdbGetRtnSnap(pRepo, &(pSyncH->rtn));
}

static void tsdbDestroySyncH(SSyncH *pSyncH) { taosTZfree(pSyncH->pBuf); }

static int32_t tsdbSyncSendMeta(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  bool       toSendMeta = false;
  SMFile     mf;

  // Send meta info to remote
  tsdbInfo("vgId:%d, metainfo will be sent", REPO_ID(pRepo));
  if (tsdbSendMetaInfo(pSynch) < 0) {
    tsdbError("vgId:%d, failed to send metainfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (pRepo->fs->cstatus->pmf == NULL) {
    // No meta file, not need to wait to retrieve meta file
    tsdbInfo("vgId:%d, metafile not exist, no need to send", REPO_ID(pRepo));
    return 0;
  }

  if (tsdbRecvDecision(pSynch, &toSendMeta) < 0) {
    tsdbError("vgId:%d, failed to recv decision while send meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  if (toSendMeta) {
    tsdbInitMFileEx(&mf, pRepo->fs->cstatus->pmf);
    if (tsdbOpenMFile(&mf, O_RDONLY) < 0) {
      tsdbError("vgId:%d, failed to open file while send metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    int32_t writeLen = mf.info.size;
    tsdbInfo("vgId:%d, metafile:%s will be sent, size:%d", REPO_ID(pRepo), mf.f.aname, writeLen);

    int32_t ret = taosSendFile(pSynch->socketFd, TSDB_FILE_FD(&mf), 0, writeLen);
    if (ret != writeLen) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbError("vgId:%d, failed to send metafile since %s, ret:%d writeLen:%d", REPO_ID(pRepo), tstrerror(terrno), ret,
                writeLen);
      tsdbCloseMFile(&mf);
      return -1;
    }

    tsdbCloseMFile(&mf);
    tsdbInfo("vgId:%d, metafile is sent", REPO_ID(pRepo));
  } else {
    tsdbInfo("vgId:%d, metafile is same, no need to send", REPO_ID(pRepo));
  }

  return 0;
}

static int32_t tsdbSyncRecvMeta(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  SMFile *   pLMFile = pRepo->fs->cstatus->pmf;

  // Recv meta info from remote
  if (tsdbRecvMetaInfo(pSynch) < 0) {
    tsdbError("vgId:%d, failed to recv metainfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // No meta file, do nothing (rm local meta file)
  if (pSynch->pmf == NULL) {
    if (pLMFile == NULL) {
      pSynch->mfChanged = false;
    } else {
      pSynch->mfChanged = true;
    }
    tsdbInfo("vgId:%d, metafile not exist in remote, no need to recv", REPO_ID(pRepo));
    return 0;
  }

  if (pLMFile == NULL || memcmp(&(pSynch->pmf->info), &(pLMFile->info), sizeof(SMFInfo)) != 0) {
    // Local has no meta file or has a different meta file, need to copy from remote
    pSynch->mfChanged = true;

    if (tsdbSendDecision(pSynch, true) < 0) {
      tsdbError("vgId:%d, failed to send decision while recv metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    tsdbInfo("vgId:%d, metafile will be received", REPO_ID(pRepo));

    // Recv from remote
    SMFile  mf;
    SDiskID did = {.level = TFS_PRIMARY_LEVEL, .id = TFS_PRIMARY_ID};
    tsdbInitMFile(&mf, did, REPO_ID(pRepo), FS_TXN_VERSION(REPO_FS(pRepo)));
    if (tsdbCreateMFile(&mf, false) < 0) {
      tsdbError("vgId:%d, failed to create file while recv metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }

    tsdbInfo("vgId:%d, metafile:%s is created", REPO_ID(pRepo), mf.f.aname);

    int32_t readLen = pSynch->pmf->info.size;
    int32_t ret = taosCopyFds(pSynch->socketFd, TSDB_FILE_FD(&mf), readLen);
    if (ret != readLen) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbError("vgId:%d, failed to recv metafile since %s, ret:%d readLen:%d", REPO_ID(pRepo), tstrerror(terrno), ret,
                readLen);
      tsdbCloseMFile(&mf);
      tsdbRemoveMFile(&mf);
      return -1;
    }

    tsdbInfo("vgId:%d, metafile is received, size:%d", REPO_ID(pRepo), readLen);

    mf.info = pSynch->pmf->info;
    tsdbCloseMFile(&mf);
    tsdbUpdateMFile(REPO_FS(pRepo), &mf);
  } else {
    pSynch->mfChanged = false;
    tsdbInfo("vgId:%d, metafile is same, no need to recv", REPO_ID(pRepo));
    if (tsdbSendDecision(pSynch, false) < 0) {
      tsdbError("vgId:%d, failed to send decision while recv metafile since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
    tsdbUpdateMFile(REPO_FS(pRepo), pLMFile);
  }

  return 0;
}

static int32_t tsdbSendMetaInfo(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint32_t   tlen = 0;
  SMFile *   pMFile = pRepo->fs->cstatus->pmf;

  if (pMFile) {
    tlen = tlen + tsdbEncodeSMFileEx(NULL, pMFile) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen + sizeof(tlen)) < 0) {
    tsdbError("vgId:%d, failed to makeroom while send metainfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  void *ptr = SYNC_BUFFER(pSynch);
  taosEncodeFixedU32(&ptr, tlen);
  void *tptr = ptr;
  if (pMFile) {
    tsdbEncodeSMFileEx(&ptr, pMFile);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, tlen);
  }

  int32_t writeLen = tlen + sizeof(uint32_t);
  int32_t ret = taosWriteMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), writeLen);
  if (ret != writeLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to send metainfo since %s, ret:%d writeLen:%d", REPO_ID(pRepo), tstrerror(terrno), ret,
              writeLen);
    return -1;
  }

  tsdbInfo("vgId:%d, metainfo is sent, tlen:%d, writeLen:%d", REPO_ID(pRepo), tlen, writeLen);
  return 0;
}

static int32_t tsdbRecvMetaInfo(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint32_t   tlen = 0;
  char       buf[64] = {0};

  int32_t readLen = sizeof(uint32_t);
  int32_t ret = taosReadMsg(pSynch->socketFd, buf, readLen);
  if (ret != readLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to recv metalen, ret:%d readLen:%d", REPO_ID(pRepo), ret, readLen);
    return -1;
  }

  taosDecodeFixedU32(buf, &tlen);

  tsdbInfo("vgId:%d, metalen is received, readLen:%d, tlen:%d", REPO_ID(pRepo), readLen, tlen);
  if (tlen == 0) {
    pSynch->pmf = NULL;
    return 0;
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen) < 0) {
    tsdbError("vgId:%d, failed to makeroom while recv metainfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  ret = taosReadMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen);
  if (ret != tlen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to recv metainfo, ret:%d tlen:%d", REPO_ID(pRepo), ret, tlen);
    return -1;
  }

  tsdbInfo("vgId:%d, metainfo is received, tlen:%d", REPO_ID(pRepo), tlen);
  if (!taosCheckChecksumWhole((uint8_t *)SYNC_BUFFER(pSynch), tlen)) {
    terrno = TSDB_CODE_TDB_MESSED_MSG;
    tsdbError("vgId:%d, failed to checksum while recv metainfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  pSynch->pmf = &(pSynch->mf);
  tsdbDecodeSMFileEx(SYNC_BUFFER(pSynch), pSynch->pmf);

  return 0;
}

static int32_t tsdbSendDecision(SSyncH *pSynch, bool toSend) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint8_t    decision = toSend;

  int32_t writeLen = sizeof(uint8_t);
  int32_t ret = taosWriteMsg(pSynch->socketFd, (void *)(&decision), writeLen);
  if (ret != writeLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to send decison, ret:%d writeLen:%d", REPO_ID(pRepo), ret, writeLen);
    return -1;
  }

  return 0;
}

static int32_t tsdbRecvDecision(SSyncH *pSynch, bool *toSend) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint8_t    decision = 0;

  int32_t readLen = sizeof(uint8_t);
  int32_t ret = taosReadMsg(pSynch->socketFd, (void *)(&decision), readLen);
  if (ret != readLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to recv decison, ret:%d readLen:%d", REPO_ID(pRepo), ret, readLen);
    return -1;
  }

  *toSend = decision;
  return 0;
}

static int32_t tsdbSyncSendDFileSetArray(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSIter    fsiter;
  SDFileSet *pSet;

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);

  do {
    pSet = tsdbFSIterNext(&fsiter);
    if (tsdbSyncSendDFileSet(pSynch, pSet) < 0) {
      tsdbError("vgId:%d, failed to send fileset:%d since %s", REPO_ID(pRepo), pSet ? pSet->fid : -1,
                tstrerror(terrno));
      return -1;
    }

    // No more file set to send, jut break
    if (pSet == NULL) {
      tsdbInfo("vgId:%d, no filesets any more", REPO_ID(pRepo));
      break;
    }
  } while (true);

  return 0;
}

static int32_t tsdbSyncRecvDFileSetArray(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  STsdbFS *  pfs = REPO_FS(pRepo);
  SFSIter    fsiter;
  SDFileSet *pLSet;  // Local file set

  tsdbFSIterInit(&fsiter, pfs, TSDB_FS_ITER_FORWARD);

  pLSet = tsdbFSIterNext(&fsiter);
  if (tsdbRecvDFileSetInfo(pSynch) < 0) {
    tsdbError("vgId:%d, failed to recv fileset since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  while (true) {
    if (pLSet == NULL && pSynch->pdf == NULL) {
      tsdbInfo("vgId:%d, all filesets is disposed", REPO_ID(pRepo));
      break;
    } else {
      tsdbInfo("vgId:%d, fileset local:%d remote:%d, will be disposed", REPO_ID(pRepo), pLSet != NULL ? pLSet->fid : -1,
               pSynch->pdf != NULL ? pSynch->pdf->fid : -1);
    }

    if (pLSet && (pSynch->pdf == NULL || pLSet->fid < pSynch->pdf->fid)) {
      // remote not has pLSet->fid set, just remove local (do nothing to remote the fset)
      tsdbInfo("vgId:%d, fileset:%d smaller than remote:%d, remove it", REPO_ID(pRepo), pLSet->fid,
               pSynch->pdf != NULL ? pSynch->pdf->fid : -1);
      pLSet = tsdbFSIterNext(&fsiter);
    } else {
      if (pLSet && pSynch->pdf && pLSet->fid == pSynch->pdf->fid && tsdbIsTowFSetSame(pLSet, pSynch->pdf)) {
        // Just keep local files and notify remote not to send
        tsdbInfo("vgId:%d, fileset:%d is same and no need to recv", REPO_ID(pRepo), pLSet->fid);

        if (tsdbUpdateDFileSet(pfs, pLSet) < 0) {
          tsdbError("vgId:%d, failed to update fileset since %s", REPO_ID(pRepo), tstrerror(terrno));
          return -1;
        }

        if (tsdbSendDecision(pSynch, false) < 0) {
          tsdbError("vgId:%d, filed to send decision since %s", REPO_ID(pRepo), tstrerror(terrno));
          return -1;
        }
      } else {
        // Need to copy from remote
        tsdbInfo("vgId:%d, fileset:%d will be received", REPO_ID(pRepo), pSynch->pdf->fid);

        // Notify remote to send there file here
        if (tsdbSendDecision(pSynch, true) < 0) {
          tsdbError("vgId:%d, failed to send decision since %s", REPO_ID(pRepo), tstrerror(terrno));
          return -1;
        }

        // Create local files and copy from remote
        SDiskID   did;
        SDFileSet fset;

        tfsAllocDisk(tsdbGetFidLevel(pSynch->pdf->fid, &(pSynch->rtn)), &(did.level), &(did.id));
        if (did.level == TFS_UNDECIDED_LEVEL) {
          terrno = TSDB_CODE_TDB_NO_AVAIL_DISK;
          tsdbError("vgId:%d, failed allc disk since %s", REPO_ID(pRepo), tstrerror(terrno));
          return -1;
        }

        tsdbInitDFileSet(&fset, did, REPO_ID(pRepo), pSynch->pdf->fid, FS_TXN_VERSION(pfs));

        // Create new FSET
        if (tsdbCreateDFileSet(&fset, false) < 0) {
          tsdbError("vgId:%d, failed to create fileset since %s", REPO_ID(pRepo), tstrerror(terrno));
          return -1;
        }

        for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
          SDFile *pDFile = TSDB_DFILE_IN_SET(&fset, ftype);         // local file
          SDFile *pRDFile = TSDB_DFILE_IN_SET(pSynch->pdf, ftype);  // remote file

          tsdbInfo("vgId:%d, file:%s will be received, osize:%" PRIu64 " rsize:%" PRIu64, REPO_ID(pRepo),
                   pDFile->f.aname, pDFile->info.size, pRDFile->info.size);

          int32_t writeLen = pRDFile->info.size;
          int32_t ret = taosCopyFds(pSynch->socketFd, pDFile->fd, writeLen);
          if (ret != writeLen) {
            terrno = TAOS_SYSTEM_ERROR(errno);
            tsdbError("vgId:%d, failed to recv file:%s since %s, ret:%d writeLen:%d", REPO_ID(pRepo), pDFile->f.aname,
                      tstrerror(terrno), ret, writeLen);
            tsdbCloseDFileSet(&fset);
            tsdbRemoveDFileSet(&fset);
            return -1;
          }

          // Update new file info
          pDFile->info = pRDFile->info;
          tsdbInfo("vgId:%d, file:%s is received, size:%d", REPO_ID(pRepo), pDFile->f.aname, writeLen);
        }

        tsdbCloseDFileSet(&fset);
        if (tsdbUpdateDFileSet(pfs, &fset) < 0) {
          tsdbInfo("vgId:%d, fileset:%d failed to update since %s", REPO_ID(pRepo), fset.fid, tstrerror(terrno));
          return -1;
        }

        tsdbInfo("vgId:%d, fileset:%d is received", REPO_ID(pRepo), pSynch->pdf->fid);
      }

      // Move forward
      if (tsdbRecvDFileSetInfo(pSynch) < 0) {
        tsdbError("vgId:%d, failed to recv fileset since %s", REPO_ID(pRepo), tstrerror(terrno));
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

static int32_t tsdbSyncSendDFileSet(SSyncH *pSynch, SDFileSet *pSet) {
  STsdbRepo *pRepo = pSynch->pRepo;
  bool toSend = false;

  if (tsdbSendDFileSetInfo(pSynch, pSet) < 0) {
    tsdbError("vgId:%d, failed to send fileset:%d info since %s", REPO_ID(pRepo), pSet->fid, tstrerror(terrno));
    return -1;
  }

  // No file any more, no need to send file, just return
  if (pSet == NULL) {
    return 0;
  }

  if (tsdbRecvDecision(pSynch, &toSend) < 0) {
    tsdbError("vgId:%d, failed to recv decision while send fileset:%d since %s", REPO_ID(pRepo), pSet->fid,
              tstrerror(terrno));
    return -1;
  }

  if (toSend) {
    tsdbInfo("vgId:%d, fileset:%d will be sent", REPO_ID(pRepo), pSet->fid);

    for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
      SDFile df = *TSDB_DFILE_IN_SET(pSet, ftype);
      
      if (tsdbOpenDFile(&df, O_RDONLY) < 0) {
        tsdbError("vgId:%d, failed to file:%s since %s", REPO_ID(pRepo), df.f.aname, tstrerror(terrno));
        return -1;
      }

      int32_t writeLen = df.info.size;
      tsdbInfo("vgId:%d, file:%s will be sent, size:%d", REPO_ID(pRepo), df.f.aname, writeLen);

      int32_t ret = taosSendFile(pSynch->socketFd, TSDB_FILE_FD(&df), 0, writeLen);
      if (ret != writeLen) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        tsdbError("vgId:%d, failed to send file:%s since %s, ret:%d writeLen:%d", REPO_ID(pRepo), df.f.aname,
                  tstrerror(terrno), ret, writeLen);
        tsdbCloseDFile(&df);
        return -1;
      }

      tsdbInfo("vgId:%d, file:%s is sent", REPO_ID(pRepo), df.f.aname);
      tsdbCloseDFile(&df);
    }

    tsdbInfo("vgId:%d, fileset:%d is sent", REPO_ID(pRepo), pSet->fid);
  } else {
    tsdbInfo("vgId:%d, fileset:%d is same, no need to send", REPO_ID(pRepo), pSet->fid);
  }

  return 0;
}

static int32_t tsdbSendDFileSetInfo(SSyncH *pSynch, SDFileSet *pSet) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint32_t   tlen = 0;

  if (pSet) {
    tlen = tsdbEncodeDFileSetEx(NULL, pSet) + sizeof(TSCKSUM);
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen + sizeof(tlen)) < 0) {
    tsdbError("vgId:%d, failed to makeroom while send fileinfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  void *ptr = SYNC_BUFFER(pSynch);
  taosEncodeFixedU32(&ptr, tlen);
  void *tptr = ptr;
  if (pSet) {
    tsdbEncodeDFileSetEx(&ptr, pSet);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, tlen);
  }

  int32_t writeLen = tlen + sizeof(uint32_t);
  int32_t ret = taosWriteMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), writeLen);
  if (ret != writeLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to send fileinfo, ret:%d writeLen:%d", REPO_ID(pRepo), ret, writeLen);
    return -1;
  }

  return 0;
}

static int32_t tsdbRecvDFileSetInfo(SSyncH *pSynch) {
  STsdbRepo *pRepo = pSynch->pRepo;
  uint32_t   tlen;
  char       buf[64] = {0};

  int32_t readLen = sizeof(uint32_t);
  int32_t ret = taosReadMsg(pSynch->socketFd, buf, readLen);
  if (ret != readLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  taosDecodeFixedU32(buf, &tlen);

  tsdbInfo("vgId:%d, fileinfo len:%d is received", REPO_ID(pRepo), tlen);
  if (tlen == 0) {
    pSynch->pdf = NULL;
    return 0;
  }

  if (tsdbMakeRoom((void **)(&SYNC_BUFFER(pSynch)), tlen) < 0) {
    tsdbError("vgId:%d, failed to makeroom while recv fileinfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  ret = taosReadMsg(pSynch->socketFd, SYNC_BUFFER(pSynch), tlen);
  if (ret != tlen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tsdbError("vgId:%d, failed to recv fileinfo, ret:%d readLen:%d", REPO_ID(pRepo), ret, tlen);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)SYNC_BUFFER(pSynch), tlen)) {
    terrno = TSDB_CODE_TDB_MESSED_MSG;
    tsdbError("vgId:%d, failed to checksum while recv fileinfo since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  pSynch->pdf = &(pSynch->df);
  tsdbDecodeDFileSetEx(SYNC_BUFFER(pSynch), pSynch->pdf);

  return 0;
}

static int tsdbReload(STsdbRepo *pRepo, bool isMfChanged) {
  // TODO: may need to stop and restart stream
  if (isMfChanged) {
    tsdbCloseMeta(pRepo);
    tsdbFreeMeta(pRepo->tsdbMeta);
    pRepo->tsdbMeta = tsdbNewMeta(REPO_CFG(pRepo));
    tsdbOpenMeta(pRepo);
    tsdbLoadMetaCache(pRepo, true);
  }

  tsdbUnRefMemTable(pRepo, pRepo->mem);
  tsdbUnRefMemTable(pRepo, pRepo->imem);
  pRepo->mem = NULL;
  pRepo->imem = NULL;

  if (tsdbRestoreInfo(pRepo) < 0) {
    tsdbError("vgId:%d failed to restore info from file since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}
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
#include "tfsInt.h"

static int32_t   tfsMount(STfs *pTfs, SDiskCfg *pCfg);
static int32_t   tfsCheck(STfs *pTfs);
static int32_t   tfsCheckAndFormatCfg(STfs *pTfs, SDiskCfg *pCfg);
static int32_t   tfsFormatDir(char *idir, char *odir);
static STfsDisk *tfsGetDiskByName(STfs *pTfs, const char *dir);
static int32_t   tfsOpendirImpl(STfs *pTfs, STfsDir *pDir);
static STfsDisk *tfsNextDisk(STfs *pTfs, SDiskIter *pIter);

STfs *tfsOpen(SDiskCfg *pCfg, int32_t ndisk) {
  if (ndisk <= 0 || ndisk > TFS_MAX_DISKS) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  STfs *pTfs = taosMemoryCalloc(1, sizeof(STfs));
  if (pTfs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (taosThreadSpinInit(&pTfs->lock, 0) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tfsClose(pTfs);
    return NULL;
  }

  for (int32_t level = 0; level < TFS_MAX_TIERS; level++) {
    STfsTier *pTier = &pTfs->tiers[level];
    if (tfsInitTier(pTier, level) < 0) {
      tfsClose(pTfs);
      return NULL;
    }
  }

  pTfs->hash = taosHashInit(TFS_MAX_DISKS * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pTfs->hash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tfsClose(pTfs);
    return NULL;
  }

  for (int32_t idisk = 0; idisk < ndisk; idisk++) {
    if (tfsMount(pTfs, &pCfg[idisk]) < 0) {
      tfsClose(pTfs);
      return NULL;
    }
  }

  if (tfsCheck(pTfs) < 0) {
    tfsClose(pTfs);
    return NULL;
  }

  tfsUpdateSize(pTfs);
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    tfsPosNextId(&pTfs->tiers[level]);
  }

  return pTfs;
}

void tfsClose(STfs *pTfs) {
  if (pTfs == NULL) return;

  for (int32_t level = 0; level < TFS_MAX_LEVEL; level++) {
    tfsDestroyTier(&pTfs->tiers[level]);
  }

  taosHashCleanup(pTfs->hash);
  taosThreadSpinDestroy(&pTfs->lock);
  taosMemoryFree(pTfs);
}

void tfsUpdateSize(STfs *pTfs) {
  SDiskSize size = {0};

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = &pTfs->tiers[level];
    tfsUpdateTierSize(pTier);
    size.total += pTier->size.total;
    size.avail += pTier->size.avail;
    size.used += pTier->size.used;
  }

  tfsLock(pTfs);
  pTfs->size = size;
  tfsUnLock(pTfs);
}

SDiskSize tfsGetSize(STfs *pTfs) {
  tfsLock(pTfs);
  SDiskSize size = pTfs->size;
  tfsUnLock(pTfs);

  return size;
}

int32_t tfsAllocDisk(STfs *pTfs, int32_t expLevel, SDiskID *pDiskId) {
  pDiskId->level = expLevel;
  pDiskId->id = -1;

  if (pDiskId->level >= pTfs->nlevel) {
    pDiskId->level = pTfs->nlevel - 1;
  }

  if (pDiskId->level < 0) {
    pDiskId->level = 0;
  }

  while (pDiskId->level >= 0) {
    pDiskId->id = tfsAllocDiskOnTier(&pTfs->tiers[pDiskId->level]);
    if (pDiskId->id < 0) {
      pDiskId->level--;
      continue;
    }

    return 0;
  }

  terrno = TSDB_CODE_FS_NO_VALID_DISK;
  return -1;
}

const char *tfsGetPrimaryPath(STfs *pTfs) { return TFS_PRIMARY_DISK(pTfs)->path; }

const char *tfsGetDiskPath(STfs *pTfs, SDiskID diskId) { return TFS_DISK_AT(pTfs, diskId)->path; }

void tfsInitFile(STfs *pTfs, STfsFile *pFile, SDiskID diskId, const char *rname) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  if (pDisk == NULL) return;

  pFile->did = diskId;
  tstrncpy(pFile->rname, rname, TSDB_FILENAME_LEN);

  char tmpName[TMPNAME_LEN] = {0};
  snprintf(tmpName, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
  tstrncpy(pFile->aname, tmpName, TSDB_FILENAME_LEN);
  pFile->pTfs = pTfs;
}

bool tfsIsSameFile(const STfsFile *pFile1, const STfsFile *pFile2) {
  if (pFile1 == NULL || pFile2 == NULL || pFile1->pTfs != pFile2->pTfs) return false;
  if (pFile1->did.level != pFile2->did.level) return false;
  if (pFile1->did.id != pFile2->did.id) return false;
  if (strncmp(pFile1->rname, pFile2->rname, TSDB_FILENAME_LEN) != 0) return false;
  return true;
}

int32_t tfsEncodeFile(void **buf, STfsFile *pFile) {
  int32_t tlen = 0;

  tlen += taosEncodeVariantI32(buf, pFile->did.level);
  tlen += taosEncodeVariantI32(buf, pFile->did.id);
  tlen += taosEncodeString(buf, pFile->rname);

  return tlen;
}

void *tfsDecodeFile(STfs *pTfs, void *buf, STfsFile *pFile) {
  SDiskID diskId = {0};
  char   *rname = NULL;

  buf = taosDecodeVariantI32(buf, &diskId.level);
  buf = taosDecodeVariantI32(buf, &diskId.id);
  buf = taosDecodeString(buf, &rname);

  tfsInitFile(pTfs, pFile, diskId, rname);

  taosMemoryFreeClear(rname);
  return buf;
}

void tfsBasename(const STfsFile *pFile, char *dest) {
  char tname[TSDB_FILENAME_LEN] = "\0";

  tstrncpy(tname, pFile->aname, TSDB_FILENAME_LEN);
  tstrncpy(dest, taosDirEntryBaseName(tname), TSDB_FILENAME_LEN);
}

void tfsDirname(const STfsFile *pFile, char *dest) {
  char tname[TSDB_FILENAME_LEN] = "\0";

  tstrncpy(tname, pFile->aname, TSDB_FILENAME_LEN);
  tstrncpy(dest, taosDirName(tname), TSDB_FILENAME_LEN);
}

void tfsAbsoluteName(STfs *pTfs, SDiskID diskId, const char *rname, char *aname) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  
  snprintf(aname, TSDB_FILENAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
}

int32_t tfsRemoveFile(const STfsFile *pFile) { return taosRemoveFile(pFile->aname); }

int32_t tfsCopyFile(const STfsFile *pFile1, const STfsFile *pFile2) {
  return taosCopyFile(pFile1->aname, pFile2->aname);
}

int32_t tfsMkdirAt(STfs *pTfs, const char *rname, SDiskID diskId) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  char      aname[TMPNAME_LEN];

  snprintf(aname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
  if (taosMkDir(aname) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int32_t tfsMkdirRecurAt(STfs *pTfs, const char *rname, SDiskID diskId) {
  if (tfsMkdirAt(pTfs, rname, diskId) < 0) {
    if (errno == ENOENT) {
      // Try to create upper
      char *s = strdup(rname);

      // Make a copy of dirname(s) because the implementation of 'dirname' differs on different platforms.
      // Some platform may modify the contents of the string passed into dirname(). Others may return a pointer to
      // internal static storage space that will be overwritten by next call. For case like that, we should not use
      // the pointer directly in this recursion.
      // See
      // https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/dirname.3.html
      char *dir = strdup(taosDirName(s));

      if (tfsMkdirRecurAt(pTfs, dir, diskId) < 0) {
        taosMemoryFree(s);
        taosMemoryFree(dir);
        return -1;
      }
      taosMemoryFree(s);
      taosMemoryFree(dir);

      if (tfsMkdirAt(pTfs, rname, diskId) < 0) {
        return -1;
      }
    } else {
      return -1;
    }
  }

  return 0;
}

int32_t tfsMkdir(STfs *pTfs, const char *rname) {
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      SDiskID did = {.id = id, .level = level};
      if (tfsMkdirAt(pTfs, rname, did) < 0) {
        return -1;
      }
    }
  }

  return 0;
}

int32_t tfsRmdir(STfs *pTfs, const char *rname) {
  char aname[TMPNAME_LEN] = "\0";

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      STfsDisk *pDisk = pTier->disks[id];
      snprintf(aname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
      taosRemoveDir(aname);
    }
  }

  return 0;
}

int32_t tfsRename(STfs *pTfs, char *orname, char *nrname) {
  char oaname[TMPNAME_LEN] = "\0";
  char naname[TMPNAME_LEN] = "\0";

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      STfsDisk *pDisk = pTier->disks[id];
      snprintf(oaname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, orname);
      snprintf(naname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, nrname);
      if (taosRenameFile(oaname, naname) != 0 && errno != ENOENT) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        fError("failed to rename %s to %s since %s", oaname, naname, terrstr());
        return -1;
      }
    }
  }

  return 0;
}

STfsDir *tfsOpendir(STfs *pTfs, const char *rname) {
  STfsDir *pDir = taosMemoryCalloc(1, sizeof(STfsDir));
  if (pDir == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SDiskID diskId = {.id = 0, .level = 0};
  pDir->iter.pDisk = TFS_DISK_AT(pTfs, diskId);
  pDir->pTfs = pTfs;
  tstrncpy(pDir->dirname, rname, TSDB_FILENAME_LEN);

  if (tfsOpendirImpl(pTfs, pDir) < 0) {
    taosMemoryFree(pDir);
    return NULL;
  }

  return pDir;
}

const STfsFile *tfsReaddir(STfsDir *pTfsDir) {
  if (pTfsDir == NULL || pTfsDir->pDir == NULL) return NULL;
  char bname[TMPNAME_LEN * 2] = "\0";

  while (true) {
    TdDirEntryPtr pDirEntry = NULL;
    pDirEntry = taosReadDir(pTfsDir->pDir);
    if (pDirEntry != NULL) {
      // Skip . and ..
      char *name = taosGetDirEntryName(pDirEntry);
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

      if (pTfsDir->dirname == NULL || pTfsDir->dirname[0] == 0) {
        snprintf(bname, TMPNAME_LEN * 2, "%s", name);
      } else {
        snprintf(bname, TMPNAME_LEN * 2, "%s%s%s", pTfsDir->dirname, TD_DIRSEP, name);
      }

      tfsInitFile(pTfsDir->pTfs, &pTfsDir->tfile, pTfsDir->did, bname);
      return &pTfsDir->tfile;
    }

    if (tfsOpendirImpl(pTfsDir->pTfs, pTfsDir) < 0) {
      return NULL;
    }

    if (pTfsDir->pDir == NULL) {
      terrno = TSDB_CODE_SUCCESS;
      return NULL;
    }
  }
}

void tfsClosedir(STfsDir *pTfsDir) {
  if (pTfsDir) {
    if (pTfsDir->pDir != NULL) {
      taosCloseDir(pTfsDir->pDir);
      pTfsDir->pDir = NULL;
    }
    taosMemoryFree(pTfsDir);
  }
}

static int32_t tfsMount(STfs *pTfs, SDiskCfg *pCfg) {
  if (tfsCheckAndFormatCfg(pTfs, pCfg) < 0) {
    return -1;
  }

  SDiskID   did = {.level = pCfg->level};
  STfsDisk *pDisk = tfsMountDiskToTier(TFS_TIER_AT(pTfs, did.level), pCfg);
  if (pDisk == NULL) {
    fError("failed to mount disk %s to level %d since %s", pCfg->dir, pCfg->level, terrstr());
    return -1;
  }
  did.id = pDisk->id;

  taosHashPut(pTfs->hash, (void *)(pCfg->dir), strnlen(pCfg->dir, TSDB_FILENAME_LEN), (void *)(&did), sizeof(did));
  if (pTfs->nlevel < pCfg->level + 1) {
    pTfs->nlevel = pCfg->level + 1;
  }

  return 0;
}

static int32_t tfsCheckAndFormatCfg(STfs *pTfs, SDiskCfg *pCfg) {
  char dirName[TSDB_FILENAME_LEN] = "\0";

  if (pCfg->level < 0 || pCfg->level >= TFS_MAX_TIERS) {
    fError("failed to mount %s to FS since invalid level %d", pCfg->dir, pCfg->level);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (pCfg->primary) {
    if (pCfg->level != 0) {
      fError("failed to mount %s to FS since disk is primary but level %d not 0", pCfg->dir, pCfg->level);
      terrno = TSDB_CODE_FS_INVLD_CFG;
      return -1;
    }

    if (TFS_PRIMARY_DISK(pTfs) != NULL) {
      fError("failed to mount %s to FS since duplicate primary mount", pCfg->dir);
      terrno = TSDB_CODE_FS_DUP_PRIMARY;
      return -1;
    }
  }

  if (tfsFormatDir(pCfg->dir, dirName) < 0) {
    fError("failed to mount %s to FS since %s", pCfg->dir, terrstr());
    return -1;
  }

  if (tfsGetDiskByName(pTfs, dirName) != NULL) {
    fError("failed to mount %s to FS since duplicate mount", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (!taosCheckAccessFile(dirName, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
    fError("failed to mount %s to FS since no R/W access rights", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (!taosIsDir(dirName)) {
    fError("failed to mount %s to FS since not a directory", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  strncpy(pCfg->dir, dirName, TSDB_FILENAME_LEN);

  return 0;
}

static int32_t tfsFormatDir(char *idir, char *odir) {
  wordexp_t wep = {0};

  int32_t code = wordexp(idir, &wep, 0);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  char tmp[PATH_MAX] = {0};
  if (realpath(wep.we_wordv[0], tmp) == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wordfree(&wep);
    return -1;
  }
  strcpy(odir, tmp);

  wordfree(&wep);
  return 0;
}

static int32_t tfsCheck(STfs *pTfs) {
  if (TFS_PRIMARY_DISK(pTfs) == NULL) {
    fError("no primary disk is set");
    terrno = TSDB_CODE_FS_NO_PRIMARY_DISK;
    return -1;
  }

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    if (TFS_TIER_AT(pTfs, level)->ndisk == 0) {
      fError("no disk at level %d", level);
      terrno = TSDB_CODE_FS_NO_MOUNT_AT_TIER;
      return -1;
    }
  }

  return 0;
}

static STfsDisk *tfsGetDiskByName(STfs *pTfs, const char *dir) {
  void *pr = taosHashGet(pTfs->hash, (void *)dir, strnlen(dir, TSDB_FILENAME_LEN));
  if (pr == NULL) return NULL;

  SDiskID   did = *(SDiskID *)pr;
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, did);

  return pDisk;
}

static int32_t tfsOpendirImpl(STfs *pTfs, STfsDir *pTfsDir) {
  STfsDisk *pDisk = NULL;
  char      adir[TMPNAME_LEN * 2] = "\0";

  if (pTfsDir->pDir != NULL) {
    taosCloseDir(pTfsDir->pDir);
    pTfsDir->pDir = NULL;
  }

  while (true) {
    pDisk = tfsNextDisk(pTfs, &pTfsDir->iter);
    if (pDisk == NULL) return 0;

    pTfsDir->did.level = pDisk->level;
    pTfsDir->did.id = pDisk->id;

    if (pDisk->path == NULL || pDisk->path[0] == 0) {
      snprintf(adir, TMPNAME_LEN * 2, "%s", pTfsDir->dirname);
    } else {
      snprintf(adir, TMPNAME_LEN * 2, "%s%s%s", pDisk->path, TD_DIRSEP, pTfsDir->dirname);
    }
    pTfsDir->pDir = taosOpenDir(adir);
    if (pTfsDir->pDir != NULL) break;
  }

  return 0;
}

static STfsDisk *tfsNextDisk(STfs *pTfs, SDiskIter *pIter) {
  if (pIter == NULL) return NULL;

  STfsDisk *pDisk = pIter->pDisk;
  if (pDisk == NULL) return NULL;

  SDiskID did = {.level = pDisk->level, .id = pDisk->id + 1};

  if (did.id < TFS_TIER_AT(pTfs, did.level)->ndisk) {
    pIter->pDisk = TFS_DISK_AT(pTfs, did);
  } else {
    did.level++;
    did.id = 0;
    if (did.level < pTfs->nlevel) {
      pIter->pDisk = TFS_DISK_AT(pTfs, did);
    } else {
      pIter->pDisk = NULL;
    }
  }

  return pDisk;
}

int32_t tfsGetMonitorInfo(STfs *pTfs, SMonDiskInfo *pInfo) {
  pInfo->datadirs = taosArrayInit(32, sizeof(SMonDiskDesc));
  if (pInfo->datadirs == NULL) return -1;

  tfsUpdateSize(pTfs);

  tfsLock(pTfs);
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = &pTfs->tiers[level];
    for (int32_t disk = 0; disk < pTier->ndisk; ++disk) {
      STfsDisk    *pDisk = pTier->disks[disk];
      SMonDiskDesc dinfo = {0};
      dinfo.size = pDisk->size;
      dinfo.level = pDisk->level;
      tstrncpy(dinfo.name, pDisk->path, sizeof(dinfo.name));
      taosArrayPush(pInfo->datadirs, &dinfo);
    }
  }
  tfsUnLock(pTfs);

  return 0;
}

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
#include "osEnv.h"
#include "tfsInt.h"

static int32_t   tfsMount(STfs *pTfs, SDiskCfg *pCfg);
static int32_t   tfsCheck(STfs *pTfs);
static int32_t   tfsCheckAndFormatCfg(STfs *pTfs, SDiskCfg *pCfg);
static int32_t   tfsFormatDir(char *idir, char *odir);
static int32_t   tfsGetDiskByName(STfs *pTfs, const char *dir, STfsDisk **ppDisk);
static int32_t   tfsOpendirImpl(STfs *pTfs, STfsDir *pDir);
static STfsDisk *tfsNextDisk(STfs *pTfs, SDiskIter *pIter);

int32_t tfsOpen(SDiskCfg *pCfg, int32_t ndisk, STfs **ppTfs) {
  int32_t code = 0;
  int32_t lino = 0;
  STfs   *pTfs = NULL;

  if (ndisk <= 0 || ndisk > TFS_MAX_DISKS) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PARA, &lino, _exit);
  }

  pTfs = taosMemoryCalloc(1, sizeof(STfs));
  if (pTfs == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  if (taosThreadSpinInit(&pTfs->lock, 0) != 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &lino, _exit);
  }

  for (int32_t level = 0; level < TFS_MAX_TIERS; level++) {
    STfsTier *pTier = &pTfs->tiers[level];
    TAOS_CHECK_GOTO(tfsInitTier(pTier, level), &lino, _exit);
  }

  pTfs->hash = taosHashInit(TFS_MAX_DISKS * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pTfs->hash == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  for (int32_t idisk = 0; idisk < ndisk; idisk++) {
    TAOS_CHECK_GOTO(tfsMount(pTfs, &pCfg[idisk]), &lino, _exit);
  }

  TAOS_CHECK_GOTO(tfsCheck(pTfs) < 0, &lino, _exit);

  tfsUpdateSize(pTfs);
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    tfsPosNextId(&pTfs->tiers[level]);
  }

_exit:
  if (code != 0) {
    tfsClose(pTfs);
    pTfs = NULL;
  }
  *ppTfs = pTfs;
  TAOS_RETURN(code);
}

void tfsClose(STfs *pTfs) {
  if (pTfs == NULL) return;

  for (int32_t level = 0; level <= TFS_MAX_LEVEL; level++) {
    tfsDestroyTier(&pTfs->tiers[level]);
  }

  taosHashCleanup(pTfs->hash);
  (void)taosThreadSpinDestroy(&pTfs->lock);
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

  (void)tfsLock(pTfs);
  pTfs->size = size;
  (void)tfsUnLock(pTfs);
}

SDiskSize tfsGetSize(STfs *pTfs) {
  (void)tfsLock(pTfs);
  SDiskSize size = pTfs->size;
  (void)tfsUnLock(pTfs);

  return size;
}

bool tfsDiskSpaceAvailable(STfs *pTfs, int32_t level) {
  if (level < 0 || level >= pTfs->nlevel) {
    return false;
  }
  STfsTier *pTier = TFS_TIER_AT(pTfs, level);
  for (int32_t id = 0; id < pTier->ndisk; id++) {
    SDiskID   diskId = {.level = level, .id = id};
    STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
    if (pDisk == NULL) {
      return false;
    }
    if (pDisk->size.avail <= 0) {
      fError("tfs disk space unavailable. level:%d, disk:%d, path:%s", level, id, pDisk->path);
      return false;
    }
  }
  return true;
}

bool tfsDiskSpaceSufficient(STfs *pTfs, int32_t level, int32_t disk) {
  if (level < 0 || level >= pTfs->nlevel) {
    return false;
  }

  STfsTier *pTier = TFS_TIER_AT(pTfs, level);
  if (disk < 0 || disk >= pTier->ndisk) {
    return false;
  }
  SDiskID   diskId = {.level = level, .id = disk};
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  return pDisk->size.avail >= tsDataSpace.reserved;
}

int32_t tfsGetDisksAtLevel(STfs *pTfs, int32_t level) {
  if (level < 0 || level >= pTfs->nlevel) {
    TAOS_RETURN(0);
  }

  STfsTier *pTier = TFS_TIER_AT(pTfs, level);
  return pTier->ndisk;
}

int32_t tfsGetLevel(STfs *pTfs) { return pTfs->nlevel; }

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

    TAOS_RETURN(0);
  }

  TAOS_RETURN(TSDB_CODE_FS_NO_VALID_DISK);
}

const char *tfsGetPrimaryPath(STfs *pTfs) { return TFS_PRIMARY_DISK(pTfs)->path; }

const char *tfsGetDiskPath(STfs *pTfs, SDiskID diskId) { return TFS_DISK_AT(pTfs, diskId)->path; }

void tfsInitFile(STfs *pTfs, STfsFile *pFile, SDiskID diskId, const char *rname) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  if (pDisk == NULL) return;

  pFile->did = diskId;
  tstrncpy(pFile->rname, rname, TSDB_FILENAME_LEN);

  char tmpName[TMPNAME_LEN] = {0};
  (void)snprintf(tmpName, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
  tstrncpy(pFile->aname, tmpName, TSDB_FILENAME_LEN);
  pFile->pTfs = pTfs;
}

bool tfsIsSameFile(const STfsFile *pFile1, const STfsFile *pFile2) {
  if (pFile1 == NULL || pFile2 == NULL || pFile1->pTfs != pFile2->pTfs) return false;
  if (pFile1->did.level != pFile2->did.level) return false;
  if (pFile1->did.id != pFile2->did.id) return false;
  char nameBuf1[TMPNAME_LEN], nameBuf2[TMPNAME_LEN];
  (void)strncpy(nameBuf1, pFile1->rname, TMPNAME_LEN);
  (void)strncpy(nameBuf2, pFile2->rname, TMPNAME_LEN);
  nameBuf1[TMPNAME_LEN - 1] = 0;
  nameBuf2[TMPNAME_LEN - 1] = 0;
  (void)taosRealPath(nameBuf1, NULL, TMPNAME_LEN);
  (void)taosRealPath(nameBuf2, NULL, TMPNAME_LEN);
  if (strncmp(nameBuf1, nameBuf2, TMPNAME_LEN) != 0) return false;
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

  (void)snprintf(aname, TSDB_FILENAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
}

int32_t tfsRemoveFile(const STfsFile *pFile) { return taosRemoveFile(pFile->aname); }

int32_t tfsCopyFile(const STfsFile *pFile1, const STfsFile *pFile2) {
  return taosCopyFile(pFile1->aname, pFile2->aname);
}

int32_t tfsMkdirAt(STfs *pTfs, const char *rname, SDiskID diskId) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  char      aname[TMPNAME_LEN];

  if (pDisk == NULL) {
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }
  (void)snprintf(aname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
  if (taosMkDir(aname) != 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(errno));
  }

  TAOS_RETURN(0);
}

int32_t tfsMkdirRecurAt(STfs *pTfs, const char *rname, SDiskID diskId) {
  int32_t code = 0;
  int32_t lino = 0;
  char   *s = NULL;
  char   *dir = NULL;
  if ((code = tfsMkdirAt(pTfs, rname, diskId)) < 0) {
    if (errno == ENOENT) {
      // Try to create upper
      if ((s = taosStrdup(rname)) == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }

      // Make a copy of dirname(s) because the implementation of 'dirname' differs on different platforms.
      // Some platform may modify the contents of the string passed into dirname(). Others may return a pointer to
      // internal static storage space that will be overwritten by next call. For case like that, we should not use
      // the pointer directly in this recursion.
      // See
      // https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/dirname.3.html

      if ((dir = taosStrdup(taosDirName(s))) == NULL) {
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }

      if (strlen(dir) >= strlen(rname)) {  // TODO: check if it is necessary for equal length
        TAOS_CHECK_GOTO(TSDB_CODE_APP_ERROR, &lino, _exit);
      }
      TAOS_CHECK_GOTO(tfsMkdirRecurAt(pTfs, dir, diskId), &lino, _exit);
      TAOS_CHECK_GOTO(tfsMkdirAt(pTfs, rname, diskId), &lino, _exit);
    } else {
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
  } else {
    TAOS_RETURN(code);
  }

_exit:
  if (code != 0) {
    fError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(s);
  taosMemoryFree(dir);
  TAOS_RETURN(code);
}

int32_t tfsMkdirRecur(STfs *pTfs, const char *rname) {
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      SDiskID did = {.id = id, .level = level};
      TAOS_CHECK_RETURN(tfsMkdirRecurAt(pTfs, rname, did));
    }
  }

  TAOS_RETURN(0);
}

int32_t tfsMkdir(STfs *pTfs, const char *rname) {
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      SDiskID did = {.id = id, .level = level};
      TAOS_CHECK_RETURN(tfsMkdirAt(pTfs, rname, did));
    }
  }

  TAOS_RETURN(0);
}

bool tfsDirExistAt(STfs *pTfs, const char *rname, SDiskID diskId) {
  STfsDisk *pDisk = TFS_DISK_AT(pTfs, diskId);
  char      aname[TMPNAME_LEN];

  (void)snprintf(aname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
  return taosDirExist(aname);
}

int32_t tfsRmdir(STfs *pTfs, const char *rname) {
  if (rname[0] == 0) {
    TAOS_RETURN(0);
  }

  char aname[TMPNAME_LEN] = "\0";

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = 0; id < pTier->ndisk; id++) {
      STfsDisk *pDisk = pTier->disks[id];
      (void)snprintf(aname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, rname);
      uInfo("tfs remove dir:%s aname:%s rname:[%s]", pDisk->path, aname, rname);
      taosRemoveDir(aname);
    }
  }

  TAOS_RETURN(0);
}

static int32_t tfsRenameAt(STfs *pTfs, SDiskID diskId, const char *orname, const char *nrname) {
  char oaname[TMPNAME_LEN] = "\0";
  char naname[TMPNAME_LEN] = "\0";

  int32_t   level = diskId.level;
  int32_t   id = diskId.id;
  STfsTier *pTier = TFS_TIER_AT(pTfs, level);
  STfsDisk *pDisk = pTier->disks[id];
  (void)snprintf(oaname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, orname);
  (void)snprintf(naname, TMPNAME_LEN, "%s%s%s", pDisk->path, TD_DIRSEP, nrname);

  if (taosRenameFile(oaname, naname) != 0 && errno != ENOENT) {
    int32_t code = TAOS_SYSTEM_ERROR(errno);  // TODO: use return value of taosRenameFile directly
    fError("%s failed to rename %s to %s since %s", __func__, oaname, naname, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(0);
}

int32_t tfsRename(STfs *pTfs, int32_t diskPrimary, const char *orname, const char *nrname) {
  for (int32_t level = pTfs->nlevel - 1; level >= 0; level--) {
    STfsTier *pTier = TFS_TIER_AT(pTfs, level);
    for (int32_t id = pTier->ndisk - 1; id >= 0; id--) {
      if (level == 0 && id == diskPrimary) {
        continue;
      }

      SDiskID diskId = {.level = level, .id = id};
      TAOS_CHECK_RETURN(tfsRenameAt(pTfs, diskId, orname, nrname));
    }
  }

  SDiskID diskId = {.level = 0, .id = diskPrimary};
  TAOS_RETURN(tfsRenameAt(pTfs, diskId, orname, nrname));
}

int32_t tfsSearch(STfs *pTfs, int32_t level, const char *fname) {
  if (level < 0 || level >= pTfs->nlevel) {
    return -1;
  }
  char      path[TMPNAME_LEN] = {0};
  STfsTier *pTier = TFS_TIER_AT(pTfs, level);

  for (int32_t id = 0; id < pTier->ndisk; id++) {
    STfsDisk *pDisk = pTier->disks[id];
    (void)snprintf(path, TMPNAME_LEN - 1, "%s%s%s", pDisk->path, TD_DIRSEP, fname);
    if (taosCheckExistFile(path)) {
      return id;
    }
  }
  return -1;
}

int32_t tfsOpendir(STfs *pTfs, const char *rname, STfsDir **ppDir) {
  int32_t  code = 0;
  STfsDir *pDir = taosMemoryCalloc(1, sizeof(STfsDir));
  if (pDir == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _exit);
  }

  SDiskID diskId = {.id = 0, .level = 0};
  pDir->iter.pDisk = TFS_DISK_AT(pTfs, diskId);
  pDir->pTfs = pTfs;
  tstrncpy(pDir->dirName, rname, TSDB_FILENAME_LEN);

  TAOS_CHECK_GOTO(tfsOpendirImpl(pTfs, pDir), NULL, _exit);

_exit:
  if (code != 0) {
    taosMemoryFreeClear(pDir);
  }
  *ppDir = pDir;
  TAOS_RETURN(code);
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

      if (pTfsDir->dirName[0] == 0) {
        (void)snprintf(bname, TMPNAME_LEN * 2, "%s", name);
      } else {
        (void)snprintf(bname, TMPNAME_LEN * 2, "%s%s%s", pTfsDir->dirName, TD_DIRSEP, name);
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
      (void)taosCloseDir(&pTfsDir->pDir);
      pTfsDir->pDir = NULL;
    }
    taosMemoryFree(pTfsDir);
  }
}

static int32_t tfsMount(STfs *pTfs, SDiskCfg *pCfg) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_GOTO(tfsCheckAndFormatCfg(pTfs, pCfg), &lino, _exit);

  SDiskID   did = {.level = pCfg->level};
  STfsDisk *pDisk = NULL;

  TAOS_CHECK_GOTO(tfsMountDiskToTier(TFS_TIER_AT(pTfs, did.level), pCfg, &pDisk), &lino, _exit);
  did.id = pDisk->id;

  if (taosHashPut(pTfs->hash, (void *)(pCfg->dir), strnlen(pCfg->dir, TSDB_FILENAME_LEN), (void *)(&did),
                  sizeof(did)) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }
  if (pTfs->nlevel < pCfg->level + 1) {
    pTfs->nlevel = pCfg->level + 1;
  }

_exit:
  if (code != 0) {
    fError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t tfsCheckAndFormatCfg(STfs *pTfs, SDiskCfg *pCfg) {
  int32_t code = 0;
  char    dirName[TSDB_FILENAME_LEN] = "\0";

  if (pCfg->level < 0 || pCfg->level >= TFS_MAX_TIERS) {
    fError("failed to mount %s to FS since invalid level %d", pCfg->dir, pCfg->level);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  if (pCfg->primary < 0 || pCfg->primary > 1) {
    fError("failed to mount %s to FS since invalid primary %d", pCfg->dir, pCfg->primary);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  if (pCfg->disable < 0 || pCfg->disable > 1) {
    fError("failed to mount %s to FS since invalid disable %" PRIi8, pCfg->dir, pCfg->disable);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  if (pCfg->primary) {
    if (pCfg->level != 0) {
      fError("failed to mount %s to FS since disk is primary but level %d not 0", pCfg->dir, pCfg->level);
      TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
    }

    if (TFS_PRIMARY_DISK(pTfs) != NULL) {
      fError("failed to mount %s to FS since duplicate primary mount", pCfg->dir);
      TAOS_RETURN(TSDB_CODE_FS_DUP_PRIMARY);
    }
  }

  if ((code = tfsFormatDir(pCfg->dir, dirName)) < 0) {
    fError("failed to mount %s to FS since %s", pCfg->dir, tstrerror(code));
    TAOS_RETURN(code);
  }

  STfsDisk *pDisk = NULL;
  if ((code = tfsGetDiskByName(pTfs, dirName, &pDisk)) != 0) {
    fError("failed to mount %s to FS since %s", pCfg->dir, tstrerror(code));
    TAOS_RETURN(code);
  }
  if (pDisk != NULL) {
    fError("failed to mount %s to FS since duplicate mount", pCfg->dir);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  if (!taosCheckAccessFile(dirName, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_READ_OK | TD_FILE_ACCESS_WRITE_OK)) {
    fError("failed to mount %s to FS since no R/W access rights", pCfg->dir);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  if (!taosIsDir(dirName)) {
    fError("failed to mount %s to FS since not a directory", pCfg->dir);
    TAOS_RETURN(TSDB_CODE_FS_INVLD_CFG);
  }

  strncpy(pCfg->dir, dirName, TSDB_FILENAME_LEN);

  TAOS_RETURN(0);
}

static int32_t tfsFormatDir(char *idir, char *odir) {
  wordexp_t wep = {0};

  int32_t code = wordexp(idir, &wep, 0);
  if (code != 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(code));
  }

  char tmp[PATH_MAX] = {0};
  if (taosRealPath(wep.we_wordv[0], tmp, PATH_MAX) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    wordfree(&wep);
    TAOS_RETURN(code);
  }
  strcpy(odir, tmp);

  wordfree(&wep);
  TAOS_RETURN(0);
}

static int32_t tfsCheck(STfs *pTfs) {
  if (TFS_PRIMARY_DISK(pTfs) == NULL) {
    fError("no primary disk is set");
    TAOS_RETURN(TSDB_CODE_FS_NO_PRIMARY_DISK);
  }

  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    if (TFS_TIER_AT(pTfs, level)->ndisk == 0) {
      fError("no disk at level %d", level);
      TAOS_RETURN(TSDB_CODE_FS_NO_MOUNT_AT_TIER);
    }

    if (level == 0) {
      tfsUpdateTierSize(TFS_TIER_AT(pTfs, level));
      if (TFS_TIER_AT(pTfs, level)->nAvailDisks == 0) {
        fError("no disk to create new file at level %d", level);
        TAOS_RETURN(TSDB_CODE_FS_NO_VALID_DISK);
      }
    }
  }

  TAOS_RETURN(0);
}

static int32_t tfsGetDiskByName(STfs *pTfs, const char *dir, STfsDisk **ppDisk) {
  void *pr = taosHashGet(pTfs->hash, (void *)dir, strnlen(dir, TSDB_FILENAME_LEN));
  if (pr == NULL) {
    TAOS_RETURN(terrno);
  }

  SDiskID did = *(SDiskID *)pr;
  *ppDisk = TFS_DISK_AT(pTfs, did);

  TAOS_RETURN(0);
}

static int32_t tfsOpendirImpl(STfs *pTfs, STfsDir *pTfsDir) {
  STfsDisk *pDisk = NULL;
  char      adir[TMPNAME_LEN * 2] = "\0";
  int32_t   code = 0;

  if (pTfsDir->pDir != NULL) {
    if ((code = taosCloseDir(&pTfsDir->pDir)) != 0) {
      fError("%s failed to close dir since %s", __func__, tstrerror(code));
      TAOS_RETURN(code);
    }
    pTfsDir->pDir = NULL;
  }

  while (true) {
    pDisk = tfsNextDisk(pTfs, &pTfsDir->iter);
    if (pDisk == NULL) TAOS_RETURN(0);

    pTfsDir->did.level = pDisk->level;
    pTfsDir->did.id = pDisk->id;

    if (pDisk->path == NULL || pDisk->path[0] == 0) {
      (void)snprintf(adir, TMPNAME_LEN * 2, "%s", pTfsDir->dirName);
    } else {
      (void)snprintf(adir, TMPNAME_LEN * 2, "%s%s%s", pDisk->path, TD_DIRSEP, pTfsDir->dirName);
    }
    pTfsDir->pDir = taosOpenDir(adir);
    if (pTfsDir->pDir != NULL) break;
    fWarn("%s failed to open dir %s since %s", __func__, adir, tstrerror(TAOS_SYSTEM_ERROR(errno)));
  }

  TAOS_RETURN(0);
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
  if (pInfo->datadirs == NULL) {
    TAOS_RETURN(terrno);
  }

  tfsUpdateSize(pTfs);

  (void)tfsLock(pTfs);
  for (int32_t level = 0; level < pTfs->nlevel; level++) {
    STfsTier *pTier = &pTfs->tiers[level];
    for (int32_t disk = 0; disk < pTier->ndisk; ++disk) {
      STfsDisk    *pDisk = pTier->disks[disk];
      SMonDiskDesc dinfo = {0};
      dinfo.size = pDisk->size;
      dinfo.level = pDisk->level;
      tstrncpy(dinfo.name, pDisk->path, sizeof(dinfo.name));
      if (taosArrayPush(pInfo->datadirs, &dinfo) == NULL) {
        (void)tfsUnLock(pTfs);
        taosArrayDestroy(pInfo->datadirs);
        pInfo->datadirs = NULL;
        TAOS_RETURN(terrno);
      }
    }
  }
  (void)tfsUnLock(pTfs);

  TAOS_RETURN(0);
}

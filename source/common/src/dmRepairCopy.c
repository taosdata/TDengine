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
#include "dmRepairCopy.h"
#include "tconfig.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlog.h"

// Lightweight TFS model — uses SDiskCfg directly with a parallel array
// of tier-local IDs assigned during construction.
typedef struct SRepairTfs {
  int32_t   ndisk;
  int32_t   nlevel;
  SDiskCfg *disks;       // array of ndisk disks (owned)
  int32_t   primaryIdx;  // index into disks[] for the primary disk
} SRepairTfs;

// Lightweight representation of a single TSDB file parsed from current.json.
typedef struct SRepairFile {
  int32_t type;      // 0=head,1=data,2=sma,3=tomb,5=stt
  SDiskID did;       // source disk ID {level, id}
  int32_t lcn;       // last chunk number (S3)
  int32_t fid;       // file set id
  int64_t cid;       // commit id
  int64_t size;      // file size in bytes
  int64_t minVer;    // minimum version (-1 if absent)
  int64_t maxVer;    // maximum version (-1 if absent)
  int32_t sttLevel;  // STT compaction level (only for type 5)
} SRepairFile;

// Lightweight representation of a TSDB file set parsed from current.json.
typedef struct SRepairFileSet {
  int32_t fid;          // file set id
  SArray *files;        // SArray of SRepairFile
  int64_t lastCompact;  // last compact timestamp
  int64_t lastCommit;   // last commit timestamp
} SRepairFileSet;

static int32_t compareInt32(const void *a, const void *b) {
  int32_t va = *(const int32_t *)a;
  int32_t vb = *(const int32_t *)b;
  if (va < vb) return -1;
  if (va > vb) return 1;
  return 0;
}

SArray *dmParseVnodeIds(const char *str) {
  if (str == NULL || str[0] == '\0') return NULL;

  SArray *pArr = taosArrayInit(8, sizeof(int32_t));
  if (pArr == NULL) return NULL;

  const char *p = str;
  while (*p != '\0') {
    // skip leading whitespace
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') break;

    // parse first number
    char   *end = NULL;
    int32_t lo = taosStr2Int32(p, &end, 10);
    if (end == p || lo <= 0) goto _err;

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == '-') {
      // range: lo-hi
      end++;
      int32_t hi = taosStr2Int32(end, &end, 10);
      if (hi < lo) goto _err;
      for (int32_t id = lo; id <= hi; id++) {
        if (taosArrayPush(pArr, &id) == NULL) goto _err;
      }
    } else {
      if (taosArrayPush(pArr, &lo) == NULL) goto _err;
    }

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == ',') {
      end++;
    } else if (*end != '\0') {
      goto _err;
    }
    p = end;
  }

  if (taosArrayGetSize(pArr) == 0) goto _err;

  taosArraySort(pArr, compareInt32);
  taosArrayRemoveDuplicate(pArr, compareInt32, NULL);

  return pArr;

_err:
  taosArrayDestroy(pArr);
  return NULL;
}

// Fetch a remote file to a local path via SSH.
// Returns 0 on success, -1 on error.
static int32_t dmSshFetchFile(const char *host, const char *remotePath, const char *localPath) {
  char cmd[1024];
  snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes %s cat '%s' > '%s' 2>/dev/null", host, remotePath, localPath);
  TdCmdPtr pCmd = taosOpenCmd(cmd);
  if (pCmd == NULL) {
    uError("repair: failed to run ssh command");
    return -1;
  }
  char buf[256];
  while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {}
  taosCloseCmd(&pCmd);

  // Verify file has content
  int64_t fsize = 0;
  if (taosStatFile(localPath, &fsize, NULL, NULL) != 0 || fsize <= 0) {
    uError("repair: ssh fetch returned empty file for %s:%s", host, remotePath);
    (void)taosRemoveFile(localPath);
    return -1;
  }
  return 0;
}

// Parse a taos.cfg file and extract SDiskCfg entries from the dataDir items.
// Returns 0 on success. On success, caller must free *ppDisks.
static int32_t dmParseSourceCfg(const char *cfgPath, SDiskCfg **ppDisks, int32_t *pNumDisks) {
  SConfig *pCfg = NULL;
  int32_t  code = cfgInit(&pCfg);
  if (code != 0) {
    uError("repair: cfgInit failed: %s", tstrerror(code));
    return -1;
  }

  // Register dataDir so cfgLoad knows how to handle it
  code = cfgAddDir(pCfg, "dataDir", "/tmp", CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL);
  if (code != 0) {
    uError("repair: cfgAddDir failed: %s", tstrerror(code));
    cfgCleanup(pCfg);
    return -1;
  }

  code = cfgLoad(pCfg, CFG_STYPE_CFG_FILE, cfgPath);
  if (code != 0) {
    uError("repair: cfgLoad failed for %s: %s", cfgPath, tstrerror(code));
    cfgCleanup(pCfg);
    return -1;
  }

  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  if (pItem == NULL) {
    uError("repair: no dataDir found in %s", cfgPath);
    cfgCleanup(pCfg);
    return -1;
  }

  int32_t ndisk = 0;
  if (pItem->array != NULL) {
    ndisk = taosArrayGetSize(pItem->array);
  }

  SDiskCfg *disks = NULL;
  if (ndisk <= 0) {
    // Single default dataDir from pItem->str
    ndisk = 1;
    disks = taosMemoryCalloc(1, sizeof(SDiskCfg));
    if (disks == NULL) {
      cfgCleanup(pCfg);
      return -1;
    }
    tstrncpy(disks[0].dir, pItem->str, TSDB_FILENAME_LEN);
    disks[0].level = 0;
    disks[0].primary = 1;
    disks[0].disable = 0;
  } else {
    if (ndisk > TFS_MAX_DISKS) ndisk = TFS_MAX_DISKS;
    disks = taosMemoryCalloc(ndisk, sizeof(SDiskCfg));
    if (disks == NULL) {
      cfgCleanup(pCfg);
      return -1;
    }
    for (int32_t i = 0; i < ndisk; i++) {
      SDiskCfg *pSrc = taosArrayGet(pItem->array, i);
      disks[i] = *pSrc;
    }
  }

  cfgCleanup(pCfg);
  *ppDisks = disks;
  *pNumDisks = ndisk;
  return 0;
}

// Build a lightweight source TFS model from SDiskCfg array.
// Assigns tier-local IDs to each disk. Returns 0 on success.
static int32_t dmBuildRepairTfs(const SDiskCfg *pCfgArr, int32_t ndisk, SRepairTfs *pTfs) {
  pTfs->ndisk = ndisk;
  pTfs->nlevel = 0;
  pTfs->primaryIdx = -1;
  pTfs->disks = taosMemoryCalloc(ndisk, sizeof(SDiskCfg));
  if (pTfs->disks == NULL) return -1;

  for (int32_t i = 0; i < ndisk; i++) {
    int32_t lvl = pCfgArr[i].level;
    if (lvl < 0 || lvl >= TFS_MAX_TIERS) {
      uError("repair: invalid disk level %d for %s", lvl, pCfgArr[i].dir);
      taosMemoryFree(pTfs->disks);
      pTfs->disks = NULL;
      return -1;
    }

    pTfs->disks[i] = pCfgArr[i];

    if (lvl + 1 > pTfs->nlevel) {
      pTfs->nlevel = lvl + 1;
    }
    if (pCfgArr[i].primary && lvl == 0) {
      pTfs->primaryIdx = i;
    }
  }

  if (pTfs->primaryIdx < 0) {
    uError("repair: no primary disk found in source config");
    taosMemoryFree(pTfs->disks);
    pTfs->disks = NULL;
    return -1;
  }

  return 0;
}

static void dmDestroyRepairTfs(SRepairTfs *pTfs) {
  if (pTfs == NULL) return;
  taosMemoryFreeClear(pTfs->disks);
  pTfs->ndisk = 0;
  pTfs->nlevel = 0;
  pTfs->primaryIdx = -1;
}

// Validate source disk paths exist (local mode only).
static int32_t dmValidateSourceDisksLocal(const SRepairTfs *pTfs) {
  for (int32_t i = 0; i < pTfs->ndisk; i++) {
    if (!taosDirExist(pTfs->disks[i].dir)) {
      uError("repair: source dataDir does not exist: %s", pTfs->disks[i].dir);
      return -1;
    }
  }
  return 0;
}

// Validate source disk paths exist (remote mode).
static int32_t dmValidateSourceDisksRemote(const char *host, const SRepairTfs *pTfs) {
  for (int32_t i = 0; i < pTfs->ndisk; i++) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes %s test -d '%s' && echo YES", host, pTfs->disks[i].dir);
    TdCmdPtr pCmd = taosOpenCmd(cmd);
    if (pCmd == NULL) {
      uError("repair: ssh connectivity failed");
      return -1;
    }
    bool found = false;
    char buf[64];
    while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {
      if (strncmp(buf, "YES", 3) == 0) found = true;
    }
    taosCloseCmd(&pCmd);
    if (!found) {
      uError("repair: remote dataDir does not exist: %s:%s", host, pTfs->disks[i].dir);
      return -1;
    }
  }
  return 0;
}

// Read an entire file into a null-terminated malloc'd buffer.
// Caller must taosMemoryFree(*ppContent).
static int32_t dmReadFileContent(const char *path, char **ppContent, int64_t *pSize) {
  int64_t fsize = 0;
  if (taosStatFile(path, &fsize, NULL, NULL) != 0 || fsize <= 0) {
    return -1;
  }
  TdFilePtr pFile = taosOpenFile(path, TD_FILE_READ);
  if (pFile == NULL) return -1;

  char *buf = taosMemoryMalloc(fsize + 1);
  if (buf == NULL) {
    taosCloseFile(&pFile);
    return -1;
  }
  int64_t nread = taosReadFile(pFile, buf, fsize);
  taosCloseFile(&pFile);
  if (nread <= 0) {
    taosMemoryFree(buf);
    return -1;
  }
  buf[nread] = '\0';
  *ppContent = buf;
  if (pSize) *pSize = nread;
  return 0;
}

// Load dnodeId from {tsDataDir}/dnode/dnode.json.
// Returns 0 on success, -1 on error (file missing or parse failure).
static int32_t dmLoadDnodeInfo(int32_t *pDnodeId) {
  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  char *content = NULL;
  if (dmReadFileContent(file, &content, NULL) != 0) {
    uError("repair: failed to read dnode.json: %s", file);
    return -1;
  }

  SJson *pJson = tjsonParse(content);
  taosMemoryFree(content);
  if (pJson == NULL) {
    uError("repair: failed to parse dnode.json");
    return -1;
  }

  int32_t code = 0;
  int32_t dnodeId = 0;
  tjsonGetInt32ValueFromDouble(pJson, "dnodeId", dnodeId, code);
  if (code < 0 || dnodeId <= 0) {
    uError("repair: invalid or missing dnodeId in dnode.json");
    tjsonDelete(pJson);
    return -1;
  }

  tjsonDelete(pJson);
  *pDnodeId = dnodeId;
  return 0;
}

// Open target TFS from global tsDiskCfg[]/tsDiskCfgNum.
// Returns 0 on success. Caller must call tfsClose(ppTfs) to free.
static int32_t dmOpenTargetTfs(STfs **ppTfs) {
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;

  SDiskCfg tmpDisk = {0};
  if (numOfDisks <= 0) {
    // Fallback: single disk from tsDataDir
    tmpDisk.level = 0;
    tmpDisk.primary = 1;
    tmpDisk.disable = 0;
    tstrncpy(tmpDisk.dir, tsDataDir, TSDB_FILENAME_LEN);
    pDisks = &tmpDisk;
    numOfDisks = 1;
  }

  int32_t code = tfsOpen(pDisks, numOfDisks, ppTfs);
  if (code != 0) {
    uError("repair: failed to open target TFS: %s", tstrerror(code));
    return -1;
  }
  return 0;
}

// Check if vnode{vid}.bak exists on any target TFS disk.
// Returns true if .bak found on any disk.
static bool dmCheckBakExists(STfs *pTgtTfs, int32_t vnodeId) {
  char relBak[TSDB_FILENAME_LEN];
  snprintf(relBak, sizeof(relBak), "vnode%svnode%d.bak", TD_DIRSEP, vnodeId);
  int32_t nlevel = tfsGetLevel(pTgtTfs);
  for (int32_t level = 0; level < nlevel; level++) {
    int32_t ndisk = tfsGetDisksAtLevel(pTgtTfs, level);
    for (int32_t id = 0; id < ndisk; id++) {
      SDiskID did = {.level = level, .id = id};
      const char *diskPath = tfsGetDiskPath(pTgtTfs, did);
      char fullPath[PATH_MAX];
      snprintf(fullPath, sizeof(fullPath), "%s%s%s", diskPath, TD_DIRSEP, relBak);
      if (taosDirExist(fullPath)) return true;
    }
  }
  return false;
}

static void dmDestroyRepairFileSets(SArray *pSets) {
  if (pSets == NULL) return;
  for (int32_t i = 0; i < taosArrayGetSize(pSets); i++) {
    SRepairFileSet *pSet = taosArrayGet(pSets, i);
    taosArrayDestroy(pSet->files);
  }
  taosArrayDestroy(pSets);
}

// File type suffix strings for all types including STT.
static const char *gRepairFTypeSuffixAll[] = {
    [0] = "head", [1] = "data", [2] = "sma", [3] = "tomb",
    [4] = NULL, [5] = "stt"};

// Parse a single file's JSON fields into SRepairFile.
static int32_t dmParseRepairFileJson(SJson *pJson, int32_t type, SRepairFile *pFile) {
  int32_t code = 0;
  pFile->type = type;
  tjsonGetInt32ValueFromDouble(pJson, "did.level", pFile->did.level, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "did.id", pFile->did.id, code);
  if (code < 0) return -1;

  pFile->lcn = 0;
  (void)tjsonGetIntValue(pJson, "lcn", &pFile->lcn);

  tjsonGetInt32ValueFromDouble(pJson, "fid", pFile->fid, code);
  if (code < 0) return -1;

  code = tjsonGetBigIntValue(pJson, "cid", &pFile->cid);
  if (code < 0) return -1;

  code = tjsonGetBigIntValue(pJson, "size", &pFile->size);
  if (code < 0) return -1;

  pFile->minVer = -1;
  pFile->maxVer = -1;
  (void)tjsonGetBigIntValue(pJson, "minVer", &pFile->minVer);
  (void)tjsonGetBigIntValue(pJson, "maxVer", &pFile->maxVer);

  pFile->sttLevel = 0;
  if (type == 5) {  // TSDB_FTYPE_STT
    tjsonGetInt32ValueFromDouble(pJson, "level", pFile->sttLevel, code);
    if (code < 0) return -1;
  }
  return 0;
}

// Parse current.json content into an SArray of SRepairFileSet.
// Returns NULL on error.
static SArray *dmParseCurrentJson(const char *content) {
  SJson *pRoot = tjsonParse(content);
  if (pRoot == NULL) {
    uError("repair: failed to parse current.json");
    return NULL;
  }

  // Check format version
  int32_t fmtv = 0;
  int32_t code = 0;
  tjsonGetInt32ValueFromDouble(pRoot, "fmtv", fmtv, code);
  if (code < 0 || fmtv != 1) {
    uError("repair: unsupported current.json format version: %d", fmtv);
    tjsonDelete(pRoot);
    return NULL;
  }

  SJson *pFsetArr = tjsonGetObjectItem(pRoot, "fset");
  if (pFsetArr == NULL) {
    uError("repair: missing 'fset' array in current.json");
    tjsonDelete(pRoot);
    return NULL;
  }

  int32_t nFsets = tjsonGetArraySize(pFsetArr);
  SArray *pSets = taosArrayInit(nFsets > 0 ? nFsets : 1, sizeof(SRepairFileSet));
  if (pSets == NULL) {
    tjsonDelete(pRoot);
    return NULL;
  }

  for (int32_t i = 0; i < nFsets; i++) {
    SJson *pFsetJson = tjsonGetArrayItem(pFsetArr, i);
    SRepairFileSet fset = {0};

    tjsonGetInt32ValueFromDouble(pFsetJson, "fid", fset.fid, code);
    if (code < 0) goto _err;

    fset.files = taosArrayInit(8, sizeof(SRepairFile));
    if (fset.files == NULL) goto _err;

    // Parse non-STT file types (head, data, sma, tomb)
    for (int32_t t = 0; t < 4; t++) {
      SJson *pFileJson = tjsonGetObjectItem(pFsetJson, gRepairFTypeSuffixAll[t]);
      if (pFileJson == NULL) continue;
      SRepairFile rf = {0};
      if (dmParseRepairFileJson(pFileJson, t, &rf) != 0) {
        taosArrayDestroy(fset.files);
        goto _err;
      }
      if (taosArrayPush(fset.files, &rf) == NULL) {
        taosArrayDestroy(fset.files);
        goto _err;
      }
    }

    // Parse STT levels
    SJson *pSttLvlArr = tjsonGetObjectItem(pFsetJson, "stt lvl");
    if (pSttLvlArr != NULL) {
      int32_t nLvls = tjsonGetArraySize(pSttLvlArr);
      for (int32_t l = 0; l < nLvls; l++) {
        SJson *pLvlJson = tjsonGetArrayItem(pSttLvlArr, l);
        SJson *pFilesArr = tjsonGetObjectItem(pLvlJson, "files");
        if (pFilesArr == NULL) continue;
        int32_t nFiles = tjsonGetArraySize(pFilesArr);
        for (int32_t f = 0; f < nFiles; f++) {
          SJson *pSttJson = tjsonGetArrayItem(pFilesArr, f);
          SRepairFile rf = {0};
          if (dmParseRepairFileJson(pSttJson, 5, &rf) != 0) {  // 5 = TSDB_FTYPE_STT
            taosArrayDestroy(fset.files);
            goto _err;
          }
          if (taosArrayPush(fset.files, &rf) == NULL) {
            taosArrayDestroy(fset.files);
            goto _err;
          }
        }
      }
    }

    // Parse file set level timestamps
    fset.lastCompact = 0;
    fset.lastCommit = 0;
    (void)tjsonGetBigIntValue(pFsetJson, "last compact", &fset.lastCompact);
    (void)tjsonGetBigIntValue(pFsetJson, "last commit", &fset.lastCommit);

    if (taosArrayPush(pSets, &fset) == NULL) {
      taosArrayDestroy(fset.files);
      goto _err;
    }
  }

  tjsonDelete(pRoot);
  return pSets;

_err:
  tjsonDelete(pRoot);
  dmDestroyRepairFileSets(pSets);
  return NULL;
}

// Read and parse local (target) current.json for a vnode.
// Returns parsed SArray of SRepairFileSet, or NULL if file doesn't exist or fails to parse.
static SArray *dmReadLocalCurrentJson(STfs *pTgtTfs, int32_t vnodeId) {
  const char *primaryPath = tfsGetPrimaryPath(pTgtTfs);
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s%svnode%svnode%d%stsdb%scurrent.json",
           primaryPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP);

  char *content = NULL;
  if (dmReadFileContent(path, &content, NULL) != 0) return NULL;
  SArray *pSets = dmParseCurrentJson(content);
  taosMemoryFree(content);
  return pSets;
}

// Build the on-disk filename for a SRepairFile.
// Pattern: {diskPath}/vnode/vnode{vid}/tsdb/v{vid}f{fid}ver{cid}.{suffix}
// S3 variant (lcn>0): ...ver{cid}.{lcn}.{suffix}
static void dmBuildTsdbFilePath(const char *diskPath, int32_t vnodeId,
                                const SRepairFile *pFile, char *buf, int32_t bufLen) {
  const char *suffix = gRepairFTypeSuffixAll[pFile->type];
  if (pFile->lcn > 0) {
    snprintf(buf, bufLen, "%s%svnode%svnode%d%stsdb%sv%df%dver%" PRId64 ".%d.%s",
             diskPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP,
             vnodeId, pFile->fid, pFile->cid, pFile->lcn, suffix);
  } else {
    snprintf(buf, bufLen, "%s%svnode%svnode%d%stsdb%sv%df%dver%" PRId64 ".%s",
             diskPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP,
             vnodeId, pFile->fid, pFile->cid, suffix);
  }
}

// Determine which source file sets need to be copied vs retained from local.
// A source fid is "retained" only if the local also has that fid AND every file
// listed in the local file set physically exists on the target disk with correct size.
// Otherwise it needs copying from source.
// Sets *ppCopyFids to a new SArray of int32_t fids that need copying (caller frees).
// Sets *ppRetainFids to a new SArray of int32_t fids that can be hard-linked from backup.
static void dmDiffFileSets(const SArray *srcSets, const SArray *localSets,
                           STfs *pTgtTfs, int32_t vnodeId,
                           SArray **ppCopyFids, SArray **ppRetainFids) {
  int32_t nSrc = taosArrayGetSize(srcSets);
  *ppCopyFids = taosArrayInit(nSrc, sizeof(int32_t));
  *ppRetainFids = taosArrayInit(nSrc, sizeof(int32_t));

  for (int32_t s = 0; s < nSrc; s++) {
    SRepairFileSet *pSrc = taosArrayGet(srcSets, s);
    int32_t srcFid = pSrc->fid;
    bool retained = false;

    if (localSets != NULL) {
      int32_t nLocal = taosArrayGetSize(localSets);
      for (int32_t l = 0; l < nLocal; l++) {
        SRepairFileSet *pLocal = taosArrayGet(localSets, l);
        if (pLocal->fid != srcFid) continue;

        // Same fid exists locally — verify every local file exists on disk
        bool allExist = true;
        int32_t nLocalFiles = taosArrayGetSize(pLocal->files);
        for (int32_t f = 0; f < nLocalFiles; f++) {
          SRepairFile *lf = taosArrayGet(pLocal->files, f);
          const char *diskPath = tfsGetDiskPath(pTgtTfs, lf->did);
          if (diskPath == NULL) { allExist = false; break; }

          char filePath[PATH_MAX];
          dmBuildTsdbFilePath(diskPath, vnodeId, lf, filePath, sizeof(filePath));

          int64_t actualSize = 0;
          if (taosStatFile(filePath, &actualSize, NULL, NULL) != 0 || actualSize <= 0) {
            allExist = false;
            break;
          }
        }
        if (allExist && nLocalFiles > 0) retained = true;
        break;
      }
    }

    if (retained) {
      (void)taosArrayPush(*ppRetainFids, &srcFid);
    } else {
      (void)taosArrayPush(*ppCopyFids, &srcFid);
    }
  }
}

// Read and parse source current.json into an SArray of SRepairFileSet.
// Returns NULL on error (file missing, SSH failure, or parse error).
static SArray *dmReadSourceCurrentJson(const SRepairTfs *pSrcTfs, const char *host, int32_t vnodeId) {
  const char *primaryDir = pSrcTfs->disks[pSrcTfs->primaryIdx].dir;
  char srcPath[PATH_MAX];
  snprintf(srcPath, sizeof(srcPath), "%s%svnode%svnode%d%stsdb%scurrent.json",
           primaryDir, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP);

  char *content = NULL;
  if (host == NULL || host[0] == '\0') {
    if (dmReadFileContent(srcPath, &content, NULL) != 0) return NULL;
  } else {
    char tmpPath[PATH_MAX];
    snprintf(tmpPath, sizeof(tmpPath), "/tmp/tdrepair_%d_v%d_current.json", (int)taosGetPId(), vnodeId);
    if (dmSshFetchFile(host, srcPath, tmpPath) != 0) return NULL;
    int32_t rc = dmReadFileContent(tmpPath, &content, NULL);
    (void)taosRemoveFile(tmpPath);
    if (rc != 0) return NULL;
  }

  SArray *pSets = dmParseCurrentJson(content);
  taosMemoryFree(content);
  return pSets;
}

// Step 5d: Backup vnodeN → vnodeN.bak on all target disks.
// Disks where vnodeN exists: rename to vnodeN.bak.
// Disks where vnodeN does not exist: create empty vnodeN.bak dir.
static int32_t dmBackupVnode(STfs *pTgtTfs, int32_t vnodeId) {
  char relVnode[TSDB_FILENAME_LEN];
  char relBak[TSDB_FILENAME_LEN];
  snprintf(relVnode, sizeof(relVnode), "vnode%svnode%d", TD_DIRSEP, vnodeId);
  snprintf(relBak, sizeof(relBak), "vnode%svnode%d.bak", TD_DIRSEP, vnodeId);

  int32_t nlevel = tfsGetLevel(pTgtTfs);
  for (int32_t level = 0; level < nlevel; level++) {
    int32_t ndisk = tfsGetDisksAtLevel(pTgtTfs, level);
    for (int32_t id = 0; id < ndisk; id++) {
      SDiskID did = {.level = level, .id = id};
      const char *diskPath = tfsGetDiskPath(pTgtTfs, did);
      char srcPath[PATH_MAX];
      char dstPath[PATH_MAX];
      snprintf(srcPath, sizeof(srcPath), "%s%s%s", diskPath, TD_DIRSEP, relVnode);
      snprintf(dstPath, sizeof(dstPath), "%s%s%s", diskPath, TD_DIRSEP, relBak);

      if (taosDirExist(srcPath)) {
        if (taosRenameFile(srcPath, dstPath) != 0) {
          uError("repair: vnode%d failed to rename %s to %s", vnodeId, srcPath, dstPath);
          return -1;
        }
        uInfo("repair: vnode%d renamed %s to .bak", vnodeId, srcPath);
      } else {
        if (taosMkDir(dstPath) != 0) {
          uError("repair: vnode%d failed to create backup dir %s", vnodeId, dstPath);
          return -1;
        }
      }
    }
  }
  return 0;
}

// Step 5e: Create vnodeN/tsdb directory tree on all target disks.
static int32_t dmCreateVnodeDirs(STfs *pTgtTfs, int32_t vnodeId) {
  char relTsdb[TSDB_FILENAME_LEN];
  snprintf(relTsdb, sizeof(relTsdb), "vnode%svnode%d%stsdb", TD_DIRSEP, vnodeId, TD_DIRSEP);
  int32_t code = tfsMkdirRecur(pTgtTfs, relTsdb);
  if (code != 0) {
    uError("repair: vnode%d failed to create directories: %s", vnodeId, tstrerror(code));
    return -1;
  }
  return 0;
}

// Step 5f: Hard-link retained tsdb files from vnodeN.bak to vnodeN.
// Each file is hard-linked on the same disk (same filesystem).
static int32_t dmHardLinkRetainedFiles(STfs *pTgtTfs, int32_t vnodeId,
                                       const SArray *retainFids, const SArray *localFileSets) {
  int32_t nRetain = taosArrayGetSize(retainFids);
  int32_t nLocal = taosArrayGetSize(localFileSets);

  for (int32_t r = 0; r < nRetain; r++) {
    int32_t fid = *(int32_t *)taosArrayGet(retainFids, r);

    // Find the local file set for this fid
    SRepairFileSet *pLocal = NULL;
    for (int32_t l = 0; l < nLocal; l++) {
      SRepairFileSet *pSet = taosArrayGet(localFileSets, l);
      if (pSet->fid == fid) { pLocal = pSet; break; }
    }
    if (pLocal == NULL) continue;

    int32_t nFiles = taosArrayGetSize(pLocal->files);
    for (int32_t f = 0; f < nFiles; f++) {
      SRepairFile *pFile = taosArrayGet(pLocal->files, f);
      const char *diskPath = tfsGetDiskPath(pTgtTfs, pFile->did);
      if (diskPath == NULL) {
        uError("repair: vnode%d fid=%d invalid disk level=%d id=%d", vnodeId, fid, pFile->did.level, pFile->did.id);
        return -1;
      }

      const char *suffix = gRepairFTypeSuffixAll[pFile->type];
      char fileName[256];
      if (pFile->lcn > 0) {
        snprintf(fileName, sizeof(fileName), "v%df%dver%" PRId64 ".%d.%s", vnodeId, pFile->fid, pFile->cid, pFile->lcn, suffix);
      } else {
        snprintf(fileName, sizeof(fileName), "v%df%dver%" PRId64 ".%s", vnodeId, pFile->fid, pFile->cid, suffix);
      }

      char bakPath[PATH_MAX];
      char newPath[PATH_MAX];
      snprintf(bakPath, sizeof(bakPath), "%s%svnode%svnode%d.bak%stsdb%s%s", diskPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP, fileName);
      snprintf(newPath, sizeof(newPath), "%s%svnode%svnode%d%stsdb%s%s", diskPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP, fileName);

      if (taosLinkFile(bakPath, newPath) != 0) {
        uError("repair: vnode%d failed to hard-link %s", vnodeId, fileName);
        return -1;
      }
      uInfo("repair: vnode%d hard-linked fid=%d %s", vnodeId, fid, fileName);
    }
  }
  return 0;
}

// Recursively copy a directory tree, optionally skipping one subdirectory by name.
// When skipSubDir is non-NULL and matches a top-level entry, that subtree is skipped.
// For files: copies with taosCopyFile(), verifies size, logs name+size.
// For dirs: creates target dir, recurses (skipSubDir only applies at depth 0).
static int32_t dmCopyDirRecursive(const char *srcDir, const char *dstDir,
                                  const char *skipSubDir, int32_t vnodeId) {
  TdDirPtr pDir = taosOpenDir(srcDir);
  if (pDir == NULL) {
    uError("repair: vnode%d cannot open source dir %s", vnodeId, srcDir);
    return -1;
  }

  TdDirEntryPtr pEntry;
  while ((pEntry = taosReadDir(pDir)) != NULL) {
    char *name = taosGetDirEntryName(pEntry);
    if (name == NULL) continue;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

    // Skip the excluded subdirectory at this level
    if (skipSubDir != NULL && strcmp(name, skipSubDir) == 0) continue;

    char srcPath[PATH_MAX];
    char dstPath[PATH_MAX];
    snprintf(srcPath, sizeof(srcPath), "%s%s%s", srcDir, TD_DIRSEP, name);
    snprintf(dstPath, sizeof(dstPath), "%s%s%s", dstDir, TD_DIRSEP, name);

    if (taosDirEntryIsDir(pEntry)) {
      if (taosMulMkDir(dstPath) != 0) {
        uError("repair: vnode%d failed to create dir %s", vnodeId, dstPath);
        taosCloseDir(&pDir);
        return -1;
      }
      uInfo("repair: vnode%d  dir: %s", vnodeId, name);
      // Recurse without skip — skipSubDir only applies at top level
      if (dmCopyDirRecursive(srcPath, dstPath, NULL, vnodeId) != 0) {
        taosCloseDir(&pDir);
        return -1;
      }
    } else {
      int64_t srcSize = 0;
      if (taosStatFile(srcPath, &srcSize, NULL, NULL) != 0) {
        uError("repair: vnode%d cannot stat source file %s", vnodeId, srcPath);
        taosCloseDir(&pDir);
        return -1;
      }
      uInfo("repair: vnode%d  file: %s (%" PRId64 " bytes)", vnodeId, name, srcSize);

      int64_t copied = taosCopyFile(srcPath, dstPath);
      if (copied < 0) {
        uError("repair: vnode%d failed to copy %s", vnodeId, srcPath);
        taosCloseDir(&pDir);
        return -1;
      }

      int64_t dstSize = 0;
      if (taosStatFile(dstPath, &dstSize, NULL, NULL) != 0 || dstSize != srcSize) {
        uError("repair: vnode%d size mismatch after copy: %s (src=%" PRId64 " dst=%" PRId64 ")",
               vnodeId, name, srcSize, dstSize);
        taosCloseDir(&pDir);
        return -1;
      }
    }
  }
  taosCloseDir(&pDir);
  return 0;
}

// Step 5g: Copy non-tsdb files from source vnodeN to target primary disk.
// Local mode: recursive copy skipping tsdb/.
// Remote mode: scp -r then remove tsdb/ from the copy.
static int32_t dmCopyNonTsdbFiles(const SRepairTfs *pSrcTfs, STfs *pTgtTfs,
                                  const char *host, int32_t vnodeId) {
  const char *tgtPrimary = tfsGetPrimaryPath(pTgtTfs);
  char dstVnodeDir[PATH_MAX];
  snprintf(dstVnodeDir, sizeof(dstVnodeDir), "%s%svnode%svnode%d",
           tgtPrimary, TD_DIRSEP, TD_DIRSEP, vnodeId);

  if (host == NULL || host[0] == '\0') {
    // Local mode: recursive copy skipping "tsdb"
    const char *srcPrimary = pSrcTfs->disks[pSrcTfs->primaryIdx].dir;
    char srcVnodeDir[PATH_MAX];
    snprintf(srcVnodeDir, sizeof(srcVnodeDir), "%s%svnode%svnode%d",
             srcPrimary, TD_DIRSEP, TD_DIRSEP, vnodeId);

    return dmCopyDirRecursive(srcVnodeDir, dstVnodeDir, "tsdb", vnodeId);
  } else {
    // Remote mode: list source vnodeN/ entries, scp each non-tsdb item individually
    const char *srcPrimary = pSrcTfs->disks[pSrcTfs->primaryIdx].dir;
    char srcVnodeDir[PATH_MAX];
    snprintf(srcVnodeDir, sizeof(srcVnodeDir), "%s%svnode%svnode%d",
             srcPrimary, TD_DIRSEP, TD_DIRSEP, vnodeId);

    // List remote directory entries with type and size via ls -lA
    struct SRemoteEntry { char name[256]; bool isDir; int64_t size; };
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes '%s' ls -lA '%s/' 2>/dev/null", host, srcVnodeDir);
    TdCmdPtr pCmd = taosOpenCmd(cmd);
    if (pCmd == NULL) {
      uError("repair: vnode%d ssh ls command failed to start", vnodeId);
      return -1;
    }

    // Collect entries with type and size
    SArray *entries = taosArrayInit(8, sizeof(struct SRemoteEntry));
    char line[512];
    while (taosGetsCmd(pCmd, sizeof(line), line) > 0) {
      // Strip trailing newline
      int32_t len = (int32_t)strlen(line);
      while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) line[--len] = '\0';
      if (len == 0) continue;
      // Skip "total NNN" line
      if (strncmp(line, "total ", 6) == 0) continue;

      // Parse: perms nlinks user group size mon day time name
      char perms[16] = {0}, user[64] = {0}, group[64] = {0}, name[256] = {0};
      char mon[8] = {0}, day[8] = {0}, timeOrYear[16] = {0};
      int32_t nlinks = 0;
      int64_t fsize = 0;
      if (sscanf(line, "%15s %d %63s %63s %" PRId64 " %7s %7s %15s %255s",
                 perms, &nlinks, user, group, &fsize, mon, day, timeOrYear, name) < 9) {
        continue;
      }
      if (strcmp(name, "tsdb") == 0) continue;

      struct SRemoteEntry re = {.isDir = (perms[0] == 'd'), .size = fsize};
      tstrncpy(re.name, name, sizeof(re.name));
      (void)taosArrayPush(entries, &re);
    }
    taosCloseCmd(&pCmd);

    // scp each entry
    int32_t nEntries = taosArrayGetSize(entries);
    for (int32_t i = 0; i < nEntries; i++) {
      struct SRemoteEntry *re = taosArrayGet(entries, i);
      if (re->isDir) {
        uInfo("repair: vnode%d scp dir: %s", vnodeId, re->name);
      } else {
        uInfo("repair: vnode%d scp file: %s (%" PRId64 " bytes)", vnodeId, re->name, re->size);
      }

      snprintf(cmd, sizeof(cmd), "scp -r -o BatchMode=yes '%s:%s/%s' '%s/' 2>/dev/null",
               host, srcVnodeDir, re->name, dstVnodeDir);
      pCmd = taosOpenCmd(cmd);
      if (pCmd == NULL) {
        uError("repair: vnode%d scp failed for %s", vnodeId, re->name);
        taosArrayDestroy(entries);
        return -1;
      }
      char buf[256];
      while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {}
      taosCloseCmd(&pCmd);

      // Verify file size after copy
      if (!re->isDir) {
        char dstPath[PATH_MAX];
        snprintf(dstPath, sizeof(dstPath), "%s%s%s", dstVnodeDir, TD_DIRSEP, re->name);
        int64_t dstSize = 0;
        if (taosStatFile(dstPath, &dstSize, NULL, NULL) != 0 || dstSize != re->size) {
          uError("repair: vnode%d scp size mismatch: %s (src=%" PRId64 " dst=%" PRId64 ")",
                 vnodeId, re->name, re->size, dstSize);
          taosArrayDestroy(entries);
          return -1;
        }
      }
    }
    taosArrayDestroy(entries);

    // Verify at least vnode.json was copied
    char vnodeJson[PATH_MAX];
    snprintf(vnodeJson, sizeof(vnodeJson), "%s%svnode.json", dstVnodeDir, TD_DIRSEP);
    if (!taosCheckExistFile(vnodeJson)) {
      uError("repair: vnode%d scp failed — vnode.json not found after copy", vnodeId);
      return -1;
    }
    uInfo("repair: vnode%d remote non-tsdb files copied via scp", vnodeId);
    return 0;
  }
}

// Lookup source disk path by disk ID {level, id}.
// Returns NULL if no matching disk found.
static const char *dmGetSourceDiskPath(const SRepairTfs *pTfs, SDiskID did) {
  int32_t count = 0;
  for (int32_t i = 0; i < pTfs->ndisk; i++) {
    if (pTfs->disks[i].level == did.level) {
      if (count == did.id) return pTfs->disks[i].dir;
      count++;
    }
  }
  return NULL;
}

// Check if a target TFS disk is disabled, based on global tsDiskCfg[].
static bool dmIsTgtDiskDisabled(int32_t level, int32_t id) {
  int32_t count = 0;
  for (int32_t i = 0; i < tsDiskCfgNum; i++) {
    if (tsDiskCfg[i].level == level) {
      if (count == id) return tsDiskCfg[i].disable;
      count++;
    }
  }
  return true;
}

// Per-tier round-robin state for target disk allocation.
typedef struct SRepairDiskAlloc {
  int32_t nextId[TFS_MAX_TIERS];
} SRepairDiskAlloc;

// Remap a source file to a target disk using round-robin allocation.
// Rules: same tier if exists → fold to highest available tier → skip disabled → check space.
// Returns 0 on success, -1 if no disk has enough space.
static int32_t dmRemapDiskId(STfs *pTgtTfs, int32_t srcLevel, int64_t fileSize,
                             SRepairDiskAlloc *pAlloc, SDiskID *pTgtDid) {
  int32_t tgtNlevel = tfsGetLevel(pTgtTfs);
  int32_t level = srcLevel;
  if (level >= tgtNlevel) level = tgtNlevel - 1;

  for (int32_t tryLevel = level; tryLevel >= 0; tryLevel--) {
    int32_t ndisk = tfsGetDisksAtLevel(pTgtTfs, tryLevel);
    if (ndisk <= 0) continue;

    int32_t startId = pAlloc->nextId[tryLevel];
    for (int32_t attempt = 0; attempt < ndisk; attempt++) {
      int32_t id = (startId + attempt) % ndisk;
      if (dmIsTgtDiskDisabled(tryLevel, id)) continue;

      SDiskID did = {.level = tryLevel, .id = id};
      const char *diskPath = tfsGetDiskPath(pTgtTfs, did);
      if (diskPath == NULL) continue;

      SDiskSize diskSize = {0};
      if (taosGetDiskSize((char *)diskPath, &diskSize) != 0) continue;
      if (diskSize.avail < fileSize + TFS_MIN_DISK_FREE_SIZE) continue;

      pAlloc->nextId[tryLevel] = (id + 1) % ndisk;
      *pTgtDid = did;
      return 0;
    }
  }
  return -1;
}

// Get remote file size via ssh stat.
// Returns file size in bytes, or -1 on error.
static int64_t dmGetRemoteFileSize(const char *host, const char *remotePath) {
  char cmd[2048];
  snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes '%s' stat -c %%s '%s' 2>/dev/null",
           host, remotePath);
  TdCmdPtr pCmd = taosOpenCmd(cmd);
  if (pCmd == NULL) return -1;

  int64_t size = -1;
  char buf[64] = {0};
  if (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {
    int32_t len = (int32_t)strlen(buf);
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) buf[--len] = '\0';
    size = taosStr2Int64(buf, NULL, 10);
  }
  taosCloseCmd(&pCmd);
  return size;
}

// Step 5h: Copy source TSDB file sets to target with disk ID remapping.
// For each file set in copyFids, remap each file's disk ID to a target disk,
// copy the file (local or remote), and verify size.
// On success, *ppRemappedSets is set to a new SArray of SRepairFileSet with
// target disk IDs (caller must free with dmDestroyRepairFileSets).
static int32_t dmCopySourceFileSets(const SRepairTfs *pSrcTfs, STfs *pTgtTfs,
                                    const char *host, int32_t vnodeId,
                                    const SArray *srcFileSets, const SArray *copyFids,
                                    SArray **ppRemappedSets) {
  SRepairDiskAlloc alloc = {0};
  int32_t nCopy = taosArrayGetSize(copyFids);
  SArray *remapped = taosArrayInit(nCopy > 0 ? nCopy : 1, sizeof(SRepairFileSet));
  if (remapped == NULL) return -1;

  for (int32_t c = 0; c < nCopy; c++) {
    int32_t fid = *(int32_t *)taosArrayGet(copyFids, c);

    // Find source file set for this fid
    SRepairFileSet *pSrcSet = NULL;
    int32_t nSets = taosArrayGetSize(srcFileSets);
    for (int32_t s = 0; s < nSets; s++) {
      SRepairFileSet *pSet = taosArrayGet(srcFileSets, s);
      if (pSet->fid == fid) { pSrcSet = pSet; break; }
    }
    if (pSrcSet == NULL) continue;

    SRepairFileSet newSet = {.fid = fid};
    newSet.files = taosArrayInit(taosArrayGetSize(pSrcSet->files), sizeof(SRepairFile));
    if (newSet.files == NULL) {
      dmDestroyRepairFileSets(remapped);
      return -1;
    }

    int32_t nFiles = taosArrayGetSize(pSrcSet->files);
    for (int32_t f = 0; f < nFiles; f++) {
      SRepairFile *pFile = taosArrayGet(pSrcSet->files, f);

      // Resolve source disk path
      const char *srcDiskPath = dmGetSourceDiskPath(pSrcTfs, pFile->did);
      if (srcDiskPath == NULL) {
        uError("repair: vnode%d fid=%d source disk not found level=%d id=%d",
               vnodeId, fid, pFile->did.level, pFile->did.id);
        taosArrayDestroy(newSet.files);
        dmDestroyRepairFileSets(remapped);
        return -1;
      }

      char srcPath[PATH_MAX];
      dmBuildTsdbFilePath(srcDiskPath, vnodeId, pFile, srcPath, sizeof(srcPath));

      // Get source physical file size
      int64_t srcSize = 0;
      if (host == NULL || host[0] == '\0') {
        if (taosStatFile(srcPath, &srcSize, NULL, NULL) != 0 || srcSize <= 0) {
          uError("repair: vnode%d cannot stat source file %s", vnodeId, srcPath);
          taosArrayDestroy(newSet.files);
          dmDestroyRepairFileSets(remapped);
          return -1;
        }
      } else {
        srcSize = dmGetRemoteFileSize(host, srcPath);
        if (srcSize <= 0) {
          uError("repair: vnode%d cannot stat remote source file %s", vnodeId, srcPath);
          taosArrayDestroy(newSet.files);
          dmDestroyRepairFileSets(remapped);
          return -1;
        }
      }

      // Remap to target disk
      SDiskID tgtDid = {0};
      if (dmRemapDiskId(pTgtTfs, pFile->did.level, srcSize, &alloc, &tgtDid) != 0) {
        uError("repair: vnode%d fid=%d no disk with enough space for %" PRId64 " bytes",
               vnodeId, fid, srcSize);
        taosArrayDestroy(newSet.files);
        dmDestroyRepairFileSets(remapped);
        return -1;
      }

      // Build target path
      SRepairFile tgtFile = *pFile;
      tgtFile.did = tgtDid;
      const char *tgtDiskPath = tfsGetDiskPath(pTgtTfs, tgtDid);
      char dstPath[PATH_MAX];
      dmBuildTsdbFilePath(tgtDiskPath, vnodeId, &tgtFile, dstPath, sizeof(dstPath));

      // Build display name
      const char *suffix = gRepairFTypeSuffixAll[pFile->type];
      char fileName[256];
      if (pFile->lcn > 0) {
        snprintf(fileName, sizeof(fileName), "v%df%dver%" PRId64 ".%d.%s",
                 vnodeId, pFile->fid, pFile->cid, pFile->lcn, suffix);
      } else {
        snprintf(fileName, sizeof(fileName), "v%df%dver%" PRId64 ".%s",
                 vnodeId, pFile->fid, pFile->cid, suffix);
      }

      uInfo("repair: vnode%d copy %s (%" PRId64 " bytes) -> level=%d id=%d",
            vnodeId, fileName, srcSize, tgtDid.level, tgtDid.id);

      // Copy file
      if (host == NULL || host[0] == '\0') {
        int64_t copied = taosCopyFile(srcPath, dstPath);
        if (copied < 0) {
          uError("repair: vnode%d failed to copy %s", vnodeId, fileName);
          taosArrayDestroy(newSet.files);
          dmDestroyRepairFileSets(remapped);
          return -1;
        }
      } else {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "scp -o BatchMode=yes '%s:%s' '%s' 2>/dev/null",
                 host, srcPath, dstPath);
        TdCmdPtr pCmd = taosOpenCmd(cmd);
        if (pCmd == NULL) {
          uError("repair: vnode%d scp failed for %s", vnodeId, fileName);
          taosArrayDestroy(newSet.files);
          dmDestroyRepairFileSets(remapped);
          return -1;
        }
        char buf[256];
        while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {}
        taosCloseCmd(&pCmd);
      }

      // Verify destination file size
      int64_t dstSize = 0;
      if (taosStatFile(dstPath, &dstSize, NULL, NULL) != 0 || dstSize != srcSize) {
        uError("repair: vnode%d size mismatch: %s (src=%" PRId64 " dst=%" PRId64 ")",
               vnodeId, fileName, srcSize, dstSize);
        taosArrayDestroy(newSet.files);
        dmDestroyRepairFileSets(remapped);
        return -1;
      }

      if (taosArrayPush(newSet.files, &tgtFile) == NULL) {
        taosArrayDestroy(newSet.files);
        dmDestroyRepairFileSets(remapped);
        return -1;
      }
    }

    if (taosArrayPush(remapped, &newSet) == NULL) {
      taosArrayDestroy(newSet.files);
      dmDestroyRepairFileSets(remapped);
      return -1;
    }
  }

  *ppRemappedSets = remapped;
  return 0;
}

// Step 5i: Generate target current.json from merged file sets.
// Combines retained file sets (from local with original disk IDs) and
// remapped file sets (copied from source with new disk IDs) into one
// current.json written to the target primary disk.
// The JSON format follows save_fs() in tsdbFS2.c.
static int32_t dmGenerateCurrentJson(STfs *pTgtTfs, int32_t vnodeId,
                                     const SArray *retainFids, const SArray *localFileSets,
                                     const SArray *remappedSets, const SArray *srcFileSets) {
  // Collect all file sets into one sorted array
  // retained: use localFileSets entries with matching fids (they have correct target disk IDs)
  // copied: use remappedSets entries (already have target disk IDs)
  int32_t nRetain = retainFids ? taosArrayGetSize(retainFids) : 0;
  int32_t nRemapped = remappedSets ? taosArrayGetSize(remappedSets) : 0;
  int32_t totalSets = nRetain + nRemapped;

  // Build sorted array of {fid, pointer to SRepairFileSet, pointer to source SRepairFileSet}
  typedef struct { int32_t fid; const SRepairFileSet *pSet; const SRepairFileSet *pSrcSet; } FSEntry;
  FSEntry *sorted = taosMemoryCalloc(totalSets, sizeof(FSEntry));
  if (sorted == NULL) return -1;

  int32_t idx = 0;
  // Add retained file sets (from local)
  for (int32_t r = 0; r < nRetain; r++) {
    int32_t fid = *(int32_t *)taosArrayGet(retainFids, r);
    int32_t nLocal = localFileSets ? taosArrayGetSize(localFileSets) : 0;
    for (int32_t l = 0; l < nLocal; l++) {
      SRepairFileSet *pLocal = taosArrayGet(localFileSets, l);
      if (pLocal->fid == fid) {
        // Find source file set for timestamps
        const SRepairFileSet *pSrcSet = NULL;
        int32_t nSrc = srcFileSets ? taosArrayGetSize(srcFileSets) : 0;
        for (int32_t s = 0; s < nSrc; s++) {
          SRepairFileSet *ps = taosArrayGet(srcFileSets, s);
          if (ps->fid == fid) { pSrcSet = ps; break; }
        }
        sorted[idx++] = (FSEntry){.fid = fid, .pSet = pLocal, .pSrcSet = pSrcSet};
        break;
      }
    }
  }
  // Add remapped (copied) file sets
  for (int32_t c = 0; c < nRemapped; c++) {
    SRepairFileSet *pRemap = taosArrayGet(remappedSets, c);
    // Find source file set for timestamps
    const SRepairFileSet *pSrcSet = NULL;
    int32_t nSrc = srcFileSets ? taosArrayGetSize(srcFileSets) : 0;
    for (int32_t s = 0; s < nSrc; s++) {
      SRepairFileSet *ps = taosArrayGet(srcFileSets, s);
      if (ps->fid == pRemap->fid) { pSrcSet = ps; break; }
    }
    sorted[idx++] = (FSEntry){.fid = pRemap->fid, .pSet = pRemap, .pSrcSet = pSrcSet};
  }
  totalSets = idx;

  // Sort by fid ascending
  for (int32_t i = 0; i < totalSets - 1; i++) {
    for (int32_t j = i + 1; j < totalSets; j++) {
      if (sorted[j].fid < sorted[i].fid) {
        FSEntry tmp = sorted[i];
        sorted[i] = sorted[j];
        sorted[j] = tmp;
      }
    }
  }

  // Build JSON: {"fmtv":1, "fset":[...]}
  SJson *pRoot = tjsonCreateObject();
  if (pRoot == NULL) { taosMemoryFree(sorted); return -1; }
  (void)tjsonAddDoubleToObject(pRoot, "fmtv", 1);

  SJson *pFsetArr = tjsonAddArrayToObject(pRoot, "fset");
  if (pFsetArr == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }

  for (int32_t i = 0; i < totalSets; i++) {
    const SRepairFileSet *pSet = sorted[i].pSet;
    const SRepairFileSet *pSrcSet = sorted[i].pSrcSet;
    if (pSet == NULL) continue;

    SJson *pFsetJson = tjsonCreateObject();
    if (pFsetJson == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }
    (void)tjsonAddItemToArray(pFsetArr, pFsetJson);
    (void)tjsonAddDoubleToObject(pFsetJson, "fid", pSet->fid);

    int32_t nFiles = taosArrayGetSize(pSet->files);

    // Serialize non-STT file types (head=0, data=1, sma=2, tomb=3) as named sub-objects
    for (int32_t t = 0; t < 4; t++) {
      // Find file of this type
      const SRepairFile *pFile = NULL;
      for (int32_t f = 0; f < nFiles; f++) {
        SRepairFile *pf = taosArrayGet(pSet->files, f);
        if (pf->type == t) { pFile = pf; break; }
      }
      if (pFile == NULL) continue;

      SJson *pFileJson = tjsonCreateObject();
      if (pFileJson == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }
      (void)tjsonAddItemToObject(pFsetJson, gRepairFTypeSuffixAll[t], pFileJson);
      (void)tjsonAddDoubleToObject(pFileJson, "did.level", pFile->did.level);
      (void)tjsonAddDoubleToObject(pFileJson, "did.id", pFile->did.id);
      (void)tjsonAddDoubleToObject(pFileJson, "lcn", pFile->lcn);
      (void)tjsonAddDoubleToObject(pFileJson, "fid", pFile->fid);
      (void)tjsonAddDoubleToObject(pFileJson, "cid", (double)pFile->cid);
      (void)tjsonAddDoubleToObject(pFileJson, "size", (double)pFile->size);
      if (pFile->minVer >= 0 && pFile->minVer <= pFile->maxVer) {
        (void)tjsonAddDoubleToObject(pFileJson, "minVer", (double)pFile->minVer);
        (void)tjsonAddDoubleToObject(pFileJson, "maxVer", (double)pFile->maxVer);
      }
    }

    // Serialize STT files grouped by sttLevel as "stt lvl" array
    // Collect distinct STT levels
    int32_t sttLevels[64] = {0};
    int32_t nSttLevels = 0;
    for (int32_t f = 0; f < nFiles; f++) {
      SRepairFile *pf = taosArrayGet(pSet->files, f);
      if (pf->type != 5) continue;
      bool found = false;
      for (int32_t sl = 0; sl < nSttLevels; sl++) {
        if (sttLevels[sl] == pf->sttLevel) { found = true; break; }
      }
      if (!found && nSttLevels < 64) sttLevels[nSttLevels++] = pf->sttLevel;
    }
    // Sort STT levels ascending
    for (int32_t a = 0; a < nSttLevels - 1; a++) {
      for (int32_t b = a + 1; b < nSttLevels; b++) {
        if (sttLevels[b] < sttLevels[a]) {
          int32_t tmp = sttLevels[a]; sttLevels[a] = sttLevels[b]; sttLevels[b] = tmp;
        }
      }
    }

    if (nSttLevels > 0) {
      SJson *pSttLvlArr = tjsonAddArrayToObject(pFsetJson, "stt lvl");
      if (pSttLvlArr == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }

      for (int32_t sl = 0; sl < nSttLevels; sl++) {
        SJson *pLvlJson = tjsonCreateObject();
        if (pLvlJson == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }
        (void)tjsonAddItemToArray(pSttLvlArr, pLvlJson);
        (void)tjsonAddDoubleToObject(pLvlJson, "level", sttLevels[sl]);

        SJson *pFilesArr = tjsonAddArrayToObject(pLvlJson, "files");
        if (pFilesArr == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }

        for (int32_t f = 0; f < nFiles; f++) {
          SRepairFile *pf = taosArrayGet(pSet->files, f);
          if (pf->type != 5 || pf->sttLevel != sttLevels[sl]) continue;

          SJson *pSttJson = tjsonCreateObject();
          if (pSttJson == NULL) { tjsonDelete(pRoot); taosMemoryFree(sorted); return -1; }
          (void)tjsonAddItemToArray(pFilesArr, pSttJson);
          (void)tjsonAddDoubleToObject(pSttJson, "did.level", pf->did.level);
          (void)tjsonAddDoubleToObject(pSttJson, "did.id", pf->did.id);
          (void)tjsonAddDoubleToObject(pSttJson, "lcn", pf->lcn);
          (void)tjsonAddDoubleToObject(pSttJson, "fid", pf->fid);
          (void)tjsonAddDoubleToObject(pSttJson, "cid", (double)pf->cid);
          (void)tjsonAddDoubleToObject(pSttJson, "size", (double)pf->size);
          if (pf->minVer >= 0 && pf->minVer <= pf->maxVer) {
            (void)tjsonAddDoubleToObject(pSttJson, "minVer", (double)pf->minVer);
            (void)tjsonAddDoubleToObject(pSttJson, "maxVer", (double)pf->maxVer);
          }
          (void)tjsonAddDoubleToObject(pSttJson, "level", pf->sttLevel);
        }
      }
    }

    // Add file set level timestamps from source
    int64_t lastCompact = pSrcSet ? pSrcSet->lastCompact : 0;
    int64_t lastCommit = pSrcSet ? pSrcSet->lastCommit : 0;
    (void)tjsonAddDoubleToObject(pFsetJson, "last compact", (double)lastCompact);
    (void)tjsonAddDoubleToObject(pFsetJson, "last commit", (double)lastCommit);
  }

  // Serialize to string and write to file
  char *jsonStr = tjsonToString(pRoot);
  tjsonDelete(pRoot);
  taosMemoryFree(sorted);
  if (jsonStr == NULL) {
    uError("repair: vnode%d failed to serialize current.json", vnodeId);
    return -1;
  }

  const char *primaryPath = tfsGetPrimaryPath(pTgtTfs);
  char outPath[PATH_MAX];
  snprintf(outPath, sizeof(outPath), "%s%svnode%svnode%d%stsdb%scurrent.json",
           primaryPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(outPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    uError("repair: vnode%d failed to create current.json: %s", vnodeId, outPath);
    taosMemoryFree(jsonStr);
    return -1;
  }

  int64_t len = (int64_t)strlen(jsonStr);
  int64_t written = taosWriteFile(pFile, jsonStr, len);
  if (written != len) {
    uError("repair: vnode%d failed to write current.json (wrote %" PRId64 "/%" PRId64 ")",
           vnodeId, written, len);
    taosCloseFile(&pFile);
    taosMemoryFree(jsonStr);
    return -1;
  }
  (void)taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(jsonStr);

  uInfo("repair: vnode%d current.json generated (%d file set(s))", vnodeId, totalSets);
  return 0;
}

// Step 5j: Update syncCfg.myIndex in vnode.json and raft_config.json.
// Finds the local dnodeId in nodeInfo[] and sets myIndex to that position.
static int32_t dmUpdateSyncIndex(STfs *pTgtTfs, int32_t vnodeId, int32_t dnodeId) {
  const char *primaryPath = tfsGetPrimaryPath(pTgtTfs);

  // --- Update vnode.json ---
  char vnodeJsonPath[PATH_MAX];
  snprintf(vnodeJsonPath, sizeof(vnodeJsonPath), "%s%svnode%svnode%d%svnode.json",
           primaryPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP);

  char *content = NULL;
  if (dmReadFileContent(vnodeJsonPath, &content, NULL) != 0) {
    uError("repair: vnode%d failed to read vnode.json", vnodeId);
    return -1;
  }

  SJson *pRoot = tjsonParse(content);
  taosMemoryFree(content);
  if (pRoot == NULL) {
    uError("repair: vnode%d failed to parse vnode.json", vnodeId);
    return -1;
  }

  SJson *pConfig = tjsonGetObjectItem(pRoot, "config");
  if (pConfig == NULL) {
    uError("repair: vnode%d vnode.json missing 'config'", vnodeId);
    tjsonDelete(pRoot);
    return -1;
  }

  // Find myIndex by matching dnodeId in syncCfg.nodeInfo[]
  SJson *pNodeInfoArr = tjsonGetObjectItem(pConfig, "syncCfg.nodeInfo");
  int32_t myIndex = -1;
  if (pNodeInfoArr != NULL) {
    int32_t nNodes = tjsonGetArraySize(pNodeInfoArr);
    for (int32_t i = 0; i < nNodes; i++) {
      SJson *pNode = tjsonGetArrayItem(pNodeInfoArr, i);
      int32_t nodeId = 0;
      int32_t code = 0;
      tjsonGetInt32ValueFromDouble(pNode, "nodeId", nodeId, code);
      if (code >= 0 && nodeId == dnodeId) {
        myIndex = i;
        break;
      }
    }
  }

  if (myIndex < 0) {
    uError("repair: vnode%d dnodeId %d not found in syncCfg.nodeInfo", vnodeId, dnodeId);
    tjsonDelete(pRoot);
    return -1;
  }

  // Replace syncCfg.myIndex
  tjsonDeleteItemFromObject(pConfig, "syncCfg.myIndex");
  (void)tjsonAddDoubleToObject(pConfig, "syncCfg.myIndex", myIndex);

  // Write back vnode.json
  char *jsonStr = tjsonToString(pRoot);
  tjsonDelete(pRoot);
  if (jsonStr == NULL) {
    uError("repair: vnode%d failed to serialize vnode.json", vnodeId);
    return -1;
  }

  TdFilePtr pFile = taosOpenFile(vnodeJsonPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    uError("repair: vnode%d failed to open vnode.json for write", vnodeId);
    taosMemoryFree(jsonStr);
    return -1;
  }
  int64_t len = (int64_t)strlen(jsonStr);
  int64_t written = taosWriteFile(pFile, jsonStr, len);
  (void)taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(jsonStr);
  if (written != len) {
    uError("repair: vnode%d failed to write vnode.json", vnodeId);
    return -1;
  }
  uInfo("repair: vnode%d vnode.json syncCfg.myIndex updated to %d", vnodeId, myIndex);

  // --- Update raft_config.json ---
  char raftCfgPath[PATH_MAX];
  snprintf(raftCfgPath, sizeof(raftCfgPath), "%s%svnode%svnode%d%ssync%sraft_config.json",
           primaryPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP, TD_DIRSEP);

  content = NULL;
  if (dmReadFileContent(raftCfgPath, &content, NULL) != 0) {
    // raft_config.json may not exist for single-replica vnodes — not an error
    uInfo("repair: vnode%d raft_config.json not found, skipping", vnodeId);
    return 0;
  }

  pRoot = tjsonParse(content);
  taosMemoryFree(content);
  if (pRoot == NULL) {
    uError("repair: vnode%d failed to parse raft_config.json", vnodeId);
    return -1;
  }

  SJson *pRaftCfg = tjsonGetObjectItem(pRoot, "RaftCfg");
  SJson *pSyncCfg = pRaftCfg ? tjsonGetObjectItem(pRaftCfg, "SSyncCfg") : NULL;
  if (pSyncCfg == NULL) {
    uError("repair: vnode%d raft_config.json missing RaftCfg.SSyncCfg", vnodeId);
    tjsonDelete(pRoot);
    return -1;
  }

  // Find myIndex by matching dnodeId in nodeInfo[]
  SJson *pRaftNodeInfo = tjsonGetObjectItem(pSyncCfg, "nodeInfo");
  int32_t raftMyIndex = -1;
  if (pRaftNodeInfo != NULL) {
    int32_t nNodes = tjsonGetArraySize(pRaftNodeInfo);
    for (int32_t i = 0; i < nNodes; i++) {
      SJson *pNode = tjsonGetArrayItem(pRaftNodeInfo, i);
      int32_t nodeId = 0;
      int32_t code = 0;
      tjsonGetInt32ValueFromDouble(pNode, "nodeId", nodeId, code);
      if (code >= 0 && nodeId == dnodeId) {
        raftMyIndex = i;
        break;
      }
    }
  }

  if (raftMyIndex < 0) {
    uError("repair: vnode%d dnodeId %d not found in raft_config nodeInfo", vnodeId, dnodeId);
    tjsonDelete(pRoot);
    return -1;
  }

  // Replace myIndex in SSyncCfg
  tjsonDeleteItemFromObject(pSyncCfg, "myIndex");
  (void)tjsonAddDoubleToObject(pSyncCfg, "myIndex", raftMyIndex);

  // Write back raft_config.json
  jsonStr = tjsonToString(pRoot);
  tjsonDelete(pRoot);
  if (jsonStr == NULL) {
    uError("repair: vnode%d failed to serialize raft_config.json", vnodeId);
    return -1;
  }

  pFile = taosOpenFile(raftCfgPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    uError("repair: vnode%d failed to open raft_config.json for write", vnodeId);
    taosMemoryFree(jsonStr);
    return -1;
  }
  len = (int64_t)strlen(jsonStr);
  written = taosWriteFile(pFile, jsonStr, len);
  (void)taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(jsonStr);
  if (written != len) {
    uError("repair: vnode%d failed to write raft_config.json", vnodeId);
    return -1;
  }
  uInfo("repair: vnode%d raft_config.json myIndex updated to %d", vnodeId, raftMyIndex);
  return 0;
}

// Step 5k: Clean sync state — delete raft_store.json and *.bak files in sync/.
static int32_t dmCleanSyncState(STfs *pTgtTfs, int32_t vnodeId) {
  const char *primaryPath = tfsGetPrimaryPath(pTgtTfs);
  char syncDir[PATH_MAX];
  snprintf(syncDir, sizeof(syncDir), "%s%svnode%svnode%d%ssync",
           primaryPath, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP);

  // Delete raft_store.json
  char raftStore[PATH_MAX];
  snprintf(raftStore, sizeof(raftStore), "%s%sraft_store.json", syncDir, TD_DIRSEP);
  if (taosCheckExistFile(raftStore)) {
    (void)taosRemoveFile(raftStore);
    uInfo("repair: vnode%d deleted raft_store.json", vnodeId);
  }

  // Delete *.bak files in sync/
  TdDirPtr pDir = taosOpenDir(syncDir);
  if (pDir == NULL) return 0;  // sync dir may not exist

  TdDirEntryPtr pEntry;
  while ((pEntry = taosReadDir(pDir)) != NULL) {
    char *name = taosGetDirEntryName(pEntry);
    if (name == NULL) continue;
    int32_t nameLen = (int32_t)strlen(name);
    if (nameLen > 4 && strcmp(name + nameLen - 4, ".bak") == 0) {
      char bakPath[PATH_MAX];
      snprintf(bakPath, sizeof(bakPath), "%s%s%s", syncDir, TD_DIRSEP, name);
      (void)taosRemoveFile(bakPath);
      uInfo("repair: vnode%d deleted sync/%s", vnodeId, name);
    }
  }
  taosCloseDir(&pDir);
  return 0;
}

// Step 5l: Delete vnodeN.bak on all target disks.
static int32_t dmDeleteBackup(STfs *pTgtTfs, int32_t vnodeId) {
  char relBak[TSDB_FILENAME_LEN];
  snprintf(relBak, sizeof(relBak), "vnode%svnode%d.bak", TD_DIRSEP, vnodeId);

  int32_t nlevel = tfsGetLevel(pTgtTfs);
  for (int32_t level = 0; level < nlevel; level++) {
    int32_t ndisk = tfsGetDisksAtLevel(pTgtTfs, level);
    for (int32_t id = 0; id < ndisk; id++) {
      SDiskID did = {.level = level, .id = id};
      const char *diskPath = tfsGetDiskPath(pTgtTfs, did);
      char fullPath[PATH_MAX];
      snprintf(fullPath, sizeof(fullPath), "%s%s%s", diskPath, TD_DIRSEP, relBak);
      if (taosDirExist(fullPath)) {
        taosRemoveDir(fullPath);
      }
    }
  }
  uInfo("repair: vnode%d backup deleted", vnodeId);
  return 0;
}

int32_t dmRepairCopyMode(const SRepairCopyOpts *pOpts) {
  bool isRemote = (pOpts->sourceHost[0] != '\0');

  uInfo("repair: starting copy-mode repair (%s mode)", isRemote ? "remote" : "local");
  uInfo("repair: source config: %s", pOpts->sourceCfg);
  if (isRemote) {
    uInfo("repair: source host: %s", pOpts->sourceHost);
  }
  int32_t nVnodes = taosArrayGetSize(pOpts->vnodeIds);
  uInfo("repair: vnodes to repair: %d", nVnodes);

  // Phase 2: Parse source config file
  const char *cfgPathToLoad = pOpts->sourceCfg;
  char        tmpCfgPath[PATH_MAX] = {0};

  if (isRemote) {
    // Fetch remote config via SSH
    snprintf(tmpCfgPath, sizeof(tmpCfgPath), "/tmp/tdrepair_%d.cfg", (int)taosGetPId());
    if (dmSshFetchFile(pOpts->sourceHost, pOpts->sourceCfg, tmpCfgPath) != 0) {
      uError("repair: failed to fetch remote config via SSH (exit code 2)");
      return 2;
    }
    cfgPathToLoad = tmpCfgPath;
  }

  SDiskCfg *srcDisks = NULL;
  int32_t   srcDiskNum = 0;
  int32_t   code = dmParseSourceCfg(cfgPathToLoad, &srcDisks, &srcDiskNum);
  if (tmpCfgPath[0] != '\0') {
    (void)taosRemoveFile(tmpCfgPath);
  }
  if (code != 0) {
    uError("repair: failed to parse source config file");
    return isRemote ? 2 : 1;
  }

  uInfo("repair: source config has %d disk(s)", srcDiskNum);

  // Build source TFS model
  SRepairTfs srcTfs = {0};
  if (dmBuildRepairTfs(srcDisks, srcDiskNum, &srcTfs) != 0) {
    uError("repair: failed to build source TFS model");
    taosMemoryFree(srcDisks);
    return 1;
  }
  taosMemoryFree(srcDisks);

  uInfo("repair: source TFS: %d level(s), %d disk(s), primary=%s",
       srcTfs.nlevel, srcTfs.ndisk, srcTfs.disks[srcTfs.primaryIdx].dir);

  // Validate source disk paths exist
  if (isRemote) {
    if (dmValidateSourceDisksRemote(pOpts->sourceHost, &srcTfs) != 0) {
      uError("repair: source disk validation failed (exit code 2)");
      dmDestroyRepairTfs(&srcTfs);
      return 2;
    }
  } else {
    if (dmValidateSourceDisksLocal(&srcTfs) != 0) {
      uError("repair: source disk validation failed");
      dmDestroyRepairTfs(&srcTfs);
      return 1;
    }
  }

  uInfo("repair: source disk validation passed");

  // Phase 3: Load dnode info and open target TFS
  int32_t dnodeId = 0;
  if (dmLoadDnodeInfo(&dnodeId) != 0) {
    uError("repair: failed to load dnode.json (exit code 1)");
    dmDestroyRepairTfs(&srcTfs);
    return 1;
  }
  uInfo("repair: local dnodeId = %d", dnodeId);

  STfs *pTgtTfs = NULL;
  if (dmOpenTargetTfs(&pTgtTfs) != 0) {
    uError("repair: failed to open target TFS (exit code 1)");
    dmDestroyRepairTfs(&srcTfs);
    return 1;
  }
  uInfo("repair: target TFS: %d level(s), primary=%s",
       tfsGetLevel(pTgtTfs), tfsGetPrimaryPath(pTgtTfs));

  // Phase 4: Per-vnode repair loop
  const char *remoteHost = isRemote ? pOpts->sourceHost : NULL;
  int32_t     nSuccess = 0;
  int32_t     nSkipped = 0;
  int32_t     nFailed = 0;

  for (int32_t v = 0; v < nVnodes; v++) {
    int32_t vnodeId = *(int32_t *)taosArrayGet(pOpts->vnodeIds, v);
    uInfo("repair: === vnode%d [%d/%d] ===", vnodeId, v + 1, nVnodes);

    // Step 5a: Check for existing .bak on any target disk
    if (dmCheckBakExists(pTgtTfs, vnodeId)) {
      uInfo("repair: vnode%d SKIPPED — vnode%d.bak already exists on target", vnodeId, vnodeId);
      nSkipped++;
      continue;
    }

    // Step 5b: Read and parse source current.json
    SArray *srcFileSets = dmReadSourceCurrentJson(&srcTfs, remoteHost, vnodeId);
    if (srcFileSets == NULL) {
      uInfo("repair: vnode%d SKIPPED — source current.json not found or unreadable", vnodeId);
      nSkipped++;
      continue;
    }

    int32_t nSets = taosArrayGetSize(srcFileSets);
    int32_t nTotalFiles = 0;
    for (int32_t s = 0; s < nSets; s++) {
      SRepairFileSet *pSet = taosArrayGet(srcFileSets, s);
      nTotalFiles += taosArrayGetSize(pSet->files);
    }
    uInfo("repair: vnode%d source has %d file set(s), %d file(s) total", vnodeId, nSets, nTotalFiles);

    // Step 5c: Read local current.json and diff against source
    SArray *localFileSets = dmReadLocalCurrentJson(pTgtTfs, vnodeId);
    SArray *copyFids = NULL;
    SArray *retainFids = NULL;
    dmDiffFileSets(srcFileSets, localFileSets, pTgtTfs, vnodeId, &copyFids, &retainFids);

    int32_t nCopy = taosArrayGetSize(copyFids);
    int32_t nRetain = taosArrayGetSize(retainFids);
    if (localFileSets != NULL) {
      uInfo("repair: vnode%d local current.json found: %d file set(s)",
           vnodeId, (int)taosArrayGetSize(localFileSets));
    } else {
      uInfo("repair: vnode%d local current.json not found, will copy all", vnodeId);
    }
    uInfo("repair: vnode%d file sets to copy: %d, to retain: %d", vnodeId, nCopy, nRetain);

    // Step 5d: Backup vnodeN → vnodeN.bak on all disks
    if (dmBackupVnode(pTgtTfs, vnodeId) != 0) {
      uError("repair: vnode%d FAILED — backup failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }
    uInfo("repair: vnode%d backup completed", vnodeId);

    // Step 5e: Create vnodeN directories on all disks
    if (dmCreateVnodeDirs(pTgtTfs, vnodeId) != 0) {
      uError("repair: vnode%d FAILED — failed to create directories", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }
    uInfo("repair: vnode%d directories created", vnodeId);

    // Step 5f: Hard-link retained tsdb files from backup
    if (nRetain > 0 && localFileSets != NULL) {
      if (dmHardLinkRetainedFiles(pTgtTfs, vnodeId, retainFids, localFileSets) != 0) {
        uError("repair: vnode%d FAILED — hard-link retained files failed", vnodeId);
        nFailed++;
        goto _vnodeCleanup;
      }
      uInfo("repair: vnode%d hard-linked %d retained file set(s)", vnodeId, nRetain);
    }

    // Step 5g: Copy non-tsdb files from source to target primary disk
    if (dmCopyNonTsdbFiles(&srcTfs, pTgtTfs, remoteHost, vnodeId) != 0) {
      uError("repair: vnode%d FAILED — copy non-tsdb files failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }
    uInfo("repair: vnode%d non-tsdb files copied", vnodeId);

    // Step 5h: Copy source TSDB file sets with disk ID remapping
    SArray *remappedSets = NULL;
    if (nCopy > 0) {
      if (dmCopySourceFileSets(&srcTfs, pTgtTfs, remoteHost, vnodeId,
                               srcFileSets, copyFids, &remappedSets) != 0) {
        uError("repair: vnode%d FAILED — copy source file sets failed", vnodeId);
        nFailed++;
        goto _vnodeCleanup;
      }
      uInfo("repair: vnode%d copied %d file set(s)", vnodeId, nCopy);
    }

    // Step 5i: Generate target current.json
    if (dmGenerateCurrentJson(pTgtTfs, vnodeId, retainFids, localFileSets,
                              remappedSets, srcFileSets) != 0) {
      uError("repair: vnode%d FAILED — generate current.json failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }

    // TODO: Steps 5j-5l — update vnode.json, clean sync state, delete backup

    // Step 5j: Update syncCfg.myIndex in vnode.json and raft_config.json
    if (dmUpdateSyncIndex(pTgtTfs, vnodeId, dnodeId) != 0) {
      uError("repair: vnode%d FAILED — update sync index failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }

    // Step 5k: Clean sync state
    if (dmCleanSyncState(pTgtTfs, vnodeId) != 0) {
      uError("repair: vnode%d FAILED — clean sync state failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }

    // Step 5l: Delete backup
    if (dmDeleteBackup(pTgtTfs, vnodeId) != 0) {
      uError("repair: vnode%d FAILED — delete backup failed", vnodeId);
      nFailed++;
      goto _vnodeCleanup;
    }

    nSuccess++;
_vnodeCleanup:
    dmDestroyRepairFileSets(remappedSets);
    taosArrayDestroy(copyFids);
    taosArrayDestroy(retainFids);
    dmDestroyRepairFileSets(localFileSets);
    dmDestroyRepairFileSets(srcFileSets);
  }

  uInfo("repair: === summary ===");
  uInfo("repair: success=%d, skipped=%d, failed=%d", nSuccess, nSkipped, nFailed);

  tfsClose(pTgtTfs);
  dmDestroyRepairTfs(&srcTfs);

  if (nFailed > 0 && nSuccess == 0) return 4;
  if (nFailed > 0) return 3;
  return 0;
}

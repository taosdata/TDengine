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

#include "dmMgmt.h"
#include "tchecksum.h"

extern char tsVersionName[];

typedef enum {
  DM_ENG_FVER_1 = 1,
} DM_ENG_FVER;

typedef enum {
  DM_ETYPE_UN = 0,
  DM_ETYPE_OS = 1,
  DM_ETYPE_TR = 2,
  DM_ETYPE_EN = 3,
} DM_ENG_TYPE;

#define DM_ENG_FVER_MAX DM_ENG_FVER_1

#define DM_ENGINE_FILE "dnode.info"
#define DM_ENGINE_FILE_T "dnode.info.t"

typedef struct {
  int8_t  type;  // 0 unknown 1 community 2 trial 3 official
  int32_t dnodeId;
  int32_t engineVer;  // tsVersion
  int64_t clusterId;
  int64_t createMs;
  int64_t updateMs;
} SEngineInfo;

// file operation (TODO: refactor to dmUtil.c)
#define DM_FILE_HEAD_SIZE 512
typedef struct {
  uint32_t version;
  uint32_t len;  // Encoded content len(checksum included)
} SDFHeader;

#define STR_CASE_CMP(s, d) (0 == strcasecmp((s), (d)))
#define STR_STR_CMP(s, d) (strstr((s), (d)))
#define STR_INT_CMP(s, d, c) (taosStr2Int32(s, 0, 10) c(d))
#define STR_STR_SIGN ("ia")
#define STR_STR_COMM ("unit")

#define DM_ERR_RTN(c) \
  do {                \
    code = (c);       \
    goto _exit;       \
  } while (0)

static const char *dmOS[10] = {"Ubuntu",  "CentOS Linux", "Red Hat", "Debian GNU", "CoreOS",
                               "FreeBSD", "openSUSE",     "SLES",    "Fedora",     "macOS"};

// declarations
static void    dmFetchEType(int8_t *type);
static void    dmGetFname(const char *fname, char *ofname);
static int32_t dmSyncEps(SDnodeData *pData);
static int32_t dmEncodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo);
static int32_t dmEncodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo);
static int32_t dmReadVars(SEngineInfo *pInfo);
static int32_t dmWriteVars(SEngineInfo *pInfo);

// implementations

static FORCE_INLINE bool dmIsCloudVer() {
#ifdef GRANTS_CFG
  return true;
#endif
  return false;
}

static int32_t dmInitPrerequisites() {
#ifndef _TD_DARWIN_64
  int32_t code = 0;

  char reName[64] = {0};
  char stName[64] = {0};
  char ver[64] = {0};

  code = (int32_t)(2147483648 | 298);
  strncpy(stName, tsVersionName, 64);

  if (STR_STR_CMP(stName, STR_STR_SIGN)) {
    DM_ERR_RTN(0);
  }
  if (taosGetOsReleaseName(reName, stName, ver, 64) != 0) {
    DM_ERR_RTN(code);
  }
  if (STR_CASE_CMP(stName, dmOS[0])) {
    if (STR_INT_CMP(ver, 17, >)) {
      DM_ERR_RTN(0);
    }
  } else if (STR_CASE_CMP(stName, dmOS[1])) {
    if (STR_INT_CMP(ver, 6, >)) {
      DM_ERR_RTN(0);
    }
  } else if (STR_STR_CMP(stName, dmOS[2]) || STR_STR_CMP(stName, dmOS[3]) || STR_STR_CMP(stName, dmOS[4]) ||
             STR_STR_CMP(stName, dmOS[5]) || STR_STR_CMP(stName, dmOS[6]) || STR_STR_CMP(stName, dmOS[7]) ||
             STR_STR_CMP(stName, dmOS[8]) || STR_STR_CMP(stName, dmOS[9])) {
    DM_ERR_RTN(0);
  }

_exit:
  if (code) terrno = code;
  return code;
#else
  return 0;
#endif
}

static int dmEncodeDFHeader(void **buf, SDFHeader *pHeader) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pHeader->version);
  tlen += taosEncodeFixedU32(buf, pHeader->len);

  return tlen;
}

static void *dmDecodeDFHeader(void *buf, SDFHeader *pHeader) {
  buf = taosDecodeFixedU32(buf, &(pHeader->version));
  buf = taosDecodeFixedU32(buf, &(pHeader->len));

  return buf;
}

static int32_t dmEncodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  int32_t ret = -1;

  if (tStartEncode(&encoder) < 0) goto _exit;
  if (tEncodeI8(&encoder, pInfo->type) < 0) goto _exit;
  if (tEncodeI32v(&encoder, pInfo->dnodeId) < 0) goto _exit;
  if (tEncodeI32v(&encoder, pInfo->engineVer) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pInfo->clusterId) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pInfo->createMs) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pInfo->updateMs) < 0) goto _exit;

  tEndEncode(&encoder);
  ret = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  return ret;
}

static int32_t dmDecodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t ret = -1;

  if (tStartDecode(&decoder) < 0) goto _exit;

  if (tDecodeI8(&decoder, &pInfo->type) < 0) goto _exit;
  if (tDecodeI32v(&decoder, &pInfo->dnodeId) < 0) goto _exit;
  if (tDecodeI32v(&decoder, &pInfo->engineVer) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pInfo->clusterId) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pInfo->createMs) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pInfo->updateMs) < 0) goto _exit;

  tEndDecode(&decoder);
  ret = 0;
_exit:
  tDecoderClear(&decoder);
  return ret;
}

static int32_t dmReadVars(SEngineInfo *pInfo) {
  int32_t   code = 0;
  void     *buffer = NULL;
  void     *ptr;
  SDFHeader dHeader;
  char      fname[FILENAME_MAX] = "\0";

  dmGetFname(DM_ENGINE_FILE, fname);

  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
  if (!pFile) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  if (!(buffer = taosMemoryMalloc(DM_FILE_HEAD_SIZE))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int64_t nRead = taosReadFile(pFile, buffer, DM_FILE_HEAD_SIZE);
  if (nRead != DM_FILE_HEAD_SIZE) {
    code = TAOS_SYSTEM_ERROR(errno);
    if (code == 0) code = TSDB_CODE_FILE_CORRUPTED;
    dError("failed to read %d bytes from file %s since %s", DM_FILE_HEAD_SIZE, fname, tstrerror(code));
    goto _exit;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buffer, DM_FILE_HEAD_SIZE)) {
    dError("header of file %s is corrupted since wrong checksum", fname);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  ptr = buffer;
  ptr = dmDecodeDFHeader(ptr, &dHeader);

  if (dHeader.version != DM_ENG_FVER_1) {
    // TODO
  }

  if (dHeader.len > 0) {
    if (dHeader.len > DM_FILE_HEAD_SIZE) {
      void *tmpBuf = NULL;
      if (!(tmpBuf = taosMemoryRealloc(buffer, dHeader.len))) {
        goto _exit;
      }
      buffer = tmpBuf;
    }

    nRead = (int)taosReadFile(pFile, buffer, dHeader.len);
    if (nRead != dHeader.len) {
      code = TAOS_SYSTEM_ERROR(errno);
      if (code == 0) code = TSDB_CODE_FILE_CORRUPTED;
      dError("failed to read %d bytes from file %s since %s", DM_FILE_HEAD_SIZE, fname, tstrerror(code));
      goto _exit;
    }

    if (!taosCheckChecksumWhole((uint8_t *)buffer, dHeader.len)) {
      dError("file %s is corrupted since wrong checksum", fname);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _exit;
    }

    ptr = buffer;
    if (dmDecodeVars(ptr, dHeader.len, pInfo) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  if (code != 0) {
    dError("failed to init dm vars, read file %s failed since %s", fname, tstrerror(code));
  }
  taosMemoryFreeClear(buffer);
  taosCloseFile(&pFile);
  return code;
}

static void dmGetFname(const char *fname, char *ofname) {
  if (fname) {
    snprintf(ofname, PATH_MAX, "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, fname);
  } else {
    snprintf(ofname, PATH_MAX, "%s%sdnode", tsDataDir, TD_DIRSEP);
  }
}

int32_t dmInitDndInfo(SDnodeData *pData) {
#ifndef _TD_DARWIN_64
  int32_t code = 0;
  char    cfname[PATH_MAX] = "\0";

  dmGetFname(DM_ENGINE_FILE, cfname);
  bool fileExist = !(taosStatFile(cfname, NULL, NULL, NULL) < 0);
  if (fileExist) {
    return code;
  }

  int8_t      eType = 0;
  SEngineInfo eInfo = {0};
  dmFetchEType(&eType);
  eInfo.type = eType;
  eInfo.dnodeId = pData->dnodeId;
  eInfo.engineVer = tsVersion;
  eInfo.clusterId = pData->clusterId;
  eInfo.createMs = taosGetTimestampMs();
  eInfo.updateMs = eInfo.createMs;

  if ((code = dmWriteVars(&eInfo)) != 0) goto _exit;

_exit:
  if (code != 0) {
    assert(0);
  }
  return code;
#else
  return 0;
#endif
}

static int32_t dmWriteVars(SEngineInfo *pInfo) {
  SDFHeader fHeader;
  void     *pBuf = NULL;
  void     *ptr;
  char      hbuf[DM_FILE_HEAD_SIZE] = "\0";
  char      tfname[PATH_MAX] = "\0";
  char      cfname[PATH_MAX] = "\0";
  int32_t   code = 0;

  dmGetFname(DM_ENGINE_FILE_T, tfname);
  dmGetFname(DM_ENGINE_FILE, cfname);

  TdFilePtr tFile = taosOpenFile(tfname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (!tFile) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }

  fHeader.version = DM_ENG_FVER_MAX;
  fHeader.len = dmEncodeVars(NULL, 0, pInfo) + sizeof(TSCKSUM);

  ptr = hbuf;
  dmEncodeDFHeader(&ptr, &fHeader);
  taosCalcChecksumAppend(0, (uint8_t *)hbuf, DM_FILE_HEAD_SIZE);

  if (taosWriteFile(tFile, hbuf, DM_FILE_HEAD_SIZE) < DM_FILE_HEAD_SIZE) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  if (fHeader.len > 0) {
    if (!(pBuf = taosMemoryMalloc(fHeader.len))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    ptr = pBuf;
    dmEncodeVars(ptr, fHeader.len - sizeof(TSCKSUM), pInfo);
    taosCalcChecksumAppend(0, (uint8_t *)pBuf, fHeader.len);

    if (taosWriteFile(tFile, pBuf, fHeader.len) < fHeader.len) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _exit;
    }
  }

  // fsync, close and rename
  if (taosFsyncFile(tFile) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }
  if (taosCloseFile(&tFile) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }
  if (taosRenameFile(tfname, cfname) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

_exit:
  taosMemoryFreeClear(pBuf);
  if (code != 0) {
    dError("failed to write dm vars to %s since %s", cfname, tstrerror(code));
    taosCloseFile(&tFile);
    taosRemoveFile(tfname);
  }

  return code;
}

static void dmFetchEType(int8_t *type) {
  int8_t eType = DM_ETYPE_UN;
  if (STR_STR_CMP(tsVersionName, STR_STR_SIGN)) {
    if (strncmp(tsVersionName, "t", 1)) {
      eType = DM_ETYPE_TR;
    } else {
      eType = DM_ETYPE_EN;
    }
  } else if (STR_STR_CMP(tsVersionName, STR_STR_COMM)) {
    eType = DM_ETYPE_OS;
  }
  if (type) *type = eType;
}

static int32_t dmInitVersion(SDnode *pDnode) {
#ifndef _TD_DARWIN_64
  int32_t     code = 0;
  int8_t      eType = 0;
  SEngineInfo eInfo = {0};

  if (dmIsCloudVer()) goto _exit;

  char cfgFile[PATH_MAX] = "\0";
  dmGetFname("dnode.json", cfgFile);
  if (taosStatFile(cfgFile, NULL, NULL, NULL) < 0) goto _exit;

  dmFetchEType(&eType);

  taosThreadRwlockRdlock(pDnode);
  if (((code = dmReadVars(&eInfo)) != 0) && (errno != ENOENT)) {
    taosThreadRwlockUnlock(pDnode);
    goto _exit;
  }
  taosThreadRwlockUnlock(pDnode);

  if (pDnode->data.engineVer == 0) {           // dnode.json history version
    if ((eInfo.type & 0x0F) == DM_ETYPE_UN) {  // without DM_ENGINE_FILE, create(handle update from history versin)
      eInfo.type = eType;
      eInfo.dnodeId = pDnode->data.dnodeId;
      eInfo.engineVer = tsVersion;
      eInfo.clusterId = pDnode->data.clusterId;
      eInfo.createMs = taosGetTimestampMs();
      eInfo.updateMs = eInfo.createMs;
      // save
      taosThreadRwlockWrlock(&pData->lock);
      if ((code = dmWriteVars(&eInfo)) != 0) {
        taosThreadRwlockUnlock(pDnode);
        goto _exit;
      }
      taosThreadRwlockUnlock(pDnode);
      code = dmSyncEps(&pDnode->data);
      goto _exit;
    }
  } else if ((eInfo.type & 0x0F) == DM_ETYPE_UN) {  // not history version, but without DM_ENGINE_FILE, fail
    dError("failed to init version since lack of file(0x:%x-%x-%x)", eType, eInfo.type, pDnode->data.engineVer);
    code = TSDB_CODE_VERSION_NOT_COMPATIBLE;
    goto _exit;
  } else if (pDnode->data.clusterId !=
             eInfo.clusterId) {  // not history version, DM_ENGINE_FILE exists, check clusterId
    dError("failed to init version since inconsistent cluster Id, %" PRIi64 ":%" PRIi64, pDnode->data.clusterId,
           eInfo.clusterId);
    code = TSDB_CODE_VERSION_NOT_COMPATIBLE;
    goto _exit;
  } else if (pDnode->data.engineVer != tsVersion) {  // updateEps
    dmSyncEps(&pDnode->data);
  }

  if (eType == DM_ETYPE_OS) {        // oss
    if (eInfo.type > DM_ETYPE_OS) {  // enterprise to oss not allowed
      code = TSDB_CODE_VERSION_NOT_COMPATIBLE;
      dError("node:%d, failed to init version since %s(0x:%x-%x-%x)", pDnode->data.dnodeId, terrstr(), eType,
             eInfo.type, pDnode->data.engineVer);
      goto _exit;
    }
  } else if (eInfo.type == DM_ETYPE_OS) {  // update oss to enterprise
    eInfo.type = eType;
    eInfo.engineVer = tsVersion;
    eInfo.updateMs = taosGetTimestampMs();
    taosThreadRwlockWrlock(&pData->lock);
    if ((code = dmWriteVars(&eInfo)) != 0) {
      taosThreadRwlockUnlock(pDnode);
      goto _exit;
    }
    taosThreadRwlockUnlock(pDnode);
  }
_exit:
  return code;
#else
  return 0;
#endif
}

static int32_t dmSyncEps(SDnodeData *pData) {
  int32_t code = 0;
  char    file[PATH_MAX] = "\0";
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  taosThreadRwlockWrlock(&pData->lock);
  bool fileExist = !(taosStatFile(file, NULL, NULL, NULL) < 0);
  if (fileExist) {
    code = dmWriteEps(pData);
  }
  taosThreadRwlockUnlock(&pData->lock);
  return code;
}

bool dmRequireNode(SDnode *pDnode, SMgmtWrapper *pWrapper) {
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  bool    required = false;
  int32_t code = (*pWrapper->func.requiredFp)(&input, &required);
  if (!required) {
    dDebug("node:%s, does not require startup", pWrapper->name);
  } else {
    dDebug("node:%s, required to startup", pWrapper->name);
  }

  return required;
}

int32_t dmInitVars(SDnode *pDnode) {
  SDnodeData *pData = &pDnode->data;
  pData->dnodeId = 0;
  pData->clusterId = 0;
  pData->dnodeVer = 0;
  pData->engineVer = 0;
  pData->updateTime = 0;
  pData->rebootTime = taosGetTimestampMs();
  pData->dropped = 0;
  pData->stopped = 0;

  pData->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pData->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadEps(pData) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pData->dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  taosThreadRwlockInit(&pData->lock, NULL);
  taosThreadMutexInit(&pDnode->mutex, NULL);
  return 0;
}

void dmClearVars(SDnode *pDnode) {
  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    taosMemoryFreeClear(pWrapper->path);
    taosThreadRwlockDestroy(&pWrapper->lock);
  }
  if (pDnode->lockfile != NULL) {
    taosUnLockFile(pDnode->lockfile);
    taosCloseFile(&pDnode->lockfile);
    pDnode->lockfile = NULL;
  }

  SDnodeData *pData = &pDnode->data;
  taosThreadRwlockWrlock(&pData->lock);
  if (pData->oldDnodeEps != NULL) {
    if (dmWriteEps(pData) == 0) {
      dmRemoveDnodePairs(pData);
    }
    taosArrayDestroy(pData->oldDnodeEps);
    pData->oldDnodeEps = NULL;
  }
  if (pData->dnodeEps != NULL) {
    taosArrayDestroy(pData->dnodeEps);
    pData->dnodeEps = NULL;
  }
  if (pData->dnodeHash != NULL) {
    taosHashCleanup(pData->dnodeHash);
    pData->dnodeHash = NULL;
  }
  taosThreadRwlockUnlock(&pData->lock);

  taosThreadRwlockDestroy(&pData->lock);
  taosThreadMutexDestroy(&pDnode->mutex);
  memset(&pDnode->mutex, 0, sizeof(pDnode->mutex));
}

// invoker
int32_t dmInitModule(SDnode *pDnode) {
  int32_t code = -1;

  if (dmInitPrerequisites() != 0) {
    goto _err;
  }
  if (dmInitVersion(pDnode) != 0) {
    terrno = TSDB_CODE_VERSION_NOT_COMPATIBLE;
    goto _err;
  }

  if (dmInitMsgHandle(pDnode) != 0) {
    dError("failed to init msg handles since %s", terrstr());
    goto _err;
  }

  if (dmInitServer(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    goto _err;
  }

  if (dmInitClient(pDnode) != 0) {
    goto _err;
  }
_exit:
  code = 0;
_err:

  return code;
}
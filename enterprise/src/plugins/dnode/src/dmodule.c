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

#if defined(GRANTS_CFG) || defined(_TD_DARWIN_64)
#define _TD_DM_SKIP_CHECK
#endif

#if !defined(ASSERT_NOT_CORE) && !defined(WINDOWS)
#define _TD_DM_CHECK_OFFSET
#define DM_CHECK_OFFSET(p1, p2, offset, flag)             \
  do {                                                    \
    int32_t off = POINTER_DISTANCE((p1), (p2));           \
    if ((offset) != abs(off)) {                           \
      dError("%s offset: %d!=%d", (flag), off, (offset)); \
      return TSDB_CODE_INTERNAL_ERROR;                    \
    }                                                     \
  } while (0)
#endif

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
#define DM_OS_ST_NAME_LEN 64

#define DM_ENGINE_FILE "dnode.info"
#define DM_ENGINE_FILE_T "dnode.info.t"
#define DNODE_CFG_FILE "dnode.json"

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

#define STR_CASE_STR_CHECK(s, d)   \
  do {                             \
    TAOS_UNUSED(strtolower(s, s)); \
    TAOS_UNUSED(strtolower(d, d)); \
    if (STR_STR_CMP(s, d)) {       \
      DM_ERR_RTN(0);               \
    }                              \
  } while (0)

#define DM_ERR_RTN(c) \
  do {                \
    code = (c);       \
    goto _exit;       \
  } while (0)

static const char *dmOS[] = {"Ubuntu", "CentOS Linux", "Red Hat", "Debian GNU/Linux", "CoreOS", "FreeBSD", "openSUSE",
                             "SLES",   "Fedora",       "macOS",   "CentOS Stream"};

// declarations
static void    dmFetchEType(int8_t *type);
static void    dmGetFname(const char *fname, char *ofname);
static int32_t dmSyncEps(SDnodeData *pData);
static int32_t dmEncodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo);
static int32_t dmEncodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo);
static int32_t dmReadVars(SEngineInfo *pInfo);
static int32_t dmWriteVars(SEngineInfo *pInfo);

// implementations

#ifdef _TD_DM_CHECK_OFFSET
static int32_t dmCheckOffset(SDnode *pDnode) {
  SDnodeData     *pData = &pDnode->data;
  TdThreadRwlock *pLock = &pData->lock;
  int32_t        *pDnodeId = &pData->dnodeId;
  int32_t        *pEngineVer = &pData->engineVer;
  int64_t        *pClusterId = &pData->clusterId;

  DM_CHECK_OFFSET(pData, pDnode, 168, "dnode data");
  DM_CHECK_OFFSET(pLock, pData, 720, "data lock");
  DM_CHECK_OFFSET(pDnodeId, pData, 0, "data dnodeId");
  DM_CHECK_OFFSET(pEngineVer, pData, 4, "data engineVer");
  DM_CHECK_OFFSET(pClusterId, pData, 8, "data clusterId");

  return TSDB_CODE_SUCCESS;
}
#endif

static int32_t dmInitPrerequisites() {
#ifndef _TD_DM_SKIP_CHECK
  int32_t code = 0;

  char reName[64] = {0};
  char stName[64] = {0};
  char ver[64] = {0};

  code = (int32_t)(2147483648 | 298);
  tstrncpy(stName, tsVersionName, 16);

  if (STR_STR_CMP(stName, STR_STR_SIGN)) {
    DM_ERR_RTN(0);
  }
  if (taosGetOsReleaseName(reName, stName, ver, DM_OS_ST_NAME_LEN) != 0) {
    int32_t errCode = TAOS_SYSTEM_ERROR(errno);
    if (errCode != 0) code = errCode;
    TAOS_CHECK_GOTO(code, NULL, _exit);
  }
  if (STR_CASE_CMP(stName, dmOS[0])) {
    if (STR_INT_CMP(ver, 17, >)) {
      DM_ERR_RTN(0);
    }
  } else if (STR_CASE_CMP(stName, dmOS[1])) {
    if (STR_INT_CMP(ver, 6, >)) {
      DM_ERR_RTN(0);
    }
  } else {
    int32_t size = sizeof(dmOS) / sizeof(dmOS[0]);
    char    os[DM_OS_ST_NAME_LEN] = {0};
    for (int32_t i = 2; i < size; ++i) {
      tstrncpy(os, dmOS[i], DM_OS_ST_NAME_LEN);
      STR_CASE_STR_CHECK(stName, os);
    }
  }

_exit:
  TAOS_RETURN(code);
#else
  TAOS_RETURN(0);
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
  int32_t code = 0;

  TAOS_CHECK_GOTO(tStartEncode(&encoder), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI8(&encoder, pInfo->type), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI32v(&encoder, pInfo->dnodeId), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI32v(&encoder, pInfo->engineVer), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI64v(&encoder, pInfo->clusterId), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI64v(&encoder, pInfo->createMs), NULL, _exit);
  TAOS_CHECK_GOTO(tEncodeI64v(&encoder, pInfo->updateMs), NULL, _exit);

  tEndEncode(&encoder);
  code = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  return code;
}

static int32_t dmDecodeVars(void *buf, int32_t bufLen, SEngineInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  int32_t code = 0;

  TAOS_CHECK_GOTO(tStartDecode(&decoder), NULL, _exit);

  TAOS_CHECK_GOTO(tDecodeI8(&decoder, &pInfo->type), NULL, _exit);
  TAOS_CHECK_GOTO(tDecodeI32v(&decoder, &pInfo->dnodeId), NULL, _exit);
  TAOS_CHECK_GOTO(tDecodeI32v(&decoder, &pInfo->engineVer), NULL, _exit);
  TAOS_CHECK_GOTO(tDecodeI64v(&decoder, &pInfo->clusterId), NULL, _exit);
  TAOS_CHECK_GOTO(tDecodeI64v(&decoder, &pInfo->createMs), NULL, _exit);
  TAOS_CHECK_GOTO(tDecodeI64v(&decoder, &pInfo->updateMs), NULL, _exit);

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  TAOS_RETURN(code);
}

static int32_t dmReadVars(SEngineInfo *pInfo) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr pFile = NULL;
  void     *buffer = NULL;
  void     *ptr;
  SDFHeader dHeader;
  char      fname[FILENAME_MAX] = "\0";

  dmGetFname(DM_ENGINE_FILE, fname);

  errno = 0;  // clear errno

  if (!taosCheckExistFile(fname)) {
    TAOS_CHECK_GOTO(TSDB_CODE_NOT_FOUND, &lino, _exit);
  }

  pFile = taosOpenFile(fname, TD_FILE_READ);
  if (!pFile) {
    if (errno == ENOENT) {
      TAOS_CHECK_GOTO(TSDB_CODE_NOT_FOUND, &lino, _exit);
    } else {
      TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &lino, _exit);
    }
  }

  if (!(buffer = taosMemoryMalloc(DM_FILE_HEAD_SIZE))) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  int64_t nRead = taosReadFile(pFile, buffer, DM_FILE_HEAD_SIZE);
  if (nRead < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &lino, _exit);
  }

  if (nRead != DM_FILE_HEAD_SIZE) {
    code = TSDB_CODE_FILE_CORRUPTED;
    dTrace("failed to read %d bytes from vars head since %s", DM_FILE_HEAD_SIZE, tstrerror(code));
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  if (!taosCheckChecksumWhole((uint8_t *)buffer, DM_FILE_HEAD_SIZE)) {
    dTrace("failed to read vars head since wrong checksum");
    TAOS_CHECK_GOTO(TSDB_CODE_CHECKSUM_ERROR, &lino, _exit);
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
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      buffer = tmpBuf;
    }

    nRead = (int)taosReadFile(pFile, buffer, dHeader.len);
    if (nRead < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    if (nRead != dHeader.len) {
      code = TSDB_CODE_FILE_CORRUPTED;
      dTrace("failed to read %d bytes from vars body since %s", dHeader.len, tstrerror(code));
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    if (!taosCheckChecksumWhole((uint8_t *)buffer, dHeader.len)) {
      dTrace("failed to read vars body since wrong checksum");
      TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
    }

    ptr = buffer;
    TAOS_CHECK_GOTO(dmDecodeVars(ptr, dHeader.len, pInfo), &lino, _exit);
  }

_exit:
  if (code != 0) {
    dError("failed to read vars at line %d since %s", lino, tstrerror(code));
  }
  taosMemoryFreeClear(buffer);
  (void)taosCloseFile(&pFile);
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
#ifndef _TD_DM_SKIP_CHECK
  int32_t code = 0;
  char    cfname[PATH_MAX] = "\0";

  dmGetFname(DNODE_CFG_FILE, cfname);
  bool fileExist = !(taosStatFile(cfname, NULL, NULL, NULL) < 0);
  if (fileExist) {  // dnode.info must be created before dnode.json
    return code;
  }
  dmGetFname(DM_ENGINE_FILE, cfname);
  fileExist = !(taosStatFile(cfname, NULL, NULL, NULL) < 0);
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
  TAOS_RETURN(code);
#else
  TAOS_RETURN(0);
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
  int32_t   lino = 0;

  dmGetFname(DM_ENGINE_FILE_T, tfname);
  dmGetFname(DM_ENGINE_FILE, cfname);

  errno = 0;  // clear errno

  TdFilePtr tFile = taosOpenFile(tfname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (!tFile) {
    code = TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  fHeader.version = DM_ENG_FVER_MAX;
  fHeader.len = dmEncodeVars(NULL, 0, pInfo) + sizeof(TSCKSUM);
  if (fHeader.len < 0) {
    TAOS_CHECK_GOTO(fHeader.len, &lino, _exit);
  }

  ptr = hbuf;
  TAOS_UNUSED((&ptr, &fHeader));
  TAOS_CHECK_EXIT(taosCalcChecksumAppend(0, (uint8_t *)hbuf, DM_FILE_HEAD_SIZE));

  if (taosWriteFile(tFile, hbuf, DM_FILE_HEAD_SIZE) < DM_FILE_HEAD_SIZE) {
    code = TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  if (fHeader.len > 0) {
    if (!(pBuf = taosMemoryMalloc(fHeader.len))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }

    ptr = pBuf;
    int32_t len = dmEncodeVars(ptr, fHeader.len - sizeof(TSCKSUM), pInfo);
    if (len < 0) {
      TAOS_CHECK_GOTO(len, &lino, _exit);
    }

    TAOS_CHECK_EXIT(taosCalcChecksumAppend(0, (uint8_t *)pBuf, fHeader.len));

    if (taosWriteFile(tFile, pBuf, fHeader.len) < fHeader.len) {
      code = TAOS_SYSTEM_ERROR(errno);
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
  }

  // fsync, close and rename
  if (taosFsyncFile(tFile) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  if (taosCloseFile(&tFile) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  if (taosRenameFile(tfname, cfname) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

_exit:
  taosMemoryFreeClear(pBuf);
  if (code != 0) {
    dError("failed to write vars at line %d since %s", lino, tstrerror(code));
    TAOS_UNUSED(taosCloseFile(&tFile));
    TAOS_UNUSED(taosRemoveFile(tfname));
  }

  return code;
}

static void dmFetchEType(int8_t *type) {
  int8_t eType = DM_ETYPE_UN;
  if (STR_STR_CMP(tsVersionName, STR_STR_SIGN)) {
    if (!strncmp(tsVersionName, "t", 1)) {
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
#ifndef _TD_DM_SKIP_CHECK
  int32_t     code = 0;
  int32_t     lino = 0;
  int8_t      eType = 0;
  SEngineInfo eInfo = {0};

  dmFetchEType(&eType);

  char cfgFile[PATH_MAX] = "\0";
  dmGetFname(DNODE_CFG_FILE, cfgFile);

  TAOS_UNUSED(taosThreadRwlockRdlock(&pDnode->data.lock));
  // dnode.json not exist, return directly
  if (taosStatFile(cfgFile, NULL, NULL, NULL) < 0) {
    TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
    goto _exit;
  }
  if (((code = dmReadVars(&eInfo)) != 0) && (code != TSDB_CODE_NOT_FOUND)) {
    TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
  TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));

  if (pDnode->data.engineVer == 0) {           // dnode.json history version
    if ((eInfo.type & 0x0F) == DM_ETYPE_UN) {  // without DM_ENGINE_FILE, create(handle update from history version)
      eInfo.type = eType;
      eInfo.dnodeId = pDnode->data.dnodeId;
      eInfo.engineVer = tsVersion;
      eInfo.clusterId = pDnode->data.clusterId;
      eInfo.createMs = taosGetTimestampMs();
      eInfo.updateMs = eInfo.createMs;
      // save
      (void)taosThreadRwlockWrlock(&pDnode->data.lock);
      if ((code = dmWriteVars(&eInfo)) != 0) {
        TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
      TAOS_CHECK_GOTO(dmSyncEps(&pDnode->data), &lino, _exit);
    }
  } else if ((eInfo.type & 0x0F) == DM_ETYPE_UN) {  // not history version, but without DM_ENGINE_FILE, fail
    dError("failed to init since inconsistent ver");
    TAOS_CHECK_GOTO(TSDB_CODE_VERSION_NOT_COMPATIBLE, &lino, _exit);
  } else if (pDnode->data.clusterId !=
             eInfo.clusterId) {  // not history version, DM_ENGINE_FILE exists, check clusterId
    if (eInfo.clusterId == 0) {
      eInfo.dnodeId = pDnode->data.dnodeId;
      eInfo.engineVer = tsVersion;
      eInfo.clusterId = pDnode->data.clusterId;
      eInfo.updateMs = taosGetTimestampMs();
      (void)taosThreadRwlockWrlock(&pDnode->data.lock);
      if ((code = dmWriteVars(&eInfo)) != 0) {
        TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
      dInfo("update clusterId from 0 to %" PRId64, pDnode->data.clusterId);
    } else {
      dError("failed to init since inconsistent cluster:%" PRIi64 ",%" PRIi64, eInfo.clusterId, pDnode->data.clusterId);
      TAOS_CHECK_GOTO(TSDB_CODE_VERSION_NOT_COMPATIBLE, &lino, _exit);
    }
  } else if (pDnode->data.engineVer != tsVersion) {  // update to latest engineVer
    TAOS_CHECK_GOTO(dmSyncEps(&pDnode->data), &lino, _exit);
  }

  if (eType == DM_ETYPE_OS) {        // oss
    if (eInfo.type > DM_ETYPE_OS) {  // enterprise to oss not allowed
      dError("failed to init since incompatible ver");
      TAOS_CHECK_GOTO(TSDB_CODE_VERSION_NOT_COMPATIBLE, &lino, _exit);
    }
  } else if (eInfo.type == DM_ETYPE_OS) {  // update oss to enterprise
    eInfo.type = eType;
    eInfo.engineVer = tsVersion;
    eInfo.updateMs = taosGetTimestampMs();
    (void)taosThreadRwlockWrlock(&pDnode->data.lock);
    if ((code = dmWriteVars(&eInfo)) != 0) {
      TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
      TAOS_CHECK_GOTO(code, &lino, _exit);
    }
    TAOS_UNUSED(taosThreadRwlockUnlock(&pDnode->data.lock));
  }

_exit:
  if (code != 0) {
    dError("failed to init version at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
#else
  TAOS_RETURN(0);
#endif
}

static int32_t dmSyncEps(SDnodeData *pData) {
  int32_t code = 0;
  char    file[PATH_MAX] = "\0";
  (void)snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  (void)taosThreadRwlockWrlock(&pData->lock);
  bool fileExist = !(taosStatFile(file, NULL, NULL, NULL) < 0);
  if (fileExist) {
    code = dmWriteEps(pData);
  }
  TAOS_UNUSED(taosThreadRwlockUnlock(&pData->lock));
  TAOS_RETURN(code);
}

// invoker
int32_t dmInitModule(SDnode *pDnode) {
  int32_t code = 0;
  int32_t lino = 0;

#ifdef _TD_DM_CHECK_OFFSET
  TAOS_CHECK_GOTO(dmCheckOffset(pDnode), &lino, _exit);
#endif

  TAOS_CHECK_GOTO(dmInitPrerequisites(), &lino, _exit);

  if (dmInitVersion(pDnode) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_VERSION_NOT_COMPATIBLE, &lino, _exit);
  }

  TAOS_CHECK_GOTO(dmInitMsgHandle(pDnode), &lino, _exit);

  TAOS_CHECK_GOTO(dmInitServer(pDnode), &lino, _exit);

  TAOS_CHECK_GOTO(dmInitClient(pDnode), &lino, _exit);

_exit:
  if (code != 0) {
    dError("failed to init module at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}
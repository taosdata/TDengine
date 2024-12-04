/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "meta.h"

#define metaErr(VGID, ERRNO)                                                                                     \
  do {                                                                                                           \
    metaError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " type:%d uid:%" PRId64 " name:%s", VGID, \
              __func__, __FILE__, __LINE__, tstrerror(ERRNO), pEntry->version, pEntry->type, pEntry->uid,        \
              pEntry->type > 0 ? pEntry->name : NULL);                                                           \
  } while (0)

// Entry Table
static int32_t metaEntryTableInsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  vgId = TD_VID(pMeta->pVnode);
  void    *value = NULL;
  int32_t  valueSize = 0;
  SEncoder encoder = {0};
  STbDbKey key = {
      .version = pEntry->version,
      .uid = pEntry->uid,
  };

  // encode entry
  tEncodeSize(metaEncodeEntry, pEntry, valueSize, code);
  if (code != 0) {
    metaErr(vgId, code);
    return code;
  }

  value = taosMemoryMalloc(valueSize);
  if (NULL == value) {
    metaErr(vgId, terrno);
    return terrno;
  }

  tEncoderInit(&encoder, value, valueSize);
  code = metaEncodeEntry(&encoder, pEntry);
  if (code) {
    metaErr(vgId, code);
    tEncoderClear(&encoder);
    taosMemoryFree(value);
    return code;
  }
  tEncoderClear(&encoder);

  // put to tdb
  code = tdbTbInsert(pMeta->pTbDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaEntryTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = TD_VID(pMeta->pVnode);

  // TODO

  return code;
}

// Schema Table
static int32_t metaSchemaTableInsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  vgId = TD_VID(pMeta->pVnode);
  SEncoder encoder = {0};
  void    *value = NULL;
  int32_t  valueSize = 0;

  const SSchemaWrapper *pSchema = NULL;
  if (pEntry->type == TSDB_SUPER_TABLE) {
    pSchema = &pEntry->stbEntry.schemaRow;
  } else if (pEntry->type == TSDB_NORMAL_TABLE) {
    pSchema = &pEntry->ntbEntry.schemaRow;
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
  SSkmDbKey key = {
      .uid = pEntry->uid,
      .sver = pSchema->version,
  };

  // encode schema
  tEncodeSize(tEncodeSSchemaWrapper, pSchema, valueSize, code);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  tEncoderInit(&encoder, value, valueSize);
  code = tEncodeSSchemaWrapper(&encoder, pSchema);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    tEncoderClear(&encoder);
    taosMemoryFree(value);
    return code;
  }
  tEncoderClear(&encoder);

  // put to tdb
  code = tdbTbInsert(pMeta->pSkmDb, &key, sizeof(key), value, valueSize, pMeta->txn);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
  }
  taosMemoryFree(value);
  return code;
}

// Uid Index
static void metaBuildEntryInfo(const SMetaEntry *pEntry, SMetaInfo *pInfo) {
  pInfo->uid = pEntry->uid;
  pInfo->version = pEntry->version;
  if (pEntry->type == TSDB_SUPER_TABLE) {
    pInfo->suid = pEntry->uid;
    pInfo->skmVer = pEntry->stbEntry.schemaRow.version;
  } else if (pEntry->type == TSDB_CHILD_TABLE) {
    pInfo->suid = pEntry->ctbEntry.suid;
    pInfo->skmVer = 0;
  } else if (pEntry->type == TSDB_NORMAL_TABLE) {
    pInfo->suid = 0;
    pInfo->skmVer = pEntry->ntbEntry.schemaRow.version;
  }
}
static int32_t metaUidIdxUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = TD_VID(pMeta->pVnode);

  // update cache
  SMetaInfo info = {0};
  metaBuildEntryInfo(pEntry, &info);
  code = metaCacheUpsert(pMeta, &info);
  if (code) {
    metaErr(vgId, code);
  }

  // put to tdb
  SUidIdxVal value = {
      .suid = 0,
      .skmVer = 0,
      .version = pEntry->version,
  };
  code = tdbTbUpsert(pMeta->pUidIdx, &pEntry->uid, sizeof(pEntry->uid), &value, sizeof(value), pMeta->txn);
  return code;
}

// Name Index
static int32_t metaNameIdxInsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = tdbTbInsert(pMeta->pNameIdx, pEntry->name, strlen(pEntry->name) + 1, &pEntry->uid, sizeof(pEntry->uid),
                             pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

static int32_t metaNameIdxDelete(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = tdbTbDelete(pMeta->pNameIdx, pEntry->name, strlen(pEntry->name) + 1, pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

// Suid Index
static int32_t metaSUidIdxInsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = tdbTbInsert(pMeta->pSuidIdx, &pEntry->uid, sizeof(pEntry->uid), NULL, 0, pMeta->txn);
  if (code) {
    metaErr(TD_VID(pMeta->pVnode), code);
  }
  return code;
}

// Tag Index

static int32_t metaHandleSuperTableCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = TD_VID(pMeta->pVnode);

  // Insert into Entry Table
  code = metaEntryTableInsert(pMeta, pEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  // Insert into Schema Table
  code = metaSchemaTableInsert(pMeta, pEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  // Insert to Uid Index
  code = metaUidIdxUpsert(pMeta, pEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  // Insert to Name Index
  code = metaNameIdxInsert(pMeta, pEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  // Insert to Suid Index
  code = metaSUidIdxInsert(pMeta, pEntry);
  if (TSDB_CODE_SUCCESS != code) {
    metaErr(vgId, code);
    return code;
  }

  return code;
}

static int32_t metaHandleSuperTableCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = TD_VID(pMeta->pVnode);

  metaWLock(pMeta);
  code = metaHandleSuperTableCreateImpl(pMeta, pEntry);
  metaULock(pMeta);

  if (TSDB_CODE_SUCCESS == code) {
    pMeta->pVnode->config.vndStats.numOfSTables++;
    pMeta->changed = true;

    metaInfo("vgId:%d, %s success, version:%" PRId64 " type:%d uid:%" PRId64 " name:%s", vgId, __func__,
             pEntry->version, pEntry->type, pEntry->uid, pEntry->name);
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, version" PRId64 " type:%d uid:%" PRId64 " name:%s", vgId, __func__,
              __FILE__, __LINE__, tstrerror(code), pEntry->version, pEntry->type, pEntry->uid, pEntry->name);
  }
  return code;
}

static int32_t metaHandleNormalTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  //   TODO
  return code;
}

static int32_t metaHandleChildTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  //   TODO
  return code;
}

static int32_t metaHandleSuperTableDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  //   TODO
  return code;
}

int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   vgId = TD_VID(pMeta->pVnode);
  SMetaInfo info = {0};
  int8_t    type = pEntry->type > 0 ? pEntry->type : -pEntry->type;

  if (NULL == pMeta || NULL == pEntry || type != TSDB_SUPER_TABLE || type != TSDB_CHILD_TABLE ||
      type != TSDB_NORMAL_TABLE) {
    metaError("%s failed at %s:%d since invalid parameter", __func__, __FILE__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pEntry->type > 0) {
    bool isExist = false;
    if (TSDB_CODE_SUCCESS == metaGetInfo(pMeta, pEntry->uid, &info, NULL)) {
      isExist = true;
    }

    switch (type) {
      case TSDB_SUPER_TABLE:
        if (isExist) {
          // code = metaHandleSuperTableUpdate(pMeta, pEntry);
        } else {
          code = metaHandleSuperTableCreate(pMeta, pEntry);
        }
        break;
      case TSDB_CHILD_TABLE:
        if (isExist) {
          // code = metaHandleChildTableUpdate(pMeta, pEntry);
        } else {
          // code = metaHandleChildTableCreate(pMeta, pEntry);
        }
        break;
      case TSDB_NORMAL_TABLE:
        if (isExist) {
          // code = metaHandleNormalTableUpdate(pMeta, pEntry);
        } else {
          // code = metaHandleNormalTableCreate(pMeta, pEntry);
        }
        break;
      default:
        code = TSDB_CODE_INVALID_PARA;
        break;
    }
  } else {
    switch (type) {
      case TSDB_SUPER_TABLE:
        // code = metaHandleSuperTableDrop(pMeta, pEntry);
        break;
      case TSDB_CHILD_TABLE:
        // code = metaHandleChildTableDrop(pMeta, pEntry);
        break;
      case TSDB_NORMAL_TABLE:
        // code = metaHandleNormalTableDrop(pMeta, pEntry);
        break;
      default:
        code = TSDB_CODE_INVALID_PARA;
        break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    metaDebug("vgId:%d, %s success, version:%" PRId64 " type:%d uid:%" PRId64 " name:%s", vgId, __func__,
              pEntry->version, pEntry->type, pEntry->uid, pEntry->type > 0 ? pEntry->name : "");
  } else {
    metaError("vgId:%d, %s failed at %s:%d since %s, version" PRId64 " type:%d uid:%" PRId64 " name:%s", vgId, __func__,
              __FILE__, __LINE__, tstrerror(code), pEntry->version, pEntry->type, pEntry->uid,
              pEntry->type > 0 ? pEntry->name : "");
  }
  TAOS_RETURN(code);
}
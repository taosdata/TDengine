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

#define metaErrLog(ERRNO, INFO)                                                                                \
  do {                                                                                                         \
    if (INFO) {                                                                                                \
      metaError("%s failed at %s:%d since %s, info:%s", __func__, __FILE__, __LINE__, tstrerror(ERRNO), INFO); \
    } else {                                                                                                   \
      metaError("%s failed at %s:%d since %s", __func__, __FILE__, __LINE__, tstrerror(ERRNO));                \
    }                                                                                                          \
  } while (0)

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

static int32_t metaHandleEntryDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  int8_t  type = -pEntry->type;

  if (type == TSDB_NORMAL_TABLE) {
    code = metaHandleNormalTableDrop(pMeta, pEntry);
  } else if (type == TSDB_CHILD_TABLE) {
    code = metaHandleChildTableDrop(pMeta, pEntry);
  } else if (type == TSDB_SUPER_TABLE) {
    code = metaHandleSuperTableDrop(pMeta, pEntry);
  } else {
    code = TSDB_CODE_INVALID_PARA;
    metaErrLog(code, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
  } else {
  }

  return code;
}

static int32_t metaHandleNormalTableEntryUpdate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO:
  return code;
}

static int32_t metaHandleNormalTableEntryCreateImpl(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO
  // code = metaSaveToTbDb(pMeta, pEntry);
  // code = metaUpdateUidIdx(pMeta, pEntry);
  // code = metaUpdatenameIdx(pMeta, pEntry);
  // code = metaSaveToSkmDb(pMeta, pEntry);
  // code = metaUpdateBtimeIdx(pMeta, pEntry);
  // code = metaUpdateNcolIdx(pMeta, pEntry);
  // code = metaUpdateTtl(pMeta, pEntry);

  return code;
}

static int32_t metaHandleNormalTableEntryCreate(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO: do neccessary check

  // do handle entry
  metaWLock(pMeta);
  code = metaHandleNormalTableEntryCreateImpl(pMeta, pEntry);
  metaULock(pMeta);

  return code;
}

static int32_t metaHandleNormalTableEntryUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SMetaInfo metaInfo = {0};

  if (TSDB_CODE_SUCCESS == metaGetInfo(pMeta, pEntry->uid, &metaInfo, NULL)) {
    code = metaHandleNormalTableEntryUpdate(pMeta, pEntry);
  } else {
    code = metaHandleNormalTableEntryCreate(pMeta, pEntry);
  }

  return code;
}

static int32_t metaHandleSuperTableEntryUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}

static int32_t metaHandleChildTableEntryUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;
  // TODO
  return code;
}

static int32_t metaHandleEntryUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pEntry->type == TSDB_NORMAL_TABLE) {
    code = metaHandleNormalTableEntryUpsert(pMeta, pEntry);
  } else if (pEntry->type == TSDB_SUPER_TABLE) {
    code = metaHandleSuperTableEntryUpsert(pMeta, pEntry);
  } else if (pEntry->type == TSDB_CHILD_TABLE) {
    code = metaHandleChildTableEntryUpsert(pMeta, pEntry);
  } else {
    code = TSDB_CODE_INVALID_PARA;
    metaErrLog(code, NULL);
  }

  return code;
}

int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pMeta == NULL || pEntry == NULL) {
    metaErrLog(TSDB_CODE_INVALID_PARA, NULL);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pEntry->type > 0) {
    code = metaHandleEntryUpsert(pMeta, pEntry);
  } else {
    code = metaHandleEntryDrop(pMeta, pEntry);
  }

  return code;
}
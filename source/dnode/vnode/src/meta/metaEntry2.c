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

#define metaErrLog(ERRNO) metaError("%s failed at %s:%d since %s", __func__, __FILE__, __LINE__, tstrerror(ERRNO));

static int32_t metaHandleEntryDrop(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO

  return code;
}

static int32_t metaHandleEntryUpsert(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO

  return code;
}

int32_t metaHandleEntry2(SMeta *pMeta, const SMetaEntry *pEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pMeta == NULL || pEntry == NULL) {
    metaErrLog(TSDB_CODE_INVALID_PARA);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pEntry->type < 0) {
    code = metaHandleEntryDrop(pMeta, pEntry);
  } else {
    code = metaHandleEntryUpsert(pMeta, pEntry);
  }
  return code;
  ;
}
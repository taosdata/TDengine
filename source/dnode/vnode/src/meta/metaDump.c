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

#include "meta.h"

#define META_DUMP_PATH "/tmp/meta_dump/"

typedef struct {
  TBC      *uidCursor;
  TdFilePtr file;
} SMetaDumpContext;

/*------------------------------ SMetaDumpContext ------------------------------*/
static int32_t metaDumpInitContext(SMetaDumpContext *ctx, SMeta *pMeta) {
  ctx->uidCursor = NULL;
  int32_t code = tdbTbcOpen(pMeta->pUidIdx, &ctx->uidCursor, NULL);
  if (code) {
    metaError("failed to open uid index cursor, reason:%s", tstrerror(code));
    ctx->uidCursor = NULL;
    return code;
  }

  code = tdbTbcMoveToFirst(ctx->uidCursor);
  if (code) {
    metaError("failed to move to first, reason:%s", tstrerror(code));
    tdbTbcClose(ctx->uidCursor);
    ctx->uidCursor = NULL;
    return code;
  }

  return code;
}

static int32_t metaDumpReleaseContext(SMetaDumpContext *ctx) {
  if (ctx->uidCursor) {
    tdbTbcClose(ctx->uidCursor);
    ctx->uidCursor = NULL;
  }
  return 0;
}

/*------------------------------ Meta Dump ------------------------------*/
static int32_t metaDumpMetaEntry(SMetaDumpContext *ctx, SMetaEntry *pEntry) {
  if (ctx->file == NULL) {
    char filePath[256];
    snprintf(filePath, sizeof(filePath), META_DUMP_PATH "meta_dump_%ld.txt", taosGetCurrentTimeMs());
    ctx->file = taosOpenFile(filePath, "w");
    if (ctx->file == NULL) {
      metaError("failed to open dump file:%s", filePath);
      return TSDB_CODE_FILE_OPEN_FAILED;
    }
    metaInfo("meta dump file created:%s", filePath);
  }

  // Write MetaEntry info to file
  taosFprintf(ctx->file, "UID: %" PRId64 "\n", pEntry->uid);
  taosFprintf(ctx->file, "Table Name: %s\n", pEntry->tableName);
  taosFprintf(ctx->file, "Database Name: %s\n", pEntry->dbName);
  taosFprintf(ctx->file, "Number of Columns: %d\n", pEntry->numOfColumns);
  taosFprintf(ctx->file, "Number of Tags: %d\n", pEntry->numOfTags);
  taosFprintf(ctx->file, "Created Time: %" PRId64 "\n", pEntry->createdTime);
  taosFprintf(ctx->file, "----------------------------------------\n");

  return TSDB_CODE_SUCCESS;
}

static void metaDumpImpl(SMeta *pMeta) {
  SMetaDumpContext ctx;
  int32_t          code = TSDB_CODE_SUCCESS;

  code = metaDumpInitContext(&ctx, pMeta);
  if (code) {
    metaError("meta dump init context failed, reason:%s", tstrerror(code));
    return;
  }

  while (true) {
    const void *pKey;
    const void *pVal;
    int32_t     keySize;
    int32_t     valSize;
    code = tdbTbcGet(ctx.uidCursor, &pKey, &keySize, &pVal, &valSize);
    if (code) {
      metaError("meta dump get failed, reason:%s", tstrerror(code));
      break;
    }

    int64_t     uid = *(int64_t *)pKey;
    SUidIdxVal *pUidIdxVal = (SUidIdxVal *)pVal;

    // Fetch MetaEntry
    SMetaEntry *pEntry = NULL;
    code = metaFetchEntryByUid(pMeta, uid, &pEntry);
    if (code) {
      metaError("meta dump fetch entry failed for uid:%" PRId64 ", reason:%s", uid, tstrerror(code));
      break;
    }

    code = metaDumpMetaEntry(&ctx, pEntry);
    if (code) {
      metaError("meta dump entry failed for uid:%" PRId64 ", reason:%s", uid, tstrerror(code));
      metaFetchEntryFree(&pEntry);
      break;
    }

    metaFetchEntryFree(&pEntry);
    code = tdbTbcMoveToNext(ctx.uidCursor);
    if (code) {
      break;
    }
  }

  metaDumpReleaseContext(&ctx);
}

void metaDump(SMeta *pMeta) {
  metaRLock(pMeta);
  metaDumpImpl(pMeta);
  metaULock(pMeta);
}
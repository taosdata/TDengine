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

#include "dev.h"

int32_t tDelBlockCreate(SDelBlock *pDelBlock) {
  memset(pDelBlock, 0, sizeof(SDelBlock));
  for (int32_t i = 0; i < 5; ++i) {
    tColDataInit(&pDelBlock->aColData[i], i + 1, TSDB_DATA_TYPE_BIGINT, 0);
  }
  return 0;
}

int32_t tDelBlockDestroy(SDelBlock *pDelBlock) {
  for (int32_t i = 0; i < 5; ++i) {
    tColDataDestroy(&pDelBlock->aColData[i]);
  }
  return 0;
}

int32_t tDelBlockClear(SDelBlock *pDelBlock) {
  for (int32_t i = 0; i < 5; ++i) {
    tColDataClear(&pDelBlock->aColData[i]);
  }
  return 0;
}

int32_t tDelBlockAppend(SDelBlock *pDelBlock, const TABLEID *tbid, const SDelData *pDelData) {
  int32_t code = 0;
  SColVal cv;

  //   TODO
  code = tColDataAppendValue(&pDelBlock->aColData[0], &cv);

  code = tColDataAppendValue(&pDelBlock->aColData[1], &cv);

  code = tColDataAppendValue(&pDelBlock->aColData[2], &cv);

  code = tColDataAppendValue(&pDelBlock->aColData[3], &cv);

  code = tColDataAppendValue(&pDelBlock->aColData[4], &cv);

  return code;
}

int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb) {
  if (tbid->suid) {
    if (pSkmTb->suid == tbid->suid) {
      pSkmTb->uid = tbid->uid;
      return 0;
    }
  } else if (pSkmTb->uid == tbid->uid) {
    return 0;
  }

  pSkmTb->suid = tbid->suid;
  pSkmTb->uid = tbid->uid;
  tDestroyTSchema(pSkmTb->pTSchema);
  return metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, tbid->suid, tbid->uid, -1, &pSkmTb->pTSchema);
}

int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow) {
  if (pSkmRow->pTSchema && pSkmRow->suid == tbid->suid) {
    if (pSkmRow->suid) {
      if (sver == pSkmRow->pTSchema->version) return 0;
    } else if (pSkmRow->uid == tbid->uid && pSkmRow->pTSchema->version == sver) {
      return 0;
    }
  }

  pSkmRow->suid = tbid->suid;
  pSkmRow->uid = tbid->uid;
  tDestroyTSchema(pSkmRow->pTSchema);
  return metaGetTbTSchemaEx(pTsdb->pVnode->pMeta, tbid->suid, tbid->uid, sver, &pSkmRow->pTSchema);
}
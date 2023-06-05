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

// SDelBlock ----------
int32_t tDelBlockInit(SDelBlock *delBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->dataArr); ++i) {
    TARRAY2_INIT(&delBlock->dataArr[i]);
  }
  return 0;
}

int32_t tDelBlockFree(SDelBlock *delBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->dataArr); ++i) {
    TARRAY2_FREE(&delBlock->dataArr[i]);
  }
  return 0;
}

int32_t tDelBlockClear(SDelBlock *delBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->dataArr); ++i) {
    TARRAY2_CLEAR(&delBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tDelBlockPut(SDelBlock *delBlock, const SDelRecord *delRecord) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->dataArr); ++i) {
    int32_t code = TARRAY2_APPEND(&delBlock->dataArr[i], delRecord->aData[i]);
    if (code) return code;
  }
  return 0;
}

// STbStatisBlock ----------
int32_t tStatisBlockInit(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    TARRAY2_INIT(&statisBlock->dataArr[i]);
  }
  return 0;
}

int32_t tStatisBlockFree(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    TARRAY2_FREE(&statisBlock->dataArr[i]);
  }
  return 0;
}

int32_t tStatisBlockClear(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    TARRAY2_CLEAR(&statisBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *statisRecord) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    int32_t code = TARRAY2_APPEND(&statisBlock->dataArr[i], statisRecord->aData[i]);
    if (code) return code;
  }
  return 0;
}

// other apis ----------
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
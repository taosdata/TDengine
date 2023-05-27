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
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->aData); ++i) {
    TARRAY2_INIT(&delBlock->aData[i]);
  }
  return 0;
}

int32_t tDelBlockFree(SDelBlock *delBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->aData); ++i) {
    TARRAY2_FREE(&delBlock->aData[i]);
  }
  return 0;
}

int32_t tDelBlockClear(SDelBlock *delBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->aData); ++i) {
    TARRAY2_CLEAR(&delBlock->aData[i], NULL);
  }
  return 0;
}

int32_t tDelBlockPut(SDelBlock *delBlock, const SDelRecord *delRecord) {
  for (int32_t i = 0; i < ARRAY_SIZE(delBlock->aData); ++i) {
    int32_t code = TARRAY2_APPEND(&delBlock->aData[i], delRecord->aData[i]);
    if (code) return code;
  }
  return 0;
}

int32_t tDelBlockEncode(SDelBlock *delBlock, void *buf, int32_t size) {
  // TODO
  return 0;
}

int32_t tDelBlockDecode(const void *buf, SDelBlock *delBlock) {
  // TODO
  return 0;
}

// STbStatisBlock ----------
int32_t tStatisBlockInit(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->aData); ++i) {
    TARRAY2_INIT(&statisBlock->aData[i]);
  }
  return 0;
}

int32_t tStatisBlockFree(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->aData); ++i) {
    TARRAY2_FREE(&statisBlock->aData[i]);
  }
  return 0;
}

int32_t tStatisBlockClear(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->aData); ++i) {
    TARRAY2_CLEAR(&statisBlock->aData[i], NULL);
  }
  return 0;
}

int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *statisRecord) {
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->aData); ++i) {
    int32_t code = TARRAY2_APPEND(&statisBlock->aData[i], statisRecord->aData[i]);
    if (code) return code;
  }
  return 0;
}

int32_t tStatisBlockEncode(STbStatisBlock *statisBlock, void *buf, int32_t size) {
  // TODO
  return 0;
}

int32_t tStatisBlockDecode(const void *buf, STbStatisBlock *statisBlock) {
  // TODO
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
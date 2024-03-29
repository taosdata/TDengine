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

#include "tsdbUtil2.h"

// SDelBlock ----------
int32_t tTombBlockInit(STombBlock *tombBlock) {
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    TARRAY2_INIT(&tombBlock->dataArr[i]);
  }
  return 0;
}

int32_t tTombBlockDestroy(STombBlock *tombBlock) {
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    TARRAY2_DESTROY(&tombBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tTombBlockClear(STombBlock *tombBlock) {
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    TARRAY2_CLEAR(&tombBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tTombBlockPut(STombBlock *tombBlock, const STombRecord *record) {
  int32_t code;
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    code = TARRAY2_APPEND(&tombBlock->dataArr[i], record->dataArr[i]);
    if (code) return code;
  }
  return 0;
}

int32_t tTombBlockGet(STombBlock *tombBlock, int32_t idx, STombRecord *record) {
  if (idx >= TOMB_BLOCK_SIZE(tombBlock)) return TSDB_CODE_OUT_OF_RANGE;
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    record->dataArr[i] = TARRAY2_GET(&tombBlock->dataArr[i], idx);
  }
  return 0;
}

int32_t tTombRecordCompare(const STombRecord *r1, const STombRecord *r2) {
  if (r1->suid < r2->suid) return -1;
  if (r1->suid > r2->suid) return 1;
  if (r1->uid < r2->uid) return -1;
  if (r1->uid > r2->uid) return 1;
  if (r1->version < r2->version) return -1;
  if (r1->version > r2->version) return 1;
  return 0;
}

// STbStatisBlock ----------
int32_t tStatisBlockInit(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; ++i) {
    TARRAY2_INIT(&statisBlock->dataArr[i]);
  }
  return 0;
}

int32_t tStatisBlockDestroy(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; ++i) {
    TARRAY2_DESTROY(&statisBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tStatisBlockClear(STbStatisBlock *statisBlock) {
  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; ++i) {
    TARRAY2_CLEAR(&statisBlock->dataArr[i], NULL);
  }
  return 0;
}

int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *record) {
  int32_t code;
  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; ++i) {
    code = TARRAY2_APPEND(&statisBlock->dataArr[i], record->dataArr[i]);
    if (code) return code;
  }
  return 0;
}

int32_t tStatisBlockGet(STbStatisBlock *statisBlock, int32_t idx, STbStatisRecord *record) {
  if (idx >= STATIS_BLOCK_SIZE(statisBlock)) return TSDB_CODE_OUT_OF_RANGE;
  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; ++i) {
    record->dataArr[i] = TARRAY2_GET(&statisBlock->dataArr[i], idx);
  }
  return 0;
}

// SBrinRecord ----------
int32_t tBrinBlockInit(SBrinBlock *brinBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); ++i) {
    TARRAY2_INIT(&brinBlock->dataArr1[i]);
  }
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr2); ++i) {
    TARRAY2_INIT(&brinBlock->dataArr2[i]);
  }
  return 0;
}

int32_t tBrinBlockDestroy(SBrinBlock *brinBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); ++i) {
    TARRAY2_DESTROY(&brinBlock->dataArr1[i], NULL);
  }
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr2); ++i) {
    TARRAY2_DESTROY(&brinBlock->dataArr2[i], NULL);
  }
  return 0;
}

int32_t tBrinBlockClear(SBrinBlock *brinBlock) {
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); ++i) {
    TARRAY2_CLEAR(&brinBlock->dataArr1[i], NULL);
  }
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr2); ++i) {
    TARRAY2_CLEAR(&brinBlock->dataArr2[i], NULL);
  }
  return 0;
}

int32_t tBrinBlockPut(SBrinBlock *brinBlock, const SBrinRecord *record) {
  int32_t code;
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); ++i) {
    code = TARRAY2_APPEND(&brinBlock->dataArr1[i], record->dataArr1[i]);
    if (code) return code;
  }
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr2); ++i) {
    code = TARRAY2_APPEND(&brinBlock->dataArr2[i], record->dataArr2[i]);
    if (code) return code;
  }
  return 0;
}

int32_t tBrinBlockGet(SBrinBlock *brinBlock, int32_t idx, SBrinRecord *record) {
  if (idx >= BRIN_BLOCK_SIZE(brinBlock)) return TSDB_CODE_OUT_OF_RANGE;
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); ++i) {
    record->dataArr1[i] = TARRAY2_GET(&brinBlock->dataArr1[i], idx);
  }
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr2); ++i) {
    record->dataArr2[i] = TARRAY2_GET(&brinBlock->dataArr2[i], idx);
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
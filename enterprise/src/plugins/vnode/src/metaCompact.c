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
#include "vnd.h"

typedef bool (*shouldKeepFn)(SMeta      *pMeta,      //
                             const void *key,        //
                             int32_t     keySize,    //
                             const void *value,      //
                             int32_t     valueSize,  //
                             int64_t     compactVersion);

static bool shouldEntryKeep(SMeta *pMeta, const void *key, int32_t keySize, const void *value, int32_t valueSize,
                            int64_t compactVersion) {
  bool       keep = false;
  SDecoder   decoder = {0};
  SMetaEntry entry = {0};
  SMetaInfo  info = {0};

  tDecoderInit(&decoder, (uint8_t *)value, valueSize);
  int32_t code = metaDecodeEntry(&decoder, &entry);
  if (code) {
    tDecoderClear(&decoder);
  }

  if (entry.version > compactVersion                          // TODO: version is compactble
      || (entry.type > 0                                      // entry is not a delete entry
          && metaGetInfo(pMeta, entry.uid, &info, NULL) == 0  // entry still exists
          && entry.version == info.version                    // entry is the newest version
          )) {
    keep = true;
  }

  return keep;
}

static int32_t metaCompactKV(SMeta *pMeta, TTB *pOldTb, TTB *pNewTb, int64_t compactVersion, shouldKeepFn shouldKeep) {
  TBC    *cursor = NULL;
  int32_t code = 0;

  // Open cursor
  code = tdbTbcOpen(pOldTb, &cursor, NULL);
  if (code) {
    metaError("vgId:%d,%s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Move to first
  code = tdbTbcMoveToFirst(cursor);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    tdbTbcClose(cursor);
    return code;
  }

  const void *key = NULL;
  int32_t     keySize = 0;
  const void *value = NULL;
  int32_t     valueSize = 0;
  while (1) {
    if (tdbTbcGet(cursor, &key, &keySize, &value, &valueSize) < 0) {
      break;
    }

    if (shouldKeep == NULL || shouldKeep(pMeta, key, keySize, value, valueSize, compactVersion)) {
      code = tdbTbInsert(pNewTb, key, keySize, value, valueSize, NULL);
      if (code) {
        metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code));
        tdbTbcClose(cursor);
        return code;
      }
    }
  }

  // Close cursor
  tdbTbcClose(cursor);
  return 0;
}

int32_t metaCompact(SMeta *pOldMeta, SMeta *pNewMeta, int64_t compactVersion) {
  // Entry table
  int32_t code = metaCompactKV(pOldMeta, pOldMeta->pTbDb, pNewMeta->pTbDb, compactVersion, shouldEntryKeep);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Schema table
  code = metaCompactKV(pOldMeta, pOldMeta->pSkmDb, pNewMeta->pSkmDb, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Uid index
  code = metaCompactKV(pOldMeta, pOldMeta->pUidIdx, pNewMeta->pUidIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Name index
  code = metaCompactKV(pOldMeta, pOldMeta->pNameIdx, pNewMeta->pNameIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Child table index
  code = metaCompactKV(pOldMeta, pOldMeta->pCtbIdx, pNewMeta->pCtbIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Super table index
  code = metaCompactKV(pOldMeta, pOldMeta->pSuidIdx, pNewMeta->pSuidIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Tag index
  code = metaCompactKV(pOldMeta, pOldMeta->pTagIdx, pNewMeta->pTagIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Btime index
  code = metaCompactKV(pOldMeta, pOldMeta->pBtimeIdx, pNewMeta->pBtimeIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Ncol index
  code = metaCompactKV(pOldMeta, pOldMeta->pNcolIdx, pNewMeta->pNcolIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // SMA index
  code = metaCompactKV(pOldMeta, pOldMeta->pSmaIdx, pNewMeta->pSmaIdx, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // Stream index
  code = metaCompactKV(pOldMeta, pOldMeta->pStreamDb, pNewMeta->pStreamDb, compactVersion, NULL);
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
              tstrerror(code));
    return code;
  }

  // TODO: move inverted index here

  return 0;
}
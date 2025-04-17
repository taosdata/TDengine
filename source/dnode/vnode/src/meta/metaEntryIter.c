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

struct SMetaEntryIterImpl {
  SMeta             *pMeta;
  int64_t            startVersion;
  int64_t            endVersion;
  TBC               *pTbc;
  int32_t            keySize;
  const void        *pKey;
  int32_t            valueSize;
  const void        *pValue;
  SMetaEntryWrapper *pEntry;
};

int32_t metaEntryIterOpen(SMeta *pMeta, int64_t startVersion, int64_t endVersion, SMetaEntryIter *iter) {
  int32_t code = 0;
  int32_t lino = 0;

  iter->impl = taosMemoryCalloc(1, sizeof(*iter->impl));
  if (iter->impl == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  iter->impl->pMeta = pMeta;
  iter->impl->startVersion = startVersion;
  iter->impl->endVersion = endVersion;

  metaRLock(pMeta);
  code = tdbTbcOpen(pMeta->pTbDb, &iter->impl->pTbc, NULL);
  if (code) {
    metaULock(pMeta);
    taosMemoryFree(iter->impl);
    iter->impl = NULL;
    goto _exit;
  }

  STbDbKey key = {
      .version = startVersion,
      .uid = 0,
  };
  int c = 0;
  code = tdbTbcMoveTo(iter->impl->pTbc, &key, sizeof(key), &c);
  if (code) {
    metaULock(pMeta);
    taosMemoryFree(iter->impl);
    iter->impl = NULL;
    goto _exit;
  }

  if (c < 0) {
    // TODO
  }

_exit:
  if (code) {
    metaError("meta/entry: %s failed at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

int32_t metaEntryIterNext(SMetaEntryIter *iter, SMetaEntryWrapper **ppEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tdbTbcMoveToNext(iter->impl->pTbc);
  if (code) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code =
      tdbTbcGet(iter->impl->pTbc, &iter->impl->pKey, &iter->impl->keySize, &iter->impl->pValue, &iter->impl->valueSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Decode the entry
  SDecoder   decoder = {0};
  SMetaEntry entry = {0};

  tDecoderInit(&decoder, (uint8_t *)iter->impl->pValue, iter->impl->valueSize);
  metaDecodeEntry(&decoder, &entry);


_exit:
  if (code) {
    metaError("vgId:%d %s failed at %s:%d since %s", TD_VID(iter->impl->pMeta->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

void metaEntryIterClose(SMetaEntryIter *iter) {
  if (iter == NULL || iter->impl == NULL) {
    return;
  }

  tdbTbcClose(iter->impl->pTbc);
  metaULock(iter->impl->pMeta);
  taosMemoryFree(iter->impl);
  iter->impl = NULL;
}
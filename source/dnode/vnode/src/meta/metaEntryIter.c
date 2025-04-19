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
  bool               isEnd;
  TBC               *pTbc;
  int32_t            keySize;
  const void        *pKey;
  int32_t            valueSize;
  const void        *pValue;
  SMetaEntryWrapper  entry;
};

static int32_t metaBuildEntryWrapper() {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  return code;
}

// startVersion: not included
// endVersion: included
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
  iter->impl->isEnd = false;

  metaRLock(pMeta);
  code = tdbTbcOpen(pMeta->pTbDb, &iter->impl->pTbc, NULL);
  if (code) {
    metaULock(pMeta);
    taosMemoryFree(iter->impl);
    iter->impl = NULL;
    goto _exit;
  }

  STbDbKey key = {
      .version = startVersion + 1,
      .uid = INT64_MIN,
  };
  int c = 0;
  code = tdbTbcMoveTo(iter->impl->pTbc, &key, sizeof(key), &c);
  if (code) {
    metaULock(pMeta);
    taosMemoryFree(iter->impl);
    iter->impl = NULL;
    goto _exit;
  }

  if (c > 0 && tdbTbcMoveToNext(iter->impl->pTbc) != 0) {
    iter->impl->isEnd = true;
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

  if (iter->impl->isEnd) {
    *ppEntry = NULL;
    goto _exit;
  }

  // Fetch the next entry
  if (tdbTbcGet(iter->impl->pTbc, &iter->impl->pKey, &iter->impl->keySize, &iter->impl->pValue,
                &iter->impl->valueSize) != 0) {
    iter->impl->isEnd = true;
    *ppEntry = NULL;
    goto _exit;
  }

  // Check if the entry is valid
  STbDbKey *pKey = (STbDbKey *)iter->impl->pKey;
  if (pKey->version > iter->impl->endVersion) {
    iter->impl->isEnd = true;
    *ppEntry = NULL;
    goto _exit;
  }

  // Decode the entry
  SDecoder   decoder = {0};
  SMetaEntry entry = {0};
  metaCloneEntryFree(&iter->impl->entry.pEntry);
  tDecoderInit(&decoder, (uint8_t *)iter->impl->pValue, iter->impl->valueSize);
  code = metaDecodeEntry(&decoder, &entry);
  if (code) {
    metaError("meta/entry: %s failed at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
    tDecoderClear(&decoder);
    goto _exit;
  }

  code = metaCloneEntry(&entry, &iter->impl->entry.pEntry);
  if (code) {
    metaError("meta/entry: %s failed at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
    tDecoderClear(&decoder);
    goto _exit;
  }

  tDecoderClear(&decoder);

  // Move the cursor to the next entry
  (void)tdbTbcMoveToNext(iter->impl->pTbc);

  *ppEntry = &iter->impl->entry;

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

  metaCloneEntryFree(&iter->impl->entry.pEntry);
  tdbTbcClose(iter->impl->pTbc);
  metaULock(iter->impl->pMeta);
  taosMemoryFree(iter->impl);
  iter->impl = NULL;
}

int32_t metaEncodeEntryWrapper(SEncoder *pEncoder, const SMetaEntryWrapper *pEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tStartEncode(pEncoder);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaEncodeEntry(pEncoder, pEntry->pEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  tEndEncode(pEncoder);

_exit:
  if (code) {
    metaError("%s failed at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

int32_t metaDecodeEntryWrapper(SDecoder *pDecoder, SMetaEntryWrapper *pEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tStartDecode(pDecoder);
  TSDB_CHECK_CODE(code, lino, _exit);

  SMetaEntry entry = {0};
  code = metaDecodeEntry(pDecoder, &entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaCloneEntry(&entry, &pEntry->pEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  tEndDecode(pDecoder);

_exit:
  if (code) {
    metaError("%s failed at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

void metaEntryWrapperFree(SMetaEntryWrapper *pEntry) {
  if (pEntry == NULL) {
    return;
  }

  metaCloneEntryFree(&pEntry->pEntry);
  pEntry->pEntry = NULL;
}
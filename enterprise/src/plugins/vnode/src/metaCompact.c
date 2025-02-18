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

static int32_t metaCompactImpl(SMeta *pOldMeta, SMeta *pNewMeta, int64_t compactVersion) {
  metaInfo("vgId:%d, compact meta data start, compact version:%" PRId64, TD_VID(pOldMeta->pVnode), compactVersion);

  do {
    int32_t     code = TSDB_CODE_SUCCESS;
    TBC        *cursor = NULL;
    const void *key = NULL;
    int32_t     keySize = 0;
    const void *value = NULL;
    int32_t     valueSize = 0;

    // Open cursor
    code = tdbTbcOpen(pOldMeta->pTbDb, &cursor, NULL);
    if (code) {
      metaError("vgId:%d, compact meta data failed since %s", TD_VID(pOldMeta->pVnode), tstrerror(code));
      return code;
    }

    // Move cursor to the first entry
    code = tdbTbcMoveToFirst(cursor);
    if (code) {
      metaError("vgId:%d, compact meta data failed at loop:%d, since %s", TD_VID(pOldMeta->pVnode), iLoop,
                tstrerror(code));
      tdbTbcClose(cursor);
      return code;
    }

    // Loop over each entry
    while (1) {
      if (tdbTbcGet(cursor, &key, &keySize, &value, &valueSize) != 0) {
        // No more data
        break;
      }

      // Decode entry
      SDecoder   decoder = {0};
      SMetaEntry entry = {0};

      tDecoderInit(&decoder, (uint8_t *)value, valueSize);
      code = metaDecodeEntry(&decoder, &entry);
      if (code) {
        metaError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code));
        tDecoderClear(&decoder);
        tdbTbcClose(cursor);
        return code;
      }

      // Handle entry
      SMetaInfo info = {0};
      if (entry.version >= compactVersion                            // TODO
          || (entry.type > 0                                         //
              && metaGetInfo(pOldMeta, entry.uid, &info, NULL) == 0  //
              && entry.version == info.version                       //
              )) {
        STbDbKey tbDbKey = {
            .uid = entry.uid,
            .version = entry.version,
        };
        code = tdbTbInsert(pNewMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), value, valueSize, NULL);
        if (code) {
          metaError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
                    tstrerror(code));
          tDecoderClear(&decoder);
          tdbTbcClose(cursor);
          return code;
        }
      }

      tDecoderClear(&decoder);

      code = tdbTbcMoveToNext(cursor);
      if (code) {
        metaError("vgId:%d, %s failed at line %s:%d since %s", TD_VID(pOldMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code));
        tdbTbcClose(cursor);
        return code;
      }
    }

    // Close cursor
    tdbTbcClose(cursor);
  } while (0);

  metaInfo("vgId:%d, compact meta data end, compact version:%" PRId64, TD_VID(pOldMeta->pVnode), compactVersion);
  return 0;
}

static int32_t metaCompactBegin() {
  // TODO
  return 0;
}

static int32_t metaCompactCommit() {
  // TODO
  return 0;
}

static int32_t metaCompactAbort() {
  // TODO
  return 0;
}

int32_t metaCompact(SVnode *pVnode) {
  int32_t code = metaCompactBegin();
  if (code) {
    // TODO
  }

  code = metaCompactImpl(NULL, NULL, INT64_MAX);
  if (code) {
    metaCompactCommit();
  } else {
    metaCompactAbort();
  }

  return 0;
}
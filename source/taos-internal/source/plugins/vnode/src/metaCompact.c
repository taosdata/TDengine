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

int32_t metaCompact(SMeta *pMeta, SMeta *pNewMeta, int64_t compactVersion) {
  int32_t code = 0;
  SVnode *pVnode = pMeta->pVnode;

  // i == 0, scan super table
  // i == 1, scan normal table and child table
  for (int i = 0; i < 2; i++) {
    TBC    *uidCursor = NULL;
    int32_t counter = 0;

    code = tdbTbcOpen(pMeta->pUidIdx, &uidCursor, NULL);
    if (code) {
      metaError("vgId:%d failed to open uid index cursor, reason:%s", TD_VID(pVnode), tstrerror(code));
      return code;
    }

    code = tdbTbcMoveToFirst(uidCursor);
    if (code) {
      metaError("vgId:%d failed to move to first, reason:%s", TD_VID(pVnode), tstrerror(code));
      tdbTbcClose(uidCursor);
      return code;
    }

    for (;;) {
      const void *pKey;
      int         kLen;
      const void *pVal;
      int         vLen;

      if (tdbTbcGet(uidCursor, &pKey, &kLen, &pVal, &vLen) < 0) {
        break;
      }

      tb_uid_t    uid = *(tb_uid_t *)pKey;
      SUidIdxVal *pUidIdxVal = (SUidIdxVal *)pVal;
      if ((i == 0 && (pUidIdxVal->suid && pUidIdxVal->suid == uid))          // super table
          || (i == 1 && (pUidIdxVal->suid == 0 || pUidIdxVal->suid != uid))  // normal table and child table
      ) {
        counter++;
        if (i == 0) {
          metaInfo("vgId:%d counter:%d new meta handle %s table uid:%" PRId64, TD_VID(pVnode), counter, "super", uid);
        } else {
          metaInfo("vgId:%d counter:%d new meta handle %s table uid:%" PRId64, TD_VID(pVnode), counter,
                   pUidIdxVal->suid == 0 ? "normal" : "child", uid);
        }

        // fetch table entry
        void *value = NULL;
        int   valueSize = 0;
        if (tdbTbGet(pMeta->pTbDb,
                     &(STbDbKey){
                         .version = pUidIdxVal->version,
                         .uid = uid,
                     },
                     sizeof(uid), &value, &valueSize) == 0) {
          SDecoder   dc = {0};
          SMetaEntry me = {0};
          tDecoderInit(&dc, value, valueSize);
          if (metaDecodeEntry(&dc, &me) == 0) {
            if (me.type == TSDB_CHILD_TABLE &&
                tdbTbGet(pMeta->pUidIdx, &me.ctbEntry.suid, sizeof(me.ctbEntry.suid), NULL, NULL) != 0) {
              metaError("vgId:%d failed to get super table uid:%" PRId64 " for child table uid:%" PRId64,
                        TD_VID(pVnode), me.ctbEntry.suid, uid);
            } else if (metaHandleEntry2(pNewMeta, &me) != 0) {
              metaError("vgId:%d failed to handle entry, uid:%" PRId64, TD_VID(pVnode), uid);
            }
          }
          tDecoderClear(&dc);
        }
        tdbFree(value);
      }

      code = tdbTbcMoveToNext(uidCursor);
      if (code) {
        metaError("vgId:%d failed to move to next, reason:%s", TD_VID(pVnode), tstrerror(code));
        return code;
      }
    }

    tdbTbcClose(uidCursor);
  }

  return 0;
}
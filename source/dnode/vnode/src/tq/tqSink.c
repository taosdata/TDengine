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

#include "tcommon.h"
#include "tq.h"

typedef struct STableSinkInfo {
  uint64_t uid;
  tstr     name;
} STableSinkInfo;

int32_t tqBuildDeleteReq(STQ* pTq, const char* stbFullName, const SSDataBlock* pDataBlock, SBatchDeleteReq* deleteReq,
                         const char* pIdStr, bool newSubTableRule) {
  int32_t          totalRows = pDataBlock->info.rows;
  SColumnInfoData* pStartTsCol = taosArrayGet(pDataBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pDataBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pGidCol = taosArrayGet(pDataBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pTbNameCol = taosArrayGet(pDataBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

  if (pStartTsCol == NULL || pEndTsCol == NULL || pGidCol == NULL || pTbNameCol == NULL) {
    return terrno;
  }

  tqDebug("s-task:%s build %d rows delete msg for table:%s", pIdStr, totalRows, stbFullName);

  for (int32_t row = 0; row < totalRows; row++) {
    int64_t skey = *(int64_t*)colDataGetData(pStartTsCol, row);
    int64_t ekey = *(int64_t*)colDataGetData(pEndTsCol, row);
    int64_t groupId = *(int64_t*)colDataGetData(pGidCol, row);

    char* name = NULL;
    char* originName = NULL;
    void* varTbName = NULL;
    if (!colDataIsNull(pTbNameCol, totalRows, row, NULL)) {
      varTbName = colDataGetVarData(pTbNameCol, row);
    }

    if (varTbName != NULL && varTbName != (void*)-1) {
      size_t cap = TMAX(TSDB_TABLE_NAME_LEN, varDataLen(varTbName) + 1);
      name = taosMemoryMalloc(cap);
      if (name == NULL) {
        return terrno;
      }

      memcpy(name, varDataVal(varTbName), varDataLen(varTbName));
      name[varDataLen(varTbName)] = '\0';
      if (newSubTableRule && !isAutoTableName(name) && !alreadyAddGroupId(name, groupId) && groupId != 0 &&
          stbFullName) {
        int32_t code = buildCtbNameAddGroupId(stbFullName, name, groupId, cap);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    } else if (stbFullName) {
      int32_t code = buildCtbNameByGroupId(stbFullName, groupId, &name);
      if (code) {
        return code;
      }
    } else {
      originName = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE);
      if (originName == NULL) {
        return terrno;
      }

      if (metaGetTableNameByUid(pTq->pVnode, groupId, originName) == 0) {
        name = varDataVal(originName);
      }
    }

    if (!name || *name == '\0') {
      tqWarn("s-task:%s failed to build delete msg groupId:%" PRId64 ", skey:%" PRId64 " ekey:%" PRId64
             " since invalid tbname:%s",
             pIdStr, groupId, skey, ekey, name ? name : "NULL");
    } else {
      tqDebug("s-task:%s build delete msg groupId:%" PRId64 ", name:%s, skey:%" PRId64 " ekey:%" PRId64, pIdStr,
              groupId, name, skey, ekey);

      SSingleDeleteReq req = {.startTs = skey, .endTs = ekey};
      tstrncpy(req.tbname, name, TSDB_TABLE_NAME_LEN);
      void* p = taosArrayPush(deleteReq->deleteReqs, &req);
      if (p == NULL) {
        return terrno;
      }
    }

    if (originName) {
      name = originName;
    }

    taosMemoryFreeClear(name);
  }

  return 0;
}

int32_t initCreateTableMsg(SVCreateTbReq* pCreateTableReq, uint64_t suid, const char* stbFullName, int32_t numOfTags) {
  pCreateTableReq->flags = 0;
  pCreateTableReq->type = TSDB_CHILD_TABLE;
  pCreateTableReq->ctb.suid = suid;

  // set super table name
  SName name = {0};

  int32_t code = tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (code == 0) {
    pCreateTableReq->ctb.stbName = taosStrdup((char*)tNameGetTableName(&name));
    if (pCreateTableReq->ctb.stbName == NULL) {  // ignore this error code
      tqError("failed to duplicate the stb name:%s, failed to init create-table msg and create req table", stbFullName);
      code = terrno;
    }
  }

  pCreateTableReq->ctb.tagNum = numOfTags;
  return code;
}

int32_t createDefaultTagColName(SArray** pColNameList) {
  *pColNameList = NULL;

  SArray* pTagColNameList = taosArrayInit(1, TSDB_COL_NAME_LEN);
  if (pTagColNameList == NULL) {
    return terrno;
  }

  char  tagNameStr[TSDB_COL_NAME_LEN] = "group_id";
  void* p = taosArrayPush(pTagColNameList, tagNameStr);
  if (p == NULL) {
    taosArrayDestroy(pTagColNameList);
    return terrno;
  }

  *pColNameList = pTagColNameList;
  return TSDB_CODE_SUCCESS;
}

int32_t setCreateTableMsgTableName(SVCreateTbReq* pCreateTableReq, SSDataBlock* pDataBlock, const char* stbFullName,
                                   int64_t gid, bool newSubTableRule, const char* id) {
  if (pDataBlock->info.parTbName[0]) {
    if (newSubTableRule && !isAutoTableName(pDataBlock->info.parTbName) &&
        !alreadyAddGroupId(pDataBlock->info.parTbName, gid) && gid != 0 && stbFullName) {
      pCreateTableReq->name = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
      if (pCreateTableReq->name == NULL) {
        return terrno;
      }

      tstrncpy(pCreateTableReq->name, pDataBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
      int32_t code = buildCtbNameAddGroupId(stbFullName, pCreateTableReq->name, gid, TSDB_TABLE_NAME_LEN);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      tqDebug("s-task:%s gen name from:%s blockdata", id, pDataBlock->info.parTbName);
    } else {
      pCreateTableReq->name = taosStrdup(pDataBlock->info.parTbName);
      if (pCreateTableReq->name == NULL) {
        return terrno;
      }
      tqDebug("s-task:%s copy name:%s from blockdata", id, pDataBlock->info.parTbName);
    }
  } else {
    int32_t code = buildCtbNameByGroupId(stbFullName, gid, &pCreateTableReq->name);
    tqDebug("s-task:%s no name in blockdata, auto-created table name:%s", id, pCreateTableReq->name);
    return code;
  }

  return 0;
}

// merge the new submit table block with the existed blocks
// if ts in the new data block overlap with existed one, replace it
int32_t doMergeExistedRows(SSubmitTbData* pExisted, const SSubmitTbData* pNew, const char* id) {
  int32_t oldLen = taosArrayGetSize(pExisted->aRowP);
  int32_t newLen = taosArrayGetSize(pNew->aRowP);
  int32_t numOfPk = 0;

  int32_t j = 0, k = 0;
  SArray* pFinal = taosArrayInit(oldLen + newLen, POINTER_BYTES);
  if (pFinal == NULL) {
    tqError("s-task:%s failed to prepare merge result datablock, code:%s", id, tstrerror(terrno));
    return terrno;
  }

  while (j < newLen && k < oldLen) {
    SRow* pNewRow = *(SRow**)TARRAY_GET_ELEM(pNew->aRowP, j);
    SRow* pOldRow = *(SRow**)TARRAY_GET_ELEM(pExisted->aRowP, k);

    if (pNewRow->ts < pOldRow->ts) {
      void* p = taosArrayPush(pFinal, &pNewRow);
      if (p == NULL) {
        return terrno;
      }
      j += 1;
    } else if (pNewRow->ts > pOldRow->ts) {
      void* p = taosArrayPush(pFinal, &pOldRow);
      if (p == NULL) {
        return terrno;
      }

      k += 1;
    } else {
      // check for the existance of primary key
      if (pNewRow->numOfPKs == 0) {
        void* p = taosArrayPush(pFinal, &pNewRow);
        if (p == NULL) {
          return terrno;
        }

        k += 1;
        j += 1;
        tRowDestroy(pOldRow);
      } else {
        numOfPk = pNewRow->numOfPKs;

        SRowKey kNew, kOld;
        tRowGetKey(pNewRow, &kNew);
        tRowGetKey(pOldRow, &kOld);

        int32_t ret = tRowKeyCompare(&kNew, &kOld);
        if (ret <= 0) {
          void* p = taosArrayPush(pFinal, &pNewRow);
          if (p == NULL) {
            return terrno;
          }

          j += 1;

          if (ret == 0) {
            k += 1;
            tRowDestroy(pOldRow);
          }
        } else {
          void* p = taosArrayPush(pFinal, &pOldRow);
          if (p == NULL) {
            return terrno;
          }

          k += 1;
        }
      }
    }
  }

  while (j < newLen) {
    SRow* pRow = *(SRow**)TARRAY_GET_ELEM(pNew->aRowP, j++);
    void* p = taosArrayPush(pFinal, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  while (k < oldLen) {
    SRow* pRow = *(SRow**)TARRAY_GET_ELEM(pExisted->aRowP, k++);
    void* p = taosArrayPush(pFinal, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  taosArrayDestroy(pNew->aRowP);
  taosArrayDestroy(pExisted->aRowP);
  pExisted->aRowP = pFinal;

  tqTrace("s-task:%s rows merged, final rows:%d, pk:%d uid:%" PRId64 ", existed auto-create table:%d, new-block:%d", id,
          (int32_t)taosArrayGetSize(pFinal), numOfPk, pExisted->uid, (pExisted->pCreateTbReq != NULL),
          (pNew->pCreateTbReq != NULL));

  tdDestroySVCreateTbReq(pNew->pCreateTbReq);
  taosMemoryFree(pNew->pCreateTbReq);
  return TSDB_CODE_SUCCESS;
}

int32_t buildAutoCreateTableReq(const char* stbFullName, int64_t suid, int32_t numOfCols, SSDataBlock* pDataBlock,
                                SArray* pTagArray, bool newSubTableRule, SVCreateTbReq** pReq, const char* id) {
  *pReq = NULL;

  SVCreateTbReq* pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (pCreateTbReq == NULL) {
    return terrno;
  }

  taosArrayClear(pTagArray);
  int32_t code = initCreateTableMsg(pCreateTbReq, suid, stbFullName, 1);
  if (code != 0) {
    return code;
  }

  STagVal tagVal = {.cid = numOfCols, .type = TSDB_DATA_TYPE_UBIGINT, .i64 = pDataBlock->info.id.groupId};
  void*   p = taosArrayPush(pTagArray, &tagVal);
  if (p == NULL) {
    return terrno;
  }

  code = tTagNew(pTagArray, 1, false, (STag**)&pCreateTbReq->ctb.pTag);
  if (pCreateTbReq->ctb.pTag == NULL || (code != 0)) {
    tdDestroySVCreateTbReq(pCreateTbReq);
    taosMemoryFreeClear(pCreateTbReq);
    return code;
  }

  code = createDefaultTagColName(&pCreateTbReq->ctb.tagName);
  if (code) {
    return code;
  }

  // set table name
  code = setCreateTableMsgTableName(pCreateTbReq, pDataBlock, stbFullName, pDataBlock->info.id.groupId, newSubTableRule,
                                    id);
  if (code) {
    return code;
  }

  *pReq = pCreateTbReq;
  return code;
}

int32_t tsAscendingSortFn(const void* p1, const void* p2) {
  SRow* pRow1 = *(SRow**)p1;
  SRow* pRow2 = *(SRow**)p2;

  if (pRow1->ts == pRow2->ts) {
    return 0;
  } else {
    return pRow1->ts > pRow2->ts ? 1 : -1;
  }
}

int32_t doConvertRows(SSubmitTbData* pTableData, const STSchema* pTSchema, SSDataBlock* pDataBlock, int64_t earlyTs,
                      const char* id) {
  int32_t numOfRows = pDataBlock->info.rows;
  int8_t  hasBlob = schemaHasBlob(pTSchema);
  int32_t code = TSDB_CODE_SUCCESS;

  SArray* pVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
  pTableData->aRowP = taosArrayInit(numOfRows, sizeof(SRow*));

  if (pVals == NULL || pTableData->aRowP == NULL) {
    tBlobRowDestroy(pTableData->pBlobRow);
    taosArrayDestroy(pVals);
    code = terrno;
    tqError("s-task:%s failed to prepare write stream res blocks, code:%s", id, tstrerror(code));
    return code;
  }
  if (hasBlob) {
    code = tBlobRowCreate(1024, 0, &pTableData->pBlobRow);
  }


  for (int32_t j = 0; j < numOfRows; j++) {
    taosArrayClear(pVals);

    int32_t dataIndex = 0;
    int64_t ts = 0;

    for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
      const STColumn* pCol = &pTSchema->columns[k];

      // primary timestamp column, for debug purpose
      if (k == 0) {
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
        if (pColData == NULL) {
          continue;
        }

        ts = *(int64_t*)colDataGetData(pColData, j);
        tqTrace("s-task:%s sink row %d, col %d ts %" PRId64, id, j, k, ts);

        if (ts < earlyTs) {
          tqError("s-task:%s ts:%" PRId64 " of generated results out of valid time range %" PRId64 " , discarded", id,
                  ts, earlyTs);
          taosArrayDestroy(pTableData->aRowP);
          pTableData->aRowP = NULL;
          taosArrayDestroy(pVals);
          return TSDB_CODE_SUCCESS;
        }
      }

      if (IS_SET_NULL(pCol)) {
        if (pCol->flags & COL_IS_KEY) {
          qError("ts:%" PRId64 " primary key column should not be null, colId:%" PRIi16 ", colType:%" PRIi8, ts,
                 pCol->colId, pCol->type);
          break;
        }
        SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
        void*   p = taosArrayPush(pVals, &cv);
        if (p == NULL) {
          return terrno;
        }
      } else {
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, dataIndex);
        if (pColData == NULL) {
          continue;
        }

        if (colDataIsNull_s(pColData, j)) {
          if (pCol->flags & COL_IS_KEY) {
            qError("ts:%" PRId64 " primary key column should not be null, colId:%" PRIi16 ", colType:%" PRIi8, ts,
                   pCol->colId, pCol->type);
            break;
          }

          SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
          void*   p = taosArrayPush(pVals, &cv);
          if (p == NULL) {
            return terrno;
          }

          dataIndex++;
        } else {
          void* colData = colDataGetData(pColData, j);
          if (IS_VAR_DATA_TYPE(pCol->type)) {  // address copy, no value
            if (IS_STR_DATA_BLOB(pCol->type)) {
              SValue sv =
                  (SValue){.type = pCol->type, .nData = blobDataLen(colData), .pData = (uint8_t*)blobDataVal(colData)};
              SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
              void*   p = taosArrayPush(pVals, &cv);
              if (p == NULL) {
                return terrno;
              }
            } else {
              SValue sv =
                  (SValue){.type = pCol->type, .nData = varDataLen(colData), .pData = (uint8_t*)varDataVal(colData)};
              SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
              void*   p = taosArrayPush(pVals, &cv);
              if (p == NULL) {
                return terrno;
              }
            }
          } else {
            SValue sv = {.type = pCol->type};
            valueSetDatum(&sv, pCol->type, colData, tDataTypes[pCol->type].bytes);
            SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
            void*   p = taosArrayPush(pVals, &cv);
            if (p == NULL) {
              return terrno;
            }
          }
          dataIndex++;
        }
      }
    }

    SRow*             pRow = NULL;
    SRowBuildScanInfo sinfo = {0};
    code = tRowBuild(pVals, (STSchema*)pTSchema, &pRow, &sinfo);
    if (code != TSDB_CODE_SUCCESS) {
      tDestroySubmitTbData(pTableData, TSDB_MSG_FLG_ENCODE);
      taosArrayDestroy(pVals);
      tqError("s-task:%s build rows for submit failed, ts:%" PRId64, id, ts);
      return code;
    }

    void* p = taosArrayPush(pTableData->aRowP, &pRow);
    if (p == NULL) {
      return terrno;
    }
  }

  taosArrayDestroy(pVals);
  return TSDB_CODE_SUCCESS;
}

int32_t tqSetDstTableDataPayload(uint64_t suid, const STSchema* pTSchema, int32_t blockIndex, SSDataBlock* pDataBlock,
                                 SSubmitTbData* pTableData, int64_t earlyTs, const char* id) {
  int32_t numOfRows = pDataBlock->info.rows;
  char*   dstTableName = pDataBlock->info.parTbName;

  tqDebug("s-task:%s sink data pipeline, build submit msg from %dth resBlock, including %d rows, dst suid:%" PRId64, id,
          blockIndex + 1, numOfRows, suid);

  // convert all rows
  int32_t code = doConvertRows(pTableData, pTSchema, pDataBlock, earlyTs, id);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("s-task:%s failed to convert rows from result block, code:%s", id, tstrerror(terrno));
    return code;
  }

  if (pTableData->aRowP != NULL) {
    taosArraySort(pTableData->aRowP, tsAscendingSortFn);
    tqTrace("s-task:%s build submit msg for dstTable:%s, numOfRows:%d", id, dstTableName, numOfRows);
  }

  return code;
}

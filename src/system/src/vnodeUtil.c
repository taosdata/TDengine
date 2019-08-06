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

#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "tast.h"
#include "tschemautil.h"
#include "vnode.h"
#include "vnodeDataFilterFunc.h"
#include "vnodeUtil.h"

#pragma GCC diagnostic ignored "-Wint-conversion"

int vnodeCheckFileIntegrity(FILE* fp) {
  /*
    int savedSessions, savedMeterSize;

    fseek(fp, TSDB_FILE_HEADER_LEN/3, SEEK_SET);
    fscanf(fp, "%d %d", &savedSessions, &savedMeterSize);
    if ( (savedSessions != tsSessionsPerVnode) || (savedMeterSize != tsMeterSizeOnFile) ) {
      dError("file structure is changed");
      return -1;

    }

    uint64_t checkSum = 0, savedCheckSum=0;
    checkSum = taosGetCheckSum(fp, TSDB_FILE_HEADER_LEN);

    fseek(fp, TSDB_FILE_HEADER_LEN - cksumsize, SEEK_SET);
    fread(&savedCheckSum, cksumsize, 1, fp);

    if ( savedCheckSum != checkSum ) {
      dError("check sum is not matched:0x%x 0x%x", checkSum, savedCheckSum);
      return -1;
    }
  */
  return 0;
}

void vnodeCreateFileHeaderFd(int fd) {
  char temp[TSDB_FILE_HEADER_LEN / 4];
  int  lineLen;

  lineLen = sizeof(temp);

  // write the first line`
  memset(temp, 0, lineLen);
  *(int16_t*)temp = vnodeFileVersion;
  sprintf(temp + sizeof(int16_t), "tsdb version: %s\n", version);
  /* *((int16_t *)(temp + TSDB_FILE_HEADER_LEN/8)) = vnodeFileVersion; */
  lseek(fd, 0, SEEK_SET);
  write(fd, temp, lineLen);

  // second line
  memset(temp, 0, lineLen);
  write(fd, temp, lineLen);

  // the third/forth line is the dynamic info
  memset(temp, 0, lineLen);
  write(fd, temp, lineLen);
  write(fd, temp, lineLen);
}

void vnodeGetHeadFileHeaderInfo(int fd, SVnodeHeadInfo* pHeadInfo) {
  lseek(fd, TSDB_FILE_HEADER_LEN / 4, SEEK_SET);
  read(fd, pHeadInfo, sizeof(SVnodeHeadInfo));
}

void vnodeUpdateHeadFileHeader(int fd, SVnodeHeadInfo* pHeadInfo) {
  lseek(fd, TSDB_FILE_HEADER_LEN / 4, SEEK_SET);
  write(fd, pHeadInfo, sizeof(SVnodeHeadInfo));
}

void vnodeCreateFileHeader(FILE* fp) {
  char temp[TSDB_FILE_HEADER_LEN / 4];
  int  lineLen;

  lineLen = sizeof(temp);

  // write the first line`
  memset(temp, 0, lineLen);
  *(int16_t*)temp = vnodeFileVersion;
  sprintf(temp + sizeof(int16_t), "tsdb version: %s\n", version);
  /* *((int16_t *)(temp + TSDB_FILE_HEADER_LEN/8)) = vnodeFileVersion; */
  fseek(fp, 0, SEEK_SET);
  fwrite(temp, lineLen, 1, fp);

  // second line
  memset(temp, 0, lineLen);
  fwrite(temp, lineLen, 1, fp);

  // the third line is the dynamic info
  memset(temp, 0, lineLen);
  fwrite(temp, lineLen, 1, fp);
  fwrite(temp, lineLen, 1, fp);
}

SSqlGroupbyExpr* vnodeCreateGroupbyExpr(SQueryMeterMsg* pQueryMsg, int32_t* code) {
  if (pQueryMsg->numOfGroupbyCols == 0) {
    return NULL;
  }

  // using group by tag columns
  SSqlGroupbyExpr* pGroupbyExpr =
      (SSqlGroupbyExpr*)malloc(sizeof(SSqlGroupbyExpr) + pQueryMsg->numOfGroupbyCols * sizeof(int16_t));
  if (pGroupbyExpr == NULL) {
    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  int16_t* pGroupbyIds = (int16_t*)pQueryMsg->groupbyTagIds;

  pGroupbyExpr->numOfGroupbyCols = pQueryMsg->numOfGroupbyCols;
  pGroupbyExpr->orderType = pQueryMsg->orderType;
  pGroupbyExpr->orderIdx = pQueryMsg->orderByIdx;

  memcpy(pGroupbyExpr->tagIndex, pGroupbyIds, sizeof(int16_t) * pGroupbyExpr->numOfGroupbyCols);

  return pGroupbyExpr;
}

static SSchema* toSchema(SQueryMeterMsg* pQuery, SColumnFilterMsg* pCols, int32_t numOfCols) {
  char* start = (char*)pQuery->colNameList;
  char* end = start;

  SSchema* pSchema = calloc(1, sizeof(SSchema) * numOfCols);
  for (int32_t i = 0; i < numOfCols; ++i) {
    pSchema[i].type = pCols[i].type;
    pSchema[i].bytes = pCols[i].bytes;
    pSchema[i].colId = pCols[i].colId;

    end = strstr(start, ",");
    memcpy(pSchema[i].name, start, end - start);
    start = end + 1;
  }

  return pSchema;
}

static int32_t id_compar(const void* left, const void* right) {
  DEFAULT_COMP(GET_INT16_VAL(left), GET_INT16_VAL(right));
}

static int32_t vnodeBuildExprFromArithmeticStr(SSqlFunctionExpr* pExpr, SQueryMeterMsg* pQueryMsg) {
  SSqlBinaryExprInfo* pBinaryExprInfo = &pExpr->pBinExprInfo;
  SColumnFilterMsg*   pColMsg = pQueryMsg->colList;

  tSQLBinaryExpr* pBinExpr = NULL;
  SSchema*        pSchema = toSchema(pQueryMsg, pColMsg, pQueryMsg->numOfCols);

  dTrace("qmsg:%p create binary expr from string:%s", pQueryMsg, pExpr->pBase.arg[0].argValue.pz);
  tSQLBinaryExprFromString(&pBinExpr, pSchema, pQueryMsg->numOfCols, pExpr->pBase.arg[0].argValue.pz,
                           pExpr->pBase.arg[0].argBytes);

  if (pBinExpr == NULL) {
    dError("qmsg:%p failed to create arithmetic expression string from:%s", pQueryMsg, pExpr->pBase.arg[0].argValue.pz);
    return TSDB_CODE_APP_ERROR;
  }

  pBinaryExprInfo->pBinExpr = pBinExpr;

  int32_t num = 0;
  int16_t ids[TSDB_MAX_COLUMNS] = {0};

  tSQLBinaryExprTrv(pBinExpr, &num, ids);
  qsort(ids, num, sizeof(int16_t), id_compar);

  int32_t i = 0, j = 0;

  while (i < num && j < num) {
    if (ids[i] == ids[j]) {
      j++;
    } else {
      ids[++i] = ids[j++];
    }
  }
  assert(i <= num);

  // there may be duplicated referenced columns.
  num = i + 1;
  pBinaryExprInfo->pReqColumns = malloc(sizeof(SColIndexEx) * num);

  for (int32_t i = 0; i < num; ++i) {
    SColIndexEx* pColIndex = &pBinaryExprInfo->pReqColumns[i];
    pColIndex->colId = ids[i];
  }

  pBinaryExprInfo->numOfCols = num;
  free(pSchema);

  return TSDB_CODE_SUCCESS;
}

static int32_t getColumnIndexInSource(SQueryMeterMsg* pQueryMsg, SSqlFuncExprMsg* pExprMsg) {
  int32_t j = 0;

  while(j < pQueryMsg->numOfCols) {
    if (pExprMsg->colInfo.colId == pQueryMsg->colList[j].colId) {
      break;
    }

    j += 1;
  }

  return j;
}

bool vnodeValidateExprColumnInfo(SQueryMeterMsg* pQueryMsg, SSqlFuncExprMsg* pExprMsg) {
  int32_t j = getColumnIndexInSource(pQueryMsg, pExprMsg);
  return j < pQueryMsg->numOfCols;
}

SSqlFunctionExpr* vnodeCreateSqlFunctionExpr(SQueryMeterMsg* pQueryMsg, int32_t* code) {
  SSqlFunctionExpr* pExprs = (SSqlFunctionExpr*)calloc(1, sizeof(SSqlFunctionExpr) * pQueryMsg->numOfOutputCols);
  if (pExprs == NULL) {
    tfree(pQueryMsg->pSqlFuncExprs);

    *code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  SSchema* pTagSchema = (SSchema*)pQueryMsg->pTagSchema;
  for (int32_t i = 0; i < pQueryMsg->numOfOutputCols; ++i) {
    pExprs[i].pBase = *((SSqlFuncExprMsg**)pQueryMsg->pSqlFuncExprs)[i];  // todo pExprs responsible for release memory
    pExprs[i].resBytes = 0;

    int16_t type = 0;
    int16_t bytes = 0;

    SColIndexEx* pColumnIndexExInfo = &pExprs[i].pBase.colInfo;

    // tag column schema is kept in pQueryMsg->pTagSchema
    if (pColumnIndexExInfo->isTag) {
      if (pColumnIndexExInfo->colIdx >= pQueryMsg->numOfTagsCols) {
        *code = TSDB_CODE_INVALID_QUERY_MSG;
        tfree(pExprs);
        break;
      }

      type = pTagSchema[pColumnIndexExInfo->colIdx].type;
      bytes = pTagSchema[pColumnIndexExInfo->colIdx].bytes;

    } else { // parse the arithmetic expression
      if (pExprs[i].pBase.functionId == TSDB_FUNC_ARITHM) {
        *code = vnodeBuildExprFromArithmeticStr(&pExprs[i], pQueryMsg);

        if (*code != TSDB_CODE_SUCCESS) {
          tfree(pExprs);
          break;
        }

        type = TSDB_DATA_TYPE_DOUBLE;
        bytes = tDataTypeDesc[type].nSize;
      } else { // parse the normal column
        int32_t j = getColumnIndexInSource(pQueryMsg, &pExprs[i].pBase);
        assert(j < pQueryMsg->numOfCols);

        SColumnFilterMsg* pCol = &pQueryMsg->colList[j];
        type = pCol->type;
        bytes = pCol->bytes;
      }
    }

    int32_t param = pExprs[i].pBase.arg[0].argValue.i64;
    getResultInfo(type, bytes, pExprs[i].pBase.functionId, param, &pExprs[i].resType, &pExprs[i].resBytes);

    assert(pExprs[i].resType != 0 && pExprs[i].resBytes != 0);
  }

  tfree(pQueryMsg->pSqlFuncExprs);
  return pExprs;
}

bool vnodeIsValidVnodeCfg(SVnodeCfg* pCfg) {
  if (pCfg == NULL) return false;

  if (pCfg->maxSessions <= 0 || pCfg->cacheBlockSize <= 0 || pCfg->daysPerFile <= 0 || pCfg->daysToKeep <= 0) {
    return false;
  }

  return true;
}

/**
 * compare if schema of two tables are identical.
 * when multi-table query is issued, the schemas of all requested tables
 * should be identical. Otherwise,query process will abort.
 */
bool vnodeMeterSchemaIdentical(SColumn* pSchema1, int32_t numOfCols1, SColumn* pSchema2, int32_t numOfCols2) {
  if (!VALIDNUMOFCOLS(numOfCols1) || !VALIDNUMOFCOLS(numOfCols2) || numOfCols1 != numOfCols2) {
    return false;
  }

  return memcmp((char*)pSchema1, (char*)pSchema2, sizeof(SColumn) * numOfCols1) == 0;
}

void vnodeFreeFields(SQuery* pQuery) {
  if (pQuery == NULL || pQuery->pFields == NULL) {
    return;
  }

  for (int32_t i = 0; i < pQuery->numOfBlocks; ++i) {
    tfree(pQuery->pFields[i]);
  }

  /*
   * pQuery->pFields does not need to be released, it is allocated at the last part of pBlock
   * so free(pBlock) can release this memory at the same time.
   */
  pQuery->pFields = NULL;
  pQuery->numOfBlocks = 0;
}

void vnodeUpdateFilterColumnIndex(SQuery* pQuery) {
  for (int32_t i = 0; i < pQuery->numOfFilterCols; ++i) {
    for (int16_t j = 0; j < pQuery->numOfCols; ++j) {
      if (pQuery->pFilterInfo[i].pFilter.data.colId == pQuery->colList[j].data.colId) {
        pQuery->pFilterInfo[i].pFilter.colIdx = pQuery->colList[j].colIdx;
        pQuery->pFilterInfo[i].pFilter.colIdxInBuf = pQuery->colList[j].colIdxInBuf;

        // supplementary scan is also require this column
        pQuery->colList[j].req[1] = 1;
        break;
      }
    }
  }

  // set the column index in buffer for arithmetic operation
  if (pQuery->pSelectExpr != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlBinaryExprInfo* pBinExprInfo = &pQuery->pSelectExpr[i].pBinExprInfo;
      if (pBinExprInfo->pBinExpr != NULL) {
        for (int16_t j = 0; j < pBinExprInfo->numOfCols; ++j) {
          for (int32_t k = 0; k < pQuery->numOfCols; ++k) {
            if (pBinExprInfo->pReqColumns[j].colId == pQuery->colList[k].data.colId) {
              pBinExprInfo->pReqColumns[j].colIdxInBuf = pQuery->colList[k].colIdxInBuf;
              assert(pQuery->colList[k].colIdxInBuf == k);
              break;
            }
          }
        }
      }
    }
  }
}

// TODO support k<12 and k<>9
int32_t vnodeCreateFilterInfo(void* pQInfo, SQuery* pQuery) {

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].data.filterOn > 0) {
      pQuery->numOfFilterCols++;
    }
  }

  if (pQuery->numOfFilterCols == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pQuery->pFilterInfo = calloc(1, sizeof(SColumnFilterInfo) * pQuery->numOfFilterCols);

  for (int32_t i = 0, j = 0; i < pQuery->numOfCols; ++i) {
    if (pQuery->colList[i].data.filterOn > 0) {
      pQuery->pFilterInfo[j].pFilter = pQuery->colList[i];
      SColumnFilterInfo* pFilterInfo = &pQuery->pFilterInfo[j];

      int32_t lower = pFilterInfo->pFilter.data.lowerRelOptr;
      int32_t upper = pFilterInfo->pFilter.data.upperRelOptr;

      int16_t type = pQuery->colList[i].data.type;
      int16_t bytes = pQuery->colList[i].data.bytes;

      __filter_func_t* rangeFilterArray = vnodeGetRangeFilterFuncArray(type);
      __filter_func_t* filterArray = vnodeGetValueFilterFuncArray(type);

      if (rangeFilterArray == NULL && filterArray == NULL) {
        dError("QInfo:%p failed to get filter function, invalid data type:%d", pQInfo, type);
        return TSDB_CODE_INVALID_QUERY_MSG;
      }

      if ((lower == TSDB_RELATION_LARGE_EQUAL || lower == TSDB_RELATION_LARGE) &&
          (upper == TSDB_RELATION_LESS_EQUAL || upper == TSDB_RELATION_LESS)) {
        if (lower == TSDB_RELATION_LARGE_EQUAL) {
          if (upper == TSDB_RELATION_LESS_EQUAL) {
            pFilterInfo->fp = rangeFilterArray[4];
          } else {
            pFilterInfo->fp = rangeFilterArray[2];
          }
        } else {
          if (upper == TSDB_RELATION_LESS_EQUAL) {
            pFilterInfo->fp = rangeFilterArray[3];
          } else {
            pFilterInfo->fp = rangeFilterArray[1];
          }
        }
      } else {  // set callback filter function
        if (lower != TSDB_RELATION_INVALID) {
          pFilterInfo->fp = filterArray[lower];

          if (upper != TSDB_RELATION_INVALID) {
            dError("pQInfo:%p failed to get filter function, invalid filter condition", pQInfo, type);
            return TSDB_CODE_INVALID_QUERY_MSG;
          }
        } else {
          pFilterInfo->fp = filterArray[upper];
        }
      }

      pFilterInfo->elemSize = bytes;
      j++;
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool vnodeDoFilterData(SQuery* pQuery, int32_t elemPos) {
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    char* pElem = pFilterInfo->pData + pFilterInfo->elemSize * elemPos;

    if(isNull(pElem, pFilterInfo->pFilter.data.type)) {
      return false;
    }

    if (!pFilterInfo->fp(&pFilterInfo->pFilter, pElem, pElem)) {
      return false;
    }
  }

  return true;
}

bool vnodeFilterData(SQuery* pQuery, int32_t* numOfActualRead, int32_t index) {
  (*numOfActualRead)++;
  if (!vnodeDoFilterData(pQuery, index)) {
    return false;
  }

  if (pQuery->limit.offset > 0) {
    pQuery->limit.offset--;  // ignore this qualified row
    return false;
  }

  return true;
}

bool vnodeIsProjectionQuery(SSqlFunctionExpr* pExpr, int32_t numOfOutput) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (pExpr[i].pBase.functionId != TSDB_FUNC_PRJ) {
      return false;
    }
  }

  return true;
}

/*
 * the pMeter->state may be changed by vnodeIsSafeToDeleteMeter and import/update processor, the check of
 * the state will not always be correct.
 *
 * The import/update/deleting is actually blocked by current query processing if the check of meter state is
 * passed, but later queries are denied.
 *
 * 1. vnodeIsSafeToDelete will wait for this complete, since it also use the vmutex to check the numOfQueries
 * 2. import will check the numOfQueries again after setting state to be TSDB_METER_STATE_IMPORTING, while the
 *    vmutex is also used.
 * 3. insert has nothing to do with the query processing.
 */
int32_t vnodeIncQueryRefCount(SQueryMeterMsg* pQueryMsg, SMeterSidExtInfo** pSids, SMeterObj** pMeterObjList,
                              int32_t* numOfInc) {
  SVnodeObj* pVnode = &vnodeList[pQueryMsg->vnode];

  int32_t num = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < pQueryMsg->numOfSids; ++i) {
    SMeterObj* pMeter = pVnode->meterList[pSids[i]->sid];

    if (pMeter == NULL || (pMeter->state > TSDB_METER_STATE_INSERT)) {
      if (pMeter == NULL || vnodeIsMeterState(pMeter, TSDB_METER_STATE_DELETING)) {
        code = TSDB_CODE_NOT_ACTIVE_SESSION;
        dError("qmsg:%p, vid:%d sid:%d, not there or will be dropped", pQueryMsg, pQueryMsg->vnode, pSids[i]->sid);
        vnodeSendMeterCfgMsg(pQueryMsg->vnode, pSids[i]->sid);
      } else {//update or import
        code = TSDB_CODE_ACTION_IN_PROGRESS;
        dTrace("qmsg:%p, vid:%d sid:%d id:%s, it is in state:%d, wait!", pQueryMsg, pQueryMsg->vnode, pSids[i]->sid,
               pMeter->meterId, pMeter->state);
      }
    } else {
      /*
       * vnodeIsSafeToDeleteMeter will wait for this function complete, and then it can
       * check if the numOfQueries is 0 or not.
       */
      pMeterObjList[(*numOfInc)++] = pMeter;
      __sync_fetch_and_add(&pMeter->numOfQueries, 1);

      // output for meter more than one query executed
      if (pMeter->numOfQueries > 1) {
        dTrace("qmsg:%p, vid:%d sid:%d id:%s, inc query ref, numOfQueries:%d", pQueryMsg, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }
  }

  dTrace("qmsg:%p, query meters: %d, inc query ref %d, numOfQueries on %d meters are 1", pQueryMsg,
         pQueryMsg->numOfSids, *numOfInc, (*numOfInc) - num);

  return code;
}

void vnodeDecQueryRefCount(SQueryMeterMsg* pQueryMsg, SMeterObj** pMeterObjList, int32_t numOfInc) {
  int32_t num = 0;

  for (int32_t i = 0; i < numOfInc; ++i) {
    SMeterObj* pMeter = pMeterObjList[i];

    if (pMeter != NULL) {  // here, do not need to lock to perform operations
      __sync_fetch_and_sub(&pMeter->numOfQueries, 1);

      if (pMeter->numOfQueries > 0) {
        dTrace("qmsg:%p, vid:%d sid:%d id:%s dec query ref, numOfQueries:%d", pQueryMsg, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }
  }

  dTrace("qmsg:%p, dec query ref for %d meters, numOfQueries on %d meters are 0", pQueryMsg, numOfInc, numOfInc - num);
}

void vnodeUpdateQueryColumnIndex(SQuery* pQuery, SMeterObj* pMeterObj) {
  if (pQuery == NULL || pMeterObj == NULL) {
    return;
  }

  int32_t i = 0, j = 0;
  while (i < pQuery->numOfCols && j < pMeterObj->numOfColumns) {
    if (pQuery->colList[i].data.colId == pMeterObj->schema[j].colId) {
      pQuery->colList[i++].colIdx = (int16_t)j++;
    } else if (pQuery->colList[i].data.colId < pMeterObj->schema[j].colId) {
      pQuery->colList[i++].colIdx = -1;
    } else if (pQuery->colList[i].data.colId > pMeterObj->schema[j].colId) {
      j++;
    }
  }

  while (i < pQuery->numOfCols) {
    pQuery->colList[i++].colIdx = -1;  // not such column in current meter
  }

  // sql expression has not been created yet
  if (pQuery->pSelectExpr == NULL) {
    return;
  }

  for(int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg* pSqlExprMsg = &pQuery->pSelectExpr[i].pBase;
    if (pSqlExprMsg->functionId == TSDB_FUNC_ARITHM || pSqlExprMsg->colInfo.isTag == true) {
      continue;
    }

    SColIndexEx* pColIndexEx = &pSqlExprMsg->colInfo;
    for(int32_t j = 0; j < pQuery->numOfCols; ++j) {
      if (pColIndexEx->colId == pQuery->colList[j].data.colId) {
        pColIndexEx->colIdx = pQuery->colList[j].colIdx;
        break;
      }
    }
  }
}

int32_t vnodeSetMeterState(SMeterObj* pMeterObj, int32_t state) {
  return __sync_val_compare_and_swap(&pMeterObj->state, TSDB_METER_STATE_READY, state);
}

void vnodeClearMeterState(SMeterObj* pMeterObj, int32_t state) {
  pMeterObj->state &= (~state);
}

bool vnodeIsMeterState(SMeterObj* pMeterObj, int32_t state) {
  if (state == TSDB_METER_STATE_READY) {
    return pMeterObj->state == TSDB_METER_STATE_READY;
  } else if (state == TSDB_METER_STATE_DELETING) {
    return pMeterObj->state >= state;
  } else {
    return (((pMeterObj->state) & state) == state);
  }
}

void vnodeSetMeterDeleting(SMeterObj* pMeterObj) {
  if (pMeterObj == NULL) {
    return;
  }

  pMeterObj->state |= TSDB_METER_STATE_DELETING;
}

bool vnodeIsSafeToDeleteMeter(SVnodeObj* pVnode, int32_t sid) {
  SMeterObj* pObj = pVnode->meterList[sid];

  if (pObj == NULL || vnodeIsMeterState(pObj, TSDB_METER_STATE_DELETED)) {
    return true;
  }

  int32_t prev = vnodeSetMeterState(pObj, TSDB_METER_STATE_DELETING);

  /*
   * if the meter is not in ready/deleting state, it must be in insert/import/update,
   * set the deleting state and wait the procedure to be completed
   */
  if (prev != TSDB_METER_STATE_READY && prev < TSDB_METER_STATE_DELETING) {
    vnodeSetMeterDeleting(pObj);

    dWarn("vid:%d sid:%d id:%s, can not be deleted, state:%d, wait", pObj->vnode, pObj->sid, pObj->meterId, prev);
    return false;
  }

  bool ready = true;

  /*
   * the query will be stopped ASAP, since the state of meter is set to TSDB_METER_STATE_DELETING,
   * and new query will abort since the meter is deleted.
   */
  pthread_mutex_lock(&pVnode->vmutex);
  if (pObj->numOfQueries > 0) {
    dWarn("vid:%d sid:%d id:%s %d queries executing on it, wait query to be finished",
          pObj->vnode, pObj->sid, pObj->meterId, pObj->numOfQueries);
    ready = false;
  }
  pthread_mutex_unlock(&pVnode->vmutex);

  return ready;
}
